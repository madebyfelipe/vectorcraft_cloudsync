//! Drawing the Home library (native only). The model, state and actions are in [`crate::cloud_home`].
//!
//! Colours, radii and fonts come from the theme tokens. The grid places its cards by hand and draws
//! only those in view, so a library of hundreds of projects costs a frame no more than a screenful.
use egui::{Align, Color32, CornerRadius, Layout, Rect, RichText, Sense, Stroke, StrokeKind, Ui, pos2, vec2};
use uuid::Uuid;

use crate::VectorcraftApp;
use crate::cloud_home::{
    Action, Badge, Card, Filter, HomeState, Query, Renaming, Sort, all_cards, edited_label, human_bytes, recents_row, validate_title, visible,
    wall_ms,
};
use crate::theme::{self, Tokens};
use crate::{icons, widgets};

/// Space between cards.
const GAP: f32 = 16.0;
/// The width cards are laid out for; they grow or shrink a little to fill the row.
const TARGET_W: f32 = 200.0;
const MIN_W: f32 = 160.0;
const MAX_W: f32 = 280.0;
/// Space inside a card.
const PAD: f32 = 8.0;
/// Height of the text under the thumbnail: name, edit time, badge.
const BODY_H: f32 = 84.0;
/// The skeleton cards shown while the library loads.
const SKELETONS: usize = 8;

/// How the cards of one row are laid out.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Metrics {
    pub cols: usize,
    pub width: f32,
    pub height: f32,
}

/// The card size and the number of columns for a row `avail` points wide.
pub fn metrics(avail: f32) -> Metrics {
    let avail = if avail.is_finite() { avail.max(1.0) } else { TARGET_W };
    let cols = (((avail + GAP) / (TARGET_W + GAP)).floor() as usize).max(1);
    let width = ((avail - GAP * (cols - 1) as f32) / cols as f32).clamp(MIN_W, MAX_W);
    Metrics { cols, width, height: PAD + (width - 2.0 * PAD) * 0.72 + BODY_H }
}

/// What a frame shows, worked out before anything is drawn.
struct Model {
    all: Vec<Card>,
    shown: Vec<Card>,
    recents: Vec<Card>,
    loading: bool,
    syncing: bool,
    connected: bool,
    offline: bool,
    banner: Option<String>,
    notice: Option<String>,
    status_line: String,
    can_go_back: bool,
}

fn model(app: &VectorcraftApp, st: &HomeState) -> Model {
    let cloud = &app.cloud_sync;
    let open = crate::cloud_sync::open_docs(app);
    let all = all_cards(&cloud.state.catalog, &st.downloading(), &open);
    let recent_projects = st.recents.projects();
    let shown = visible(&all, &recent_projects, &Query { text: &st.search, filter: st.filter, sort: st.sort });
    let recents = recents_row(&all, &recent_projects);
    let status = &cloud.state.status;
    Model {
        recents,
        shown,
        loading: cloud.loading(),
        syncing: matches!(status, craft_cloud_sync::SyncStatus::Syncing),
        connected: cloud.connected(),
        offline: cloud.connected() && matches!(status, craft_cloud_sync::SyncStatus::Offline),
        banner: cloud.banner(),
        notice: cloud.notice().map(str::to_owned),
        status_line: format!("{}   ·   Cache: {} of {}", cloud.label(), human_bytes(cloud.state.cache_bytes), human_bytes(cloud.state.cache_limit)),
        can_go_back: app.ui.home.is_some() && app.session.active().is_some(),
        all,
    }
}

/// Draw the Home library over `rect` and carry out what the user asked for.
pub fn show(app: &mut VectorcraftApp, ui: &mut Ui, rect: Rect) {
    let t = Tokens::get(ui.ctx());
    ui.painter().rect_filled(rect, 0.0, t.panel_darker);
    // The state is held here while the frame is drawn; commands that run meanwhile see an empty one,
    // so everything they must share lives in `CloudSync` itself.
    let mut st = std::mem::take(&mut app.cloud_sync.home);
    let m = model(app, &st);
    let mut actions: Vec<Action> = Vec::new();
    let now_ms = wall_ms();
    let ctx = ui.ctx().clone();
    let pending = st.thumbs.poll(&ctx);
    st.focused = None;
    st.hovered = None;

    let width = (rect.width() - 56.0).clamp(260.0, 1280.0);
    let left = rect.center().x - width / 2.0;
    let body = Rect::from_min_max(pos2(left, rect.top() + 20.0), pos2(left + width, rect.bottom() - 8.0));
    let mut child = ui.new_child(egui::UiBuilder::new().max_rect(body).layout(Layout::top_down(Align::Min)));
    let ui = &mut child;

    header(ui, &t, &m, &mut actions);
    toolbar(ui, &t, &mut st);
    banners(ui, &t, &m, &mut actions);
    ui.add_space(6.0);
    egui::ScrollArea::vertical().id_salt("cloud-home-scroll").auto_shrink([false, false]).show(ui, |ui| {
        library(ui, &t, &m, &mut st, now_ms, &mut actions);
        ui.add_space(30.0);
        ui.label(RichText::new("On this computer").font(theme::semibold(18.0)).color(t.text_strong));
        ui.label(RichText::new("Files you open from disk and quick ways to start a new one.").size(12.0).color(t.text_dim));
        crate::canvas::home_local(app, ui);
        ui.add_space(30.0);
    });

    // F2 renames the card the keyboard is on, else the one under the pointer.
    if std::mem::take(&mut st.rename_requested)
        && let Some(version) = st.focused.or(st.hovered)
    {
        actions.push(Action::StartRename(version));
    }
    if pending || m.loading || (!m.shown.is_empty() && st.opening.len() + st.requested.len() > 0) {
        // Decodes finish, the worker answers: look again soon even when nothing else moves.
        ctx.request_repaint_after(std::time::Duration::from_millis(120));
    }
    app.cloud_sync.home = st;
    crate::cloud_home::apply(app, actions);
}

fn header(ui: &mut Ui, t: &Tokens, m: &Model, actions: &mut Vec<Action>) {
    ui.horizontal(|ui| {
        ui.label(RichText::new("Projects").font(theme::semibold(26.0)).color(t.text_strong));
        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            if widgets::primary_button(ui, "New").clicked() {
                actions.push(Action::New);
            }
            if widgets::secondary_button(ui, "Account").clicked() {
                actions.push(Action::Account);
            }
            if widgets::secondary_button(ui, "Open file…").clicked() {
                actions.push(Action::OpenFile);
            }
            if m.can_go_back && widgets::secondary_button(ui, "Back to editor").clicked() {
                actions.push(Action::Back);
            }
        });
    });
    ui.horizontal(|ui| {
        if m.syncing || m.loading {
            ui.add(egui::Spinner::new().size(12.0).color(t.accent));
        }
        ui.label(RichText::new(&m.status_line).size(12.0).color(t.text_dim));
    });
}

fn toolbar(ui: &mut Ui, t: &Tokens, st: &mut HomeState) {
    ui.add_space(10.0);
    ui.horizontal_wrapped(|ui| {
        ui.spacing_mut().item_spacing = vec2(8.0, 8.0);
        let id = egui::Id::new("cloud-home-search");
        egui::Frame::NONE
            .fill(t.input)
            .stroke(Stroke::new(1.0, t.input_border))
            .corner_radius(t.cr())
            .inner_margin(egui::Margin::symmetric(8, 4))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    icons::icon(ui, "search", 14.0, t.icon);
                    ui.add(
                        egui::TextEdit::singleline(&mut st.search)
                            .id(id)
                            .frame(egui::Frame::NONE)
                            .hint_text(RichText::new("Search projects").italics())
                            .desired_width(200.0),
                    );
                });
            });
        if std::mem::take(&mut st.focus_search) {
            ui.memory_mut(|m| m.request_focus(id));
        }
        let labels = Sort::ALL.map(Sort::label);
        if let Some(i) = widgets::dropdown(ui, "cloud-home-sort", st.sort.label(), &labels, 124.0)
            && let Some(sort) = Sort::ALL.get(i)
        {
            st.sort = *sort;
        }
        ui.add_space(4.0);
        for filter in Filter::ALL {
            if chip(ui, t, filter.label(), st.filter == filter).clicked() {
                st.filter = filter;
            }
        }
    });
}

/// A filter chip.
fn chip(ui: &mut Ui, t: &Tokens, text: &str, selected: bool) -> egui::Response {
    let color = if selected { Color32::WHITE } else { t.text };
    let galley = ui.painter().layout_no_wrap(text.to_owned(), theme::semibold(12.0), color);
    let (rect, resp) = ui.allocate_exact_size(vec2(galley.size().x + 24.0, 26.0), Sense::click());
    let radius = CornerRadius::same(13);
    if selected {
        ui.painter().rect_filled(rect, radius, t.accent_strong);
    } else {
        if resp.hovered() {
            ui.painter().rect_filled(rect, radius, t.hover);
        }
        ui.painter().rect_stroke(rect, radius, Stroke::new(1.0, t.button_border), StrokeKind::Inside);
    }
    ui.painter().galley(rect.center() - galley.size() / 2.0, galley, color);
    resp.widget_info(|| egui::WidgetInfo::selected(egui::WidgetType::Button, true, selected, text));
    resp
}

/// A line across the width with `text`, outlined in `color`; whether its button (if any) was clicked.
fn banner(ui: &mut Ui, t: &Tokens, color: Color32, text: &str, button: Option<&str>) -> bool {
    let mut clicked = false;
    egui::Frame::NONE.fill(t.panel).stroke(Stroke::new(1.0, color)).corner_radius(t.cr()).inner_margin(egui::Margin::symmetric(12, 8)).show(
        ui,
        |ui| {
            ui.set_width(ui.available_width());
            ui.horizontal(|ui| {
                if let Some(label) = button {
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        clicked = widgets::secondary_button(ui, label).clicked();
                        ui.with_layout(Layout::left_to_right(Align::Center), |ui| {
                            ui.add(egui::Label::new(RichText::new(text).size(12.5).color(t.text)).wrap());
                        });
                    });
                } else {
                    ui.add(egui::Label::new(RichText::new(text).size(12.5).color(t.text)).wrap());
                }
            });
        },
    );
    ui.add_space(6.0);
    clicked
}

fn banners(ui: &mut Ui, t: &Tokens, m: &Model, actions: &mut Vec<Action>) {
    ui.add_space(4.0);
    if let Some(message) = &m.banner {
        if banner(ui, t, t.error, message, Some("Retry")) {
            actions.push(Action::RetrySync);
        }
    } else if m.offline {
        banner(ui, t, t.warning, "Offline — showing projects on this computer. Edits are saved locally and upload when you reconnect.", None);
    }
    if let Some(notice) = &m.notice
        && banner(ui, t, t.accent, notice, Some("Account"))
    {
        actions.push(Action::Account);
    }
    // Not connected, but local projects to show: the grid stays, a line says what connecting adds.
    if !m.loading && !m.connected && !m.all.is_empty() && banner(ui, t, t.accent, "Connect your account to see cloud projects", Some("Account")) {
        actions.push(Action::Account);
    }
}

fn section_title(ui: &mut Ui, t: &Tokens, text: &str, count: Option<usize>) {
    ui.horizontal(|ui| {
        ui.label(RichText::new(text).font(theme::semibold(15.0)).color(t.text));
        if let Some(n) = count {
            ui.label(RichText::new(n.to_string()).size(12.5).color(t.text_dim));
        }
    });
    ui.add_space(6.0);
}

/// The library: its empty states, the Recents row and the grid.
fn library(ui: &mut Ui, t: &Tokens, m: &Model, st: &mut HomeState, now_ms: u64, actions: &mut Vec<Action>) {
    ui.add_space(8.0);
    if m.loading {
        ui.horizontal(|ui| {
            ui.add(egui::Spinner::new().size(14.0).color(t.accent));
            ui.label(RichText::new("Loading your projects…").size(13.0).color(t.text_dim));
        });
        ui.add_space(10.0);
        let mx = metrics(ui.available_width());
        let count = SKELETONS.min(mx.cols);
        let (area, _) = ui.allocate_exact_size(vec2(ui.available_width(), mx.height), Sense::hover());
        for i in 0..count {
            skeleton(ui, t, Rect::from_min_size(area.min + vec2(i as f32 * (mx.width + GAP), 0.0), vec2(mx.width, mx.height)));
        }
        return;
    }
    if m.all.is_empty() {
        let (title, hint, account) = if m.connected {
            ("No projects yet", "Projects you create appear here and are saved to your cloud automatically.", false)
        } else {
            ("Connect your account to see cloud projects", "Projects you create here are saved on this computer until you connect.", true)
        };
        empty_state(ui, t, title, hint, account, actions);
        return;
    }
    let filtering = st.filter != Filter::All || !st.search.trim().is_empty();
    if !filtering && !m.recents.is_empty() {
        section_title(ui, t, "Recents", None);
        recents(ui, t, m, st, now_ms, actions);
        ui.add_space(22.0);
    }
    let title = if filtering { "Results" } else { "All projects" };
    section_title(ui, t, title, Some(m.shown.len()));
    if m.shown.is_empty() {
        ui.label(RichText::new("No projects match your search or filter.").size(13.0).color(t.text_dim));
        ui.add_space(6.0);
        if widgets::secondary_button(ui, "Clear search and filters").clicked() {
            actions.push(Action::ClearFilters);
        }
        return;
    }
    grid(ui, t, &m.shown, "grid", st, now_ms, actions);
}

fn empty_state(ui: &mut Ui, t: &Tokens, title: &str, hint: &str, account: bool, actions: &mut Vec<Action>) {
    ui.add_space(36.0);
    ui.vertical_centered(|ui| {
        icons::icon(ui, "cloud", 40.0, t.text_disabled);
        ui.add_space(10.0);
        ui.label(RichText::new(title).font(theme::semibold(20.0)).color(t.text_strong));
        ui.add_space(4.0);
        ui.label(RichText::new(hint).size(13.0).color(t.text_dim));
        ui.add_space(14.0);
        if widgets::primary_button(ui, "New").clicked() {
            actions.push(Action::New);
        }
        if account {
            ui.add_space(6.0);
            if widgets::secondary_button(ui, "Account").clicked() {
                actions.push(Action::Account);
            }
        }
    });
    ui.add_space(24.0);
}

/// The Recents row: a single line of cards that scrolls sideways when they do not all fit.
fn recents(ui: &mut Ui, t: &Tokens, m: &Model, st: &mut HomeState, now_ms: u64, actions: &mut Vec<Action>) {
    let mx = metrics(TARGET_W);
    egui::ScrollArea::horizontal().id_salt("cloud-home-recents").auto_shrink([false, true]).show(ui, |ui| {
        let (area, _) = ui.allocate_exact_size(vec2(m.recents.len() as f32 * (mx.width + GAP) - GAP, mx.height), Sense::hover());
        for (i, card) in m.recents.iter().enumerate() {
            let rect = Rect::from_min_size(area.min + vec2(i as f32 * (mx.width + GAP), 0.0), vec2(mx.width, mx.height));
            if ui.is_rect_visible(rect) {
                draw_card(ui, rect, card, "recents", t, now_ms, st, actions);
            }
        }
    });
}

/// The grid: `cards` in rows as wide as the space allows.
fn grid(ui: &mut Ui, t: &Tokens, cards: &[Card], scope: &str, st: &mut HomeState, now_ms: u64, actions: &mut Vec<Action>) {
    let mx = metrics(ui.available_width());
    let rows = cards.len().div_ceil(mx.cols);
    let total = (rows as f32 * (mx.height + GAP) - GAP).max(0.0);
    let (area, _) = ui.allocate_exact_size(vec2(ui.available_width(), total), Sense::hover());
    for (i, card) in cards.iter().enumerate() {
        let (row, col) = (i / mx.cols, i % mx.cols);
        let rect = Rect::from_min_size(area.min + vec2(col as f32 * (mx.width + GAP), row as f32 * (mx.height + GAP)), vec2(mx.width, mx.height));
        if ui.is_rect_visible(rect) {
            draw_card(ui, rect, card, scope, t, now_ms, st, actions);
        }
    }
}

/// A grey stand-in for a card.
fn skeleton(ui: &Ui, t: &Tokens, rect: Rect) {
    let painter = ui.painter();
    painter.rect_filled(rect, CornerRadius::same(8), t.panel);
    painter.rect_stroke(rect, CornerRadius::same(8), Stroke::new(1.0, t.divider), StrokeKind::Inside);
    let thumb = Rect::from_min_max(rect.min + vec2(PAD, PAD), pos2(rect.max.x - PAD, rect.max.y - BODY_H));
    painter.rect_filled(thumb, CornerRadius::same(4), t.hover);
    let bar = |y: f32, w: f32| Rect::from_min_size(pos2(rect.left() + PAD, thumb.bottom() + y), vec2((rect.width() - 2.0 * PAD) * w, 10.0));
    painter.rect_filled(bar(14.0, 0.7), CornerRadius::same(3), t.hover);
    painter.rect_filled(bar(36.0, 0.45), CornerRadius::same(3), t.hover);
}

fn badge_color(t: &Tokens, badge: Badge) -> Color32 {
    match badge {
        Badge::NotUploaded => t.warning,
        Badge::Downloading => t.accent,
        Badge::Conflict => t.error,
        Badge::OnComputer | Badge::InCloud => t.text_dim,
    }
}

/// A small outlined label whose top-left corner is `at`; its rectangle.
fn pill(ui: &Ui, t: &Tokens, at: egui::Pos2, text: &str, color: Color32, max_w: f32) -> Rect {
    let galley = {
        let mut job = egui::text::LayoutJob::simple_singleline(text.to_owned(), theme::semibold(11.0), color);
        job.wrap = egui::text::TextWrapping::truncate_at_width((max_w - 14.0).max(10.0));
        ui.painter().layout_job(job)
    };
    let rect = Rect::from_min_size(at, galley.size() + vec2(14.0, 6.0));
    let radius = CornerRadius::same(10);
    ui.painter().rect_filled(rect, radius, t.panel.gamma_multiply(0.92));
    ui.painter().rect_stroke(rect, radius, Stroke::new(1.0, color), StrokeKind::Inside);
    ui.painter().galley(rect.min + vec2(7.0, 3.0), galley, color);
    rect
}

/// `text` on one line, cut with an ellipsis where it does not fit `width`.
fn ellipsized(ui: &Ui, text: &str, font: egui::FontId, color: Color32, width: f32) -> std::sync::Arc<egui::Galley> {
    let mut job = egui::text::LayoutJob::simple_singleline(text.to_owned(), font, color);
    job.wrap = egui::text::TextWrapping::truncate_at_width(width.max(10.0));
    ui.painter().layout_job(job)
}

fn card_menu(ui: &mut Ui, version: Uuid, actions: &mut Vec<Action>) {
    ui.set_min_width(150.0);
    if widgets::menu_item(ui, "Open", true, false) {
        actions.push(Action::Open(version));
    }
    if widgets::menu_item(ui, "Rename", true, false) {
        actions.push(Action::StartRename(version));
    }
    if widgets::menu_item(ui, "Save a copy…", true, false) {
        actions.push(Action::SaveCopy(version));
    }
}

/// One card: thumbnail, name, edit time and badge, with its menu. A click opens the project.
#[allow(clippy::too_many_arguments)]
fn draw_card(ui: &mut Ui, rect: Rect, card: &Card, scope: &str, t: &Tokens, now_ms: u64, st: &mut HomeState, actions: &mut Vec<Action>) {
    let id = egui::Id::new(("cloud-card", scope, card.version));
    let resp = ui.interact(rect, id, Sense::click());
    let focused = resp.has_focus();
    if focused {
        st.focused = Some(card.version);
    }
    if resp.hovered() {
        st.hovered = Some(card.version);
    }
    let renaming = st.renaming.as_ref().is_some_and(|r| r.version == card.version);
    let radius = CornerRadius::same(8);
    let stroke = if focused {
        Stroke::new(2.0, t.accent)
    } else if resp.hovered() {
        Stroke::new(1.0, t.accent)
    } else {
        Stroke::new(1.0, t.divider)
    };
    ui.painter().rect_filled(rect, radius, t.panel);
    if resp.hovered() {
        ui.painter().rect_filled(rect, radius, t.hover.gamma_multiply(0.35));
    }
    ui.painter().rect_stroke(rect, radius, stroke, StrokeKind::Inside);

    // Thumbnail: the preview, or a neutral placeholder.
    let thumb = Rect::from_min_max(rect.min + vec2(PAD, PAD), pos2(rect.max.x - PAD, rect.max.y - BODY_H));
    ui.painter().rect_filled(thumb, CornerRadius::same(4), t.pasteboard);
    let texture = st.thumbs.texture(ui.ctx(), card.version, card.preview.as_deref()).cloned();
    match texture {
        Some(tex) if tex.size_vec2().x > 0.0 && tex.size_vec2().y > 0.0 => {
            let size = tex.size_vec2();
            let scale = (thumb.width() / size.x).min(thumb.height() / size.y);
            let image = Rect::from_center_size(thumb.center(), size * scale);
            let uv = Rect::from_min_max(pos2(0.0, 0.0), pos2(1.0, 1.0));
            ui.painter().image(tex.id(), image, uv, Color32::WHITE);
            ui.painter().rect_stroke(image, 0.0, Stroke::new(1.0, t.border), StrokeKind::Outside);
        }
        _ => icons::paint(ui, "image", Rect::from_center_size(thumb.center(), vec2(30.0, 30.0)), t.text_disabled),
    }
    let busy = st.opening.contains(&card.version) || st.requested.contains(&card.version);
    if busy {
        ui.painter().rect_filled(thumb, CornerRadius::same(4), t.panel_darker.gamma_multiply(0.6));
        egui::Spinner::new().size(26.0).color(t.accent).paint_at(ui, Rect::from_center_size(thumb.center(), vec2(26.0, 26.0)));
    }
    if let Some((i, n)) = card.conflict {
        pill(ui, t, thumb.min + vec2(6.0, 6.0), &format!("Conflict copy {i}/{n}"), t.error, thumb.width() - 48.0);
    }

    // The menu button over the thumbnail's corner.
    let more_rect = Rect::from_min_size(pos2(thumb.right() - 30.0, thumb.top() + 6.0), vec2(24.0, 24.0));
    let more = ui.interact(more_rect, id.with("more"), Sense::click());
    ui.painter().circle_filled(more_rect.center(), 12.0, t.panel.gamma_multiply(0.92));
    icons::paint(ui, "ellipsis", more_rect.shrink(4.0), if more.hovered() { t.text_strong } else { t.icon });
    more.widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Button, true, "Project actions"));
    egui::Popup::menu(&more).show(|ui| card_menu(ui, card.version, actions));
    resp.context_menu(|ui| card_menu(ui, card.version, actions));

    // Name (or its editor), then the edit time or what went wrong.
    let inner_w = rect.width() - 2.0 * PAD;
    let name_rect = Rect::from_min_size(pos2(rect.left() + PAD, thumb.bottom() + 8.0), vec2(inner_w, 20.0));
    let error = st.errors.get(&card.version).map(|e| e.message.clone());
    let mut edit_error = None;
    if renaming {
        if let Some(r) = st.renaming.as_mut() {
            edit_error = r.error.clone();
            rename_editor(ui, id, name_rect, r, actions);
        }
    } else {
        let galley = ellipsized(ui, &card.title, theme::semibold(13.5), t.text_strong, inner_w);
        ui.painter().galley(name_rect.min + vec2(0.0, 1.0), galley, t.text_strong);
    }
    let line_rect = Rect::from_min_size(pos2(rect.left() + PAD, name_rect.bottom() + 2.0), vec2(inner_w, 16.0));
    let (line, line_color) = match (&edit_error, &error) {
        (Some(e), _) | (None, Some(e)) => (e.clone(), t.error),
        (None, None) => (edited_label(now_ms, card.created_ms), t.text_dim),
    };
    let galley = ellipsized(ui, &line, egui::FontId::proportional(11.5), line_color, inner_w);
    ui.painter().galley(line_rect.min, galley, line_color);

    let badge_at = pos2(rect.left() + PAD, rect.bottom() - PAD - 22.0);
    if error.is_some() {
        let retry = ui.interact(Rect::from_min_size(badge_at, vec2(52.0, 23.0)), id.with("retry"), Sense::click());
        pill(ui, t, badge_at, "Retry", if retry.hovered() { t.text_strong } else { t.accent }, 80.0);
        retry.widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Button, true, "Retry"));
        if retry.clicked() {
            actions.push(Action::Retry(card.version));
        }
    } else {
        pill(ui, t, badge_at, card.badge.label(), badge_color(t, card.badge), inner_w);
    }

    resp.widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Button, true, &card.title));
    if !renaming && !busy && resp.clicked() {
        actions.push(Action::Open(card.version));
    }
    if !renaming {
        resp.on_hover_text(format!("{}\n{}", card.title, card.badge.label()));
    }
}

/// The name field of a card being renamed: Enter commits, Esc or a click elsewhere cancels.
fn rename_editor(ui: &mut Ui, card_id: egui::Id, rect: Rect, r: &mut Renaming, actions: &mut Vec<Action>) {
    let edit_id = card_id.with("rename");
    let resp = ui.put(rect, egui::TextEdit::singleline(&mut r.text).id(edit_id).font(theme::semibold(13.5)).desired_width(rect.width()));
    if !r.focused {
        resp.request_focus();
        r.focused = true;
        return;
    }
    let enter = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
    if enter {
        // A name the client would not take stays open with the reason under it.
        match validate_title(&r.text) {
            Ok(title) => actions.push(Action::CommitRename(r.version, title)),
            Err(message) => {
                r.error = Some(message.into());
                resp.request_focus();
            }
        }
    } else if resp.lost_focus() || ui.input(|i| i.key_pressed(egui::Key::Escape)) {
        actions.push(Action::CancelRename);
    } else if r.error.is_some() && validate_title(&r.text).is_ok() {
        r.error = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_grid_fills_the_row_with_cards_about_200_points_wide() {
        for (avail, cols) in [(200.0, 1), (415.0, 1), (416.0, 2), (700.0, 3), (1000.0, 4), (1224.0, 5), (2000.0, 9)] {
            let m = metrics(avail);
            assert_eq!(m.cols, cols, "{avail}");
            assert!((MIN_W..=MAX_W).contains(&m.width), "{avail}: {}", m.width);
            assert!(m.cols as f32 * m.width + (m.cols - 1) as f32 * GAP <= avail.max(MIN_W) + 0.01, "{avail}: cards fit the row");
            assert!(m.height > m.width * 0.6 && m.height < m.width * 1.6);
        }
        // Nonsense widths give a usable layout rather than a panic.
        for avail in [0.0, -5.0, f32::NAN, f32::INFINITY, f32::NEG_INFINITY, 1e12] {
            let m = metrics(avail);
            assert!(m.cols >= 1 && m.width.is_finite() && m.height.is_finite(), "{avail}");
        }
    }
}
