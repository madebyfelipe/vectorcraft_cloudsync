//! The Home library (native only): every cloud project of the account, and every project saved
//! only on this computer so far, as a grid of cards, with search, sort and filters, recents, and
//! open / rename / save-a-copy / new. This module is the model (what the cards are, in what order,
//! with which badge), the state the screen keeps, the actions it performs and the commands agents
//! drive it with; [`crate::cloud_home_ui`] draws it.
//!
//! The shared client owns the catalog; the adapter ([`crate::cloud_sync`]) owns the worker thread
//! that talks to it. Nothing here reads files, decodes images or waits for the network: those go
//! through the worker, and the screen shows the state it reports.
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use craft_cloud_sync::{DownloadedRevision, SyncStatus};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::VectorcraftApp;
use crate::cloud_preview::Thumbs;

/// Extension of the projects this app can open.
pub const EXTENSION: &str = "vectorcraft";
/// Most recent projects remembered and shown in the Recents row.
pub const MAX_RECENTS: usize = 8;
/// File of recents in the adapter's private folder.
pub const RECENTS_FILE: &str = "recents.json";
/// A recents file larger than this is not ours: it is ignored.
const MAX_RECENTS_FILE: u64 = 16 * 1024;
/// Longest project name, in bytes (the shared client's limit).
pub const MAX_TITLE: usize = 256;
/// Local cache limits offered in the Account dialog.
pub const CACHE_PRESETS: [u64; 5] = [512 << 20, 1 << 30, 2 << 30, 5 << 30, 10 << 30];
/// The limit until the user picks another.
pub const DEFAULT_CACHE_LIMIT: u64 = 2 << 30;

/// Which projects the grid shows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Filter {
    #[default]
    All,
    Recents,
    Conflicts,
    Offline,
    NotUploaded,
}

impl Filter {
    pub const ALL: [Filter; 5] = [Filter::All, Filter::Recents, Filter::Conflicts, Filter::Offline, Filter::NotUploaded];

    pub fn label(self) -> &'static str {
        match self {
            Filter::All => "All",
            Filter::Recents => "Recents",
            Filter::Conflicts => "Conflicts",
            Filter::Offline => "Available offline",
            Filter::NotUploaded => "Not uploaded yet",
        }
    }

    pub fn key(self) -> &'static str {
        match self {
            Filter::All => "all",
            Filter::Recents => "recents",
            Filter::Conflicts => "conflicts",
            Filter::Offline => "offline",
            Filter::NotUploaded => "notUploaded",
        }
    }

    pub fn parse(s: &str) -> Option<Filter> {
        Filter::ALL.into_iter().find(|f| f.key().eq_ignore_ascii_case(s))
    }
}

/// The order of the grid.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Sort {
    #[default]
    LastEdited,
    NameAsc,
    NameDesc,
}

impl Sort {
    pub const ALL: [Sort; 3] = [Sort::LastEdited, Sort::NameAsc, Sort::NameDesc];

    pub fn label(self) -> &'static str {
        match self {
            Sort::LastEdited => "Last edited",
            Sort::NameAsc => "Name A–Z",
            Sort::NameDesc => "Name Z–A",
        }
    }

    pub fn key(self) -> &'static str {
        match self {
            Sort::LastEdited => "lastEdited",
            Sort::NameAsc => "nameAsc",
            Sort::NameDesc => "nameDesc",
        }
    }

    pub fn parse(s: &str) -> Option<Sort> {
        Sort::ALL.into_iter().find(|f| f.key().eq_ignore_ascii_case(s))
    }
}

/// Where a card's project is, as its badge says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Badge {
    /// Saved on this computer, not yet in the cloud.
    NotUploaded,
    /// Its payload was asked for and is on its way.
    Downloading,
    /// Concurrent edits left several copies of the project.
    Conflict,
    /// In the cloud and on this computer.
    OnComputer,
    /// In the cloud only.
    InCloud,
}

impl Badge {
    pub fn label(self) -> &'static str {
        match self {
            Badge::NotUploaded => "Not uploaded yet",
            Badge::Downloading => "Downloading…",
            Badge::Conflict => "Conflict",
            Badge::OnComputer => "On this computer",
            Badge::InCloud => "In the cloud",
        }
    }

    pub fn key(self) -> &'static str {
        match self {
            Badge::NotUploaded => "notUploaded",
            Badge::Downloading => "downloading",
            Badge::Conflict => "conflict",
            Badge::OnComputer => "onThisComputer",
            Badge::InCloud => "inCloud",
        }
    }
}

/// The badge of a card. What is more urgent wins: a payload on its way, then work not yet uploaded,
/// then a conflict, then where the payload is.
pub fn badge_for(pending: bool, downloading: bool, conflict: bool, cached: bool) -> Badge {
    if downloading {
        Badge::Downloading
    } else if pending {
        Badge::NotUploaded
    } else if conflict {
        Badge::Conflict
    } else if cached {
        Badge::OnComputer
    } else {
        Badge::InCloud
    }
}

/// A document open in the editor and the cloud revision it is bound to (if any yet).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenDoc {
    pub uid: u64,
    pub project: Option<Uuid>,
    pub version: Option<Uuid>,
}

/// One card: one head revision of a project.
#[derive(Clone, Debug)]
pub struct Card {
    pub project: Uuid,
    pub version: Uuid,
    pub title: String,
    pub created_ms: Option<u64>,
    pub badge: Badge,
    /// `(i, n)`: this is copy `i` of the `n` that concurrent edits left (newest first).
    pub conflict: Option<(usize, usize)>,
    pub cached: bool,
    pub pending: bool,
    pub preview: Option<PathBuf>,
    /// The editor document this card's project is open in.
    pub open_uid: Option<u64>,
}

/// The editor document that holds `rev`: the one bound to exactly that revision, else (a project with
/// a single head) the one bound to the project.
pub fn open_doc_for(open: &[OpenDoc], project: Uuid, version: Uuid, single_head: bool) -> Option<u64> {
    open.iter()
        .find(|d| d.version == Some(version))
        .or_else(|| if single_head { open.iter().find(|d| d.project == Some(project)) } else { None })
        .map(|d| d.uid)
}

/// The head revisions this app can open.
pub fn heads(catalog: &[DownloadedRevision]) -> impl Iterator<Item = &DownloadedRevision> {
    catalog.iter().filter(|r| r.extension == EXTENSION && r.heads.contains(&r.version_id))
}

/// A card for every head. A project that concurrent edits left with several heads has a card for each,
/// numbered newest first.
pub fn all_cards(catalog: &[DownloadedRevision], downloading: &BTreeSet<Uuid>, open: &[OpenDoc]) -> Vec<Card> {
    let mut by_project: BTreeMap<Uuid, Vec<&DownloadedRevision>> = BTreeMap::new();
    for rev in heads(catalog) {
        by_project.entry(rev.project_id).or_default().push(rev);
    }
    for copies in by_project.values_mut() {
        copies.sort_by(|a, b| b.created_ms.cmp(&a.created_ms).then_with(|| a.version_id.cmp(&b.version_id)));
    }
    heads(catalog)
        .map(|rev| {
            let copies = by_project.get(&rev.project_id).map_or(&[][..], Vec::as_slice);
            let n = copies.len().max(1);
            let i = copies.iter().position(|c| c.version_id == rev.version_id).map_or(1, |p| p + 1);
            let conflict = (n > 1).then_some((i, n));
            Card {
                project: rev.project_id,
                version: rev.version_id,
                title: rev.title.clone(),
                created_ms: rev.created_ms,
                badge: badge_for(rev.pending, downloading.contains(&rev.version_id) && !rev.cached, conflict.is_some(), rev.cached),
                conflict,
                cached: rev.cached,
                pending: rev.pending,
                preview: rev.preview_path.clone(),
                open_uid: open_doc_for(open, rev.project_id, rev.version_id, conflict.is_none()),
            }
        })
        .collect()
}

/// What the top bar asks for.
#[derive(Clone, Copy, Debug, Default)]
pub struct Query<'a> {
    pub text: &'a str,
    pub filter: Filter,
    pub sort: Sort,
}

fn name_key(card: &Card) -> (String, &str) {
    (card.title.to_lowercase(), card.title.as_str())
}

/// The cards the grid shows: those the filter lets through and whose title contains the search text
/// (case-insensitive), in the chosen order.
pub fn visible(cards: &[Card], recents: &[Uuid], q: &Query) -> Vec<Card> {
    let needle = q.text.trim().to_lowercase();
    let mut shown: Vec<Card> = cards
        .iter()
        .filter(|c| match q.filter {
            Filter::All => true,
            Filter::Recents => recents.contains(&c.project),
            Filter::Conflicts => c.conflict.is_some(),
            Filter::Offline => c.cached,
            Filter::NotUploaded => c.pending,
        })
        .filter(|c| needle.is_empty() || c.title.to_lowercase().contains(&needle))
        .cloned()
        .collect();
    let newest_first = |a: &Card, b: &Card| b.created_ms.cmp(&a.created_ms).then_with(|| a.version.cmp(&b.version));
    match q.sort {
        Sort::LastEdited => shown.sort_by(|a, b| newest_first(a, b).then_with(|| name_key(a).cmp(&name_key(b)))),
        Sort::NameAsc => shown.sort_by(|a, b| name_key(a).cmp(&name_key(b)).then_with(|| newest_first(a, b))),
        Sort::NameDesc => shown.sort_by(|a, b| name_key(b).cmp(&name_key(a)).then_with(|| newest_first(a, b))),
    }
    shown
}

/// The Recents row: the newest head of each recently opened project, most recently opened first, at most
/// [`MAX_RECENTS`].
pub fn recents_row(cards: &[Card], recents: &[Uuid]) -> Vec<Card> {
    recents
        .iter()
        .take(MAX_RECENTS)
        .filter_map(|p| {
            cards.iter().filter(|c| c.project == *p).max_by(|a, b| a.created_ms.cmp(&b.created_ms).then_with(|| b.version.cmp(&a.version)))
        })
        .cloned()
        .collect()
}

/// The revisions to pin: those the open documents are bound to. The client never evicts them.
pub fn pinned_set(open: &[OpenDoc]) -> BTreeSet<Uuid> {
    open.iter().filter_map(|d| d.version).collect()
}

/// A project name as the shared client takes it: not empty, at most 256 bytes, no control characters.
/// Leading and trailing white space is dropped.
pub fn validate_title(input: &str) -> Result<String, &'static str> {
    let title = input.trim();
    if title.is_empty() {
        Err("Enter a name")
    } else if title.chars().any(char::is_control) {
        Err("A name can't contain control characters")
    } else if title.len() > MAX_TITLE {
        Err("A name can be at most 256 characters")
    } else {
        Ok(title.to_owned())
    }
}

/// "Edited 5 minutes ago", or a dash for a revision from an older client that carries no time.
pub fn edited_label(now_ms: u64, created_ms: Option<u64>) -> String {
    match created_ms {
        None => "—".into(),
        Some(then) => format!("Edited {}", ago(now_ms.saturating_sub(then))),
    }
}

fn ago(delta_ms: u64) -> String {
    let plural = |n: u64, unit: &str| if n == 1 { format!("1 {unit} ago") } else { format!("{n} {unit}s ago") };
    let secs = delta_ms / 1000;
    let (minutes, hours, days) = (secs / 60, secs / 3600, secs / 86_400);
    if secs < 60 {
        "just now".into()
    } else if minutes < 60 {
        plural(minutes, "minute")
    } else if hours < 24 {
        plural(hours, "hour")
    } else if days < 30 {
        plural(days, "day")
    } else if days < 365 {
        plural(days / 30, "month")
    } else {
        plural(days / 365, "year")
    }
}

/// "1.5 GiB".
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    let name = UNITS.get(unit).copied().unwrap_or("B");
    if unit == 0 {
        format!("{bytes} B")
    } else if value >= 100.0 {
        format!("{value:.0} {name}")
    } else if value >= 10.0 {
        format!("{value:.1} {name}")
    } else {
        format!("{value:.2} {name}")
    }
}

/// Milliseconds since the UNIX epoch (zero if the clock is before it).
pub fn wall_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok().and_then(|d| u64::try_from(d.as_millis()).ok()).unwrap_or(0)
}

/// A file name for a project title: no path separators or control characters, not too long.
pub fn file_name_for(title: &str) -> String {
    let cleaned: String =
        title.chars().map(|c| if c.is_control() || matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') { '_' } else { c }).collect();
    let cleaned: String = cleaned.trim().trim_matches('.').chars().take(100).collect();
    if cleaned.is_empty() { "Untitled".into() } else { cleaned }
}

// --- Recents ---------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct RecentEntry {
    project: String,
    opened_ms: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct RecentsFile {
    version: u32,
    projects: Vec<RecentEntry>,
}

/// The projects this app opened most recently on this computer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Recents {
    entries: Vec<RecentEntry>,
}

impl Recents {
    /// What a recents file holds. A file that is too large, damaged or from the future is empty;
    /// entries that are not project ids are dropped.
    pub fn parse(bytes: &[u8]) -> Recents {
        if bytes.len() as u64 > MAX_RECENTS_FILE {
            return Recents::default();
        }
        let Ok(file) = serde_json::from_slice::<RecentsFile>(bytes) else { return Recents::default() };
        if file.version != 1 {
            return Recents::default();
        }
        let mut recents = Recents::default();
        for entry in file.projects {
            if Uuid::parse_str(&entry.project).is_ok() {
                recents.add(entry);
            }
        }
        recents
    }

    fn add(&mut self, entry: RecentEntry) {
        match self.entries.iter_mut().find(|e| e.project == entry.project) {
            Some(known) => known.opened_ms = known.opened_ms.max(entry.opened_ms),
            None => self.entries.push(entry),
        }
        self.entries.sort_by(|a, b| b.opened_ms.cmp(&a.opened_ms).then_with(|| a.project.cmp(&b.project)));
        self.entries.truncate(MAX_RECENTS);
    }

    /// Note that `project` was opened at `now_ms`.
    pub fn touch(&mut self, project: Uuid, now_ms: u64) {
        let newest = self.entries.iter().map(|e| e.opened_ms).max().unwrap_or(0);
        // Opened last, even if the clock stepped back.
        let opened_ms = now_ms.max(newest.saturating_add(1));
        let id = project.to_string();
        self.entries.retain(|e| e.project != id);
        self.add(RecentEntry { project: id, opened_ms });
    }

    /// Fold in what another copy remembers (the file read at start-up).
    pub fn merge(&mut self, other: Recents) {
        for entry in other.entries {
            self.add(entry);
        }
    }

    /// Project ids, most recently opened first.
    pub fn projects(&self) -> Vec<Uuid> {
        self.entries.iter().filter_map(|e| Uuid::parse_str(&e.project).ok()).collect()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&RecentsFile { version: 1, projects: self.entries.clone() }).unwrap_or_default()
    }
}

/// Read the recents in `dir` (empty when there are none or they are unusable). Never reads more than
/// the limit.
pub fn read_recents(dir: &Path) -> Recents {
    use std::io::Read;
    let mut bytes = Vec::new();
    let read = std::fs::File::open(dir.join(RECENTS_FILE)).and_then(|f| f.take(MAX_RECENTS_FILE + 1).read_to_end(&mut bytes));
    if read.is_err() {
        return Recents::default();
    }
    Recents::parse(&bytes)
}

/// Write the recents into `dir`, all or nothing.
pub fn write_recents(dir: &Path, recents: &Recents) -> Result<(), String> {
    vectorcraft_format::write_atomic(&dir.join(RECENTS_FILE), &recents.to_bytes()).map_err(|e| e.to_string())
}

// --- State -----------------------------------------------------------------------------------

/// What to do again when a card's failure is retried.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Retry {
    Open,
    Rename(String),
    Copy(String),
}

/// A failure shown on a card.
#[derive(Clone, Debug)]
pub struct CardError {
    pub message: String,
    pub retry: Retry,
}

/// A name being edited in place.
#[derive(Clone, Debug)]
pub struct Renaming {
    pub version: Uuid,
    pub text: String,
    pub error: Option<String>,
    pub focused: bool,
}

/// What the Home screen keeps between frames.
#[derive(Default)]
pub struct HomeState {
    pub search: String,
    pub sort: Sort,
    pub filter: Filter,
    pub renaming: Option<Renaming>,
    /// Ctrl/Cmd+F: move the keyboard to the search field.
    pub focus_search: bool,
    /// F2: rename the card the keyboard (or pointer) is on.
    pub rename_requested: bool,
    /// The card the keyboard was on, and the one under the pointer, in the last frame drawn.
    pub focused: Option<Uuid>,
    pub hovered: Option<Uuid>,
    /// Opens waiting for the payload to download.
    pub opening: BTreeSet<Uuid>,
    /// Payloads asked for a rename or a copy.
    pub requested: BTreeSet<Uuid>,
    pub errors: BTreeMap<Uuid, CardError>,
    /// Renames and copies waiting for their payload: version → (project, new name) / target path.
    pub pending_rename: BTreeMap<Uuid, (Uuid, String)>,
    pub pending_copy: BTreeMap<Uuid, String>,
    pub recents: Recents,
    pub thumbs: Thumbs,
}

impl HomeState {
    /// The versions whose payload is on its way.
    pub fn downloading(&self) -> BTreeSet<Uuid> {
        self.opening.union(&self.requested).copied().collect()
    }

    /// Show `message` on the card of `version`.
    pub fn fail(&mut self, version: Uuid, message: String, retry: Retry) {
        self.opening.remove(&version);
        self.requested.remove(&version);
        self.errors.insert(version, CardError { message, retry });
    }
}

/// What the screen asks for; performed after the frame is drawn.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    Open(Uuid),
    StartRename(Uuid),
    CommitRename(Uuid, String),
    CancelRename,
    SaveCopy(Uuid),
    Retry(Uuid),
    New,
    OpenFile,
    Account,
    Back,
    RetrySync,
    ClearFilters,
}

/// Perform `actions`.
pub fn apply(app: &mut VectorcraftApp, actions: Vec<Action>) {
    for action in actions {
        let result = match action {
            Action::Open(version) => open_version(app, version).map(|_| ()),
            Action::StartRename(version) => {
                let title = app.cloud_sync.state.catalog.iter().find(|r| r.version_id == version).map(|r| r.title.clone());
                if let Some(text) = title {
                    app.cloud_sync.home.renaming = Some(Renaming { version, text, error: None, focused: false });
                }
                Ok(())
            }
            Action::CommitRename(version, text) => match validate_title(&text) {
                Ok(title) => {
                    app.cloud_sync.home.renaming = None;
                    rename_version(app, version, &title).map(|_| ())
                }
                Err(message) => {
                    if let Some(r) = &mut app.cloud_sync.home.renaming {
                        r.error = Some(message.into());
                    }
                    Ok(())
                }
            },
            Action::CancelRename => {
                app.cloud_sync.home.renaming = None;
                Ok(())
            }
            // Through the command table: the file dialog it asks for runs as an entry (see `picks`).
            Action::SaveCopy(version) => app.run("file.cloudSaveCopy", json!({"version": version.to_string()})).map(|_| ()),
            Action::Retry(version) => retry(app, version),
            Action::New => {
                app.cloud_sync.new_from_home = true;
                app.run("file.newDialog", json!({})).map(|_| ())
            }
            Action::OpenFile => app.run("file.open", json!({})).map(|_| ()),
            Action::Account => {
                app.cloud_sync.visible = true;
                Ok(())
            }
            Action::Back => {
                app.ui.home = None;
                Ok(())
            }
            Action::RetrySync => {
                app.cloud_sync.retry_sync();
                Ok(())
            }
            Action::ClearFilters => {
                let home = &mut app.cloud_sync.home;
                home.search.clear();
                home.filter = Filter::All;
                Ok(())
            }
        };
        // A cancelled file dialog is not a failure.
        if let Err(e) = result
            && e != "cancelled"
        {
            app.status(e);
        }
    }
}

fn catalog_revision(app: &VectorcraftApp, version: Uuid) -> Option<DownloadedRevision> {
    app.cloud_sync.state.catalog.iter().find(|r| r.version_id == version && r.extension == EXTENSION).cloned()
}

fn single_head(app: &VectorcraftApp, rev: &DownloadedRevision) -> bool {
    heads(&app.cloud_sync.state.catalog).filter(|r| r.project_id == rev.project_id).count() <= 1
}

/// Open the project of card `version`: switch to the document it is already open in, else open it
/// (downloading its payload first when it is not on this computer). An earlier revision is opened as
/// that revision, whatever is open.
pub fn open_version(app: &mut VectorcraftApp, version: Uuid) -> Result<Value, String> {
    let rev = catalog_revision(app, version).ok_or("No such verified cloud revision or local checkpoint")?;
    app.cloud_sync.home.errors.remove(&version);
    let is_head = rev.heads.contains(&rev.version_id);
    let open = crate::cloud_sync::open_docs(app);
    if is_head && let Some(uid) = open_doc_for(&open, rev.project_id, rev.version_id, single_head(app, &rev)) {
        let index = app.session.documents().iter().position(|d| d.uid == uid).ok_or("The project's document is no longer open")?;
        app.session.set_active(index);
        app.ui.home = None;
        app.cloud_sync.touch_recent(rev.project_id);
        return Ok(json!({"switched": true, "uid": uid}));
    }
    // A second click while it downloads opens nothing more.
    if app.cloud_sync.home.opening.contains(&version) {
        return Ok(json!({"pending": true}));
    }
    crate::cloud_sync::open_revision(app, rev)?;
    Ok(json!({"pending": true}))
}

/// Rename the project of card `version` to `raw` (already validated or not: it is validated here).
pub fn rename_version(app: &mut VectorcraftApp, version: Uuid, raw: &str) -> Result<Value, String> {
    let title = validate_title(raw).map_err(str::to_owned)?;
    let rev = catalog_revision(app, version).ok_or("No such cloud project")?;
    app.cloud_sync.home.errors.remove(&version);
    let open = crate::cloud_sync::open_docs(app);
    if let Some(uid) = open_doc_for(&open, rev.project_id, rev.version_id, single_head(app, &rev)) {
        // The open document carries the name: the next autosave submits it.
        let index = app.session.documents().iter().position(|d| d.uid == uid).ok_or("The project's document is no longer open")?;
        if app.session.documents().get(index).is_some_and(|d| d.path.is_some()) {
            return Err("This project is a file on this computer: rename it with Save As".into());
        }
        let before = app.session.active_index();
        app.session.set_active(index);
        let result = app.run("file.info", json!({"title": title}));
        if let Some(before) = before {
            app.session.set_active(before);
        }
        result?;
        return Ok(json!({"renamed": title, "open": true}));
    }
    if rev.cached {
        app.cloud_sync.rename_revision(rev.project_id, rev.version_id, title.clone());
    } else {
        let home = &mut app.cloud_sync.home;
        home.pending_rename.insert(version, (rev.project_id, title.clone()));
        home.requested.insert(version);
        app.cloud_sync.request_payload(version);
    }
    Ok(json!({"renamed": title, "open": false}))
}

/// Write the project of card `version` to a file the user picks (`path`: the one agents name).
pub fn save_copy(app: &mut VectorcraftApp, version: Uuid, path: Option<String>) -> Result<Value, String> {
    let rev = catalog_revision(app, version).ok_or("No such cloud project")?;
    let target = match path {
        Some(p) => p,
        None => {
            let pick = crate::FilePick {
                name: format!("{}.{EXTENSION}", file_name_for(&rev.title)),
                folder: None,
                filters: vec![("VectorCraft", &[EXTENSION])],
            };
            crate::picks::save(app, &pick).ok_or("cancelled")?
        }
    };
    let target = if Path::new(&target).extension().is_some() { target } else { format!("{target}.{EXTENSION}") };
    vectorcraft_engine::file_access::check_write(&target)?;
    app.cloud_sync.home.errors.remove(&version);
    if rev.cached {
        let write = app.services.write_shared.clone();
        app.cloud_sync.copy_payload(rev.local_path, target.clone(), write);
    } else {
        let home = &mut app.cloud_sync.home;
        home.pending_copy.insert(version, target.clone());
        home.requested.insert(version);
        app.cloud_sync.request_payload(version);
    }
    Ok(json!({"pending": true, "path": target}))
}

fn retry(app: &mut VectorcraftApp, version: Uuid) -> Result<(), String> {
    let Some(error) = app.cloud_sync.home.errors.remove(&version) else { return Ok(()) };
    match error.retry {
        Retry::Open => open_version(app, version).map(|_| ()),
        Retry::Rename(title) => rename_version(app, version, &title).map(|_| ()),
        Retry::Copy(path) => save_copy(app, version, Some(path)).map(|_| ()),
    }
}

/// The payload of `version` arrived: carry on with a rename or a copy that waited for it.
pub fn payload_arrived(app: &mut VectorcraftApp, version: Uuid, path: PathBuf) {
    let home = &mut app.cloud_sync.home;
    home.requested.remove(&version);
    let rename = home.pending_rename.remove(&version);
    let copy = home.pending_copy.remove(&version);
    if let Some((project, title)) = rename {
        app.cloud_sync.rename_revision(project, version, title);
    }
    if let Some(target) = copy {
        let write = app.services.write_shared.clone();
        app.cloud_sync.copy_payload(path, target, write);
    }
}

/// The payload of `version` could not be fetched: say so on its card.
pub fn payload_failed(app: &mut VectorcraftApp, version: Uuid, error: &str) {
    let home = &mut app.cloud_sync.home;
    let message = format!("Cannot download the project: {error}");
    if let Some((_, title)) = home.pending_rename.remove(&version) {
        home.fail(version, message, Retry::Rename(title));
    } else if let Some(path) = home.pending_copy.remove(&version) {
        home.fail(version, message, Retry::Copy(path));
    } else if home.requested.remove(&version) {
        home.fail(version, message, Retry::Open);
    }
}

// --- Keyboard --------------------------------------------------------------------------------

/// Keys the Home screen owns while it is up: Ctrl/Cmd+F finds, F2 renames, Enter opens the card the
/// keyboard is on, Esc leaves for the open documents. Taken before the editor's shortcuts see them, so
/// that Enter does not also open a tool's dialog behind Home.
pub fn consume_keys(app: &mut VectorcraftApp, ctx: &egui::Context) {
    use egui::{Key, Modifiers};
    if !crate::menus::home_showing(app) {
        return;
    }
    if ctx.input_mut(|i| i.consume_key(Modifiers::COMMAND, Key::F)) {
        app.cloud_sync.home.focus_search = true;
    }
    if ctx.egui_wants_keyboard_input() {
        return;
    }
    if ctx.input_mut(|i| i.consume_key(Modifiers::NONE, Key::F2)) {
        app.cloud_sync.home.rename_requested = true;
    }
    if let Some(version) = app.cloud_sync.home.focused
        && ctx.input_mut(|i| i.consume_key(Modifiers::NONE, Key::Enter))
        && let Err(e) = open_version(app, version)
    {
        app.status(e);
    }
    if app.ui.home.is_some() && app.session.active().is_some() && ctx.input_mut(|i| i.consume_key(Modifiers::NONE, Key::Escape)) {
        app.ui.home = None;
    }
}

// --- Agents ----------------------------------------------------------------------------------

/// The status word agents read.
fn status_word(status: &SyncStatus) -> &'static str {
    match status {
        SyncStatus::Idle => "idle",
        SyncStatus::Syncing => "syncing",
        SyncStatus::Offline => "offline",
        SyncStatus::Error(_) => "error",
    }
}

/// `file.cloudList`: the projects the screen would show.
pub fn list_command(app: &VectorcraftApp, p: &Value) -> Result<Value, String> {
    let text = p.get("query").and_then(Value::as_str).unwrap_or("");
    let filter = match p.get("filter").and_then(Value::as_str) {
        Some(f) => Filter::parse(f).ok_or("filter must be all, recents, conflicts, offline or notUploaded")?,
        None => Filter::All,
    };
    let sort = match p.get("sort").and_then(Value::as_str) {
        Some(s) => Sort::parse(s).ok_or("sort must be lastEdited, nameAsc or nameDesc")?,
        None => Sort::LastEdited,
    };
    let cloud = &app.cloud_sync;
    let open = crate::cloud_sync::open_docs(app);
    let all = all_cards(&cloud.state.catalog, &cloud.home.downloading(), &open);
    let recents = cloud.home.recents.projects();
    let shown = visible(&all, &recents, &Query { text, filter, sort });
    let projects: Vec<Value> = shown
        .iter()
        .map(|c| {
            json!({
                "version": c.version.to_string(),
                "project": c.project.to_string(),
                "title": c.title,
                "badge": c.badge.key(),
                "badgeLabel": c.badge.label(),
                "conflict": c.conflict.map(|(i, n)| format!("{i}/{n}")),
                "cached": c.cached,
                "pending": c.pending,
                "open": c.open_uid.is_some(),
                "editedMs": c.created_ms,
                "hasPreview": c.preview.is_some(),
            })
        })
        .collect();
    Ok(json!({
        "ready": cloud.worker_ready(),
        "loading": cloud.loading(),
        "connected": cloud.connected(),
        "status": status_word(&cloud.state.status),
        "label": cloud.label(),
        "total": all.len(),
        "projects": projects,
        "recents": recents_row(&all, &recents).iter().map(|c| c.version.to_string()).collect::<Vec<_>>(),
        "cacheBytes": cloud.state.cache_bytes,
        "cacheLimit": cloud.state.cache_limit,
    }))
}

fn version_param(p: &Value) -> Result<Uuid, String> {
    let s = p.get("version").and_then(Value::as_str).ok_or("missing `version`")?;
    Uuid::parse_str(s).map_err(|_| "invalid version id".to_owned())
}

/// The UI commands of the Home library (`file.cloudList`, `file.cloudNew`, `file.cloudRename`,
/// `file.cloudSaveCopy`). `None`: not one of them.
pub fn command(app: &mut VectorcraftApp, id: &str, p: &Value) -> Option<Result<Value, String>> {
    Some(match id {
        "file.cloudList" => list_command(app, p),
        "file.cloudNew" => {
            app.cloud_sync.new_from_home = true;
            let params = if p.is_object() { p.clone() } else { json!({}) };
            let r = app.run("file.new", params);
            if r.is_err() {
                app.cloud_sync.new_from_home = false;
            }
            r
        }
        "file.cloudRename" => version_param(p).and_then(|v| rename_version(app, v, p.get("title").and_then(Value::as_str).unwrap_or(""))),
        "file.cloudSaveCopy" => version_param(p).and_then(|v| save_copy(app, v, p.get("path").and_then(Value::as_str).map(str::to_owned))),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rev(n: u128, project: u128, title: &str, created: Option<u64>) -> DownloadedRevision {
        DownloadedRevision {
            project_id: Uuid::from_u128(project),
            version_id: Uuid::from_u128(n),
            parents: Vec::new(),
            heads: vec![Uuid::from_u128(n)],
            title: title.into(),
            extension: EXTENSION.into(),
            local_path: PathBuf::from(format!("/nowhere/{n}")),
            created_ms: created,
            size: 10,
            preview_path: None,
            cached: false,
            pending: false,
        }
    }

    fn catalog() -> Vec<DownloadedRevision> {
        let mut poster = rev(1, 1, "Poster", Some(3_000));
        poster.cached = true;
        let mut logo = rev(2, 2, "logo draft", Some(5_000));
        logo.pending = true;
        logo.cached = true;
        let old = rev(3, 3, "Annual Report", Some(1_000));
        let undated = rev(4, 4, "undated", None);
        // Project 5 has two heads (a conflict); revision 9 is an earlier, non-head revision of it.
        let mut copy_a = rev(5, 5, "Brochure", Some(4_000));
        let mut copy_b = rev(6, 5, "Brochure", Some(2_000));
        copy_a.heads = vec![Uuid::from_u128(5), Uuid::from_u128(6)];
        copy_b.heads = copy_a.heads.clone();
        let mut earlier = rev(9, 5, "Brochure", Some(500));
        earlier.heads = copy_a.heads.clone();
        let mut other_app = rev(7, 7, "Not ours", Some(9_000));
        other_app.extension = "psd".into();
        vec![poster, logo, old, undated, copy_a, copy_b, earlier, other_app]
    }

    fn titles(cards: &[Card]) -> Vec<&str> {
        cards.iter().map(|c| c.title.as_str()).collect()
    }

    #[test]
    fn the_card_list_has_one_card_per_head_and_numbers_conflict_copies() {
        let cards = all_cards(&catalog(), &BTreeSet::new(), &[]);
        // Five projects; the conflicted one has two cards; earlier revisions and other apps' files have none.
        assert_eq!(cards.len(), 6);
        let copies: Vec<_> = cards.iter().filter(|c| c.project == Uuid::from_u128(5)).collect();
        assert_eq!(copies.len(), 2);
        // Newest first: copy 1/2 is the one edited last.
        let first = copies.iter().find(|c| c.version == Uuid::from_u128(5)).unwrap();
        let second = copies.iter().find(|c| c.version == Uuid::from_u128(6)).unwrap();
        assert_eq!((first.conflict, second.conflict), (Some((1, 2)), Some((2, 2))));
        assert!(cards.iter().filter(|c| c.project != Uuid::from_u128(5)).all(|c| c.conflict.is_none()));
        assert!(cards.iter().all(|c| c.version != Uuid::from_u128(9) && c.version != Uuid::from_u128(7)));
    }

    #[test]
    fn search_is_a_case_insensitive_substring_of_the_title() {
        let cards = all_cards(&catalog(), &BTreeSet::new(), &[]);
        let q = |text| Query { text, ..Default::default() };
        assert_eq!(titles(&visible(&cards, &[], &q("LOGO"))), ["logo draft"]);
        assert_eq!(titles(&visible(&cards, &[], &q("  port "))), ["Annual Report"]);
        assert_eq!(visible(&cards, &[], &q("brochure")).len(), 2, "both conflict copies match");
        assert!(visible(&cards, &[], &q("zzz")).is_empty());
        assert_eq!(visible(&cards, &[], &q("")).len(), cards.len());
    }

    #[test]
    fn sort_orders_by_time_or_name_and_puts_undated_projects_last() {
        let cards = all_cards(&catalog(), &BTreeSet::new(), &[]);
        let sorted =
            |sort| titles(&visible(&cards, &[], &Query { text: "", filter: Filter::All, sort })).into_iter().map(str::to_owned).collect::<Vec<_>>();
        assert_eq!(sorted(Sort::LastEdited), ["logo draft", "Brochure", "Poster", "Brochure", "Annual Report", "undated"]);
        assert_eq!(sorted(Sort::NameAsc), ["Annual Report", "Brochure", "Brochure", "logo draft", "Poster", "undated"]);
        assert_eq!(sorted(Sort::NameDesc), ["undated", "Poster", "logo draft", "Brochure", "Brochure", "Annual Report"]);
        assert_eq!(Sort::default(), Sort::LastEdited);
        assert_eq!(Filter::default(), Filter::All);
    }

    #[test]
    fn filters_pick_recents_conflicts_offline_copies_and_pending_uploads() {
        let cards = all_cards(&catalog(), &BTreeSet::new(), &[]);
        let recents = [Uuid::from_u128(3), Uuid::from_u128(4)];
        let with = |filter| {
            let mut t = titles(&visible(&cards, &recents, &Query { text: "", filter, sort: Sort::NameAsc }))
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            t.dedup();
            t
        };
        assert_eq!(with(Filter::All).len(), 5);
        assert_eq!(with(Filter::Recents), ["Annual Report", "undated"]);
        assert_eq!(with(Filter::Conflicts), ["Brochure"]);
        assert_eq!(visible(&cards, &recents, &Query { filter: Filter::Conflicts, ..Default::default() }).len(), 2);
        assert_eq!(with(Filter::Offline), ["logo draft", "Poster"]);
        assert_eq!(with(Filter::NotUploaded), ["logo draft"]);
        // Filter and search combine.
        let both = visible(&cards, &recents, &Query { text: "o", filter: Filter::Offline, sort: Sort::NameAsc });
        assert_eq!(titles(&both), ["logo draft", "Poster"]);
        assert_eq!(Filter::parse("notUploaded"), Some(Filter::NotUploaded));
        assert_eq!(Sort::parse("NAMEDESC"), Some(Sort::NameDesc));
        assert_eq!(Filter::parse("nope"), None);
    }

    #[test]
    fn the_badge_is_the_most_urgent_state() {
        // (pending, downloading, conflict, cached)
        assert_eq!(badge_for(true, false, false, true), Badge::NotUploaded);
        assert_eq!(badge_for(false, true, false, false), Badge::Downloading);
        assert_eq!(badge_for(false, false, false, true), Badge::OnComputer);
        assert_eq!(badge_for(false, false, false, false), Badge::InCloud);
        assert_eq!(badge_for(false, false, true, true), Badge::Conflict);
        assert_eq!(badge_for(true, false, true, true), Badge::NotUploaded, "work not yet uploaded comes first");
        assert_eq!(badge_for(false, true, true, false), Badge::Downloading);
        assert_eq!(
            [Badge::NotUploaded, Badge::Downloading, Badge::Conflict, Badge::OnComputer, Badge::InCloud].map(Badge::label),
            ["Not uploaded yet", "Downloading…", "Conflict", "On this computer", "In the cloud"]
        );
        // From the catalog: a payload being downloaded only counts while it is not on this computer.
        let mut downloading = BTreeSet::new();
        downloading.insert(Uuid::from_u128(1));
        downloading.insert(Uuid::from_u128(3));
        let cards = all_cards(&catalog(), &downloading, &[]);
        let badge = |n: u128| cards.iter().find(|c| c.version == Uuid::from_u128(n)).map(|c| c.badge);
        assert_eq!(badge(1), Some(Badge::OnComputer), "cached: nothing to download");
        assert_eq!(badge(3), Some(Badge::Downloading));
        assert_eq!(badge(2), Some(Badge::NotUploaded));
        assert_eq!(badge(4), Some(Badge::InCloud));
        assert_eq!(badge(5), Some(Badge::Conflict));
    }

    #[test]
    fn rename_validation_follows_the_shared_clients_rules() {
        assert_eq!(validate_title("  Poster v2 \n"), Ok("Poster v2".into()));
        assert!(validate_title("").is_err());
        assert!(validate_title("   \t ").is_err());
        assert!(validate_title("tab\there").is_err());
        assert!(validate_title("nul\0byte").is_err());
        assert!(validate_title(&"a".repeat(256)).is_ok());
        assert!(validate_title(&"a".repeat(257)).is_err());
        // Counted the way the client does (bytes), so that anything accepted here is accepted there.
        assert!(validate_title(&"é".repeat(129)).is_err());
        assert!(validate_title(&"é".repeat(128)).is_ok());
    }

    #[test]
    fn open_documents_are_matched_by_revision_then_by_a_single_headed_project() {
        let (p, v) = (Uuid::from_u128(1), Uuid::from_u128(10));
        let open = [OpenDoc { uid: 7, project: Some(p), version: Some(Uuid::from_u128(11)) }, OpenDoc { uid: 8, project: None, version: None }];
        assert_eq!(open_doc_for(&open, p, Uuid::from_u128(11), false), Some(7));
        assert_eq!(open_doc_for(&open, p, v, true), Some(7), "one head: the project's document");
        assert_eq!(open_doc_for(&open, p, v, false), None, "a conflict copy is not the document of the other copy");
        assert_eq!(open_doc_for(&open, Uuid::from_u128(2), v, true), None);
        let cards =
            all_cards(&catalog(), &BTreeSet::new(), &[OpenDoc { uid: 3, project: Some(Uuid::from_u128(1)), version: Some(Uuid::from_u128(1)) }]);
        assert_eq!(cards.iter().filter(|c| c.open_uid.is_some()).count(), 1);
    }

    #[test]
    fn pinned_set_is_exactly_the_bound_revisions_of_open_documents() {
        let docs = [
            OpenDoc { uid: 1, project: Some(Uuid::from_u128(1)), version: Some(Uuid::from_u128(10)) },
            OpenDoc { uid: 2, project: None, version: None },
            OpenDoc { uid: 3, project: Some(Uuid::from_u128(2)), version: Some(Uuid::from_u128(10)) },
            OpenDoc { uid: 4, project: Some(Uuid::from_u128(3)), version: Some(Uuid::from_u128(12)) },
        ];
        assert_eq!(pinned_set(&docs), BTreeSet::from([Uuid::from_u128(10), Uuid::from_u128(12)]));
        assert!(pinned_set(&[]).is_empty());
        assert!(pinned_set(&docs[1..2]).is_empty());
    }

    #[test]
    fn recents_keep_the_eight_newest_and_survive_a_round_trip() {
        let mut recents = Recents::default();
        for i in 1..=12u128 {
            recents.touch(Uuid::from_u128(i), 1_000 + i as u64);
        }
        let projects = recents.projects();
        assert_eq!(projects.len(), MAX_RECENTS);
        assert_eq!(projects.first(), Some(&Uuid::from_u128(12)));
        // Opening a project again moves it to the front without duplicating it, even with the clock stepped back.
        recents.touch(Uuid::from_u128(7), 5);
        assert_eq!(recents.projects().first(), Some(&Uuid::from_u128(7)));
        assert_eq!(recents.projects().iter().filter(|p| **p == Uuid::from_u128(7)).count(), 1);
        assert_eq!(Recents::parse(&recents.to_bytes()), recents);
        // The row shows the newest head of each recent project that is in the catalog, in recency order.
        let cards = all_cards(&catalog(), &BTreeSet::new(), &[]);
        let mut r = Recents::default();
        r.touch(Uuid::from_u128(5), 10);
        r.touch(Uuid::from_u128(9999), 11);
        r.touch(Uuid::from_u128(1), 12);
        let row = recents_row(&cards, &r.projects());
        assert_eq!(row.iter().map(|c| c.version).collect::<Vec<_>>(), [Uuid::from_u128(1), Uuid::from_u128(5)]);
    }

    #[test]
    fn recents_files_that_are_corrupt_oversized_or_foreign_are_empty() {
        assert_eq!(Recents::parse(b""), Recents::default());
        assert_eq!(Recents::parse(b"{not json"), Recents::default());
        assert_eq!(Recents::parse(br#"{"version":2,"projects":[]}"#), Recents::default());
        assert_eq!(Recents::parse(br#"{"projects":[]}"#), Recents::default());
        let huge = format!(
            r#"{{"version":1,"projects":[{}]}}"#,
            vec![r#"{"project":"00000000-0000-0000-0000-000000000001","opened_ms":1}"#; 2000].join(",")
        );
        assert!(huge.len() as u64 > MAX_RECENTS_FILE);
        assert_eq!(Recents::parse(huge.as_bytes()), Recents::default());
        // Entries that are not project ids are dropped, the rest kept (and capped).
        let mixed =
            br#"{"version":1,"projects":[{"project":"nope","opened_ms":9},{"project":"00000000-0000-0000-0000-000000000001","opened_ms":1}]}"#;
        assert_eq!(Recents::parse(mixed).projects(), [Uuid::from_u128(1)]);
        let many: Vec<String> = (1..=30u128).map(|i| format!(r#"{{"project":"{}","opened_ms":{i}}}"#, Uuid::from_u128(i))).collect();
        let capped = format!(r#"{{"version":1,"projects":[{}]}}"#, many.join(","));
        assert_eq!(Recents::parse(capped.as_bytes()).projects().len(), MAX_RECENTS);
    }

    #[test]
    fn recents_persist_atomically_in_the_private_folder_and_a_damaged_file_is_ignored() {
        let dir = std::env::temp_dir().join(format!("vector-cloud-recents-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(read_recents(&dir), Recents::default(), "no file yet");
        let mut recents = Recents::default();
        recents.touch(Uuid::from_u128(4), 44);
        write_recents(&dir, &recents).unwrap();
        assert_eq!(read_recents(&dir), recents);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1, "no temporary file is left behind");
        std::fs::write(dir.join(RECENTS_FILE), b"\xff\xfe garbage").unwrap();
        assert_eq!(read_recents(&dir), Recents::default());
        std::fs::write(dir.join(RECENTS_FILE), vec![b' '; 1 << 20]).unwrap();
        assert_eq!(read_recents(&dir), Recents::default());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn times_sizes_and_file_names_read_well() {
        let now = 10_000_000_000_000;
        let ago_label = |secs: u64| edited_label(now, Some(now - secs * 1000));
        assert_eq!(ago_label(5), "Edited just now");
        assert_eq!(ago_label(60), "Edited 1 minute ago");
        assert_eq!(ago_label(5 * 60 + 10), "Edited 5 minutes ago");
        assert_eq!(ago_label(3600), "Edited 1 hour ago");
        assert_eq!(ago_label(3 * 3600), "Edited 3 hours ago");
        assert_eq!(ago_label(2 * 86_400), "Edited 2 days ago");
        assert_eq!(ago_label(65 * 86_400), "Edited 2 months ago");
        assert_eq!(ago_label(800 * 86_400), "Edited 2 years ago");
        assert_eq!(edited_label(now, Some(now + 5_000)), "Edited just now", "a clock that is ahead");
        assert_eq!(edited_label(now, None), "—");
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1536), "1.50 KiB");
        assert_eq!(human_bytes(512 << 20), "512 MiB");
        assert_eq!(human_bytes(2 << 30), "2.00 GiB");
        assert_eq!(human_bytes(u64::MAX).ends_with("TiB"), true);
        assert_eq!(file_name_for("a/b:c*?"), "a_b_c__");
        assert_eq!(file_name_for("  .. "), "Untitled");
        assert_eq!(file_name_for(&"x".repeat(300)).len(), 100);
    }

    fn app() -> VectorcraftApp {
        VectorcraftApp::new(vectorcraft_engine::Session::new(), crate::Services::default())
    }

    #[test]
    fn the_list_command_reports_projects_badges_and_rejects_bad_filters() {
        let mut app = app();
        app.cloud_sync.state.catalog = catalog();
        let r = list_command(&app, &json!({})).unwrap();
        assert_eq!(r["total"], 6);
        assert_eq!(r["projects"].as_array().map(Vec::len), Some(6));
        assert_eq!(r["projects"][0]["title"], "logo draft");
        assert_eq!(r["projects"][0]["badge"], "notUploaded");
        let conflicts = list_command(&app, &json!({"filter": "conflicts", "sort": "nameAsc"})).unwrap();
        assert_eq!(conflicts["projects"].as_array().map(Vec::len), Some(2));
        assert_eq!(conflicts["projects"][0]["conflict"], "1/2");
        assert!(list_command(&app, &json!({"filter": "weird"})).is_err());
        assert!(list_command(&app, &json!({"sort": "weird"})).is_err());
        let found = list_command(&app, &json!({"query": "poster"})).unwrap();
        assert_eq!(found["projects"].as_array().map(Vec::len), Some(1));
        // Through the command table too.
        let via = app.run("file.cloudList", json!({"query": "report"})).unwrap();
        assert_eq!(via["projects"][0]["title"], "Annual Report");
    }

    #[test]
    fn opening_a_card_switches_to_its_open_document_instead_of_opening_it_twice() {
        let mut app = app();
        app.cloud_sync.state.catalog = catalog();
        app.run("file.new", json!({"width": 100, "height": 100})).unwrap();
        app.run("file.new", json!({"width": 100, "height": 100})).unwrap();
        let uid = app.session.documents().first().map(|d| d.uid).unwrap();
        let st = app.session.documents().first().unwrap();
        app.cloud_sync.test_bind(st, "cloud-test", Uuid::from_u128(1), Uuid::from_u128(1));
        app.ui.home = Some(crate::menus::home_key(&app));
        let r = open_version(&mut app, Uuid::from_u128(1)).unwrap();
        assert_eq!(r["switched"], true);
        assert_eq!(app.session.active().map(|d| d.uid), Some(uid));
        assert!(app.ui.home.is_none(), "Home gives way to the document");
        assert_eq!(app.session.documents().len(), 2);
        assert!(open_version(&mut app, Uuid::from_u128(12345)).is_err());
    }

    #[test]
    fn renaming_an_open_project_renames_its_document_and_keeps_the_view() {
        let mut app = app();
        app.cloud_sync.state.catalog = catalog();
        app.run("file.new", json!({"width": 100, "height": 100})).unwrap();
        let first = app.session.active().map(|d| d.uid).unwrap();
        app.run("file.new", json!({"width": 100, "height": 100})).unwrap();
        let second = app.session.active().map(|d| d.uid).unwrap();
        let st = app.session.documents().iter().find(|d| d.uid == first).unwrap();
        app.cloud_sync.test_bind(st, "cloud-test", Uuid::from_u128(1), Uuid::from_u128(1));
        app.ui.home = Some(crate::menus::home_key(&app));
        let r = app.run("file.cloudRename", json!({"version": Uuid::from_u128(1).to_string(), "title": "  New name "})).unwrap();
        assert_eq!(r["open"], true);
        assert_eq!(app.session.documents().iter().find(|d| d.uid == first).map(|d| d.title()), Some("New name".into()));
        assert_eq!(app.session.active().map(|d| d.uid), Some(second), "the active document and Home are as they were");
        assert!(app.ui.home.is_some());
        assert!(app.run("file.cloudRename", json!({"version": Uuid::from_u128(1).to_string(), "title": ""})).is_err());
        assert!(app.run("file.cloudRename", json!({"version": "nope", "title": "x"})).is_err());
        assert!(app.run("file.cloudRename", json!({"version": Uuid::from_u128(77).to_string(), "title": "x"})).is_err());
    }

    #[test]
    fn keys_belong_to_home_only_while_it_is_up() {
        use egui::{Event, Key, Modifiers};
        let mut app = app();
        app.run("file.new", json!({"width": 100, "height": 100})).unwrap();
        let ctx = egui::Context::default();
        let press = |key: Key, modifiers: Modifiers| Event::Key { key, physical_key: None, pressed: true, repeat: false, modifiers };
        let run = |app: &mut VectorcraftApp, events: Vec<Event>| {
            let raw = egui::RawInput { events, ..Default::default() };
            let mut out = ctx.run_ui(raw, |ui| consume_keys(app, ui.ctx()));
            out.textures_delta.clear();
        };
        // A document is showing: Esc and Ctrl+F are the editor's.
        run(&mut app, vec![press(Key::Escape, Modifiers::NONE), press(Key::F, Modifiers::COMMAND)]);
        assert!(!app.cloud_sync.home.focus_search);
        app.ui.home = Some(crate::menus::home_key(&app));
        run(&mut app, vec![press(Key::F, Modifiers::COMMAND)]);
        assert!(app.cloud_sync.home.focus_search);
        run(&mut app, vec![press(Key::F2, Modifiers::NONE)]);
        assert!(app.cloud_sync.home.rename_requested);
        assert!(app.ui.home.is_some());
        run(&mut app, vec![press(Key::Escape, Modifiers::NONE)]);
        assert!(app.ui.home.is_none(), "Esc leaves Home for the open document");
        // With no document there is nothing to go back to.
        app.run("file.close", json!({})).unwrap();
        app.ui.home = Some(crate::menus::home_key(&app));
        run(&mut app, vec![press(Key::Escape, Modifiers::NONE)]);
        assert!(app.ui.home.is_some());
    }
}
