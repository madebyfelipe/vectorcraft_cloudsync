//! Native-only, local-first checkpoints. The worker owns all encoding, filesystem and network work.
//! A checkpoint never calls SaveJob::finish or changes the editor's manual-save/recovery state.
use crate::VectorcraftApp;
use craft_cloud_sync::{Config, DownloadedRevision, Snapshot, SyncEvent, SyncService, SyncState, SyncStatus};
use serde_json::json;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        mpsc::{self, Receiver, Sender},
    },
    time::Duration,
};
use vectorcraft_doc::{Document, ImageBlob};
use vectorcraft_engine::cmd::fileio::{self, ReadFile};

struct Track {
    observed: Arc<Document>,
    durable: Option<Arc<Document>>,
    busy: usize,
    queued: Option<Arc<Document>>,
    failed: Option<Arc<Document>>,
    changed: f64,
    last: f64,
    key: String,
}
enum Work {
    Connect(Config),
    Capture { uid: u64, key: String, title: String, doc: Arc<Document> },
    Open(OpenSource, vectorcraft_engine::file_access::ScopeHandle),
    Bind { key: String, revision: DownloadedRevision },
}
enum Done {
    State(SyncState),
    Captured(u64, Arc<Document>, Result<Option<String>, String>),
    Opened(OpenSource, Result<Box<ReadFile>, String>),
    Local(LocalDraft),
    Account(Account),
    CredentialNotice(Option<String>),
    Error(String),
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Account {
    endpoint: String,
    username: String,
}
impl From<&Config> for Account {
    fn from(config: &Config) -> Self {
        Self { endpoint: config.endpoint.clone(), username: config.username.clone() }
    }
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct LocalDraft {
    key: String,
    title: String,
    path: PathBuf,
    #[serde(default)]
    warning: Option<String>,
    #[serde(default)]
    token: String,
    #[serde(default = "pending_default")]
    needs_submit: bool,
    #[serde(default)]
    portable: bool,
    #[serde(default)]
    captured_at: u128,
    #[serde(default)]
    sequence: u64,
}
#[derive(Clone)]
enum OpenSource {
    Cloud(DownloadedRevision),
    Local(LocalDraft),
}
#[derive(Default)]
pub struct CloudSync {
    pub visible: bool,
    endpoint: String,
    username: String,
    password: String,
    send: Option<Sender<Work>>,
    recv: Option<Receiver<Done>>,
    tracks: BTreeMap<u64, Track>,
    state: SyncState,
    error: Option<String>,
    network_error: Option<String>,
    credential_notice: Option<String>,
    instance: String,
    closing: bool,
    local: Vec<LocalDraft>,
}
impl CloudSync {
    pub fn label(&self) -> &str {
        if self.error.is_some() || self.local.iter().any(|draft| draft.warning.is_some()) {
            "Cloud: Error"
        } else if self.tracks.values().any(|t| t.busy > 0 || t.durable.as_ref().is_none_or(|d| !Arc::ptr_eq(d, &t.observed))) {
            "Cloud: Saving…"
        } else {
            match &self.state.status {
                SyncStatus::Offline => "Cloud: Offline",
                SyncStatus::Error(_) => "Cloud: Error",
                SyncStatus::Syncing => "Cloud: Saving…",
                SyncStatus::Idle if self.state.pending > 0 => "Cloud: Saving…",
                SyncStatus::Idle => "Cloud: Saved",
            }
        }
    }
    fn send(&self, work: Work) {
        if let Some(tx) = &self.send {
            let _ = tx.send(work);
        }
    }
}
fn cache() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os("VECTORCRAFT_CLOUD_CACHE") {
        return Ok(PathBuf::from(path));
    }
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")).map(PathBuf::from);
    let base = if cfg!(target_os = "windows") {
        std::env::var_os("LOCALAPPDATA").map(PathBuf::from)
    } else if cfg!(target_os = "macos") {
        home.as_ref().map(|h| h.join("Library/Application Support"))
    } else {
        std::env::var_os("XDG_DATA_HOME").map(PathBuf::from).filter(|p| p.is_absolute()).or_else(|| home.map(|h| h.join(".local/share")))
    };
    base.map(|p| p.join("vectorcraft/cloud-sync")).ok_or_else(|| "Cannot find the local cloud project folder".into())
}
/// A cloud open waiting for its revision payload to download.
type AwaitingOpen = (OpenSource, vectorcraft_engine::file_access::ScopeHandle);
fn take_waiting(waiting: &mut Vec<AwaitingOpen>, version: uuid::Uuid) -> Vec<AwaitingOpen> {
    let (done, rest) = std::mem::take(waiting).into_iter().partition(|(source, _)| matches!(source, OpenSource::Cloud(r) if r.version_id == version));
    *waiting = rest;
    done
}
fn read_source(revision: &OpenSource, scope: vectorcraft_engine::file_access::ScopeHandle) -> Result<Box<ReadFile>, String> {
    scope
        .run(|| {
            vectorcraft_engine::guard::catch_panic(|| {
                let (path, title) = match revision {
                    OpenSource::Cloud(r) => (&r.local_path, &r.title),
                    OpenSource::Local(r) => (&r.path, &r.title),
                };
                vectorcraft_engine::file_access::check_read(&path.to_string_lossy())?;
                std::fs::read(path).map_err(|e| e.to_string()).and_then(|bytes| {
                    without_source_reads(|| {
                        fileio::read_for_open(&format!("{}.vectorcraft", title), &bytes, None, &json!({}), false).map_err(|e| e.to_string())
                    })
                })
            })
        })
        .unwrap_or_else(|e| Err(format!("Cloud project could not open: {e}")))
        .map(Box::new)
}
fn launch(cloud: &mut CloudSync) {
    let (tx, work) = mpsc::channel();
    let (done, rx) = mpsc::channel();
    let spawned = std::thread::Builder::new().name("vectorcraft-cloud-checkpoints".into()).spawn(move || {
        let result = cache().and_then(|path| {
            SyncService::new(Config { endpoint: String::new(), username: String::new(), password: String::new() }, path, "vectorcraft")
                .map_err(|e| e.to_string())
        });
        let mut service = match result {
            Ok(s) => Some(s),
            Err(e) => {
                let _ = done.send(Done::Error(e));
                None
            }
        };
        if let Ok(path) = cache()
            && path.join("account.json").exists()
        {
            match read_account(&path) {
                Ok(account) => {
                    let _ = done.send(Done::Account(account.clone()));
                    if !account.endpoint.is_empty() {
                        match craft_cloud_sync::credentials::load_credential("vectorcraft", &account.endpoint, &account.username) {
                            Ok(Some(password)) => {
                                if let Err(e) =
                                    replace_service(&mut service, Config { endpoint: account.endpoint, username: account.username, password })
                                {
                                    let _ = done.send(Done::Error(e));
                                }
                            }
                            Ok(None) => {
                                let _ =
                                    done.send(Done::CredentialNotice(Some("Reconnect to resume cloud sync; no saved credential was found.".into())));
                            }
                            Err(e) => {
                                let _ = done
                                    .send(Done::CredentialNotice(Some(format!("{e}. This connection is session only; reconnect after restarting."))));
                            }
                        }
                    }
                }
                Err(e) => {
                    let _ = done.send(Done::Error(e));
                }
            }
        }
        let mut drafts = BTreeMap::new();
        let mut waiting: Vec<AwaitingOpen> = Vec::new();
        let mut retried: Option<std::time::Instant> = None;
        if let Ok(path) = cache()
            && let Ok(entries) = std::fs::read_dir(path.join("local-checkpoints"))
        {
            for entry in entries.flatten() {
                if entry.path().extension().is_some_and(|e| e == "json")
                    && let Ok(bytes) = std::fs::read(entry.path())
                    && let Ok(draft) = serde_json::from_slice::<LocalDraft>(&bytes)
                {
                    drafts.insert(draft.path.clone(), draft.clone());
                    let _ = done.send(Done::Local(draft));
                }
            }
        }
        loop {
            match work.recv_timeout(Duration::from_millis(250)) {
                Ok(Work::Connect(config)) => {
                    for (revision, _) in waiting.drain(..) {
                        let _ = done.send(Done::Opened(revision, Err("Cloud download interrupted by reconnecting; open the project again".into())));
                    }
                    let account = Account::from(&config);
                    let password = config.password.clone();
                    match replace_service(&mut service, config) {
                        Ok(()) => {
                            if let Err(e) = cache().and_then(|p| save_account(&p, &account)) {
                                let _ = done.send(Done::Error(e));
                            }
                            let notice = if account.endpoint.is_empty() {
                                None
                            } else {
                                craft_cloud_sync::credentials::save_credential("vectorcraft", &account.endpoint, &account.username, &password)
                                    .err()
                                    .map(|e| format!("{e}. This connection is session only; reconnect after restarting."))
                            };
                            let _ = done.send(Done::CredentialNotice(notice));
                            let _ = done.send(Done::Account(account));
                        }
                        Err(e) => {
                            let _ = done.send(Done::Error(e));
                        }
                    }
                }
                Ok(Work::Capture { uid, key, title, doc }) => {
                    let result = vectorcraft_engine::guard::catch_panic(|| {
                        let native = encode_native(&doc)?;
                        let title = if title.trim().is_empty() { "Untitled".to_owned() } else { title };
                        // Preserve the exact local editing state before checking remote portability.
                        let sequence = drafts.values().map(|d| d.sequence).max().unwrap_or(0).saturating_add(1);
                        let mut draft = save_local(&key, &title, &native, sequence)?;
                        drafts.insert(draft.path.clone(), draft.clone());
                        let remote = portable(&doc, 0).and_then(|portable| encode_native(&portable)).and_then(|native| {
                            // Freeze validated sources locally before enqueue, so restart retries
                            // use the same portable payload even when a linked file later moves.
                            vectorcraft_format::write_atomic(&draft.path, &native).map_err(|e| e.to_string())?;
                            draft.portable = true;
                            save_metadata(&draft)?;
                            service
                                .as_ref()
                                .ok_or("Cloud outbox is unavailable; local checkpoints remain available")?
                                .submit(Snapshot {
                                    local_key: key,
                                    title,
                                    extension: "vectorcraft".into(),
                                    local_token: draft.token.clone(),
                                    native,
                                    preview: None,
                                })
                                .map(|_| ())
                                .map_err(|e| e.to_string())
                        });
                        draft.needs_submit = remote.is_err();
                        draft.warning = remote.err().map(|e| format!("{e}. Local checkpoint: {}", draft.path.display()));
                        if let Err(e) = save_metadata(&draft) {
                            draft.warning = Some(format!("{e}. Native checkpoint retained at {}", draft.path.display()));
                        }
                        let warning = draft.warning.clone();
                        drafts.insert(draft.path.clone(), draft.clone());
                        let _ = done.send(Done::Local(draft));
                        Ok(warning)
                    })
                    .unwrap_or_else(|e| Err(format!("Cloud checkpoint failed: {e}")));
                    let _ = done.send(Done::Captured(uid, doc, result));
                }
                Ok(Work::Open(revision, scope)) => match (&revision, service.as_ref()) {
                    (OpenSource::Cloud(r), Some(sync)) => {
                        // Payloads download on demand: request it and finish the open when the sync service reports it fetched.
                        sync.request_payload(r.version_id);
                        waiting.push((revision, scope));
                    }
                    (OpenSource::Cloud(_), None) => {
                        let _ = done.send(Done::Opened(revision, Err("Cloud outbox is unavailable".into())));
                    }
                    (OpenSource::Local(_), _) => {
                        let result = read_source(&revision, scope);
                        let _ = done.send(Done::Opened(revision, result));
                    }
                },
                Ok(Work::Bind { key, revision }) => {
                    if let Err(e) = service
                        .as_ref()
                        .ok_or_else(|| "Cloud outbox is unavailable".to_owned())
                        .and_then(|s| s.bind(&key, revision.project_id, revision.version_id).map_err(|e| e.to_string()))
                    {
                        let _ = done.send(Done::Error(e.to_string()));
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
            if retried.is_none_or(|at| at.elapsed() >= Duration::from_secs(15)) {
                retried = Some(std::time::Instant::now());
                let mut pending: Vec<_> = drafts.values_mut().filter(|d| d.needs_submit).collect();
                pending.sort_by_key(|d| (d.sequence, d.captured_at));
                for draft in pending {
                    let result = service
                        .as_ref()
                        .ok_or_else(|| "Cloud outbox is unavailable; local checkpoints remain available".to_owned())
                        .and_then(|service| vectorcraft_engine::guard::catch_panic(|| retry_draft(service, draft)).unwrap_or_else(Err));
                    draft.warning = result.err().map(|e| format!("{e}. Local checkpoint: {}", draft.path.display()));
                    if let Err(e) = save_metadata(draft) {
                        let _ = done.send(Done::Error(e));
                    }
                    let _ = done.send(Done::Local(draft.clone()));
                }
            }
            let state = service.as_ref().map(SyncService::poll).unwrap_or_else(|| SyncState {
                status: SyncStatus::Error("Cloud outbox is unavailable. Local checkpoints keep saving.".into()),
                ..Default::default()
            });
            for event in &state.events {
                match event {
                    SyncEvent::Fetched { version_id, .. } => {
                        for (revision, scope) in take_waiting(&mut waiting, *version_id) {
                            let result = read_source(&revision, scope);
                            let _ = done.send(Done::Opened(revision, result));
                        }
                    }
                    SyncEvent::FetchFailed { version_id, error } => {
                        for (revision, _) in take_waiting(&mut waiting, *version_id) {
                            let _ = done.send(Done::Opened(revision, Err(format!("Cannot download the cloud project: {error}"))));
                        }
                    }
                    _ => {}
                }
            }
            if done.send(Done::State(state)).is_err() {
                break;
            }
        }
    });
    match spawned {
        Ok(_) => {
            cloud.send = Some(tx);
            cloud.recv = Some(rx);
            cloud.instance = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|v| v.as_nanos().to_string()).unwrap_or_default();
        }
        Err(e) => cloud.error = Some(e.to_string()),
    }
}
/// Reject preview-only or missing source data rather than quietly publishing an incomplete project.
fn portable(doc: &Document, depth: usize) -> Result<Document, String> {
    without_source_reads(|| portable_inner(doc, depth))
}
fn portable_inner(doc: &Document, depth: usize) -> Result<Document, String> {
    if depth > 16 {
        return Err("Cloud checkpoint: linked documents exceed the nesting limit".into());
    }
    let mut out = doc.clone();
    let mut error = None;
    doc.visit_images(|_, image| {
        if doc.images.get(&image.key).is_none_or(|b| b.is_proxy() || b.bytes.is_empty()) {
            error = Some("Cloud checkpoint needs the original image data; relink or update missing images first".to_owned());
        }
    });
    doc.visit_placed(|_, placed| {
        let result = (|| {
            let blob = doc.images.get(&placed.key).ok_or("Cloud checkpoint needs the original placed document")?;
            if depth > 0 && blob.is_proxy() {
                return Err("Cloud checkpoint needs self-contained original placed sources; update and save the linked document first".into());
            }
            let bytes = vectorcraft_doc::placed_document::full_bytes_cached(placed, blob)
                .ok_or("Cloud checkpoint needs the original placed document; relink or update it first")?;
            // Native sources can themselves refer to missing linked assets. Keep their original
            // bytes only when self-contained, so another device gets the exact editable source.
            let nested = fileio::load(placed.link.name(), &bytes).map_err(|e| e.to_string())?;
            if nested.preview_only {
                return Err("Cloud checkpoint needs the editable original placed source".into());
            }
            portable_inner(&nested.doc, depth + 1)?;
            out.images.insert(placed.key.clone(), ImageBlob { mime: blob.mime.clone(), bytes, proxy: blob.proxy.clone() });
            Ok::<_, String>(())
        })();
        if let Err(e) = result {
            error = Some(e);
        }
    });
    if let Some(e) = error { Err(e) } else { Ok(out) }
}
fn due(track: &Track, now: f64) -> bool {
    track.busy == 0
        && track.durable.as_ref().is_none_or(|d| !Arc::ptr_eq(d, &track.observed))
        && (now - track.changed >= 1.5 || now - track.last >= 15.0)
}
pub fn frame(app: &mut VectorcraftApp, ctx: &egui::Context, now: f64) {
    if !cfg!(test) && app.cloud_sync.send.is_none() && app.cloud_sync.error.is_none() {
        launch(&mut app.cloud_sync);
    }
    let messages: Vec<_> = app.cloud_sync.recv.as_ref().map(|rx| rx.try_iter().collect()).unwrap_or_default();
    for message in messages {
        match message {
            Done::State(state) => {
                for event in &state.events {
                    if let SyncEvent::Error(e) = event {
                        app.cloud_sync.network_error = Some(e.clone());
                    }
                }
                if matches!(state.status, SyncStatus::Idle) {
                    app.cloud_sync.network_error = None;
                }
                app.cloud_sync.state = state;
            }
            Done::Error(e) => app.cloud_sync.error = Some(e),
            Done::CredentialNotice(notice) => app.cloud_sync.credential_notice = notice,
            Done::Account(account) => {
                app.cloud_sync.endpoint = account.endpoint;
                app.cloud_sync.username = account.username;
            }
            Done::Local(draft) => {
                app.cloud_sync.local.retain(|d| d.path != draft.path);
                app.cloud_sync.local.push(draft);
            }
            Done::Captured(uid, doc, result) => {
                if let Some(track) = app.cloud_sync.tracks.get_mut(&uid) {
                    track.busy = track.busy.saturating_sub(1);
                    if track.busy == 0 {
                        track.queued = None;
                    }
                    if result.is_ok() {
                        track.failed = None;
                        track.durable = Some(doc);
                    } else {
                        track.failed = Some(doc);
                        track.last = now;
                        track.changed = now + 13.5;
                    }
                }
                match result {
                    Ok(_) => {}
                    Err(e) => app.cloud_sync.error = Some(e),
                }
            }
            Done::Opened(revision, result) => match result.and_then(|read| fileio::open_read(&mut app.session, *read).map_err(|e| e.to_string())) {
                Ok(value) => {
                    crate::io::opened(app, &value);
                    if let Ok(st) = app.session.doc() {
                        let key = match revision {
                            OpenSource::Cloud(revision) => {
                                let key = format!("cloud-{}-{}", revision.project_id, st.uid);
                                app.cloud_sync.send(Work::Bind { key: key.clone(), revision });
                                key
                            }
                            OpenSource::Local(draft) => draft.key,
                        };
                        app.cloud_sync.tracks.insert(
                            st.uid,
                            Track {
                                observed: st.doc.clone(),
                                durable: Some(st.doc.clone()),
                                busy: 0,
                                queued: None,
                                failed: None,
                                changed: now,
                                last: now,
                                key,
                            },
                        );
                    }
                }
                Err(e) => app.cloud_sync.error = Some(e),
            },
        }
    }
    app.cloud_sync
        .local
        .sort_by(|a, b| b.sequence.cmp(&a.sequence).then_with(|| b.captured_at.cmp(&a.captured_at)).then_with(|| b.path.cmp(&a.path)));
    for st in app.session.documents() {
        let track = app.cloud_sync.tracks.entry(st.uid).or_insert_with(|| Track {
            observed: st.doc.clone(),
            durable: None,
            busy: 0,
            queued: None,
            failed: None,
            changed: now,
            last: now,
            key: local_key(st.path.as_deref(), &app.cloud_sync.instance, st.uid),
        });
        if !Arc::ptr_eq(&track.observed, &st.doc) {
            track.observed = st.doc.clone();
            track.changed = now;
        }
        if due(track, now) {
            track.busy += 1;
            track.queued = Some(track.observed.clone());
            track.last = now;
            let work = Work::Capture { uid: st.uid, key: track.key.clone(), title: st.title(), doc: st.doc.clone() };
            app.cloud_sync.send(work);
        }
    }
    app.cloud_sync.tracks.retain(|uid, track| track.busy > 0 || app.session.documents().iter().any(|st| st.uid == *uid));
    if app.cloud_sync.closing {
        checkpoint_all(app);
        if app.cloud_sync.tracks.values().all(|t| t.busy == 0) {
            app.cloud_sync.closing = false;
            if let Err(e) = crate::unsaved::close_all(app, "quit") {
                app.status(e);
            }
        }
    }
    ctx.request_repaint_after(Duration::from_millis(250));
    window(&mut app.cloud_sync, ctx);
}
fn window(cloud: &mut CloudSync, ctx: &egui::Context) {
    if !cloud.visible {
        return;
    }
    let mut visible = true;
    egui::Window::new("Cloud Projects").open(&mut visible).default_width(440.0).show(ctx, |ui| {
        ui.label(cloud.label());
        if let Some(e) = &cloud.error {
            ui.label(e);
        }
        if let Some(e) = &cloud.network_error {
            ui.label(e);
        }
        if let Some(notice) = &cloud.credential_notice {
            ui.label(notice);
        }
        if let Some(warning) = cloud.local.iter().find_map(|d| d.warning.as_ref()) {
            ui.label(warning);
        }
        if let SyncStatus::Error(e) = &cloud.state.status {
            ui.label(e);
        }
        ui.collapsing("Connection", |ui| {
            ui.label("Personal WebDAV HTTPS endpoint");
            ui.text_edit_singleline(&mut cloud.endpoint);
            ui.label("Username");
            ui.text_edit_singleline(&mut cloud.username);
            ui.label("Password");
            ui.add(egui::TextEdit::singleline(&mut cloud.password).password(true));
            ui.small("Credentials use the OS credential vault when available. Otherwise this connection is session only.");
            if ui.button("Connect").clicked() {
                let config =
                    Config { endpoint: cloud.endpoint.clone(), username: cloud.username.clone(), password: std::mem::take(&mut cloud.password) };
                if let Err(e) = connect_config(cloud, config) {
                    cloud.error = Some(e);
                }
            }
        });
        ui.separator();
        let mut open = None;
        egui::ScrollArea::vertical().max_height(360.0).show(ui, |ui| {
            let mut copies = BTreeMap::new();
            for revision in cloud.state.catalog.iter().filter(|r| r.extension == "vectorcraft" && r.heads.contains(&r.version_id)) {
                let copy = copies.entry(revision.project_id).or_insert(0usize);
                *copy += 1;
                let label = if revision.heads.len() > 1 { format!("{} · conflict copy {}", revision.title, copy) } else { revision.title.clone() };
                if ui.button(label).clicked() {
                    open = Some(OpenSource::Cloud(revision.clone()));
                }
            }
            ui.collapsing("Earlier revisions", |ui| {
                for (index, revision) in
                    cloud.state.catalog.iter().filter(|r| r.extension == "vectorcraft" && !r.heads.contains(&r.version_id)).enumerate()
                {
                    if ui.button(format!("{} · earlier copy {}", revision.title, index + 1)).clicked() {
                        open = Some(OpenSource::Cloud(revision.clone()));
                    }
                }
            });
        });
        if cloud.state.catalog.is_empty() {
            ui.label("Projects appear here automatically after connecting.");
        }
        ui.collapsing("Local checkpoints", |ui| {
            let mut shown = std::collections::BTreeSet::new();
            let mut earlier = Vec::new();
            for draft in &cloud.local {
                if !shown.insert(&draft.key) {
                    earlier.push(draft);
                    continue;
                }
                if let Some(warning) = &draft.warning {
                    ui.label(warning);
                }
                if ui.button(&draft.title).clicked() {
                    open = Some(OpenSource::Local(draft.clone()));
                }
            }
            ui.collapsing("Earlier local copies", |ui| {
                for (index, draft) in earlier.into_iter().enumerate() {
                    if let Some(warning) = &draft.warning {
                        ui.label(warning);
                    }
                    if ui.button(format!("{} · earlier copy {}", draft.title, index + 1)).clicked() {
                        open = Some(OpenSource::Local(draft.clone()));
                    }
                }
            });
        });
        if let Some(revision) = open {
            cloud.send(Work::Open(revision, vectorcraft_engine::file_access::ScopeHandle::capture()));
        }
    });
    cloud.visible = visible;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn continuous_changes_flush_at_maximum_interval_and_inflight_is_not_duplicated() {
        let doc = Arc::new(Document::new(100.0, 100.0));
        let mut track = Track { observed: doc, durable: None, busy: 0, queued: None, failed: None, changed: 14.9, last: 0.0, key: "test".into() };
        assert!(!due(&track, 14.9));
        assert!(due(&track, 15.0));
        track.busy += 1;
        track.queued = Some(track.observed.clone());
        assert!(!due(&track, 40.0));
        track.busy = 0;
        track.queued = None;
        track.durable = Some(track.observed.clone());
        assert!(!due(&track, 40.0));
        track.observed = Arc::new(Document::new(200.0, 100.0));
        assert!(due(&track, 40.0));
    }
    #[test]
    fn clean_native_snapshot_round_trips_without_marking_manual_save() {
        let mut session = vectorcraft_engine::Session::new();
        session.execute("file.new", &json!({"width":100,"height":80})).unwrap();
        session.execute("shape.rectangle", &json!({"x":0,"y":0,"width":10,"height":10})).unwrap();
        let doc = session.doc().unwrap().doc.clone();
        assert!(session.doc().unwrap().is_dirty());
        let portable = portable(&doc, 0).unwrap();
        let bytes =
            fileio::encode_all(&portable, "vectorcraft", &json!({"version":4,"includeLinked":true,"embedProfiles":true})).unwrap().files.remove(0).1;
        assert_eq!(vectorcraft_format::load(&bytes).unwrap().artboards, doc.artboards);
        assert!(session.doc().unwrap().is_dirty());
        assert!(Arc::ptr_eq(&doc, &session.doc().unwrap().doc));
    }
    #[test]
    fn closing_tab_queues_latest_revision_even_while_older_capture_is_running() {
        let mut app = VectorcraftApp::new(vectorcraft_engine::Session::new(), crate::Services::default());
        app.run("file.new", json!({"width":100,"height":80})).unwrap();
        let (tx, rx) = mpsc::channel();
        app.cloud_sync.send = Some(tx);
        checkpoint_document(&mut app, 0);
        let old = match rx.try_recv().unwrap() {
            Work::Capture { doc, .. } => doc,
            _ => panic!("expected snapshot"),
        };
        app.run("shape.rectangle", json!({"x":0,"y":0,"width":10,"height":10})).unwrap();
        let latest = app.session.doc().unwrap().doc.clone();
        assert!(!Arc::ptr_eq(&old, &latest));
        app.run("file.close", json!({})).unwrap();
        app.ui.dialog.as_mut().unwrap().fields.insert("discard".into(), json!(true));
        crate::unsaved::confirm(&mut app).unwrap();
        let captured = match rx.try_recv().unwrap() {
            Work::Capture { doc, .. } => doc,
            _ => panic!("expected snapshot"),
        };
        assert!(Arc::ptr_eq(&captured, &latest));
        assert!(app.session.documents().is_empty());
        frame(&mut app, &egui::Context::default(), 0.0);
        assert_eq!(app.cloud_sync.tracks.values().next().unwrap().busy, 2);
    }
    #[test]
    fn restart_retries_same_draft_token_without_duplicate_cloud_revision() {
        let folder = std::env::temp_dir().join(format!(
            "vector-cloud-retry-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let config = || Config { endpoint: String::new(), username: String::new(), password: String::new() };
        let native = encode_native(&Document::new(100.0, 80.0)).unwrap();
        let mut draft = save_local_at(&folder.join("drafts"), "offline-draft", "Offline draft", &native, 1).unwrap();
        let mut service = SyncService::new(config(), folder.join("service"), "vectorcraft").unwrap();
        retry_draft(&service, &mut draft).unwrap();
        assert_eq!(service.poll().pending, 1);
        // Simulate a process stopping after outbox durability but before clearing draft metadata.
        service.shutdown();
        drop(service);
        let service = SyncService::new(config(), folder.join("service"), "vectorcraft").unwrap();
        let mut draft: LocalDraft = serde_json::from_slice(&std::fs::read(draft.path.with_extension("json")).unwrap()).unwrap();
        assert!(draft.needs_submit);
        retry_draft(&service, &mut draft).unwrap();
        assert_eq!(service.poll().pending, 1);
        drop(service);
        std::fs::remove_dir_all(folder).unwrap();
    }
    #[test]
    fn newer_raw_checkpoint_never_inherits_older_portable_or_submitted_metadata() {
        let folder = std::env::temp_dir().join(format!(
            "vector-cloud-versions-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let original = encode_native(&Document::new(100.0, 80.0)).unwrap();
        let mut old = save_local_at(&folder, "same-document", "Project", &original, 1).unwrap();
        old.portable = true;
        old.needs_submit = false;
        save_metadata(&old).unwrap();
        let newer = encode_native(&Document::new(240.0, 180.0)).unwrap();
        let mut latest = save_local_at(&folder, "same-document", "Project", &newer, 2).unwrap();
        latest.captured_at = 0; // A clock rollback must not make the older capture appear current.
        save_metadata(&latest).unwrap();
        assert_ne!(old.token, latest.token);
        assert_ne!(old.path, latest.path);
        let previous: LocalDraft = serde_json::from_slice(&std::fs::read(old.path.with_extension("json")).unwrap()).unwrap();
        let restored: LocalDraft = serde_json::from_slice(&std::fs::read(latest.path.with_extension("json")).unwrap()).unwrap();
        assert!(previous.portable);
        assert!(!previous.needs_submit);
        assert!(!restored.portable);
        assert!(restored.needs_submit);
        assert_eq!(std::fs::read(&previous.path).unwrap(), original);
        assert_eq!(std::fs::read(&restored.path).unwrap(), newer);
        let mut app = VectorcraftApp::new(vectorcraft_engine::Session::new(), crate::Services::default());
        app.cloud_sync.local = vec![previous, restored];
        frame(&mut app, &egui::Context::default(), 0.0);
        assert_eq!(app.cloud_sync.local.first().unwrap().path, latest.path);
        std::fs::remove_dir_all(folder).unwrap();
    }
    #[test]
    fn persisted_account_restores_nonsecret_fields_and_excludes_password() {
        let folder = std::env::temp_dir().join(format!(
            "vector-cloud-account-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&folder).unwrap();
        let config = Config {
            endpoint: "https://sync.example.test/personal/".into(),
            username: "test-user".into(),
            password: "never-persist-this-secret".into(),
        };
        save_account(&folder, &Account::from(&config)).unwrap();
        let bytes = std::fs::read(folder.join("account.json")).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value.get("password").is_none());
        assert!(!String::from_utf8(bytes).unwrap().contains(&config.password));
        let restored = read_account(&folder).unwrap();
        assert_eq!(restored.endpoint, config.endpoint);
        assert_eq!(restored.username, config.username);
        std::fs::remove_dir_all(folder).unwrap();
    }
    #[test]
    fn corrupt_cloud_queue_keeps_independent_native_checkpoint_and_existing_queue_bytes() {
        let folder = std::env::temp_dir().join(format!(
            "vector-cloud-corrupt-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let queue = folder.join("queue");
        std::fs::create_dir_all(&queue).unwrap();
        let corrupt = b"corrupt-existing-journal";
        std::fs::write(queue.join("journal.json"), corrupt).unwrap();
        assert!(
            SyncService::new(Config { endpoint: String::new(), username: String::new(), password: String::new() }, &queue, "vectorcraft").is_err()
        );
        let native = encode_native(&Document::new(100.0, 80.0)).unwrap();
        let local = save_local_at(&folder.join("drafts"), "independent-doc", "Project", &native, 1).unwrap();
        assert_eq!(std::fs::read(local.path).unwrap(), native);
        assert_eq!(std::fs::read(queue.join("journal.json")).unwrap(), corrupt);
        std::fs::remove_dir_all(folder).unwrap();
    }
    #[test]
    fn cloud_open_enforces_read_root_and_carries_scope_to_the_reader() {
        use vectorcraft_engine::file_access::{self, AutomationRoots};
        let folder = std::env::temp_dir().join(format!("vector-cloud-scope-{}", uuid::Uuid::new_v4()));
        let readable = folder.join("readable");
        let denied = folder.join("denied");
        std::fs::create_dir_all(&denied).unwrap();
        let draft = save_local_at(&readable, "scoped-doc", "Project", &encode_native(&Document::new(100.0, 80.0)).unwrap(), 1).unwrap();
        let (tx, rx) = mpsc::channel();
        let mut app = VectorcraftApp::new(vectorcraft_engine::Session::new(), crate::Services::default());
        app.cloud_sync = CloudSync { send: Some(tx), local: vec![draft.clone()], ..Default::default() };
        let rejected = AutomationRoots::new(Some(&denied), None).unwrap().unwrap();
        let error = file_access::confine(Some(&rejected), || app.run("file.cloudOpen", json!({"localToken":draft.token}))).unwrap_err();
        assert!(error.contains("outside the read root"));
        assert!(rx.try_recv().is_err());
        let allowed = AutomationRoots::new(Some(&readable), None).unwrap().unwrap();
        file_access::confine(Some(&allowed), || app.run("file.cloudOpen", json!({"localToken":draft.token}))).unwrap();
        let scope = match rx.try_recv().unwrap() {
            Work::Open(_, scope) => scope,
            _ => panic!("expected scoped open"),
        };
        let outside = denied.join("secret.vectorcraft");
        std::fs::write(&outside, b"private").unwrap();
        assert!(std::thread::spawn(move || scope.run(|| file_access::check_read(&outside.to_string_lossy()))).join().unwrap().is_err());
        std::fs::remove_dir_all(folder).unwrap();
    }
    #[test]
    fn cloud_open_waits_for_its_payload_and_reads_the_fetched_file() {
        use vectorcraft_engine::file_access::ScopeHandle;
        let folder = std::env::temp_dir().join(format!("vector-cloud-lazy-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&folder).unwrap();
        let revision = |version: u128| DownloadedRevision {
            project_id: uuid::Uuid::from_u128(1),
            version_id: uuid::Uuid::from_u128(version),
            parents: Vec::new(),
            heads: Vec::new(),
            title: "Doc".into(),
            extension: "vectorcraft".into(),
            local_path: folder.join(format!("payload-{version}")),
            created_ms: None,
            size: 0,
            preview_path: None,
            cached: false,
            pending: false,
        };
        let mut waiting: Vec<AwaitingOpen> =
            vec![(OpenSource::Cloud(revision(1)), ScopeHandle::capture()), (OpenSource::Cloud(revision(2)), ScopeHandle::capture())];
        assert_eq!(take_waiting(&mut waiting, uuid::Uuid::from_u128(1)).len(), 1);
        assert_eq!(waiting.len(), 1);
        assert!(take_waiting(&mut waiting, uuid::Uuid::from_u128(9)).is_empty());
        // Before the payload arrives the file is absent: an error, never a panic.
        assert!(read_source(&OpenSource::Cloud(revision(2)), ScopeHandle::capture()).is_err());
        std::fs::write(folder.join("payload-2"), encode_native(&Document::new(100.0, 80.0)).unwrap()).unwrap();
        assert!(read_source(&OpenSource::Cloud(revision(2)), ScopeHandle::capture()).is_ok());
        std::fs::remove_dir_all(folder).unwrap();
    }
    #[test]
    fn automatic_capture_never_reads_original_paths_from_preview_only_placed_links() {
        let _session = vectorcraft_engine::Session::new();
        let folder = std::env::temp_dir().join(format!("vector-cloud-link-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&folder).unwrap();
        let outside = folder.join("outside-original.vectorcraft");
        let mut secret = Document::new(300.0, 200.0);
        secret.title = "private outside source".into();
        let original = encode_native(&secret).unwrap();
        std::fs::write(&outside, &original).unwrap();
        let mut doc = Document::new(100.0, 80.0);
        let key = format!("uncached-{}", uuid::Uuid::new_v4());
        let preview = Arc::new(vec![1, 2, 3]);
        doc.images.insert(key.clone(), ImageBlob { mime: "application/json".into(), bytes: preview.clone(), proxy: Some(preview.clone()) });
        let placed = vectorcraft_doc::PlacedDocument {
            link: vectorcraft_doc::LinkInfo::new(outside.to_string_lossy()),
            key: key.clone(),
            bounding: false,
            width: 20.0,
            height: 10.0,
            xf: vectorcraft_geom::Affine::IDENTITY,
            placement: Default::default(),
        };
        assert!(vectorcraft_doc::placed_document::full_bytes_cached(&placed, doc.images.get(&key).unwrap()).is_none());
        let node = vectorcraft_doc::Node::new(doc.alloc_id(), vectorcraft_doc::NodeKind::PlacedDocument(Box::new(placed.clone())));
        doc.layers.push(Arc::new(node));
        let local = vectorcraft_format::load(&encode_native(&doc).unwrap()).unwrap();
        assert_eq!(local.images.get(&key).unwrap().bytes.as_ref(), preview.as_ref());
        assert!(portable(&doc, 0).is_err());
        assert!(vectorcraft_doc::placed_document::full_bytes_cached(&placed, doc.images.get(&key).unwrap()).is_none());
        // The source is genuinely readable through the normal reader; automatic capture's
        // restriction, rather than a missing fixture or reader, prevented the prior read.
        assert_eq!(vectorcraft_doc::placed_document::full_bytes(&placed, doc.images.get(&key).unwrap()).unwrap().as_ref(), &original);
        std::fs::remove_dir_all(folder).unwrap();
    }
    #[test]
    fn preview_only_source_is_rejected() {
        let mut doc = Document::new(100.0, 80.0);
        let bytes = Arc::new(vec![1, 2, 3]);
        doc.images.insert("preview".into(), ImageBlob { mime: "image/png".into(), bytes: bytes.clone(), proxy: Some(bytes) });
        // A referenced linked image is needed; an unused preview blob does not affect portability.
        let image = vectorcraft_doc::ImageObject {
            key: "preview".into(),
            width: 1,
            height: 1,
            xf: vectorcraft_geom::Affine::IDENTITY,
            link: Some(vectorcraft_doc::LinkInfo::new("missing.png")),
            placement: Default::default(),
        };
        let node = vectorcraft_doc::Node::new(doc.alloc_id(), vectorcraft_doc::NodeKind::Image(Box::new(image)));
        doc.layers.push(Arc::new(node));
        assert!(portable(&doc, 0).is_err());
        // Portability failure must not prevent a restart-readable local editing checkpoint.
        let folder = std::env::temp_dir().join(format!(
            "vector-cloud-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let local = save_local_at(&folder, "missing-original", "Missing original", &encode_native(&doc).unwrap(), 1).unwrap();
        let metadata: LocalDraft = serde_json::from_slice(&std::fs::read(local.path.with_extension("json")).unwrap()).unwrap();
        let restored = vectorcraft_format::load(&std::fs::read(metadata.path).unwrap()).unwrap();
        assert!(restored.images.contains_key("preview"));
        assert_eq!(restored.layers.len(), doc.layers.len());
        std::fs::remove_dir_all(folder).unwrap();
        let mut png = std::io::Cursor::new(Vec::new());
        image::RgbaImage::from_pixel(2, 2, image::Rgba([23, 42, 91, 128])).write_to(&mut png, image::ImageFormat::Png).unwrap();
        let original = png.into_inner();
        doc.images.insert("preview".into(), ImageBlob::new("image/png", original.clone()));
        let native = portable(&doc, 0).unwrap();
        let bytes =
            fileio::encode_all(&native, "vectorcraft", &json!({"version":4,"includeLinked":true,"embedProfiles":true})).unwrap().files.remove(0).1;
        let loaded = vectorcraft_format::load(&bytes).unwrap();
        assert_eq!(loaded.images.get("preview").unwrap().bytes.as_ref(), &original);
        let mut linked = false;
        loaded.visit_images(|_, image| {
            linked = image.link.is_some();
        });
        assert!(linked, "native checkpoint keeps editable linked-image metadata");
    }
}

/// Hold a requested native close until the latest captured revisions are durable locally.
/// The event loop keeps running while the worker flushes, so no disk/network waits block it.
pub fn prepare_close(app: &mut VectorcraftApp) -> bool {
    checkpoint_all(app);
    let ready = app.cloud_sync.tracks.values().all(|t| t.busy == 0);
    app.cloud_sync.closing = !ready;
    ready
}
/// Queue the latest revision before an individual tab is removed, even during the debounce.
pub fn checkpoint_document(app: &mut VectorcraftApp, index: usize) {
    let Some(st) = app.session.documents().get(index) else {
        return;
    };
    if app.cloud_sync.send.is_none() {
        return;
    }
    let track = app.cloud_sync.tracks.entry(st.uid).or_insert_with(|| Track {
        observed: st.doc.clone(),
        durable: None,
        busy: 0,
        queued: None,
        failed: None,
        changed: 0.0,
        last: 0.0,
        key: local_key(st.path.as_deref(), &app.cloud_sync.instance, st.uid),
    });
    track.observed = st.doc.clone();
    if track.durable.as_ref().is_none_or(|d| !Arc::ptr_eq(d, &st.doc))
        && track.queued.as_ref().is_none_or(|d| !Arc::ptr_eq(d, &st.doc))
        && track.failed.as_ref().is_none_or(|d| !Arc::ptr_eq(d, &st.doc))
    {
        track.busy += 1;
        track.queued = Some(st.doc.clone());
        let work = Work::Capture { uid: st.uid, key: track.key.clone(), title: st.title(), doc: st.doc.clone() };
        app.cloud_sync.send(work);
    }
}
pub fn checkpoint_all(app: &mut VectorcraftApp) {
    for index in 0..app.session.documents().len() {
        checkpoint_document(app, index);
    }
}

fn local_key(path: Option<&str>, instance: &str, uid: u64) -> String {
    use sha2::{Digest, Sha256};
    if let Some(path) = path { format!("file-{:x}", Sha256::digest(path.as_bytes())) } else { format!("untitled-{instance}-{uid}") }
}

fn save_local(key: &str, title: &str, native: &[u8], sequence: u64) -> Result<LocalDraft, String> {
    save_local_at(&cache()?.join("local-checkpoints"), key, title, native, sequence)
}
fn save_local_at(folder: &std::path::Path, key: &str, title: &str, native: &[u8], sequence: u64) -> Result<LocalDraft, String> {
    std::fs::create_dir_all(folder).map_err(|e| e.to_string())?;
    let captured_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|t| t.as_nanos()).unwrap_or_default();
    let token = uuid::Uuid::new_v4().to_string();
    let draft = LocalDraft {
        key: key.into(),
        title: title.into(),
        path: folder.join(format!("{token}.vectorcraft")),
        warning: Some("Waiting for portability check and publication".into()),
        token,
        needs_submit: true,
        portable: false,
        captured_at,
        sequence,
    };
    // The pending metadata is durable before the first payload exists. A crash cannot
    // pair new raw bytes with an older token's portable/already-submitted metadata.
    save_metadata(&draft)?;
    vectorcraft_format::write_atomic(&draft.path, native).map_err(|e| e.to_string())?;
    Ok(draft)
}

fn save_metadata(draft: &LocalDraft) -> Result<(), String> {
    vectorcraft_format::write_atomic(&draft.path.with_extension("json"), &serde_json::to_vec(draft).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())
}
fn encode_native(doc: &Document) -> Result<Vec<u8>, String> {
    without_source_reads(|| {
        fileio::encode_all(doc, "vectorcraft", &json!({"version":4,"includeLinked":true,"embedProfiles":true}))
            .map_err(|e| e.to_string())?
            .files
            .into_iter()
            .next()
            .map(|(_, bytes)| bytes)
            .ok_or_else(|| "No native checkpoint was encoded".into())
    })
}
fn without_source_reads<T>(work: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    // Existing access scopes require at least one root to be confined. This write-only
    // scope permits no file reads; encoding itself writes only to an in-memory buffer.
    let roots = vectorcraft_engine::file_access::AutomationRoots::new(None, Some(&std::env::temp_dir()))?;
    vectorcraft_engine::file_access::confine(roots.as_ref(), work)
}

fn pending_default() -> bool {
    true
}
fn retry_draft(service: &SyncService, draft: &mut LocalDraft) -> Result<(), String> {
    if draft.token.is_empty() {
        draft.token = format!(
            "legacy-{}-{}",
            local_key(Some(&draft.key), "", 0),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|t| t.as_nanos()).unwrap_or_default()
        );
        save_metadata(draft)?;
    }
    let bytes = std::fs::read(&draft.path).map_err(|e| e.to_string())?;
    let native = if draft.portable {
        bytes
    } else {
        let loaded = without_source_reads(|| fileio::load("checkpoint.vectorcraft", &bytes).map_err(|e| e.to_string()))?;
        let portable = portable(&loaded.doc, 0)?;
        let native = encode_native(&portable)?;
        vectorcraft_format::write_atomic(&draft.path, &native).map_err(|e| e.to_string())?;
        draft.portable = true;
        save_metadata(draft)?;
        native
    };
    service
        .submit(Snapshot {
            local_key: draft.key.clone(),
            title: draft.title.clone(),
            extension: "vectorcraft".into(),
            local_token: draft.token.clone(),
            native,
            preview: None,
        })
        .map_err(|e| e.to_string())?;
    draft.needs_submit = false;
    Ok(())
}

fn save_account(folder: &std::path::Path, account: &Account) -> Result<(), String> {
    vectorcraft_format::write_atomic(&folder.join("account.json"), &serde_json::to_vec(account).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())
}
fn read_account(folder: &std::path::Path) -> Result<Account, String> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(folder.join("account.json")).map_err(|e| e.to_string())?.take(65537).read_to_end(&mut bytes).map_err(|e| e.to_string())?;
    if bytes.len() > 65536 {
        return Err("Cloud account settings exceed the size limit".into());
    }
    serde_json::from_slice(&bytes).map_err(|e| format!("Could not restore cloud account settings: {e}"))
}

fn replace_service(service: &mut Option<SyncService>, config: Config) -> Result<(), String> {
    if let Some(mut previous) = service.take() {
        previous.shutdown();
    }
    match cache().and_then(|p| SyncService::new(config, p, "vectorcraft").map_err(|e| e.to_string())) {
        Ok(next) => {
            *service = Some(next);
            Ok(())
        }
        Err(error) => {
            // Invalid or changed account settings never disable durable local submissions.
            if let Ok(fallback) = cache().and_then(|p| {
                SyncService::new(Config { endpoint: String::new(), username: String::new(), password: String::new() }, p, "vectorcraft")
                    .map_err(|e| e.to_string())
            }) {
                *service = Some(fallback);
            }
            Err(error)
        }
    }
}

fn connect_config(cloud: &mut CloudSync, config: Config) -> Result<(), String> {
    let send = cloud.send.as_ref().ok_or("Cloud worker is not available")?;
    cloud.endpoint = config.endpoint.clone();
    cloud.username = config.username.clone();
    send.send(Work::Connect(config)).map_err(|_| "Cloud worker stopped".to_owned())?;
    cloud.error = None;
    cloud.network_error = None;
    Ok(())
}
/// Open a catalog revision (its payload downloads on demand) or a local checkpoint.
pub fn open_command(cloud: &mut CloudSync, params: &serde_json::Value) -> Result<serde_json::Value, String> {
    let source = if let Some(version) = params.get("version").and_then(serde_json::Value::as_str) {
        let id = uuid::Uuid::parse_str(version).map_err(|_| "invalid revision id")?;
        cloud.state.catalog.iter().find(|r| r.version_id == id && r.extension == "vectorcraft").cloned().map(OpenSource::Cloud)
    } else if let Some(token) = params.get("localToken").and_then(serde_json::Value::as_str) {
        cloud.local.iter().find(|d| d.token == token).cloned().map(OpenSource::Local)
    } else {
        None
    }
    .ok_or("No such verified cloud revision or local checkpoint")?;
    let path = match &source {
        OpenSource::Cloud(r) => &r.local_path,
        OpenSource::Local(d) => &d.path,
    };
    vectorcraft_engine::file_access::check_read(&path.to_string_lossy())?;
    cloud
        .send
        .as_ref()
        .ok_or("Cloud worker is not available")?
        .send(Work::Open(source, vectorcraft_engine::file_access::ScopeHandle::capture()))
        .map_err(|_| "Cloud worker stopped")?;
    Ok(json!({"pending":true}))
}
