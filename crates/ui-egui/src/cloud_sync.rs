//! Native-only, local-first checkpoints. The worker owns all encoding, filesystem and network work.
//! A checkpoint never calls SaveJob::finish or changes the editor's manual-save/recovery state.
use crate::VectorcraftApp;
use crate::cloud_home::{OpenDoc, Recents};
use craft_cloud_sync::{Config, DownloadedRevision, Snapshot, SyncEvent, SyncService, SyncState, SyncStatus};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        Arc,
        mpsc::{self, Receiver, Sender},
    },
    time::{Duration, Instant},
};
use uuid::Uuid;
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
    Capture {
        uid: u64,
        key: String,
        title: String,
        doc: Arc<Document>,
    },
    Open(OpenSource, vectorcraft_engine::file_access::ScopeHandle),
    Bind {
        key: String,
        revision: DownloadedRevision,
    },
    /// Download a revision's payload (a rename or a copy needs it on this computer).
    Request(Uuid),
    /// A new revision of `project` with another name; the payload of `version` must be cached.
    Rename {
        operation: Uuid,
        project: Uuid,
        version: Uuid,
        title: String,
    },
    /// The revisions open in the editor, which the client never evicts.
    Pin(BTreeSet<Uuid>),
    CacheLimit(u64),
    /// Sync again now (the Retry button).
    Sync,
    Recents(Recents),
    /// The local keys of the documents open in the editor (their drafts are never cleaned up).
    OpenKeys(BTreeSet<String>),
    /// Save a copy of a cached payload to a file the user picked.
    Copy {
        operation: Uuid,
        source: PathBuf,
        target: String,
        write: Option<crate::background::SharedWriteFn>,
    },
    /// Drain every command before this one, stop the service and acknowledge it.
    Shutdown(Sender<()>),
}
enum Done {
    State(SyncState),
    Captured(u64, Arc<Document>, Result<Option<String>, String>),
    Opened(OpenSource, Result<Box<ReadFile>, String>),
    Local(LocalDraft),
    Account(Account),
    CredentialNotice(Option<String>),
    Error(String),
    /// A checkpoint of the document with this local key became revision `version` of `project`.
    Submitted {
        key: String,
        project: Uuid,
        version: Uuid,
    },
    Renamed {
        operation: Uuid,
        old: Uuid,
        title: String,
        result: Result<Uuid, String>,
    },
    Copied {
        operation: Uuid,
        target: String,
        result: Result<(), String>,
    },
    /// A payload could not be requested at all (no sync service).
    FetchFailed {
        version: Uuid,
        error: String,
    },
    Recents(Recents),
    /// A local draft that was safe in the cloud was cleaned up.
    Dropped(PathBuf),
}

#[derive(Clone)]
enum Operation {
    Pending { kind: String },
    Complete { kind: String, result: Value },
    Failed { kind: String, error: String },
}

const MAX_PENDING_OPERATIONS: usize = 64;
const MAX_OPERATION_HISTORY: usize = 256;
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Account {
    endpoint: String,
    username: String,
    /// The local cache limit in bytes, once the user chose one (never the password).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache_limit: Option<u64>,
}
impl From<&Config> for Account {
    fn from(config: &Config) -> Self {
        Self { endpoint: config.endpoint.clone(), username: config.username.clone(), cache_limit: None }
    }
}
impl Account {
    /// The limit to apply: the saved one when it is a sensible size, else the default.
    fn limit(&self) -> u64 {
        const MIN: u64 = 64 << 20;
        const MAX: u64 = 1 << 40;
        self.cache_limit.filter(|b| (MIN..=MAX).contains(b)).unwrap_or(crate::cloud_home::DEFAULT_CACHE_LIMIT)
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
    worker: Option<std::thread::JoinHandle<()>>,
    tracks: BTreeMap<u64, Track>,
    pub(crate) state: SyncState,
    error: Option<String>,
    network_error: Option<String>,
    credential_notice: Option<String>,
    instance: String,
    closing: bool,
    local: Vec<LocalDraft>,
    /// What the Home library keeps between frames.
    pub(crate) home: crate::cloud_home::HomeState,
    /// Documents created from Home or opened from the cloud (and no file path): their checkpoint is
    /// their copy, so closing one that is durable needs no Save As.
    managed: BTreeSet<u64>,
    /// The next new document comes from Home (its New button, a preset, `file.cloudNew`).
    pub(crate) new_from_home: bool,
    /// Local key → the (project, revision) its latest checkpoint is, as far as this session knows.
    bindings: BTreeMap<String, (Uuid, Uuid)>,
    /// The revisions last pinned in the client, and the local keys last reported open.
    pinned: Option<BTreeSet<Uuid>>,
    open_keys: Option<BTreeSet<String>>,
    /// The worker has reported at least once; a listing has completed since.
    got_state: bool,
    listed: bool,
    /// Tabs and Close All / Quit waiting for a checkpoint to be durable before they close.
    closing_tabs: BTreeMap<u64, Instant>,
    closing_then: Option<(String, Instant)>,
    /// Closing has waited long enough: it asks what it always asked instead of waiting again.
    no_hold: bool,
    /// Rename/save-copy operations exposed to automation until their worker answer arrives.
    operations: BTreeMap<Uuid, Operation>,
    /// A native close waits for captures and operations, but never without a deadline.
    closing_started: Option<Instant>,
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
    /// The worker runs and has reported once.
    pub(crate) fn worker_ready(&self) -> bool {
        self.send.is_some() && self.got_state
    }
    /// Home has nothing to show yet: the worker is starting, or the first listing is still running.
    pub(crate) fn loading(&self) -> bool {
        self.error.is_none() && (!self.worker_ready() || (!self.listed && self.state.catalog.is_empty()))
    }
    /// An account is set up (its endpoint is known).
    pub(crate) fn connected(&self) -> bool {
        !self.endpoint.is_empty()
    }
    /// What went wrong, for Home's error banner (first match).
    pub(crate) fn banner(&self) -> Option<String> {
        self.error
            .clone()
            .or_else(|| if let SyncStatus::Error(e) = &self.state.status { Some(e.clone()) } else { None })
            .or_else(|| self.network_error.clone())
            .or_else(|| self.local.iter().find_map(|d| d.warning.clone()))
    }
    /// A note about the stored credential (session-only connection, none saved).
    pub(crate) fn notice(&self) -> Option<&str> {
        self.credential_notice.as_deref()
    }
    /// The Retry button: forget the errors shown and sync again.
    pub(crate) fn retry_sync(&mut self) {
        self.error = None;
        self.network_error = None;
        self.send(Work::Sync);
    }
    /// Note that `project` was opened now, and keep the note.
    pub(crate) fn touch_recent(&mut self, project: Uuid) {
        self.home.recents.touch(project, crate::cloud_home::wall_ms());
        self.send(Work::Recents(self.home.recents.clone()));
    }
    pub(crate) fn rename_revision(&self, project: Uuid, version: Uuid, title: String) {
        self.send(Work::Rename { project, version, title });
    }
    /// Ask for the payload of `version` to be downloaded. The answer comes back as a fetch event.
    pub(crate) fn request_payload(&self, version: Uuid) {
        if self.send.is_some() {
            self.send(Work::Request(version));
        }
    }
    pub(crate) fn copy_payload(&self, source: PathBuf, target: String, write: Option<crate::background::SharedWriteFn>) {
        self.send(Work::Copy { source, target, write });
    }
    pub(crate) fn set_cache_limit(&self, bytes: u64) {
        self.send(Work::CacheLimit(bytes));
    }
    /// The cache limit in force, as the client reports it.
    pub(crate) fn cache_limit(&self) -> u64 {
        self.state.cache_limit
    }
    /// Make `uid` (an open document) the checkpoint of `key`, bound to a revision, as opening it would.
    #[cfg(test)]
    pub(crate) fn test_bind(&mut self, st: &vectorcraft_engine::DocState, key: &str, project: Uuid, version: Uuid) {
        self.tracks.insert(
            st.uid,
            Track {
                observed: st.doc.clone(),
                durable: Some(st.doc.clone()),
                busy: 0,
                queued: None,
                failed: None,
                changed: 0.0,
                last: 0.0,
                key: key.into(),
            },
        );
        self.bindings.insert(key.into(), (project, version));
        self.managed.insert(st.uid);
    }
}
/// A unit test that runs the worker points it at a folder of its own: the process environment cannot be changed safely.
#[cfg(test)]
static TEST_CACHE: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);
fn cache() -> Result<PathBuf, String> {
    #[cfg(test)]
    if let Some(path) = TEST_CACHE.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone() {
        return Ok(path);
    }
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
    // Resolve the adapter folder before the thread starts. Besides making startup deterministic, this
    // keeps every operation in one folder if the process environment changes while the worker lives.
    let cache_path = cache();
    let spawned = std::thread::Builder::new().name("vectorcraft-cloud-checkpoints".into()).spawn(move || {
        let cache = || cache_path.clone();
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
        // The account as saved. `account_saved` is false when the file exists but could not be read:
        // it is then left alone, whatever the user changes meanwhile.
        let mut account = Account { endpoint: String::new(), username: String::new(), cache_limit: None };
        let mut account_saved = true;
        if let Ok(path) = cache()
            && path.join("account.json").exists()
        {
            match read_account(&path) {
                Ok(read) => {
                    account = read;
                    let _ = done.send(Done::Account(account.clone()));
                    if !account.endpoint.is_empty() {
                        match craft_cloud_sync::credentials::load_credential("vectorcraft", &account.endpoint, &account.username) {
                            Ok(Some(password)) => {
                                if let Err(e) = replace_service(
                                    &mut service,
                                    Config { endpoint: account.endpoint.clone(), username: account.username.clone(), password },
                                ) {
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
                    account_saved = false;
                    let _ = done.send(Done::Error(e));
                }
            }
        }
        // The cache limit and what is pinned are the client's to hold, so they are set again whenever the
        // service is replaced.
        let mut cache_limit = account.limit();
        let mut pinned: BTreeSet<Uuid> = BTreeSet::new();
        let configure = |service: &Option<SyncService>, cache_limit: u64, pinned: &BTreeSet<Uuid>| {
            if let Some(s) = service {
                s.set_cache_limit(cache_limit);
                s.set_pinned(pinned.clone());
            }
        };
        configure(&service, cache_limit, &pinned);
        if let Ok(path) = cache() {
            let _ = done.send(Done::Recents(crate::cloud_home::read_recents(&path)));
        }
        let previews = crate::cloud_preview::PreviewWorker::spawn();
        // Tokens the client reported published, and the documents open in the editor (unknown until it says).
        let mut published: BTreeSet<String> = BTreeSet::new();
        let mut open_keys: Option<BTreeSet<String>> = None;
        let mut cleanup = false;
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
                    let mut next = Account::from(&config);
                    next.cache_limit = Some(cache_limit);
                    let password = config.password.clone();
                    match replace_service(&mut service, config) {
                        Ok(()) => {
                            if let Err(e) = cache().and_then(|p| save_account(&p, &next)) {
                                let _ = done.send(Done::Error(e));
                            }
                            let notice = if next.endpoint.is_empty() {
                                None
                            } else {
                                craft_cloud_sync::credentials::save_credential("vectorcraft", &next.endpoint, &next.username, &password)
                                    .err()
                                    .map(|e| format!("{e}. This connection is session only; reconnect after restarting."))
                            };
                            let _ = done.send(Done::CredentialNotice(notice));
                            let _ = done.send(Done::Account(next.clone()));
                            account = next;
                            account_saved = true;
                        }
                        Err(e) => {
                            let _ = done.send(Done::Error(e));
                        }
                    }
                    configure(&service, cache_limit, &pinned);
                }
                Ok(Work::Capture { uid, key, title, doc }) => {
                    // The preview renders beside the encoding and the disk writes; it is waited for only until its
                    // budget is spent, and a checkpoint never fails or waits for lack of one.
                    let started = Instant::now();
                    let preview = previews.as_ref().and_then(|p| p.request(doc.clone()));
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
                            let preview =
                                crate::cloud_preview::collect(preview, crate::cloud_preview::remaining(started, crate::cloud_preview::BUDGET));
                            let submitted = service
                                .as_ref()
                                .ok_or("Cloud outbox is unavailable; local checkpoints remain available")?
                                .submit(Snapshot {
                                    local_key: key.clone(),
                                    title,
                                    extension: "vectorcraft".into(),
                                    local_token: draft.token.clone(),
                                    native,
                                    preview,
                                })
                                .map_err(|e| e.to_string())?;
                            let _ = done.send(Done::Submitted { key, project: submitted.project_id, version: submitted.version_id });
                            Ok(())
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
                Ok(Work::Request(version)) => match service.as_ref() {
                    Some(s) => s.request_payload(version),
                    None => {
                        let _ = done.send(Done::FetchFailed { version, error: "Cloud outbox is unavailable".into() });
                    }
                },
                Ok(Work::Rename { project, version, title }) => {
                    let result = service
                        .as_ref()
                        .ok_or_else(|| "Cloud outbox is unavailable".to_owned())
                        .and_then(|s| s.rename(project, version, &title).map(|r| r.version_id).map_err(|e| e.to_string()));
                    let _ = done.send(Done::Renamed { old: version, title, result });
                }
                Ok(Work::Pin(set)) => {
                    pinned = set;
                    configure(&service, cache_limit, &pinned);
                }
                Ok(Work::CacheLimit(bytes)) => {
                    cache_limit = bytes;
                    if let Some(s) = &service {
                        s.set_cache_limit(bytes);
                    }
                    account.cache_limit = Some(bytes);
                    if account_saved && let Err(e) = cache().and_then(|p| save_account(&p, &account)) {
                        let _ = done.send(Done::Error(e));
                    }
                }
                Ok(Work::Sync) => {
                    if let Some(s) = &service {
                        s.request_sync();
                    }
                }
                Ok(Work::Recents(recents)) => {
                    // Recents are a convenience: failing to keep them is not worth an error.
                    let _ = cache().and_then(|p| crate::cloud_home::write_recents(&p, &recents));
                }
                Ok(Work::OpenKeys(keys)) => {
                    open_keys = Some(keys);
                    cleanup = true;
                }
                Ok(Work::Copy { source, target, write }) => {
                    let result = copy_payload(&source, &target, write.as_ref());
                    let _ = done.send(Done::Copied { target, result });
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
                    SyncEvent::Published { local_token, .. } => {
                        published.insert(local_token.clone());
                        cleanup = true;
                    }
                    _ => {}
                }
            }
            if std::mem::take(&mut cleanup)
                && let Ok(folder) = cache().map(|p| p.join("local-checkpoints"))
            {
                for path in prune_drafts(&mut drafts, &folder, &published, open_keys.as_ref()) {
                    let _ = done.send(Done::Dropped(path));
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
/// Copy a project's payload to the file the user picked: through the host's writer when it has one, else atomically.
fn copy_payload(source: &std::path::Path, target: &str, write: Option<&crate::background::SharedWriteFn>) -> Result<(), String> {
    use std::io::Read;
    // A payload is at most 256 MiB (the shared client's limit).
    const MAX: u64 = 256 << 20;
    let mut bytes = Vec::new();
    std::fs::File::open(source).and_then(|f| f.take(MAX + 1).read_to_end(&mut bytes)).map_err(|e| format!("Cannot read the cached project: {e}"))?;
    if bytes.len() as u64 > MAX {
        return Err("The cached project is larger than the size limit".into());
    }
    match write {
        Some(write) => write(target, &bytes),
        None => vectorcraft_format::write_atomic(std::path::Path::new(target), &bytes).map_err(|e| e.to_string()),
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
                    match event {
                        SyncEvent::Error(e) => app.cloud_sync.network_error = Some(e.clone()),
                        SyncEvent::Fetched { version_id, path, .. } => crate::cloud_home::payload_arrived(app, *version_id, path.clone()),
                        SyncEvent::FetchFailed { version_id, error } => crate::cloud_home::payload_failed(app, *version_id, error),
                        _ => {}
                    }
                }
                if matches!(state.status, SyncStatus::Idle) {
                    app.cloud_sync.network_error = None;
                }
                // A listing is done once the client is no longer fetching one (offline and failed ones count: the
                // catalog on this computer is all there is).
                app.cloud_sync.got_state = true;
                app.cloud_sync.listed |= !matches!(state.status, SyncStatus::Syncing);
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
            Done::Submitted { key, project, version } => {
                let first = !app.cloud_sync.bindings.contains_key(&key);
                app.cloud_sync.bindings.insert(key.clone(), (project, version));
                // A project made from Home counts as opened once it exists.
                let from_home = app.cloud_sync.tracks.iter().any(|(uid, t)| t.key == key && app.cloud_sync.managed.contains(uid));
                if first && from_home {
                    app.cloud_sync.touch_recent(project);
                }
            }
            Done::Renamed { old, title, result } => match result {
                Ok(version) => {
                    for (_, v) in app.cloud_sync.bindings.values_mut().filter(|(_, v)| *v == old) {
                        *v = version;
                    }
                    app.cloud_sync.home.errors.remove(&old);
                }
                Err(e) => app.cloud_sync.home.fail(old, e, crate::cloud_home::Retry::Rename(title)),
            },
            Done::Copied { target, result } => match result {
                Ok(()) => app.status(format!("Saved a copy to {target}")),
                Err(e) => app.status(format!("Can't save a copy to {target}: {e}")),
            },
            Done::FetchFailed { version, error } => crate::cloud_home::payload_failed(app, version, &error),
            Done::Recents(stored) => {
                app.cloud_sync.home.recents.merge(stored);
            }
            Done::Dropped(path) => app.cloud_sync.local.retain(|d| d.path != path),
            Done::Opened(revision, result) => match result.and_then(|read| fileio::open_read(&mut app.session, *read).map_err(|e| e.to_string())) {
                Ok(value) => {
                    crate::io::opened(app, &value);
                    if let OpenSource::Cloud(r) = &revision {
                        app.cloud_sync.home.opening.remove(&r.version_id);
                        app.cloud_sync.home.errors.remove(&r.version_id);
                        // A recent is a project that actually opened, rather than one whose download was
                        // merely requested (and may have failed).
                        app.cloud_sync.touch_recent(r.project_id);
                    }
                    if let Ok(st) = app.session.doc() {
                        let uid = st.uid;
                        let doc = st.doc.clone();
                        let key = match revision {
                            OpenSource::Cloud(revision) => {
                                let key = format!("cloud-{}-{}", revision.project_id, uid);
                                app.cloud_sync.bindings.insert(key.clone(), (revision.project_id, revision.version_id));
                                app.cloud_sync.send(Work::Bind { key: key.clone(), revision });
                                key
                            }
                            OpenSource::Local(draft) => draft.key,
                        };
                        // Its checkpoint is its copy: no file, so closing it needs no Save As once it is durable.
                        app.cloud_sync.managed.insert(uid);
                        app.cloud_sync.tracks.insert(
                            uid,
                            Track { observed: doc.clone(), durable: Some(doc), busy: 0, queued: None, failed: None, changed: now, last: now, key },
                        );
                    }
                }
                Err(e) => match revision {
                    OpenSource::Cloud(r) => app.cloud_sync.home.fail(r.version_id, e, crate::cloud_home::Retry::Open),
                    OpenSource::Local(_) => app.cloud_sync.error = Some(e),
                },
            },
        }
    }
    app.cloud_sync
        .local
        .sort_by(|a, b| b.sequence.cmp(&a.sequence).then_with(|| b.captured_at.cmp(&a.captured_at)).then_with(|| b.path.cmp(&a.path)));
    // The next new document is Home's when it was asked for there; the ask lapses with its dialog.
    let from_home = app.cloud_sync.new_from_home;
    let mut claimed = false;
    for st in app.session.documents() {
        let fresh = !app.cloud_sync.tracks.contains_key(&st.uid);
        if fresh && from_home && !claimed && st.path.is_none() {
            app.cloud_sync.managed.insert(st.uid);
            claimed = true;
        }
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
    if claimed || (from_home && app.ui.dialog.as_ref().is_none_or(|d| d.kind != "newDocument")) {
        app.cloud_sync.new_from_home = false;
    }
    app.cloud_sync.tracks.retain(|uid, track| track.busy > 0 || app.session.documents().iter().any(|st| st.uid == *uid));
    let open: BTreeSet<u64> = app.session.documents().iter().map(|st| st.uid).collect();
    app.cloud_sync.managed.retain(|uid| open.contains(uid));
    let keys: BTreeSet<&String> = app.cloud_sync.tracks.values().map(|t| &t.key).collect();
    app.cloud_sync.bindings.retain(|key, _| keys.contains(key));
    pin_open_revisions(app);
    finish_closing(app);
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
    window(app, ctx);
}

/// Tell the worker what is open, whenever that changes: the revisions the open documents are on (the client
/// never evicts them) and their local keys (their drafts are never cleaned up).
fn pin_open_revisions(app: &mut VectorcraftApp) {
    if app.cloud_sync.send.is_none() {
        return;
    }
    let set = crate::cloud_home::pinned_set(&open_docs(app));
    if app.cloud_sync.pinned.as_ref() != Some(&set) {
        app.cloud_sync.send(Work::Pin(set.clone()));
        app.cloud_sync.pinned = Some(set);
    }
    let keys: BTreeSet<String> = app.session.documents().iter().filter_map(|st| app.cloud_sync.tracks.get(&st.uid)).map(|t| t.key.clone()).collect();
    if app.cloud_sync.open_keys.as_ref() != Some(&keys) {
        app.cloud_sync.send(Work::OpenKeys(keys.clone()));
        app.cloud_sync.open_keys = Some(keys);
    }
}

/// The editor's documents and the cloud revisions their checkpoints are bound to.
pub fn open_docs(app: &VectorcraftApp) -> Vec<OpenDoc> {
    app.session
        .documents()
        .iter()
        .map(|st| {
            let bound = app.cloud_sync.tracks.get(&st.uid).and_then(|t| app.cloud_sync.bindings.get(&t.key));
            OpenDoc { uid: st.uid, project: bound.map(|(p, _)| *p), version: bound.map(|(_, v)| *v) }
        })
        .collect()
}

/// Open the project of `rev`: its payload downloads first when it is not on this computer.
pub fn open_revision(app: &mut VectorcraftApp, rev: DownloadedRevision) -> Result<(), String> {
    vectorcraft_engine::file_access::check_read(&rev.local_path.to_string_lossy())?;
    let tx = app.cloud_sync.send.as_ref().ok_or("Cloud worker is not available")?;
    let version = rev.version_id;
    tx.send(Work::Open(OpenSource::Cloud(rev), vectorcraft_engine::file_access::ScopeHandle::capture())).map_err(|_| "Cloud worker stopped")?;
    app.cloud_sync.home.opening.insert(version);
    Ok(())
}

/// Whether the document at `st` needs no Save As: it is Home's (no file, created there or opened from the
/// cloud) and its latest revision is already in the local checkpoint.
pub fn covers(app: &VectorcraftApp, st: &vectorcraft_engine::DocState) -> bool {
    st.path.is_none()
        && app.cloud_sync.managed.contains(&st.uid)
        && app.cloud_sync.tracks.get(&st.uid).is_some_and(|t| t.durable.as_ref().is_some_and(|d| Arc::ptr_eq(d, &st.doc)))
}

/// A document of Home's with changes whose checkpoint is still on its way (and has not failed).
fn waiting_for_checkpoint(app: &VectorcraftApp, st: &vectorcraft_engine::DocState) -> bool {
    st.is_dirty()
        && st.path.is_none()
        && app.cloud_sync.managed.contains(&st.uid)
        && app.cloud_sync.send.is_some()
        && !covers(app, st)
        && app.cloud_sync.tracks.get(&st.uid).is_some_and(|t| t.failed.as_ref().is_none_or(|f| !Arc::ptr_eq(f, &st.doc)))
}

/// How long closing waits for a checkpoint before it asks the question it always asked.
const CLOSE_PATIENCE: Duration = Duration::from_secs(15);

/// What closing the tab at `index` does.
pub enum CloseDecision {
    /// Its latest revision is durable: close it, no question.
    Close,
    /// Its checkpoint is queued: the tab closes (or asks) when the checkpoint answers.
    Wait,
    /// Not Home's, or the checkpoint cannot be made: the usual Save / Don't Save / Cancel.
    Ask,
}

pub fn close_decision(app: &mut VectorcraftApp, index: usize) -> CloseDecision {
    let Some(st) = app.session.documents().get(index) else { return CloseDecision::Ask };
    let uid = st.uid;
    if covers(app, st) {
        return CloseDecision::Close;
    }
    if !waiting_for_checkpoint(app, st) {
        return CloseDecision::Ask;
    }
    checkpoint_document(app, index);
    app.cloud_sync.closing_tabs.entry(uid).or_insert_with(Instant::now);
    CloseDecision::Wait
}

/// Close All / Quit with documents of Home's whose changes are not durable yet: queue their checkpoints and
/// carry on when they answer. Whether it is waiting.
pub fn hold_close_all(app: &mut VectorcraftApp, then: &str) -> bool {
    if app.cloud_sync.no_hold || !app.session.documents().iter().any(|st| waiting_for_checkpoint(app, st)) {
        return false;
    }
    checkpoint_all(app);
    app.cloud_sync.closing_then.get_or_insert_with(|| (then.to_owned(), Instant::now()));
    true
}

/// Close what was waiting for its checkpoint once it is durable, ask when it could not be made.
fn finish_closing(app: &mut VectorcraftApp) {
    let tabs: Vec<(u64, Instant)> = app.cloud_sync.closing_tabs.iter().map(|(u, at)| (*u, *at)).collect();
    for (uid, since) in tabs {
        let Some(index) = app.session.documents().iter().position(|st| st.uid == uid) else {
            app.cloud_sync.closing_tabs.remove(&uid);
            continue;
        };
        let covered = app.session.documents().get(index).is_some_and(|st| covers(app, st));
        let waiting = app.session.documents().get(index).is_some_and(|st| waiting_for_checkpoint(app, st));
        if covered {
            app.cloud_sync.closing_tabs.remove(&uid);
            if let Err(e) = crate::unsaved::close_now(app, index) {
                app.status(e);
            }
        } else if !waiting || since.elapsed() > CLOSE_PATIENCE {
            // The checkpoint failed, or took too long: the question the document always asked.
            app.cloud_sync.closing_tabs.remove(&uid);
            if let Err(e) = crate::unsaved::ask(app, index, "close") {
                app.status(e);
            }
        } else {
            // Changed again meanwhile: queue the newer revision too.
            checkpoint_document(app, index);
        }
    }
    if let Some((then, since)) = app.cloud_sync.closing_then.clone() {
        checkpoint_all(app);
        let waiting = app.session.documents().iter().any(|st| waiting_for_checkpoint(app, st));
        if !waiting || since.elapsed() > CLOSE_PATIENCE || app.cloud_sync.send.is_none() {
            app.cloud_sync.closing_then = None;
            app.cloud_sync.no_hold = true;
            let closed = crate::unsaved::close_all(app, &then);
            app.cloud_sync.no_hold = false;
            if let Err(e) = closed {
                app.status(e);
            }
        }
    }
}

/// The Account dialog: connection, local cache limit, and the recovery lists. Projects are in Home.
fn window(app: &mut VectorcraftApp, ctx: &egui::Context) {
    if !app.cloud_sync.visible {
        return;
    }
    let mut visible = true;
    let mut home = false;
    let mut limit = None;
    let mut open = None;
    {
        let cloud = &mut app.cloud_sync;
        egui::Window::new("Cloud Account").open(&mut visible).default_width(440.0).show(ctx, |ui| {
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
            egui::CollapsingHeader::new("Connection").default_open(true).show(ui, |ui| {
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
            ui.label("Local cache limit");
            ui.horizontal(|ui| {
                let current = cloud.cache_limit();
                let shown = if crate::cloud_home::CACHE_PRESETS.contains(&current) {
                    crate::cloud_home::human_bytes(current)
                } else {
                    format!("Custom ({})", crate::cloud_home::human_bytes(current))
                };
                egui::ComboBox::from_id_salt("cloud-cache-limit").selected_text(shown).show_ui(ui, |ui| {
                    for preset in crate::cloud_home::CACHE_PRESETS {
                        if ui.selectable_label(preset == current, crate::cloud_home::human_bytes(preset)).clicked() {
                            limit = Some(preset);
                        }
                    }
                });
                ui.label(format!("{} used", crate::cloud_home::human_bytes(cloud.state.cache_bytes)));
            });
            ui.small("Open projects, work not yet uploaded and conflict copies are never removed to make room.");
            ui.separator();
            if ui.button("Back to projects").clicked() {
                home = true;
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
        });
    }
    if let Some(bytes) = limit {
        app.cloud_sync.set_cache_limit(bytes);
    }
    if let Some(revision) = open {
        match revision {
            OpenSource::Cloud(rev) => {
                // That exact revision, even when its project is open at a later one.
                if let Err(e) = open_revision(app, rev) {
                    app.status(e);
                }
            }
            local => app.cloud_sync.send(Work::Open(local, vectorcraft_engine::file_access::ScopeHandle::capture())),
        }
    }
    if home {
        app.ui.home = Some(crate::menus::home_key(app));
        visible = false;
    }
    app.cloud_sync.visible = visible;
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

    // --- Home ---------------------------------------------------------------------------------

    fn temp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("vector-cloud-{name}-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// An app whose worker channel is in the test's hands: what the UI asks of the worker, and a way to answer.
    fn app_with_worker() -> (VectorcraftApp, Receiver<Work>, Sender<Done>) {
        let mut app = VectorcraftApp::new(vectorcraft_engine::Session::new(), crate::Services::default());
        let (tx, work) = mpsc::channel();
        let (done, rx) = mpsc::channel();
        app.cloud_sync.send = Some(tx);
        app.cloud_sync.recv = Some(rx);
        (app, work, done)
    }

    fn queued(work: &Receiver<Work>) -> Vec<Work> {
        work.try_iter().collect()
    }

    fn captures(work: &[Work]) -> Vec<Arc<Document>> {
        work.iter().filter_map(|w| if let Work::Capture { doc, .. } = w { Some(doc.clone()) } else { None }).collect()
    }

    fn rectangle(app: &mut VectorcraftApp) {
        app.run("shape.rectangle", json!({"x":0,"y":0,"width":10,"height":10})).unwrap();
    }

    fn quit_pending(app: &VectorcraftApp) -> bool {
        app.ui.dialog.as_ref().is_some_and(|d| d.kind == crate::unsaved::KIND)
    }

    /// Frames until `done` stops changing the number of documents (the answer to a checkpoint arrives in one).
    fn frame_at(app: &mut VectorcraftApp, at: f64) {
        frame(app, &egui::Context::default(), at);
    }

    #[test]
    fn a_project_made_in_home_closes_without_the_question_once_its_checkpoint_is_durable() {
        let (mut app, work, done) = app_with_worker();
        app.run("file.cloudNew", json!({"width":100,"height":80})).unwrap();
        frame_at(&mut app, 0.0);
        let uid = app.session.active().map(|d| d.uid).unwrap();
        assert!(app.cloud_sync.managed.contains(&uid), "made in Home");
        assert!(!app.cloud_sync.new_from_home, "the ask is spent");
        rectangle(&mut app);
        assert!(app.session.active().is_some_and(|d| d.is_dirty() && d.path.is_none()));
        // The checkpoint is not durable yet: the tab waits for it instead of asking for a file.
        assert_eq!(crate::unsaved::close(&mut app, 0).unwrap(), json!({"pending":"cloudCheckpoint"}));
        assert_eq!((app.session.documents().len(), app.ui.dialog.is_none()), (1, true));
        let latest = app.session.active().map(|d| d.doc.clone()).unwrap();
        let asked = captures(&queued(&work));
        assert!(asked.last().is_some_and(|d| Arc::ptr_eq(d, &latest)), "the latest revision was queued");
        // The worker answers: the revision is in the local draft, and the tab closes with no question.
        done.send(Done::Captured(uid, latest, Ok(None))).unwrap();
        frame_at(&mut app, 0.1);
        assert!(app.session.documents().is_empty());
        assert!(app.ui.dialog.is_none(), "no Save As, no dialog");
    }

    #[test]
    fn a_checkpoint_that_fails_falls_back_to_the_usual_question() {
        let (mut app, work, done) = app_with_worker();
        app.run("file.cloudNew", json!({"width":100,"height":80})).unwrap();
        frame_at(&mut app, 0.0);
        let uid = app.session.active().map(|d| d.uid).unwrap();
        rectangle(&mut app);
        assert_eq!(crate::unsaved::close(&mut app, 0).unwrap(), json!({"pending":"cloudCheckpoint"}));
        let latest = captures(&queued(&work)).pop().unwrap();
        done.send(Done::Captured(uid, latest, Err("disk full".into()))).unwrap();
        frame_at(&mut app, 0.1);
        assert_eq!(app.session.documents().len(), 1, "nothing is lost");
        assert!(quit_pending(&app), "Save / Don't Save / Cancel, as always");
        // Without a worker there is nothing to wait for: it asks at once.
        let mut app = VectorcraftApp::new(vectorcraft_engine::Session::new(), crate::Services::default());
        app.run("file.cloudNew", json!({"width":100,"height":80})).unwrap();
        frame_at(&mut app, 0.0);
        rectangle(&mut app);
        assert_eq!(crate::unsaved::close(&mut app, 0).unwrap(), json!({"pending": crate::unsaved::KIND}));
    }

    #[test]
    fn only_path_less_cloud_documents_skip_the_question() {
        let (mut app, _work, _done) = app_with_worker();
        // An ordinary File → New document keeps the question, whatever its checkpoint.
        app.run("file.new", json!({"width":100,"height":80})).unwrap();
        frame_at(&mut app, 0.0);
        rectangle(&mut app);
        let (doc, uid) = app.session.active().map(|d| (d.doc.clone(), d.uid)).unwrap();
        app.cloud_sync.tracks.get_mut(&uid).unwrap().durable = Some(doc);
        assert!(!app.cloud_sync.managed.contains(&uid));
        assert!(crate::unsaved::any_dirty(&app));
        assert_eq!(crate::unsaved::close(&mut app, 0).unwrap(), json!({"pending": crate::unsaved::KIND}));
        app.ui.dialog = None;
        // A document with a real file keeps the question, even one opened from the cloud and saved to a path.
        app.session.document_mut(uid).unwrap().path = Some("/tmp/vector-cloud-real-file.vectorcraft".into());
        app.cloud_sync.managed.insert(uid);
        assert!(crate::unsaved::any_dirty(&app));
        assert_eq!(crate::unsaved::close(&mut app, 0).unwrap(), json!({"pending": crate::unsaved::KIND}));
        app.ui.dialog = None;
        // A cloud document (no path) that is durable closes at once; a changed one is not durable any more.
        app.session.document_mut(uid).unwrap().path = None;
        assert!(!crate::unsaved::any_dirty(&app));
        rectangle(&mut app);
        assert!(crate::unsaved::any_dirty(&app), "changed since the checkpoint: the question stays");
        let doc = app.session.active().map(|d| d.doc.clone()).unwrap();
        app.cloud_sync.tracks.get_mut(&uid).unwrap().durable = Some(doc);
        assert!(!crate::unsaved::any_dirty(&app));
        assert_eq!(crate::unsaved::close(&mut app, 0).unwrap(), json!({}).get("x").cloned().unwrap_or(serde_json::Value::Null));
        assert!(app.session.documents().is_empty() && app.ui.dialog.is_none());
    }

    #[test]
    fn quit_and_close_all_wait_for_checkpoints_then_go_without_questions() {
        let (mut app, work, done) = app_with_worker();
        for _ in 0..2 {
            app.run("file.cloudNew", json!({"width":100,"height":80})).unwrap();
            frame_at(&mut app, 0.0);
            rectangle(&mut app);
        }
        let uids: Vec<u64> = app.session.documents().iter().map(|d| d.uid).collect();
        assert!(crate::unsaved::any_dirty(&app), "no checkpoint is durable yet");
        // Quit asks the worker first (nothing durable yet), and keeps the windows open meanwhile.
        assert_eq!(app.run("app.quit", json!({})).unwrap(), json!({"pending":"cloudCheckpoint"}));
        let asked = captures(&queued(&work));
        assert_eq!(asked.len(), 2, "one checkpoint per document");
        assert!(app.ui.dialog.is_none() && app.ui.status != "quit");
        for (uid, doc) in uids.iter().zip(asked) {
            done.send(Done::Captured(*uid, doc, Ok(None))).unwrap();
        }
        frame_at(&mut app, 0.2);
        assert!(app.ui.dialog.is_none());
        assert_eq!(app.ui.status, "quit", "nothing left to ask about");
        assert!(!crate::unsaved::any_dirty(&app));
        // Close All does the same.
        app.ui.status.clear();
        assert_eq!(app.run("file.closeAll", json!({})).unwrap()["closed"], 2);
        assert!(app.session.documents().is_empty() && app.ui.dialog.is_none());
    }

    #[test]
    fn a_new_project_creates_no_file_outside_the_adapters_folders() {
        use std::sync::Mutex;
        let writes = Arc::new(Mutex::new(Vec::<String>::new()));
        let picks = Arc::new(Mutex::new(0usize));
        let (w, p) = (writes.clone(), picks.clone());
        let services = crate::Services {
            write: Some(Box::new(move |path: &str, _: &[u8]| {
                w.lock().unwrap().push(path.to_owned());
                Ok(())
            })),
            pick_save: Some(Box::new(move |_: &crate::FilePick| {
                *p.lock().unwrap() += 1;
                None
            })),
            ..Default::default()
        };
        let mut app = VectorcraftApp::new(vectorcraft_engine::Session::new(), services);
        let (tx, work) = mpsc::channel();
        app.cloud_sync.send = Some(tx);
        app.run("file.cloudNew", json!({"width":100,"height":80})).unwrap();
        frame_at(&mut app, 0.0);
        frame_at(&mut app, 2.0);
        let st = app.session.active().unwrap();
        assert!(st.path.is_none(), "no file path");
        // Autosave takes it from here: the capture the worker runs writes into its own draft folder only.
        let queued: Vec<Work> = work.try_iter().collect();
        let Some(Work::Capture { key, title, doc, .. }) = queued.into_iter().find(|w| matches!(w, Work::Capture { .. })) else {
            panic!("autosave did not start")
        };
        assert!(key.starts_with("untitled-"), "an untitled document, not a file's: {key}");
        let adapter = temp("new-project");
        let draft = save_local_at(&adapter.join("local-checkpoints"), &key, &title, &encode_native(&doc).unwrap(), 1).unwrap();
        let mut files: Vec<PathBuf> = walk(&adapter);
        files.sort();
        assert!(files.iter().all(|f| f.starts_with(&adapter)), "{files:?}");
        assert!(files.contains(&draft.path));
        assert!(writes.lock().unwrap().is_empty(), "the app's writer (Save, Export) was not used");
        assert_eq!(*picks.lock().unwrap(), 0, "no Save As");
        std::fs::remove_dir_all(adapter).unwrap();
    }

    fn walk(dir: &std::path::Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk(&path));
            } else {
                out.push(path);
            }
        }
        out
    }

    #[test]
    fn open_documents_are_pinned_and_their_keys_reported_only_when_they_change() {
        let (mut app, work, done) = app_with_worker();
        app.run("file.new", json!({"width":100,"height":80})).unwrap();
        app.run("file.new", json!({"width":100,"height":80})).unwrap();
        let first = app.session.documents().first().unwrap();
        let (project, version) = (Uuid::from_u128(1), Uuid::from_u128(2));
        app.cloud_sync.test_bind(first, "cloud-first", project, version);
        frame_at(&mut app, 0.0);
        let sent = queued(&work);
        let pins: Vec<_> = sent.iter().filter_map(|w| if let Work::Pin(p) = w { Some(p.clone()) } else { None }).collect();
        assert_eq!(pins, vec![BTreeSet::from([version])], "only the bound revision of the open documents");
        let keys: Vec<_> = sent.iter().filter_map(|w| if let Work::OpenKeys(k) = w { Some(k.len()) } else { None }).collect();
        assert_eq!(keys, vec![2]);
        frame_at(&mut app, 0.1);
        assert!(queued(&work).iter().all(|w| !matches!(w, Work::Pin(_) | Work::OpenKeys(_))), "unchanged: nothing sent");
        // A checkpoint of the second document binds it; the pin follows, and a rename moves it on.
        let second_key = app.cloud_sync.tracks.values().find(|t| t.key != "cloud-first").map(|t| t.key.clone()).unwrap();
        done.send(Done::Submitted { key: second_key, project: Uuid::from_u128(3), version: Uuid::from_u128(4) }).unwrap();
        frame_at(&mut app, 0.2);
        let pins: Vec<_> = queued(&work).iter().filter_map(|w| if let Work::Pin(p) = w { Some(p.clone()) } else { None }).collect();
        assert_eq!(pins, vec![BTreeSet::from([version, Uuid::from_u128(4)])]);
        done.send(Done::Renamed { old: version, title: "x".into(), result: Ok(Uuid::from_u128(9)) }).unwrap();
        frame_at(&mut app, 0.3);
        let pins: Vec<_> = queued(&work).iter().filter_map(|w| if let Work::Pin(p) = w { Some(p.clone()) } else { None }).collect();
        assert_eq!(pins, vec![BTreeSet::from([Uuid::from_u128(9), Uuid::from_u128(4)])]);
        // Closing everything releases the pins.
        app.run("file.closeAll", json!({})).unwrap();
        frame_at(&mut app, 0.4);
        let sent = queued(&work);
        assert!(sent.iter().any(|w| matches!(w, Work::Pin(p) if p.is_empty())));
        assert!(sent.iter().any(|w| matches!(w, Work::OpenKeys(k) if k.is_empty())));
    }

    #[test]
    fn the_cache_limit_is_saved_with_the_account_never_the_password() {
        let folder = temp("limit");
        let config = Config { endpoint: "https://sync.example.test/personal/".into(), username: "u".into(), password: "hunter2-secret".into() };
        let mut account = Account::from(&config);
        assert_eq!(account.limit(), 2 << 30, "2 GiB until the user chooses");
        account.cache_limit = Some(512 << 20);
        save_account(&folder, &account).unwrap();
        let text = std::fs::read_to_string(folder.join("account.json")).unwrap();
        assert!(!text.contains("hunter2") && !text.contains("password"));
        assert_eq!(read_account(&folder).unwrap().limit(), 512 << 20);
        // Settings from before the limit existed, and nonsense limits, mean the default.
        std::fs::write(folder.join("account.json"), br#"{"endpoint":"","username":""}"#).unwrap();
        assert_eq!(read_account(&folder).unwrap().limit(), 2 << 30);
        for bad in [0u64, 1, u64::MAX] {
            account.cache_limit = Some(bad);
            assert_eq!(account.limit(), 2 << 30, "{bad}");
        }
        for preset in crate::cloud_home::CACHE_PRESETS {
            account.cache_limit = Some(preset);
            assert_eq!(account.limit(), preset);
        }
        std::fs::remove_dir_all(folder).unwrap();
    }

    fn pump(rx: &Receiver<Done>, mut seen: impl FnMut(Done) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(90);
        while Instant::now() < deadline {
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(message) => {
                    if seen(message) {
                        return;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => panic!("the worker stopped"),
            }
        }
        panic!("the worker did not answer in time");
    }

    #[test]
    fn the_worker_submits_a_preview_with_the_checkpoint_and_serves_rename_copy_limit_and_recents() {
        let folder = temp("worker");
        *TEST_CACHE.lock().unwrap() = Some(folder.clone());
        let mut cloud = CloudSync::default();
        launch(&mut cloud);
        *TEST_CACHE.lock().unwrap() = None;
        let (tx, rx) = (cloud.send.take().unwrap(), cloud.recv.take().unwrap());
        let mut session = vectorcraft_engine::Session::new();
        session.execute("file.new", &json!({"width":600,"height":400})).unwrap();
        session.execute("shape.rectangle", &json!({"x":10,"y":10,"width":300,"height":200})).unwrap();
        let doc = session.doc().unwrap().doc.clone();
        tx.send(Work::Capture { uid: 1, key: "untitled-test-1".into(), title: "Poster".into(), doc }).unwrap();
        // The revision enters the catalog with its preview: a PNG whose long side is 256 pixels, under 512 KiB.
        let (mut version, mut project, mut revision) = (None, None, None);
        pump(&rx, |m| {
            match m {
                Done::Submitted { project: p, version: v, .. } => (project, version) = (Some(p), Some(v)),
                Done::State(st) => revision = st.catalog.iter().find(|r| Some(r.version_id) == version).cloned(),
                Done::Error(e) => panic!("{e}"),
                _ => {}
            }
            revision.is_some()
        });
        let (project, version, revision) = (project.unwrap(), version.unwrap(), revision.unwrap());
        let png = std::fs::read(revision.preview_path.as_ref().expect("the revision has a preview")).unwrap();
        assert!(png.starts_with(b"\x89PNG\r\n\x1a\n") && png.len() <= 512 * 1024);
        let decoded = crate::cloud_preview::decode_png(&png).unwrap();
        assert_eq!(decoded.size, [256, 170], "600 x 400 points, long side 256");
        assert!(revision.pending && revision.cached);
        // Renaming a cached project makes a new revision with the new title, keeping the preview.
        tx.send(Work::Rename { project, version, title: "Poster v2".into() }).unwrap();
        let mut renamed = None;
        let mut head = None;
        pump(&rx, |m| {
            match m {
                Done::Renamed { result, .. } => renamed = Some(result.expect("renamed")),
                Done::State(st) => head = st.catalog.iter().find(|r| Some(r.version_id) == renamed).cloned(),
                _ => {}
            }
            head.is_some()
        });
        let head = head.unwrap();
        assert_eq!((head.title.as_str(), head.preview_path.is_some(), head.parents.as_slice()), ("Poster v2", true, &[version][..]));
        // Save a copy: the cached payload, byte for byte, loadable as a document.
        let target = folder.join("copy.vectorcraft");
        tx.send(Work::Copy { source: head.local_path.clone(), target: target.to_string_lossy().into_owned(), write: None }).unwrap();
        pump(&rx, |m| matches!(m, Done::Copied { result: Ok(()), .. }));
        assert_eq!(std::fs::read(&target).unwrap(), std::fs::read(&head.local_path).unwrap());
        assert!(vectorcraft_format::load(&std::fs::read(&target).unwrap()).is_ok());
        tx.send(Work::Copy { source: folder.join("missing"), target: target.to_string_lossy().into_owned(), write: None }).unwrap();
        pump(&rx, |m| matches!(m, Done::Copied { result: Err(_), .. }));
        // The cache limit is applied by the client, and kept with the account (no password).
        tx.send(Work::CacheLimit(512 << 20)).unwrap();
        pump(&rx, |m| matches!(m, Done::State(st) if st.cache_limit == 512 << 20));
        let saved = read_account(&folder).unwrap();
        assert_eq!((saved.limit(), saved.endpoint.as_str()), (512 << 20, ""));
        assert!(!std::fs::read_to_string(folder.join("account.json")).unwrap().contains("password"));
        // Recents are kept in the folder for the next session.
        let mut recents = Recents::default();
        recents.touch(project, 5);
        tx.send(Work::Recents(recents.clone())).unwrap();
        for _ in 0..200 {
            if crate::cloud_home::read_recents(&folder) == recents {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        assert_eq!(crate::cloud_home::read_recents(&folder), recents);
        // Everything above ran in the adapter's folder.
        assert!(walk(&folder).iter().all(|f| f.starts_with(&folder)));
        drop(tx);
        // The worker winds down once nothing can ask it anything more.
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline && !matches!(rx.recv_timeout(Duration::from_millis(100)), Err(mpsc::RecvTimeoutError::Disconnected)) {}
        std::fs::remove_dir_all(&folder).ok();
    }

    fn catalog_revision(n: u128, project: u128, title: &str, created: u64, payload: Option<&std::path::Path>) -> DownloadedRevision {
        DownloadedRevision {
            project_id: Uuid::from_u128(project),
            version_id: Uuid::from_u128(n),
            parents: Vec::new(),
            heads: vec![Uuid::from_u128(n)],
            title: title.into(),
            extension: "vectorcraft".into(),
            local_path: payload.map_or_else(|| PathBuf::from(format!("/nowhere/{n}")), PathBuf::from),
            created_ms: Some(created),
            size: 10,
            preview_path: None,
            cached: payload.is_some(),
            pending: false,
        }
    }

    /// A ready app with a catalog of a cached project (version 1, "Poster") and one only in the cloud (version 2).
    fn home_app() -> (VectorcraftApp, Receiver<Work>, Sender<Done>, PathBuf) {
        let dir = temp("home");
        let payload = dir.join("poster.vectorcraft");
        std::fs::write(&payload, encode_native(&Document::new(100.0, 80.0)).unwrap()).unwrap();
        let (mut app, work, done) = app_with_worker();
        app.cloud_sync.state.catalog =
            vec![catalog_revision(1, 1, "Poster", 2_000, Some(&payload)), catalog_revision(2, 2, "Remote logo", 1_000, None)];
        app.cloud_sync.got_state = true;
        app.cloud_sync.listed = true;
        app.cloud_sync.endpoint = "https://sync.example.test/personal/".into();
        (app, work, done, dir)
    }

    fn v(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn state_with(events: Vec<SyncEvent>, catalog: Vec<DownloadedRevision>) -> SyncState {
        SyncState { status: SyncStatus::Idle, catalog, events, ..Default::default() }
    }

    #[test]
    fn home_opens_cached_and_remote_projects_through_the_worker_and_shows_failures_on_the_card() {
        let (mut app, work, done, dir) = home_app();
        let catalog = app.cloud_sync.state.catalog.clone();
        // A cached project and a remote one both go to the worker, which downloads the second first.
        crate::cloud_home::open_version(&mut app, v(1)).unwrap();
        crate::cloud_home::open_version(&mut app, v(2)).unwrap();
        crate::cloud_home::open_version(&mut app, v(2)).unwrap();
        let opens: Vec<Uuid> =
            queued(&work).iter().filter_map(|w| if let Work::Open(OpenSource::Cloud(r), _) = w { Some(r.version_id) } else { None }).collect();
        assert_eq!(opens, vec![v(1), v(2)], "a second click while it downloads asks for nothing more");
        assert_eq!(app.cloud_sync.home.opening, BTreeSet::from([v(1), v(2)]));
        let cards = crate::cloud_home::all_cards(&catalog, &app.cloud_sync.home.downloading(), &[]);
        assert_eq!(cards.iter().find(|c| c.version == v(2)).map(|c| c.badge), Some(crate::cloud_home::Badge::Downloading));
        assert_eq!(cards.iter().find(|c| c.version == v(1)).map(|c| c.badge), Some(crate::cloud_home::Badge::OnComputer));
        // The download fails (offline): the error shows on the card, with Retry, and the spinner stops.
        done.send(Done::Opened(OpenSource::Cloud(catalog[1].clone()), Err("Cannot download the cloud project: offline".into()))).unwrap();
        frame_at(&mut app, 0.0);
        assert!(!app.cloud_sync.home.opening.contains(&v(2)));
        assert!(app.cloud_sync.home.errors.get(&v(2)).is_some_and(|e| e.message.contains("offline")));
        assert!(app.cloud_sync.error.is_none(), "a failed download is not an error of the whole app");
        crate::cloud_home::apply(&mut app, vec![crate::cloud_home::Action::Retry(v(2))]);
        assert!(app.cloud_sync.home.errors.is_empty());
        assert!(queued(&work).iter().any(|w| matches!(w, Work::Open(OpenSource::Cloud(r), _) if r.version_id == v(2))));
        // The cached project's payload is read: it opens, stays bound, is Home's, and is a recent project.
        let read = read_source(&OpenSource::Cloud(catalog[0].clone()), vectorcraft_engine::file_access::ScopeHandle::capture()).unwrap();
        done.send(Done::Opened(OpenSource::Cloud(catalog[0].clone()), Ok(read))).unwrap();
        frame_at(&mut app, 0.1);
        let st = app.session.active().unwrap();
        let uid = st.uid;
        assert_eq!(app.session.documents().len(), 1);
        assert!(app.cloud_sync.managed.contains(&uid) && st.path.is_none());
        assert_eq!(open_docs(&app), vec![OpenDoc { uid, project: Some(v(1)), version: Some(v(1)) }]);
        assert!(queued(&work).iter().any(|w| matches!(w, Work::Bind { revision, .. } if revision.version_id == v(1))));
        assert_eq!(app.cloud_sync.home.recents.projects().first(), Some(&v(1)));
        assert!(!app.cloud_sync.home.opening.contains(&v(1)));
        // Opening it again switches to the document instead of opening a second one.
        app.ui.home = Some(crate::menus::home_key(&app));
        assert_eq!(crate::cloud_home::open_version(&mut app, v(1)).unwrap()["switched"], true);
        assert!(app.ui.home.is_none() && app.session.documents().len() == 1);
        assert!(queued(&work).iter().all(|w| !matches!(w, Work::Open(..))));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn home_renames_and_copies_projects_downloading_the_payload_first_when_it_is_not_here() {
        let (mut app, work, done, dir) = home_app();
        let catalog = app.cloud_sync.state.catalog.clone();
        // Cached and not open: a new revision with the new name, at once.
        app.run("file.cloudRename", json!({"version": v(1).to_string(), "title": "  Poster v2  "})).unwrap();
        assert!(
            matches!(&queued(&work)[..], [Work::Rename { project, version, title }] if (*project, *version, title.as_str()) == (v(1), v(1), "Poster v2"))
        );
        // Not on this computer: the payload is asked for, and the rename waits for it.
        app.run("file.cloudRename", json!({"version": v(2).to_string(), "title": "Logo"})).unwrap();
        assert!(matches!(&queued(&work)[..], [Work::Request(version)] if *version == v(2)));
        assert_eq!(app.cloud_sync.home.requested, BTreeSet::from([v(2)]));
        done.send(Done::State(state_with(
            vec![SyncEvent::Fetched { project_id: v(2), version_id: v(2), path: PathBuf::from("/cache/2") }],
            catalog.clone(),
        )))
        .unwrap();
        frame_at(&mut app, 0.0);
        let sent = queued(&work);
        assert!(sent.iter().any(|w| matches!(w, Work::Rename { version, title, .. } if (*version, title.as_str()) == (v(2), "Logo"))));
        assert!(app.cloud_sync.home.requested.is_empty() && app.cloud_sync.home.pending_rename.is_empty());
        // A download that fails leaves the error and a Retry that renames again.
        app.run("file.cloudRename", json!({"version": v(2).to_string(), "title": "Logo 2"})).unwrap();
        queued(&work);
        done.send(Done::State(state_with(vec![SyncEvent::FetchFailed { version_id: v(2), error: "offline".into() }], catalog.clone()))).unwrap();
        frame_at(&mut app, 0.1);
        let error = app.cloud_sync.home.errors.get(&v(2)).cloned().unwrap();
        assert!(error.message.contains("offline") && error.retry == crate::cloud_home::Retry::Rename("Logo 2".into()));
        assert!(app.cloud_sync.home.requested.is_empty());
        // Invalid names are refused before anything is asked.
        for bad in ["", "   ", "tab\tname"] {
            assert!(app.run("file.cloudRename", json!({"version": v(1).to_string(), "title": bad})).is_err(), "{bad:?}");
        }
        assert!(queued(&work).is_empty());
        // Save a copy: the cached payload is copied by the worker; a remote one is fetched first; the name gets its extension.
        let target = dir.join("my copy");
        let r = app.run("file.cloudSaveCopy", json!({"version": v(1).to_string(), "path": target.to_string_lossy()})).unwrap();
        assert_eq!(r["path"], format!("{}.vectorcraft", target.to_string_lossy()));
        let sent = queued(&work);
        assert!(
            matches!(&sent[..], [Work::Copy { source, target: t, .. }] if source == &catalog[0].local_path && t.ends_with("my copy.vectorcraft"))
        );
        app.run("file.cloudSaveCopy", json!({"version": v(2).to_string(), "path": dir.join("remote.vectorcraft").to_string_lossy()})).unwrap();
        assert!(matches!(&queued(&work)[..], [Work::Request(version)] if *version == v(2)));
        done.send(Done::State(state_with(vec![SyncEvent::Fetched { project_id: v(2), version_id: v(2), path: PathBuf::from("/cache/2") }], catalog)))
            .unwrap();
        frame_at(&mut app, 0.2);
        assert!(matches!(&queued(&work)[..], [Work::Copy { source, .. }] if source == &PathBuf::from("/cache/2")));
        assert!(app.run("file.cloudSaveCopy", json!({"version": "nope"})).is_err());
        // The copy goes only where the automation roots allow writing.
        use vectorcraft_engine::file_access::{self, AutomationRoots};
        let allowed = dir.join("allowed");
        std::fs::create_dir_all(&allowed).unwrap();
        let roots = AutomationRoots::new(Some(&dir), Some(&allowed)).unwrap().unwrap();
        let outside = dir.join("outside.vectorcraft");
        let e = file_access::confine(Some(&roots), || {
            app.run("file.cloudSaveCopy", json!({"version": v(1).to_string(), "path": outside.to_string_lossy()}))
        })
        .unwrap_err();
        assert!(e.contains("write root"), "{e}");
        assert!(queued(&work).is_empty());
        // A finished copy and a failed one are said in the status bar.
        done.send(Done::Copied { target: "/x/a.vectorcraft".into(), result: Ok(()) }).unwrap();
        frame_at(&mut app, 0.3);
        assert!(app.ui.status.contains("Saved a copy"));
        done.send(Done::Copied { target: "/x/b.vectorcraft".into(), result: Err("disk full".into()) }).unwrap();
        frame_at(&mut app, 0.4);
        assert!(app.ui.status.contains("disk full"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// One frame of the Home screen (no document open) with `events`: the texts drawn and where.
    fn home_texts(app: &mut VectorcraftApp, ctx: &egui::Context, events: Vec<egui::Event>) -> Vec<(String, egui::Rect)> {
        fn walk(s: &egui::Shape, out: &mut Vec<(String, egui::Rect)>) {
            match s {
                egui::Shape::Text(t) => out.push((t.galley.text().to_string(), t.visual_bounding_rect())),
                egui::Shape::Vec(v) => v.iter().for_each(|s| walk(s, out)),
                _ => {}
            }
        }
        let raw = egui::RawInput {
            screen_rect: Some(egui::Rect::from_min_size(egui::Pos2::ZERO, egui::vec2(1000.0, 900.0))),
            events,
            ..Default::default()
        };
        let mut out = ctx.run_ui(raw, |ui| crate::canvas::show(app, ui));
        out.textures_delta.clear();
        let mut texts = vec![];
        out.shapes.iter().for_each(|c| walk(&c.shape, &mut texts));
        texts
    }

    fn has(texts: &[(String, egui::Rect)], s: &str) -> bool {
        texts.iter().any(|(t, _)| t == s)
    }

    fn click(app: &mut VectorcraftApp, ctx: &egui::Context, at: egui::Pos2, button: egui::PointerButton) {
        let press = |pressed| egui::Event::PointerButton { pos: at, button, pressed, modifiers: Default::default() };
        home_texts(app, ctx, vec![egui::Event::PointerMoved(at)]);
        home_texts(app, ctx, vec![press(true)]);
        home_texts(app, ctx, vec![press(false)]);
    }

    #[test]
    fn home_shows_its_states_and_a_card_click_opens_the_project() {
        let ctx = egui::Context::default();
        crate::theme::install_fonts(&ctx);
        // Starting: the worker has not answered yet.
        let mut app = VectorcraftApp::new(vectorcraft_engine::Session::new(), crate::Services::default());
        let texts = home_texts(&mut app, &ctx, vec![]);
        assert!(has(&texts, "Projects") && has(&texts, "Loading your projects…") && has(&texts, "New") && has(&texts, "Account"));
        assert!(!has(&texts, "No projects yet"));
        assert!(has(&texts, "Recent Files") || has(&texts, "On this computer"), "the file shortcuts stay under the library");
        // Connected, listing done, nothing in it, offline.
        let (mut app, work, _done, dir) = home_app();
        app.cloud_sync.state.catalog.clear();
        app.cloud_sync.state.status = SyncStatus::Offline;
        let texts = home_texts(&mut app, &ctx, vec![]);
        assert!(has(&texts, "No projects yet"), "{texts:?}");
        assert!(has(&texts, "Offline — showing projects on this computer. Edits are saved locally and upload when you reconnect."));
        // Not connected: local projects are listed and the banner says what connecting adds.
        let (mut app, _work, _done, dir2) = home_app();
        app.cloud_sync.endpoint.clear();
        app.cloud_sync.state.catalog.truncate(1);
        let texts = home_texts(&mut app, &ctx, vec![]);
        assert!(has(&texts, "Poster") && has(&texts, "Connect your account to see cloud projects"));
        app.cloud_sync.state.catalog.clear();
        let texts = home_texts(&mut app, &ctx, vec![]);
        assert!(has(&texts, "Connect your account to see cloud projects") && !has(&texts, "No projects yet"));
        // An error shows its message with Retry, which syncs again.
        let (mut app, work2, _done, dir3) = home_app();
        app.cloud_sync.state.status = SyncStatus::Error("the server said no".into());
        let texts = home_texts(&mut app, &ctx, vec![]);
        assert!(has(&texts, "the server said no") && has(&texts, "Retry"));
        let retry = texts.iter().find(|(t, _)| t == "Retry").map(|(_, r)| r.center()).unwrap();
        click(&mut app, &ctx, retry, egui::PointerButton::Primary);
        assert!(queued(&work2).iter().any(|w| matches!(w, Work::Sync)));
        // The cards: titles, edit times, badges, the conflict marker, the filters, search and sort.
        let (mut app, work, _done, dir4) = home_app();
        let mut conflict_a = catalog_revision(3, 3, "Brochure", 3_000, None);
        let mut conflict_b = catalog_revision(4, 3, "Brochure", 500, None);
        conflict_a.heads = vec![v(3), v(4)];
        conflict_b.heads = vec![v(3), v(4)];
        let mut pending = catalog_revision(5, 5, "Unsent sketch", 4_000, Some(&dir.join("poster.vectorcraft")));
        pending.pending = true;
        app.cloud_sync.state.catalog.extend([conflict_a, conflict_b, pending]);
        let texts = home_texts(&mut app, &ctx, vec![]);
        for expected in [
            "Poster",
            "Remote logo",
            "Unsent sketch",
            "On this computer",
            "In the cloud",
            "Not uploaded yet",
            "Conflict copy 1/2",
            "Conflict copy 2/2",
            "All projects",
            "Last edited",
        ] {
            assert!(has(&texts, expected), "{expected}: {texts:?}");
        }
        assert!(texts.iter().filter(|(t, _)| t == "Brochure").count() == 2, "a card per head");
        assert!(texts.iter().any(|(t, _)| t.starts_with("Edited ") && t.ends_with(" ago")));
        app.cloud_sync.home.search = "LOGO".into();
        let texts = home_texts(&mut app, &ctx, vec![]);
        assert!(has(&texts, "Remote logo") && !has(&texts, "Poster") && has(&texts, "Results"));
        app.cloud_sync.home.search = "zzz".into();
        assert!(has(&home_texts(&mut app, &ctx, vec![]), "No projects match your search or filter."));
        app.cloud_sync.home.search.clear();
        app.cloud_sync.home.filter = crate::cloud_home::Filter::Conflicts;
        let texts = home_texts(&mut app, &ctx, vec![]);
        assert!(!has(&texts, "Poster") && has(&texts, "Brochure"));
        app.cloud_sync.home.filter = crate::cloud_home::Filter::All;
        // Recents above the grid, once a project has been opened.
        app.cloud_sync.home.recents.touch(v(1), 9);
        assert!(has(&home_texts(&mut app, &ctx, vec![]), "Recents"));
        // A click on a card opens its project (here: asks the worker for it).
        queued(&work);
        let texts = home_texts(&mut app, &ctx, vec![]);
        let at = texts.iter().filter(|(t, _)| t == "Remote logo").map(|(_, r)| r.center()).next_back().unwrap();
        click(&mut app, &ctx, at, egui::PointerButton::Primary);
        assert!(
            queued(&work).iter().any(|w| matches!(w, Work::Open(OpenSource::Cloud(r), _) if r.version_id == v(2))),
            "{:?}",
            app.cloud_sync.home.opening
        );
        assert!(has(&home_texts(&mut app, &ctx, vec![]), "Remote logo"), "the card shows the project while it downloads");
        // The right-click menu has the card's actions.
        click(&mut app, &ctx, at, egui::PointerButton::Secondary);
        let texts = home_texts(&mut app, &ctx, vec![]);
        for item in ["Open", "Rename", "Save a copy…"] {
            assert!(texts.iter().any(|(t, _)| t.trim() == item), "{item}: {texts:?}");
        }
        for d in [dir, dir2, dir3, dir4] {
            std::fs::remove_dir_all(d).ok();
        }
    }

    fn draft(folder: &std::path::Path, key: &str, sequence: u64, submitted: bool) -> LocalDraft {
        let mut d = save_local_at(folder, key, "Doc", &encode_native(&Document::new(10.0, 10.0)).unwrap(), sequence).unwrap();
        d.needs_submit = !submitted;
        save_metadata(&d).unwrap();
        d
    }

    #[test]
    fn a_local_draft_is_deleted_only_when_its_revision_is_published_and_it_is_not_the_newest_of_an_open_document() {
        let folder = temp("prune");
        let drafts_dir = folder.join("local-checkpoints");
        let elsewhere = folder.join("elsewhere");
        std::fs::create_dir_all(&elsewhere).unwrap();
        let old_published = draft(&drafts_dir, "doc-a", 1, true);
        let old_unpublished = draft(&drafts_dir, "doc-a", 2, true);
        let newest = draft(&drafts_dir, "doc-a", 3, true);
        let never_submitted = draft(&drafts_dir, "doc-b", 1, false);
        let other_newest = draft(&drafts_dir, "doc-b", 2, true);
        let outside = draft(&elsewhere, "doc-c", 1, true);
        let all = [&old_published, &old_unpublished, &newest, &never_submitted, &other_newest, &outside];
        let load = || all.iter().map(|d| (d.path.clone(), (*d).clone())).collect::<BTreeMap<_, _>>();
        let tokens = |list: &[&LocalDraft]| list.iter().map(|d| d.token.clone()).collect::<BTreeSet<_>>();
        let exists = |d: &LocalDraft| d.path.exists() || d.path.with_extension("json").exists();
        // Not told what is open: only drafts a newer one supersedes can go, and only published ones.
        let mut drafts = load();
        let published = tokens(&[&old_published, &newest, &never_submitted, &outside]);
        let removed = prune_drafts(&mut drafts, &drafts_dir, &published, None);
        assert_eq!(removed, vec![old_published.path.clone()]);
        assert!(!exists(&old_published));
        for kept in [&old_unpublished, &newest, &never_submitted, &other_newest, &outside] {
            assert!(exists(kept), "{}", kept.key);
        }
        assert!(!drafts.contains_key(&old_published.path) && drafts.len() == 5);
        // The newest draft of an open document stays even when published; a closed document's goes.
        let open: BTreeSet<String> = ["doc-a".to_owned()].into();
        let mut drafts = load();
        let published = tokens(&[&newest, &other_newest, &never_submitted]);
        let removed: BTreeSet<_> = prune_drafts(&mut drafts, &drafts_dir, &published, Some(&open)).into_iter().collect();
        assert_eq!(removed, BTreeSet::from([other_newest.path.clone()]), "doc-b is closed and its newest draft is published");
        assert!(exists(&newest) && exists(&never_submitted), "open document; pending work is never removed");
        assert!(!exists(&other_newest));
        // Nothing outside the adapter's own folder is ever touched, whatever the metadata says.
        let mut drafts = load();
        let published = tokens(&[&outside]);
        assert!(prune_drafts(&mut drafts, &drafts_dir, &published, Some(&BTreeSet::new())).is_empty());
        assert!(exists(&outside));
        // A draft whose file name is not its token is left alone.
        let mut liar = newest.clone();
        liar.token = "someone-else".into();
        let mut drafts = BTreeMap::from([(liar.path.clone(), liar.clone())]);
        assert!(prune_drafts(&mut drafts, &drafts_dir, &BTreeSet::from(["someone-else".to_owned()]), Some(&BTreeSet::new())).is_empty());
        assert!(exists(&newest));
        std::fs::remove_dir_all(folder).unwrap();
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

/// Which local drafts may go, and which go. A draft is given up only when every one of these holds:
///
/// * it was handed to the client's outbox (`needs_submit` is off) and the client reported its token published,
///   so the revision it holds is in the cloud;
/// * either a newer draft of the same document exists, or the document is not open (`open_keys`; `None`, not
///   yet known, counts every document as open);
/// * it is `<token>.vectorcraft` directly in `folder`, the adapter's own draft folder: nothing else is ever deleted.
///
/// Drafts leave `drafts`, metadata first, so that a crash between the two removals leaves an orphan payload no
/// list shows, never a listed draft without a payload. Returns the paths removed.
fn prune_drafts(
    drafts: &mut BTreeMap<PathBuf, LocalDraft>,
    folder: &std::path::Path,
    published: &BTreeSet<String>,
    open_keys: Option<&BTreeSet<String>>,
) -> Vec<PathBuf> {
    let mut newest: BTreeMap<&str, (u64, u128)> = BTreeMap::new();
    for d in drafts.values() {
        let order = (d.sequence, d.captured_at);
        let slot = newest.entry(d.key.as_str()).or_insert(order);
        *slot = (*slot).max(order);
    }
    let goes = |d: &LocalDraft| {
        let superseded = newest.get(d.key.as_str()).is_some_and(|n| (d.sequence, d.captured_at) < *n);
        let closed = open_keys.is_some_and(|open| !open.contains(&d.key));
        !d.needs_submit
            && !d.token.is_empty()
            && published.contains(&d.token)
            && (superseded || closed)
            && d.path.parent() == Some(folder)
            && d.path.extension().is_some_and(|e| e == "vectorcraft")
            && d.path.file_stem().is_some_and(|stem| stem == d.token.as_str())
    };
    let doomed: Vec<PathBuf> = drafts.values().filter(|d| goes(d)).map(|d| d.path.clone()).collect();
    let mut removed = Vec::new();
    for path in doomed {
        let gone = |p: &std::path::Path| std::fs::remove_file(p).is_ok() || !p.exists();
        if gone(&path.with_extension("json")) && gone(&path) {
            drafts.remove(&path);
            removed.push(path);
        }
    }
    removed
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
pub(crate) fn without_source_reads<T>(work: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
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
/// Open a catalog revision (its payload downloads on demand; a project already open is switched to) or a
/// local checkpoint.
pub fn open_command(app: &mut VectorcraftApp, params: &serde_json::Value) -> Result<serde_json::Value, String> {
    if let Some(version) = params.get("version").and_then(serde_json::Value::as_str) {
        let id = uuid::Uuid::parse_str(version).map_err(|_| "invalid revision id")?;
        return crate::cloud_home::open_version(app, id);
    }
    let cloud = &mut app.cloud_sync;
    let source = params
        .get("localToken")
        .and_then(serde_json::Value::as_str)
        .and_then(|token| cloud.local.iter().find(|d| d.token == token).cloned())
        .map(OpenSource::Local)
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
