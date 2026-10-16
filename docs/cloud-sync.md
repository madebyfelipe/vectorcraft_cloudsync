# Personal cloud projects (desktop)

VectorCraft automatically checkpoints open documents, including untitled documents, into a durable native local outbox. Changes settle for 1.5 seconds before capture, with a 15-second capture interval during continuous editing whenever the worker is available. Encoding, writes and transfers run on a worker. Closing the app waits asynchronously for current checkpoints to reach local disk. Manual Save, its dirty indicator, and Data Recovery remain independent.

Open **Home** (the house button in the application bar, **File → Home**, the **Home** button beside the cloud status, or **File → Cloud Projects…**), press **Account**, and enter the personal WebDAV HTTPS endpoint, username and password. Use the dedicated account and root configured by the deployment instructions. The endpoint and username are saved in `account.json`; it never contains a password. Credentials use the operating system credential vault when available, so the connection resumes automatically after restart. If the vault is absent, locked, or unavailable, the window explicitly reports session-only operation; reconnect after restarting. Local pending snapshots and submission retries survive restart. Exact submission tokens prevent duplicate revisions when a process stops between outbox durability and local acknowledgement. **Local checkpoints** in this window reopens the latest native snapshot of each document, including snapshots blocked from upload by missing linked sources. Earlier local copies remain available. Every capture has a unique payload and metadata file; durable capture sequence numbers preserve their order across clock changes. Never reuse this cache with a different personal account/root.

## Home: the projects library

With no document open, and whenever you pick **Home**, the window shows every project of the account as a grid of cards: a thumbnail, the name, **Edited <time ago>** and a badge — **Not uploaded yet** (saved on this computer, waiting to upload), **Downloading…**, **On this computer**, **In the cloud** or **Conflict**. A project that concurrent edits left with several heads has one card per head, marked **Conflict copy i/n** (newest first). The top bar has a search field (case-insensitive substring of the name), a sort menu (**Last edited**, **Name A–Z**, **Name Z–A**), the filters **All**, **Recents**, **Conflicts**, **Available offline** and **Not uploaded yet**, **New**, the cache use ("Cache: X of Y") and the sync status. Above the grid, while the filter is All and the search is empty, **Recents** shows the 8 projects this app opened most recently on this computer (`recents.json` in the cloud-sync folder).

* **Open** (click a card, Enter on the focused card, or its menu): a project already open is switched to; otherwise it opens at once when its payload is on this computer, and else downloads first (a spinner on the card; a failure shows on the card with **Retry**). Opened projects stay bound, so autosave continues the same project.
* **New** uses the normal New Document dialog; the document has no file path and is checkpointed by autosave at once. Nothing is written to Documents, the Desktop or any other folder: only the adapter's private draft folder and the client cache.
* **Rename** (the card's menu, right-click, or F2): edits the name in place (1–256 bytes, no control characters). An open project renames its document, so the next autosave carries the name; otherwise the cached project gets a new revision with the new name (its payload is downloaded first if needed).
* **Save a copy…** writes the project to a `.vectorcraft` file you pick, the only way a project becomes a loose file.
* **Ctrl/Cmd+F** finds, **Esc** leaves Home for the open documents (or **Back to editor**).
* Closing a tab of a project made in Home or opened from the cloud, or quitting, does not ask for a file when its latest revision is already in the local checkpoint; if it is not yet, the checkpoint is made first and the usual Save / Don't Save / Cancel is asked only if that fails. Documents with a real file keep the usual question.

Home says when it is loading (spinner and placeholder cards), when there are no projects yet, when no account is connected ("Connect your account to see cloud projects"; projects saved only on this computer are still listed), when the client is offline ("Offline — showing projects on this computer. Edits are saved locally and upload when you reconnect.") and when sync failed (the message and **Retry**).

Previews are made when a checkpoint is taken: the first artboard is rendered off the UI thread (at most 256 px on its long side, a PNG under 512 KiB), alongside the checkpoint's other work, and a checkpoint waits for it at most 0.75 s; without a preview the revision is submitted without one and the card shows a placeholder.

The **Account** dialog holds the connection settings, the **Local cache limit** (512 MiB, 1, 2, 5 or 10 GiB; 2 GiB until you choose; saved in `account.json`, never the password) and the recovery lists (earlier revisions and local checkpoints). The client never removes the revisions open in the editor, work not yet uploaded or conflict copies to make room. Once a local draft's revision is published and a newer draft of the same document exists (or the document is closed), the draft file is deleted.

Projects and their native revisions download automatically. Concurrent heads appear as separate **conflict copy** cards; earlier revisions remain available under **Account → Earlier revisions**. Opening a revision binds subsequent checkpoints to that exact base. There is no automatic conflict merge, shared team workspace, realtime collaboration, or browser sync.

Snapshots retain native v4 editable artwork, original embedded image and placed-document bytes, linked originals and loaded ICC profiles. Remote payloads currently have a 256 MiB limit; larger projects keep their local checkpoint and report an upload error. Every capture first writes a native local checkpoint. A missing image/source or preview-only linked source produces an explicit error and keeps that local checkpoint available, while suppressing publication of incomplete data. Automatic capture reads only already loaded/cached originals; it cannot follow paths from a document to read new external files. Use the normal update/embed workflow for missing originals. Nested placed sources must already be self-contained; update and save the source document first. Text retains its editable native model and font references; fonts must be installed on the other device as for normal native files.

The compact status in the status bar opens the Account dialog (the **Home** button beside it shows the library): **Saved**, **Saving…**, **Offline**, or **Error**. Saved means the client has no outstanding work; Offline may still have durable local changes waiting for connection. Detailed errors appear in the window.

Local data lives under `vectorcraft/cloud-sync` in the platform application-data directory (`~/.local/share` on Linux, `~/Library/Application Support` on macOS, `%LOCALAPPDATA%` on Windows). `VECTORCRAFT_CLOUD_CACHE` overrides that location for isolated tests. The outbox contains private native documents; protect it as you protect your working files.

## Build arrangement

This implementation provisionally shares the standalone Rust package `craft-cloud-sync` from `../photocraft_cloudsync/cloud-sync`. Keep the PhotoCraft and VectorCraft checkouts as siblings with those directory names. The dependency is native-only; WebAssembly keeps its existing offline workflow. Moving the shared package to its own versioned repository is future packaging work.

Useful validation:

```sh
cargo check -p vectorcraft-ui-egui
cargo test -p vectorcraft-ui-egui cloud_sync::tests
```

The Home library has its own tests (`cloud_home`, `cloud_preview`, and the Home tests in `cloud_sync::tests`): search/sort/filter, badges, conflict numbering, recents persistence, rename validation, preview generation, pinning, closing without Save As only for durable path-less cloud documents, and that a new project writes no file outside the adapter's folders. Agents can drive it with `file.cloudList`, `file.cloudOpen {version}`, `file.cloudNew`, `file.cloudRename` and `file.cloudSaveCopy`.

The adapter tests cover continuous-edit scheduling, native round-trip and manual dirty state, byte-for-byte linked-image retention, local fallback reopening, tab closure during an in-flight checkpoint, and idempotent outbox retry after restart. The shared client owns transport, durable journal, offline retry and conflict-graph tests. Live two-device validation additionally requires the deployed dedicated WebDAV account.
