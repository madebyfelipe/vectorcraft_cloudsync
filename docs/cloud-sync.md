# Personal cloud projects (desktop)

VectorCraft automatically checkpoints open documents, including untitled documents, into a durable native local outbox. Changes settle for 1.5 seconds before capture, with a 15-second capture interval during continuous editing whenever the worker is available. Encoding, writes and transfers run on a worker. Closing the app waits asynchronously for current checkpoints to reach local disk. Manual Save, its dirty indicator, and Data Recovery remain independent.

Open **File → Cloud Projects…**, expand **Connection**, and enter the personal WebDAV HTTPS endpoint, username and password. Use the dedicated account and root configured by the deployment instructions. The endpoint and username are saved in `account.json`; it never contains a password. Credentials use the operating system credential vault when available, so the connection resumes automatically after restart. If the vault is absent, locked, or unavailable, the window explicitly reports session-only operation; reconnect after restarting. Local pending snapshots and submission retries survive restart. Exact submission tokens prevent duplicate revisions when a process stops between outbox durability and local acknowledgement. **Local checkpoints** in this window reopens the latest native snapshot of each document, including snapshots blocked from upload by missing linked sources. Earlier local copies remain available. Every capture has a unique payload and metadata file; durable capture sequence numbers preserve their order across clock changes. Never reuse this cache with a different personal account/root.

Projects and their native revisions download automatically. Open a revision from Cloud Projects to edit it in the normal editor. Concurrent heads appear as separate **conflict copy** entries; earlier revisions remain available. Opening a revision binds subsequent checkpoints to that exact base. There is no automatic conflict merge, shared team workspace, realtime collaboration, or browser sync.

Snapshots retain native v4 editable artwork, original embedded image and placed-document bytes, linked originals and loaded ICC profiles. Remote payloads currently have a 256 MiB limit; larger projects keep their local checkpoint and report an upload error. Every capture first writes a native local checkpoint. A missing image/source or preview-only linked source produces an explicit error and keeps that local checkpoint available, while suppressing publication of incomplete data. Automatic capture reads only already loaded/cached originals; it cannot follow paths from a document to read new external files. Use the normal update/embed workflow for missing originals. Nested placed sources must already be self-contained; update and save the source document first. Text retains its editable native model and font references; fonts must be installed on the other device as for normal native files.

The compact status opens Cloud Projects: **Saved**, **Saving…**, **Offline**, or **Error**. Saved means the client has no outstanding work; Offline may still have durable local changes waiting for connection. Detailed errors appear in the window.

Local data lives under `vectorcraft/cloud-sync` in the platform application-data directory (`~/.local/share` on Linux, `~/Library/Application Support` on macOS, `%LOCALAPPDATA%` on Windows). `VECTORCRAFT_CLOUD_CACHE` overrides that location for isolated tests. The outbox contains private native documents; protect it as you protect your working files.

## Build arrangement

This implementation provisionally shares the standalone Rust package `craft-cloud-sync` from `../photocraft_cloudsync/cloud-sync`. Keep the PhotoCraft and VectorCraft checkouts as siblings with those directory names. The dependency is native-only; WebAssembly keeps its existing offline workflow. Moving the shared package to its own versioned repository is future packaging work.

Useful validation:

```sh
cargo check -p vectorcraft-ui-egui
cargo test -p vectorcraft-ui-egui cloud_sync::tests
```

The adapter tests cover continuous-edit scheduling, native round-trip and manual dirty state, byte-for-byte linked-image retention, local fallback reopening, tab closure during an in-flight checkpoint, and idempotent outbox retry after restart. The shared client owns transport, durable journal, offline retry and conflict-graph tests. Live two-device validation additionally requires the deployed dedicated WebDAV account.
