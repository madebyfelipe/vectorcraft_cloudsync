//! Previews for the Home library (native only).
//!
//! * Making them: when the adapter checkpoints a document, the first artboard is rasterised so its
//!   longest side is at most [`MAX_SIDE`] pixels and encoded as a PNG of at most [`MAX_PNG_BYTES`].
//!   The render runs on its own thread, alongside the checkpoint's encoding and disk writes, and the
//!   checkpoint waits for it only for [`BUDGET`]: a slow or failing preview means the revision is
//!   submitted without one, never that autosave waits or fails.
//! * Showing them: the grid decodes the PNGs the shared client verified on a thread of its own and
//!   uploads the pixels as textures kept in a small least-recently-used cache.
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::{Duration, Instant};

use uuid::Uuid;
use vectorcraft_doc::Document;
use vectorcraft_render::Renderer;

/// Longest side of a preview, in pixels.
pub const MAX_SIDE: u32 = 256;
/// Largest preview the shared client accepts (512 KiB).
pub const MAX_PNG_BYTES: usize = 512 * 1024;
/// How long a checkpoint waits for its preview before it is submitted without one.
pub const BUDGET: Duration = Duration::from_millis(750);
/// Previews larger than this are not decoded: the file is not one of ours.
const MAX_DECODE_SIDE: u32 = 1024;
/// Textures kept for cards (256 × 256 × 4 bytes each: about 16 MiB at most).
const MAX_TEXTURES: usize = 64;
/// Decodes running at once.
const MAX_INFLIGHT: usize = 6;
const PNG_SIGNATURE: &[u8] = b"\x89PNG\r\n\x1a\n";

/// Whether `bytes` is a PNG the shared client accepts as a preview.
pub fn valid_png(bytes: &[u8]) -> bool {
    bytes.len() <= MAX_PNG_BYTES && bytes.starts_with(PNG_SIGNATURE)
}

/// The part of `doc` a preview shows: the first artboard, else the art.
fn target_rect(doc: &Document) -> Option<vectorcraft_geom::Rect> {
    let rect = doc.artboards.first().map(|a| a.rect).or_else(|| doc.art_bounds())?;
    let finite = [rect.x0, rect.y0, rect.x1, rect.y1].iter().all(|v| v.is_finite() && v.abs() < 1e9);
    (finite && rect.width() >= 1e-3 && rect.height() >= 1e-3).then_some(rect)
}

/// A PNG preview of `doc`: its first artboard on white, longest side [`MAX_SIDE`]. `None` when there
/// is nothing to show or anything fails.
pub fn render_png(renderer: &mut Renderer, doc: &Document) -> Option<Vec<u8>> {
    let rect = target_rect(doc)?;
    let longest = rect.width().max(rect.height());
    // Half a pixel short, so that rounding the long side never gives MAX_SIDE + 1.
    let scale = (f64::from(MAX_SIDE) - 0.5) / longest;
    let (w, h) = vectorcraft_render::raster_size(rect, scale).ok()?;
    if w > MAX_SIDE || h > MAX_SIDE {
        return None;
    }
    let png = renderer.render_region(doc, rect, scale, true).to_png().ok()?;
    valid_png(&png).then_some(png)
}

struct Job {
    doc: Arc<Document>,
    reply: Sender<Option<Vec<u8>>>,
}

/// The thread that renders previews. One preview at a time: while it is busy a request gets none,
/// so a document too heavy for the budget costs one wait, not a queue of them.
pub struct PreviewWorker {
    jobs: Sender<Job>,
    busy: Arc<AtomicBool>,
}

impl PreviewWorker {
    pub fn spawn() -> Option<Self> {
        let (jobs, rx) = mpsc::channel::<Job>();
        let busy = Arc::new(AtomicBool::new(false));
        let flag = busy.clone();
        std::thread::Builder::new()
            .name("vectorcraft-cloud-preview".into())
            .spawn(move || {
                let mut renderer = Renderer::new();
                while let Ok(job) = rx.recv() {
                    // Rendering reads only what the document already holds, as the checkpoint's encoding does.
                    let png =
                        crate::cloud_sync::without_source_reads(|| vectorcraft_engine::guard::catch_panic(|| render_png(&mut renderer, &job.doc)))
                            .unwrap_or_else(|_| {
                                // A renderer that panicked may be half-way through a frame: start afresh.
                                renderer = Renderer::new();
                                None
                            });
                    flag.store(false, Ordering::Release);
                    // The checkpoint may have stopped waiting; a closed reply is fine.
                    let _ = job.reply.send(png);
                }
            })
            .ok()?;
        Some(Self { jobs, busy })
    }

    /// Start a preview of `doc`; `None` when the worker is busy or gone.
    pub fn request(&self, doc: Arc<Document>) -> Option<Receiver<Option<Vec<u8>>>> {
        if self.busy.swap(true, Ordering::AcqRel) {
            return None;
        }
        let (reply, rx) = mpsc::channel();
        if self.jobs.send(Job { doc, reply }).is_err() {
            self.busy.store(false, Ordering::Release);
            return None;
        }
        Some(rx)
    }
}

/// The preview that [`PreviewWorker::request`] started, if it is ready within `budget`.
pub fn collect(pending: Option<Receiver<Option<Vec<u8>>>>, budget: Duration) -> Option<Vec<u8>> {
    pending?.recv_timeout(budget).ok().flatten().filter(|png| valid_png(png))
}

/// A decoded preview: straight RGBA pixels.
pub struct Decoded {
    pub size: [usize; 2],
    pub rgba: Vec<u8>,
}

/// Decode a PNG, refusing anything larger than a preview.
pub fn decode_png(bytes: &[u8]) -> Option<Decoded> {
    let mut reader = image::ImageReader::with_format(std::io::Cursor::new(bytes), image::ImageFormat::Png);
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(MAX_DECODE_SIDE);
    limits.max_image_height = Some(MAX_DECODE_SIDE);
    limits.max_alloc = Some(32 << 20);
    reader.limits(limits);
    let img = reader.decode().ok()?.to_rgba8();
    let (w, h) = img.dimensions();
    let size = [usize::try_from(w).ok()?, usize::try_from(h).ok()?];
    let rgba = img.into_raw();
    (size[0] > 0 && size[1] > 0 && size[0].checked_mul(size[1])?.checked_mul(4)? == rgba.len()).then_some(Decoded { size, rgba })
}

/// Read and decode the preview file at `path` (bounded: a preview is at most [`MAX_PNG_BYTES`]).
pub fn decode_file(path: &Path) -> Option<Decoded> {
    let mut bytes = Vec::new();
    std::fs::File::open(path).ok()?.take(MAX_PNG_BYTES as u64 + 1).read_to_end(&mut bytes).ok()?;
    if !valid_png(&bytes) {
        return None;
    }
    decode_png(&bytes)
}

enum Slot {
    Pending,
    Failed,
    Ready(egui::TextureHandle),
}
struct Entry {
    slot: Slot,
    used: u64,
}
struct Decoder {
    requests: Sender<(Uuid, PathBuf)>,
    results: Receiver<(Uuid, Option<Decoded>)>,
    inflight: usize,
}

/// Textures for cards, by revision id (revisions never change, so neither does their preview).
#[derive(Default)]
pub struct Thumbs {
    decoder: Option<Decoder>,
    entries: HashMap<Uuid, Entry>,
    clock: u64,
}

impl Thumbs {
    /// Textures held now.
    pub fn len(&self) -> usize {
        self.entries.values().filter(|e| matches!(e.slot, Slot::Ready(_))).count()
    }

    /// Upload what the decoder finished. Whether anything is still on its way.
    pub fn poll(&mut self, ctx: &egui::Context) -> bool {
        let mut done = Vec::new();
        if let Some(d) = &mut self.decoder {
            while let Ok(result) = d.results.try_recv() {
                d.inflight = d.inflight.saturating_sub(1);
                done.push(result);
            }
        }
        for (version, decoded) in done {
            self.finish(ctx, version, decoded);
        }
        self.decoder.as_ref().is_some_and(|d| d.inflight > 0)
    }

    fn finish(&mut self, ctx: &egui::Context, version: Uuid, decoded: Option<Decoded>) {
        let clock = self.clock;
        let slot = match decoded {
            Some(d) => {
                let image = egui::ColorImage::from_rgba_unmultiplied(d.size, &d.rgba);
                Slot::Ready(ctx.load_texture(format!("cloud-preview-{version}"), image, egui::TextureOptions::LINEAR))
            }
            None => Slot::Failed,
        };
        self.entries.insert(version, Entry { slot, used: clock });
        // Keep memory bounded: the least recently shown textures go first.
        while self.len() > MAX_TEXTURES {
            let oldest = self.entries.iter().filter(|(_, e)| matches!(e.slot, Slot::Ready(_))).min_by_key(|(_, e)| e.used).map(|(v, _)| *v);
            match oldest {
                Some(v) => {
                    self.entries.remove(&v);
                }
                None => break,
            }
        }
    }

    /// The texture of the preview of `version`, started decoding when first asked for (`path`: the
    /// verified file, if the revision has one). `None` while it is on its way, failed or absent.
    pub fn texture(&mut self, ctx: &egui::Context, version: Uuid, path: Option<&Path>) -> Option<&egui::TextureHandle> {
        self.clock = self.clock.wrapping_add(1);
        if !self.entries.contains_key(&version) {
            let path = path?;
            self.request(ctx, version, path);
        }
        let entry = self.entries.get_mut(&version)?;
        entry.used = self.clock;
        match &entry.slot {
            Slot::Ready(texture) => Some(texture),
            Slot::Pending | Slot::Failed => None,
        }
    }

    fn request(&mut self, ctx: &egui::Context, version: Uuid, path: &Path) {
        // Unit tests have no thread to wait for: decode in line.
        if cfg!(test) {
            let decoded = decode_file(path);
            self.finish(ctx, version, decoded);
            return;
        }
        if self.decoder.is_none() {
            self.decoder = spawn_decoder(ctx);
        }
        let Some(d) = &mut self.decoder else {
            self.entries.insert(version, Entry { slot: Slot::Failed, used: self.clock });
            return;
        };
        if d.inflight >= MAX_INFLIGHT {
            // Asked again on a later frame.
            return;
        }
        if d.requests.send((version, path.to_path_buf())).is_ok() {
            d.inflight += 1;
            self.entries.insert(version, Entry { slot: Slot::Pending, used: self.clock });
        } else {
            self.entries.insert(version, Entry { slot: Slot::Failed, used: self.clock });
        }
    }
}

fn spawn_decoder(ctx: &egui::Context) -> Option<Decoder> {
    let (requests, rx) = mpsc::channel::<(Uuid, PathBuf)>();
    let (tx, results) = mpsc::channel();
    let ctx = ctx.clone();
    std::thread::Builder::new()
        .name("vectorcraft-cloud-thumbs".into())
        .spawn(move || {
            while let Ok((version, path)) = rx.recv() {
                let decoded = vectorcraft_engine::guard::catch_panic(|| decode_file(&path)).ok().flatten();
                if tx.send((version, decoded)).is_err() {
                    break;
                }
                ctx.request_repaint();
            }
        })
        .ok()?;
    Some(Decoder { requests, results, inflight: 0 })
}

/// Time left of `budget` since `started`.
pub fn remaining(started: Instant, budget: Duration) -> Duration {
    budget.saturating_sub(started.elapsed())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc_with_art(w: f64, h: f64) -> Document {
        let mut s = vectorcraft_engine::Session::new();
        s.execute("file.new", &serde_json::json!({"width": w, "height": h})).unwrap();
        s.execute("shape.rectangle", &serde_json::json!({"x": 0, "y": 0, "width": w / 2.0, "height": h / 2.0})).unwrap();
        (*s.doc().unwrap().doc).clone()
    }

    #[test]
    fn previews_are_pngs_with_the_longest_side_capped_and_small() {
        let mut renderer = Renderer::new();
        // Wide, tall, small and huge artboards all end up with a 256 px long side.
        let mut docs: Vec<(f64, f64, Document)> =
            [(1920.0, 1080.0), (100.0, 4000.0), (50.0, 50.0)].into_iter().map(|(w, h)| (w, h, doc_with_art(w, h))).collect();
        docs.extend([(60000.0, 30000.0), (0.5, 0.25)].into_iter().map(|(w, h)| (w, h, Document::new(w, h))));
        for (w, h, doc) in docs {
            let png = render_png(&mut renderer, &doc).unwrap_or_else(|| panic!("no preview for {w}x{h}"));
            assert!(png.starts_with(PNG_SIGNATURE) && png.len() <= MAX_PNG_BYTES, "{w}x{h}");
            let decoded = decode_png(&png).unwrap();
            assert!(decoded.size.iter().all(|s| (1..=MAX_SIDE as usize).contains(s)), "{w}x{h}: {:?}", decoded.size);
            assert_eq!(decoded.size.iter().copied().max(), Some(MAX_SIDE as usize), "{w}x{h}: the long side is used in full");
        }
    }

    #[test]
    fn a_preview_that_cannot_be_made_is_none_not_an_error() {
        let mut renderer = Renderer::new();
        // No artboard and no art, a degenerate artboard, a non-finite one.
        let mut empty = Document::new(100.0, 100.0);
        empty.artboards.clear();
        assert!(render_png(&mut renderer, &empty).is_none());
        let mut flat = Document::new(100.0, 100.0);
        if let Some(a) = flat.artboards.first_mut() {
            a.rect = vectorcraft_geom::Rect::new(0.0, 0.0, 0.0, 50.0);
        }
        assert!(render_png(&mut renderer, &flat).is_none());
        let mut nan = Document::new(100.0, 100.0);
        if let Some(a) = nan.artboards.first_mut() {
            a.rect = vectorcraft_geom::Rect::new(0.0, 0.0, f64::NAN, 50.0);
        }
        assert!(render_png(&mut renderer, &nan).is_none());
        // A reply that never comes is given up on once the budget is spent.
        let (_keep_open, rx) = mpsc::channel::<Option<Vec<u8>>>();
        let started = Instant::now();
        assert!(collect(Some(rx), Duration::from_millis(30)).is_none());
        assert!(started.elapsed() < Duration::from_secs(5));
        // So is a reply that is not a preview the client would take.
        let (tx, rx) = mpsc::channel();
        tx.send(Some(b"not a png".to_vec())).unwrap();
        assert!(collect(Some(rx), Duration::from_millis(30)).is_none());
        let (tx, rx) = mpsc::channel();
        tx.send(Some([PNG_SIGNATURE, &vec![0; MAX_PNG_BYTES]].concat())).unwrap();
        assert!(collect(Some(rx), Duration::from_millis(30)).is_none(), "over 512 KiB");
        assert!(collect(None, Duration::from_millis(30)).is_none());
    }

    #[test]
    fn the_worker_renders_one_preview_at_a_time_and_survives_unusable_documents() {
        let worker = PreviewWorker::spawn().unwrap();
        let doc = Arc::new(doc_with_art(300.0, 200.0));
        let rx = worker.request(doc.clone()).unwrap();
        // While it is busy, a second request gets nothing and the caller carries on without a preview.
        let second = worker.request(doc.clone());
        let png = collect(Some(rx), Duration::from_secs(60)).unwrap();
        assert!(valid_png(&png));
        if let Some(rx) = second {
            drop(rx);
        }
        // An unusable document answers None, and the worker still works afterwards.
        let mut broken = Document::new(10.0, 10.0);
        broken.artboards.clear();
        for _ in 0..200 {
            if let Some(rx) = worker.request(Arc::new(broken.clone())) {
                assert!(collect(Some(rx), Duration::from_secs(60)).is_none());
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        for _ in 0..200 {
            if let Some(rx) = worker.request(doc.clone()) {
                assert!(collect(Some(rx), Duration::from_secs(60)).is_some());
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("the worker never became free");
    }

    #[test]
    fn decoding_refuses_what_is_not_a_small_png() {
        assert!(decode_png(b"nope").is_none());
        assert!(decode_png(&[PNG_SIGNATURE, b"garbage"].concat()).is_none());
        let mut big = std::io::Cursor::new(Vec::new());
        image::RgbaImage::from_pixel(MAX_DECODE_SIDE + 1, 2, image::Rgba([1, 2, 3, 255])).write_to(&mut big, image::ImageFormat::Png).unwrap();
        assert!(decode_png(&big.into_inner()).is_none(), "wider than any preview");
        let mut ok = std::io::Cursor::new(Vec::new());
        image::RgbaImage::from_pixel(8, 4, image::Rgba([1, 2, 3, 255])).write_to(&mut ok, image::ImageFormat::Png).unwrap();
        assert_eq!(decode_png(&ok.into_inner()).map(|d| d.size), Some([8, 4]));
        assert!(decode_file(Path::new("/definitely/not/here.png")).is_none());
    }

    #[test]
    fn textures_are_decoded_once_and_bounded() {
        let dir = std::env::temp_dir().join(format!("vector-cloud-thumbs-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut png = std::io::Cursor::new(Vec::new());
        image::RgbaImage::from_pixel(8, 8, image::Rgba([9, 9, 9, 255])).write_to(&mut png, image::ImageFormat::Png).unwrap();
        let file = dir.join("p.png");
        std::fs::write(&file, png.into_inner()).unwrap();
        let bad = dir.join("bad.png");
        std::fs::write(&bad, b"not a png").unwrap();
        let ctx = egui::Context::default();
        let mut thumbs = Thumbs::default();
        for i in 0..(MAX_TEXTURES as u128 + 10) {
            assert!(thumbs.texture(&ctx, Uuid::from_u128(i + 1), Some(&file)).is_some());
        }
        assert_eq!(thumbs.len(), MAX_TEXTURES);
        // A preview that fails to decode shows the placeholder and is not tried again.
        assert!(thumbs.texture(&ctx, Uuid::from_u128(9999), Some(&bad)).is_none());
        assert!(thumbs.texture(&ctx, Uuid::from_u128(9999), Some(&file)).is_none());
        assert!(thumbs.texture(&ctx, Uuid::from_u128(10000), None).is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
