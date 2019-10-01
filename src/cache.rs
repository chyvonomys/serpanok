use super::FileKey;
use icon;
use std::sync::Arc;

struct CacheEntry(String);

impl Drop for CacheEntry {
    fn drop(&mut self) {
        println!("deleting file {} -> {}", self.0, std::fs::remove_file(&self.0).is_ok());
    }
}

fn pickup_disk_cache() -> DiskCache<FileKey> {
    let mut hm = std::collections::HashMap::new();

    std::fs::read_dir("./").unwrap()
        .filter_map(|r| r.ok())
        .map(|e| e.file_name())
        .for_each(|f| {
            let fname = f.to_str().unwrap();
            let ofk = icon::filename_to_filekey(fname);
            println!("entry: {:?} -> {:?}", f, ofk);
            if let Some(fk) = ofk {
                hm.insert(fk, CacheEntry(fname.to_owned()));
            }
        });

    Arc::new(std::sync::Mutex::new(hm))
}

use futures::{future, Future, stream};

type SharedFut<T> = future::Shared<Box<
    dyn Future<
        Item = Arc<T>,
        Error = String
    > + Send
>>;

pub type MemCache<K, V> = Arc<std::sync::Mutex<std::collections::HashMap<
    K,
    (chrono::DateTime<chrono::Utc>, SharedFut<V>)
>>>;

pub fn purge_mem_cache() {
    let now = chrono::Utc::now();
    MEM_CACHE.lock().unwrap()
        .retain(move |key, (insert_time, _)| {
            let keep = now - *insert_time < chrono::Duration::minutes(1);
            if !keep {
                println!("remove cache future {:?}", key)
            }
            keep
        })
}

pub fn purge_disk_cache() {
    let deadline = chrono::Utc::now() - chrono::Duration::hours(1);
    DISK_CACHE.lock().unwrap()
        .retain(|k, _| k.get_modelrun_tm() + chrono::Duration::hours(k.timestep as i64) > deadline)
}

type DiskCache<K> = Arc<std::sync::Mutex<std::collections::HashMap<
    K,
    CacheEntry
>>>;

lazy_static! {
    static ref DISK_CACHE: DiskCache<FileKey> = pickup_disk_cache();
    static ref MEM_CACHE: MemCache<FileKey, grib::GribMessage> = Arc::new(std::sync::Mutex::new(std::collections::HashMap::default()));
}

pub fn disk_stats() -> Vec<FileKey> {
    DISK_CACHE.lock().unwrap().keys().cloned().collect()
}

pub fn mem_stats() -> Vec<FileKey> {
    MEM_CACHE.lock().unwrap().keys().cloned().collect()
}

pub fn fetch_grid(
    log: Arc<TaggedLog>, file_key: FileKey
) -> impl Future<Item=Arc<grib::GribMessage>, Error=String > {
    MEM_CACHE
        .lock()
        .unwrap()
        .entry(file_key.clone())
        .or_insert_with(move || (chrono::Utc::now(), make_fetch_grid_fut(log, file_key).shared()))
        .1
        .clone()
        .map(|x| { let xd: &Arc<grib::GribMessage> = &x; xd.clone() })
        .map_err(|e| { let ed: &String = &e; ed.clone() })
}

#[test]
fn simultaneous_fetch() {

    let key = FileKey::test_new();
    let log = Arc::new(TaggedLog {tag: "test".to_owned()});

    let task = Future::join4(
        fetch_grid(log.clone(), key.clone()),
        fetch_grid(log.clone(), key.clone()),
        fetch_grid(log.clone(), key.clone()),
        fetch_grid(log.clone(), key.clone()),
    )
    .map(|_| ())
    .map_err(|e| println!("error: {}", e));

    tokio::run(task);
}

use std::io::Write;

fn save_to_file(bytes: &[u8], path: &str) -> Result<(), String> {
    std::fs::File::create("tempfile") // TODO:
        .map_err(|e| format!("create tempfile failed: {}", e))
        .and_then(|f| {
            std::io::BufWriter::new(f)
                .write_all(bytes)
                .map_err(|e| format!("write to tempfile failed: {}", e))
        })
        .and_then(|()| {
            std::fs::rename("tempfile", path)
                .map_err(|e| format!("rename failed: {:?}", e))
        })
}

use grib;

fn parse_grib2(bytes: &[u8]) -> impl Future<Item=(grib::GribMessage, usize), Error=String> {
    let res = grib::parse_message(bytes)
        .map(move |m| (m.1, m.0.len()))
        .map_err(|e| match e {
            nom::Err::Error(nom::Context::Code(i, ek)) =>
                format!("grib parse failed: {:?} {:?}", &i[..std::cmp::min(i.len(), 10)], ek),
            _ => e.to_string(),
        });
    future::result(res) // TODO:
}

use super::TaggedLog;
use futures::Stream;

fn download_grid_fut(
    log: Arc<TaggedLog>, icon_file: Arc<icon::IconFile>
) -> impl Future<Item=Vec<u8>, Error=String> {

    let from = icon_file.available_from();
    let to = icon_file.available_to();
    let now = chrono::Utc::now();

    let (desc, action) = if now < from {
        ("wait until model runs and files appear", Some((from, to)))
    } else if now >= from && now < to {
        ("model has run, poll while makes sense", Some((now, to)))
    } else {
        ("too old, skip, go to next", None)
    };

    log.add_line(&format!("file should be available from {} to {}, (now {}, so `{}`)",
                from.to_rfc3339(), to.to_rfc3339(), now.to_rfc3339(), desc
    ));

    let res: Result<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>), String> = action.ok_or("file is no longer available".to_owned());
    future::result(res)
        .and_then(move |(from, to)| {

            let attempt_schedule = (0..)
                .map(move |i| from + chrono::Duration::minutes(i * 10))
                .take_while(move |t| t < &to);

            stream::iter_ok(attempt_schedule)
                .and_then({ let log = log.clone(); move |t| {
                    let now = chrono::Utc::now();
                    log.add_line(&format!("wait until: {}, now: {}", t.to_rfc3339(), now.to_rfc3339()));
                    let wait = if t > now {
                        t - now
                    } else {
                        chrono::Duration::seconds(0)
                    };

                    // TODO: skip 0-wait?

                    tokio::timer::Delay::new(std::time::Instant::now() + wait.to_std().unwrap())
                        .map_err(|e| format!("delay error: {}", e)) // TODO: mixed errors, some are acceptable for retry, other not
                        .and_then({ let log = log.clone(); let icon_file = icon_file.clone(); move |_| {
                            log.add_line(&format!("attempt at {}", chrono::Utc::now().to_rfc3339()));
                            icon_file.fetch_bytes(log)
                        }})
                }})
                .inspect_err(move |e| log.add_line(&format!("inspect err: {}", e)))
                .then(|result: Result<Vec<u8>, String>| future::ok(result)) // stream of vec --> stream of results
                .filter_map(|item: Result<Vec<u8>, String>| item.ok())
                .into_future()
                .map_err(|x: (String, _)| x.0)
                .and_then(|x: (Option<Vec<u8>>, _)| future::result(x.0.ok_or("give up, file did not appear".to_owned())))
        })
}

use std::io::Read;

fn make_fetch_grid_fut(
    log: Arc<TaggedLog>, file_key: FileKey
) -> Box<dyn Future<Item=Arc<grib::GribMessage>, Error=String> + Send> {

    let icon_file = Arc::new(icon::IconFile::new(file_key));

    log.add_line(&format!("fetch grid: {}...", icon_file.cache_filename()));
    let res = std::fs::File::open(icon_file.cache_filename())
        .map_err(|e| format!("file open failed: {}", e))
        .and_then(|f| {
            log.add_line("cache hit!");
            let mut v = Vec::new();
            std::io::BufReader::new(f)
                .read_to_end(&mut v)
                .map_err(|e| format!("file read failed: {}", e))
                .map(move |_n| v)
        });

    let fut = future::result(res) // TODO:
        .or_else(move |e: String| {
            let log = log.clone();
            log.add_line(&format!("cache miss: {}", &e));
            download_grid_fut(log.clone(), icon_file.clone())
                .map(move |v: Vec<u8>| {
                    let res = save_to_file(&v, icon_file.cache_filename());
                    log.add_line(&format!("save to cache: {:?}", res));
                    v
                })
        })
        .and_then(|grib2: Vec<u8>| parse_grib2(&grib2))
        .map(|(g, _)| Arc::new(g));

    Box::new(fut)
}
