use super::{FileKey, TaggedLog, DebugRFC3339, DataFile};
use crate::icon;
use crate::gfs;
use crate::grib;
use std::sync::Arc;
use lazy_static::*;
use serde_derive::Serialize;
use futures::{future, Future, FutureExt, TryFutureExt, stream, StreamExt, TryStreamExt};

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
            let ofk = icon::filename_to_filekey(fname)
                .or_else(|| gfs::filename_to_filekey(fname));
            println!("entry: {:?} -> {:?}", f, ofk);
            if let Some(fk) = ofk {
                hm.insert(fk, CacheEntry(fname.to_owned()));
            }
        });

    Arc::new(std::sync::Mutex::new(hm))
}

type SharedFut<T> = future::Shared<Box<
    dyn Future<Output=Result<Arc<T>, String>> + Unpin + Send
>>;

pub type MemCache<K, V> = Arc<std::sync::Mutex<std::collections::HashMap<
    K,
    (chrono::DateTime<chrono::Utc>, SharedFut<V>)
>>>;

pub fn purge_mem_cache() {
    let now = chrono::Utc::now();
    if let Ok(mut g) = MEM_CACHE.lock() {
        g.retain(move |key, (insert_time, _)| {
            let keep = now - *insert_time < chrono::Duration::minutes(1);
            if !keep {
                println!("remove cache future {:?}", key)
            }
            keep
        });
    }
}

pub fn purge_disk_cache() {
    let deadline = chrono::Utc::now() - chrono::Duration::hours(1);
    if let Ok(mut g) = DISK_CACHE.lock() {
        g.retain(|k, _| k.get_modelrun_tm() + chrono::Duration::hours(i64::from(k.timestep)) > deadline);
    }
}

type DiskCache<K> = Arc<std::sync::Mutex<std::collections::HashMap<
    K,
    CacheEntry
>>>;

lazy_static! {
    static ref DISK_CACHE: DiskCache<FileKey> = pickup_disk_cache();
    static ref MEM_CACHE: MemCache<FileKey, grib::GribMessage> = Arc::new(std::sync::Mutex::new(std::collections::HashMap::default()));
}

#[derive(Serialize)]
pub struct CacheStats {
    disk_cache: Vec<FileKey>,
    mem_cache: Vec<FileKey>,
}

pub fn stats() -> CacheStats {
    CacheStats {
        disk_cache: DISK_CACHE.lock().unwrap().keys().cloned().collect(),
        mem_cache: MEM_CACHE.lock().unwrap().keys().cloned().collect(),
    }
}

pub fn avail_grid(
    log: Arc<TaggedLog>, file_key: FileKey
) -> impl Future<Output=Result<(), String>> {
    if let Some((_, sh)) = MEM_CACHE.lock().unwrap().get(&file_key) {
        match sh.peek() {
            Some(Ok(_)) => future::ok( () ).left_future(),
            Some(Err(e)) => future::err(e.clone()).left_future(),
            None => make_avail_grid_fut(log, file_key).right_future(),
        }
    } else {
        make_avail_grid_fut(log, file_key).right_future()
    }
}

pub fn fetch_grid(
    log: Arc<TaggedLog>, file_key: FileKey
) -> impl Future<Output=Result<Arc<grib::GribMessage>, String>> {
    MEM_CACHE
        .lock()
        .unwrap()
        .entry(file_key.clone())
        .or_insert_with(move || (chrono::Utc::now(), make_fetch_grid_fut(log, file_key).shared()))
        .1
        .clone()
        .map_ok(|x| { let xd: &Arc<grib::GribMessage> = &x; xd.clone() })
        .map_err(|e| { let ed: &String = &e; ed.clone() })
}

#[test]
fn simultaneous_fetch() {

    let key = FileKey::test_new();
    let log = Arc::new(TaggedLog {tag: "test".to_owned()});

    let task = future::try_join4(
        fetch_grid(log.clone(), key.clone()),
        fetch_grid(log.clone(), key.clone()),
        fetch_grid(log.clone(), key.clone()),
        fetch_grid(log.clone(), key.clone()),
    )
    .map_ok(|_| ())
    .map_err(|e| println!("error: {}", e));

    tokio::spawn(task.map(|_| ()));
}

fn save_to_file(bytes: &[u8], path: &str) -> Result<(), String> {
    use std::io::Write;

    let tempname = format!("{}.temp", path);
    std::fs::File::create(&tempname) // TODO:
        .map_err(|e| format!("create tempfile failed: {}", e))
        .and_then(|f| {
            std::io::BufWriter::new(f)
                .write_all(bytes)
                .map_err(|e| format!("write to tempfile failed: {}", e))
        })
        .and_then(move |()| {
            std::fs::rename(tempname, path)
                .map_err(|e| format!("rename failed: {:?}", e))
        })
}

fn parse_grib2(bytes: &[u8]) -> impl Future<Output=Result<(grib::GribMessage, usize), String>> {
    let res = grib::parse_message(bytes)
        .map(move |m| (m.1, m.0.len()))
        .map_err(|e| match e {
            nom::Err::Error(nom::Context::Code(i, ek)) =>
                format!(
                    "grib parse failed: {:?} (at +{}) {:?}",
                    &i[..std::cmp::min(i.len(), 20)], i.as_ptr() as usize - bytes.as_ptr() as usize, ek
                ),
            _ => e.to_string(),
        });
    future::ready(res) // TODO:
}

fn download_grid_fut(
    log: Arc<TaggedLog>, data_file: Box<dyn DataFile>
) -> impl Future<Output=Result<Vec<u8>, String>> {

    let from = data_file.available_from();
    let to = data_file.available_to();
    let now = chrono::Utc::now();

    let (desc, action) = if now < from {
        ("NOT YET", Some((from, to)))
    } else if now >= from && now < to {
        ("LET'S GO", Some((now, to)))
    } else {
        ("TOO OLD", None)
    };

    log.add_line(&format!("avail from {} to {}, (now {}, so `{}`)",
                from.to_rfc3339_debug(), to.to_rfc3339_debug(), now.to_rfc3339_debug(), desc
    ));

    let res: Result<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>), String> =
        action.ok_or_else(|| "file is no longer available".to_owned());

    future::ready(res)
        .and_then(move |(from, to)| {

            let attempt_schedule = (0..)
                .map(move |i| from + chrono::Duration::minutes(i * 10))
                .take_while(move |t| *t < to);

            let data_file = Arc::new(data_file);

            stream::iter(attempt_schedule)
                .then({ let log = log.clone(); move |t| {
                    let now = chrono::Utc::now();
                    log.add_line(&format!("wait until: {}, now: {}", t.to_rfc3339_debug(), now.to_rfc3339_debug()));
                    
                    if t > now {
                        tokio::time::delay_for((t - now).to_std().unwrap()).left_future()
                    } else {
                        future::ready( () ).right_future()
                    }
                    .then({
                        let log = log.clone(); let data_file = data_file.clone();
                        move |()| data_file.fetch_bytes(log)
                    })
                }})
                .inspect_err(move |e| log.add_line(&format!("attempt stream err: {}", e)))
                .filter_map(|item: Result<Vec<u8>, String>| future::ready(item.ok()))
                .into_future()
                .then(|x: (Option<Vec<u8>>, _)| future::ready(x.0.ok_or_else(|| "give up, file did not appear".to_owned())))
        })
}

fn make_avail_grid_fut(
    log: Arc<TaggedLog>, file_key: FileKey
) -> Box<dyn Future<Output=Result<(), String>> + Send + Unpin> {
    let icon_file = Arc::new(icon::IconFile::new(&file_key));
    let path = icon_file.cache_filename().to_owned();

    log.add_line(&format!("avail grid: {}...", &path));

    let res = std::fs::File::open(&path)
        .map_err(|e| format!("file open failed: {}", e))
        .map(|_| {
            log.add_line("cache hit!");
            ()
        });

    let fut = future::ready(res) // TODO:
        .or_else(move |e: String| {
            let log = log.clone();
            log.add_line(&format!("cache miss: {}", &e));
            icon_file.check_avail(log)
        });

    Box::new(fut)
}

pub fn make_fetch_grid_fut(
    log: Arc<TaggedLog>, file_key: FileKey
) -> Box<dyn Future<Output=Result<Arc<grib::GribMessage>, String>> + Send + Unpin> {
    use std::io::Read;

    let data_file = file_key.build_data_file();
    let path = data_file.cache_filename().to_owned();

    log.add_line(&format!("fetch grid: {}...", &path));

    let res = std::fs::File::open(&path)
        .map_err(|e| format!("file open failed: {}", e))
        .and_then(|f| {
            log.add_line("cache hit!");
            let mut v = Vec::new();
            std::io::BufReader::new(f)
                .read_to_end(&mut v)
                .map_err(|e| format!("file read failed: {}", e))
                .map(move |_n| v)
        });

    // TODO: do this to make sure lazy init (pickup) occurs before any new file saves.
    let _res = DISK_CACHE.lock().map_err(|e| e.to_string()).map(|_| ());

    let fut = future::ready(res) // TODO:
        .or_else(move |e: String| {
            let log = log.clone();
            log.add_line(&format!("cache miss: {}", &e));
            download_grid_fut(log.clone(), data_file)
                .map_ok(move |v: Vec<u8>| {
                    let res = save_to_file(&v, &path)
                        .and_then(|()| {
                            DISK_CACHE.lock().map_err(|e| e.to_string()).and_then(|mut g| {
                                if g.insert(file_key, CacheEntry(path)).is_some() {
                                    Err("DISK CACHE already had this item".to_owned())
                                } else {
                                    Ok( () )
                                }
                            })
                        });
                    log.add_line(&format!("save to cache: {:?}", res));
                    v
                })
        })
        .and_then(|grib2: Vec<u8>| parse_grib2(&grib2))
        .map_ok(|(g, _)| Arc::new(g));

    Box::new(fut)
}
