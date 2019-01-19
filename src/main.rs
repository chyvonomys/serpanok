#![recursion_limit="128"]
#![type_length_limit="2097152"]
extern crate futures;
extern crate tokio;
extern crate time;
extern crate itertools;
#[macro_use] extern crate nom;
extern crate bzip2;
extern crate reqwest;
extern crate either;
#[macro_use] extern crate lazy_static;
extern crate hyper;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;
extern crate regex;

trait Timeseries {
    fn to_u8(&self) -> u8;
}

impl Timeseries for u8 {
    fn to_u8(&self) -> u8 {
        *self
    }
}

fn icon_timestep_iter(mr: u8) -> impl Iterator<Item=u8> {
    if mr % 6 == 0 {
        either::Either::Left(Iterator::chain(
            0u8..78,
            (78..=120).step(3)
        ))
    } else {
        either::Either::Right(0u8..=30)
    }
}

fn icon_modelrun_iter() -> impl Iterator<Item=u8> {
    (0u8..=21).step(3)
}

fn trunc_days_utc(t: time::Tm) -> time::Tm {
    let tu = t.to_utc();
    let mut res = time::empty_tm();
    res.tm_year = tu.tm_year;
    res.tm_mon =  tu.tm_mon;
    res.tm_mday = tu.tm_mday;
    res.tm_yday = tu.tm_yday;
    res.tm_wday = tu.tm_wday;
    res
}

fn trunc_hours_utc(t: time::Tm) -> time::Tm {
    let tu = t.to_utc();
    let mut res = time::empty_tm();
    res.tm_year = tu.tm_year;
    res.tm_mon =  tu.tm_mon;
    res.tm_mday = tu.tm_mday;
    res.tm_yday = tu.tm_yday;
    res.tm_wday = tu.tm_wday;
    res.tm_hour = tu.tm_hour;
    res
}

fn forecast_iterator<MR, TS, TSI, MRI, MRIF, TSIF>(
    start_time: time::Tm, target_time: time::Tm, mri: MRIF, tsi: TSIF
) -> impl Iterator<Item=(time::Tm, MR, time::Tm, TS, time::Tm, TS)>
where
    MR: Timeseries + Clone,
    TS: Timeseries + Clone,
    TSI: Iterator<Item=TS>,
    MRI: Iterator<Item=MR>,
    MRIF: Fn() -> MRI,
    TSIF: Fn(MR) -> TSI,
{

    let start_time_d = trunc_days_utc(start_time); // trunc to days
    let start_time_h = trunc_hours_utc(start_time); // trunc to hours

    (0..)
        .flat_map(move |day| mri().map(
            move |hh| (start_time_d + time::Duration::hours(day * 24 + hh.to_u8() as i64), hh)
        ))
        .tuple_windows::<(_, _)>()
        .skip_while(move |(_, t)| t.0 <= start_time_h)
        .map(|(f, _)| f)
        .take_while(move |(hhtime, _)| target_time >= *hhtime)
        .map(move |(hhtime, hh)| {
            let opt = tsi(hh.clone())
             .map(|ts| (hhtime + time::Duration::hours(ts.to_u8() as i64), ts))
             .tuple_windows::<(_, _)>()
             .take_while(|(f, _)| target_time >= f.0)
             .skip_while(|(_, t)| target_time >= t.0)
             .next();
            (hhtime, hh, opt)
        })
        .filter_map(|(s, mr, o)| o.map(|(x0, x1)| (s, mr, x0, x1)))
        .map(|(s, mr, ts, ts1)| (s, mr, ts.0, ts.1, ts1.0, ts1.1))
}

use itertools::Itertools;
use std::io::{Read, Write};

mod grib;

fn unpack_bzip2(bytes: &[u8]) -> impl Future<Item=Vec<u8>, Error=String> {
    let mut vec = Vec::new();
    let res = bzip2::read::BzDecoder::new(bytes)
        .read_to_end(&mut vec)
        .map(move |_sz| vec)
        .map_err(|e| format!("unpack error: {:?}", e));
    future::result(res) // TODO:
}

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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
struct FileKey {
    param: Parameter,
    yyyy: u16,
    mm: u8,
    dd: u8,
    modelrun: u8,
    timestep: u8,
}

impl FileKey {
    fn new(param: Parameter, tm: time::Tm, modelrun: u8, timestep: u8) -> Self {
        Self {
            param,
            yyyy: tm.tm_year as u16 + 1900,
            mm: tm.tm_mon as u8+ 1,
            dd: tm.tm_mday as u8,
            modelrun,
            timestep,
        }
    }

    fn get_modelrun_tm(&self) -> time::Tm {
        let mut res = time::empty_tm();
        res.tm_year = self.yyyy as i32 - 1900;
        res.tm_mon = self.mm as i32 - 1;
        res.tm_mday = self.dd as i32;
        res.tm_hour = self.modelrun as i32;
        res
    }
}

fn filename_to_filekey(filename: &str) -> Option<FileKey> {
    lazy_static! {
        static ref RE: regex::Regex = regex::Regex::new(
            r"^icon-eu_europe_regular-lat-lon_single-level_(\d{4})(\d{2})(\d{2})(\d{2})_(\d{3})_([0-9_A-Z]+).grib2$"
        ).unwrap();
    }

    RE.captures(filename).and_then(|cs| {
        let oyyyy = cs.get(1).and_then(|x| x.as_str().parse::<u16>().ok());
        let omm = cs.get(2).and_then(|x| x.as_str().parse::<u8>().ok());
        let odd = cs.get(3).and_then(|x| x.as_str().parse::<u8>().ok());
        let omr = cs.get(4).and_then(|x| x.as_str().parse::<u8>().ok());
        let ots = cs.get(5).and_then(|x| x.as_str().parse::<u8>().ok());
        let op = cs.get(6).map(|x| x.as_str());
        if let (Some(yyyy), Some(mm), Some(dd), Some(modelrun), Some(timestep), Some(p)) = (oyyyy, omm, odd, omr, ots, op) {
            match p {
                "T_2M"     => Some(Parameter::Temperature2m),
                "U_10M"    => Some(Parameter::WindSpeedU10m),
                "V_10M"    => Some(Parameter::WindSpeedV10m),
                "CLCT"     => Some(Parameter::TotalCloudCover),
                "TOT_PREC" => Some(Parameter::TotalAccumPrecip),
                "SNOW_CON" => Some(Parameter::ConvectiveSnow),
                "RAIN_CON" => Some(Parameter::ConvectiveRain),
                "SNOW_GSP" => Some(Parameter::LargeScaleSnow),
                "RAIN_GSP" => Some(Parameter::LargeScaleRain),
                "H_SNOW"   => Some(Parameter::SnowDepth),
                _ => None
            }.map(move |param| FileKey {yyyy, mm, dd, modelrun, timestep, param})
        } else {
            None
        }
    })
}

struct CacheEntry(String);

impl Drop for CacheEntry {
    fn drop(&mut self) {
        println!("deleting file {} -> {}", self.0, std::fs::remove_file(&self.0).is_ok());
    }
}

fn pickup_disk_cache() -> DiskCache {
    let mut hm = std::collections::HashMap::new();

    std::fs::read_dir("./").unwrap()
        .filter_map(|r| r.ok())
        .map(|e| e.file_name())
        .for_each(|f| {
            let fname = f.to_str().unwrap();
            let ofk = filename_to_filekey(fname);
            println!("entry: {:?} -> {:?}", f, ofk);
            if let Some(fk) = ofk {
                hm.insert(fk, CacheEntry(fname.to_owned()));
            }
        });

    std::sync::Arc::new(std::sync::Mutex::new(hm))
}

fn purge_disk_cache() {
    let deadline = time::now_utc() - time::Duration::hours(1);
    DISK_CACHE.lock().unwrap()
        .retain(|k, _| k.get_modelrun_tm() + time::Duration::hours(k.timestep as i64) > deadline)
}

struct IconFile {
    prefix: String,
    filename: String,
    modelrun_time: time::Tm,
}

impl IconFile {
    fn new(key: FileKey) -> Self {

        let paramstr = match key.param {
            Parameter::Temperature2m => "T_2M",
            Parameter::WindSpeedU10m => "U_10M",
            Parameter::WindSpeedV10m => "V_10M",
            Parameter::TotalCloudCover => "CLCT",
            Parameter::TotalAccumPrecip => "TOT_PREC",
            Parameter::ConvectiveSnow => "SNOW_CON",
            Parameter::ConvectiveRain => "RAIN_CON",
            Parameter::LargeScaleSnow => "SNOW_GSP",
            Parameter::LargeScaleRain => "RAIN_GSP",
            Parameter::SnowDepth => "H_SNOW",
        };

        let yyyymmdd = format!("{}{:02}{:02}", key.yyyy, key.mm, key.dd);

        Self {        
            prefix: format!("https://opendata.dwd.de/weather/nwp/icon-eu/grib/{:02}/{}/", key.modelrun, paramstr.to_lowercase()),
            filename: format!("icon-eu_europe_regular-lat-lon_single-level_{}{:02}_{:03}_{}.grib2", yyyymmdd, key.modelrun, key.timestep, paramstr),
            modelrun_time: key.get_modelrun_tm(),
        }
    }

    fn cache_filename(&self) -> &str {
        &self.filename
    }

    // ICON specific: download and unpack
    fn fetch_bytes(&self, sub_id: (i64, i32), http_client: &'static reqwest::async::Client) -> impl Future<Item=Vec<u8>, Error=String> {
        fetch_url(sub_id, format!("{}{}.bz2", self.prefix, self.filename), http_client)
            .and_then(|bzip2: Vec<u8>| unpack_bzip2(&bzip2))
    }

    fn available_from(&self) -> time::Tm {
        self.modelrun_time + time::Duration::hours(2) + time::Duration::minutes(30)
    }

    fn available_to(&self) -> time::Tm {
        self.modelrun_time + time::Duration::hours(26) + time::Duration::minutes(30)
    }
}

type SharedFut = future::Shared<Box<
    dyn Future<
        Item = std::sync::Arc<grib::GribMessage>,
        Error = String
    > + Send
>>;

type MemCache = std::sync::Arc<std::sync::Mutex<std::collections::HashMap<
    FileKey,
    (time::Tm, SharedFut)
>>>;

fn purge_mem_cache() {
    let now = time::now_utc();
    MEM_CACHE.lock().unwrap()
        .retain(move |key, (insert_time, _)| {
            let keep = now - *insert_time < time::Duration::minutes(1);
            if !keep {
                println!("remove cache future {:?}", key)
            }
            keep
        })
}

type DiskCache = std::sync::Arc<std::sync::Mutex<std::collections::HashMap<
    FileKey,
    CacheEntry
>>>;

lazy_static! {
    static ref DISK_CACHE: DiskCache = pickup_disk_cache();
    static ref MEM_CACHE: MemCache = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::default()));
}

fn fetch_grid(sub_id: (i64, i32), file_key: FileKey) -> impl Future< Item=std::sync::Arc<grib::GribMessage>, Error=String > {
    MEM_CACHE
        .lock()
        .unwrap()
        .entry(file_key.clone())
        .or_insert_with(move || (time::now_utc(), make_fetch_grid_fut(sub_id, file_key).shared()))
        .1
        .clone()
        .map(|x| { let xd: &std::sync::Arc<grib::GribMessage> = &x; xd.clone() })
        .map_err(|e| { let ed: &String = &e; ed.clone() })
}

fn download_grid_fut(sub_id: (i64, i32), icon_file: std::sync::Arc<IconFile>) -> impl Future<Item=Vec<u8>, Error=String> {

    let from = icon_file.available_from();
    let to = icon_file.available_to();
    let now = time::now_utc();

    let (desc, action) = if now < from {
        ("wait until model runs and files appear", Some((from, to)))
    } else if now >= from && now < to {
        ("model has run, poll while makes sense", Some((now, to)))
    } else {
        ("too old, skip, go to next", None)
    };

    log(sub_id, &format!("file should be available from {} to {}, (now {}, so `{}`)",
                from.rfc3339(), to.rfc3339(), now.rfc3339(), desc
    ));

    let res: Result<(time::Tm, time::Tm), String> = action.ok_or("file is no longer available".to_owned());
    future::result(res)
        .and_then(move |(from, to)| {

            let attempt_schedule = (0..)
                .map(move |i| from + time::Duration::minutes(i * 10))
                .take_while(move |t| t < &to);

            stream::iter_ok(attempt_schedule)
                .and_then(move |t| {
                    let now = time::now_utc();
                    log(sub_id, &format!("wait until: {}, now: {}", t.rfc3339(), now.rfc3339()));
                    let wait = if t > now {
                        t - now
                    } else {
                        time::Duration::seconds(0)
                    };

                    // TODO: skip 0-wait?

                    let ifile = icon_file.clone();

                    tokio::timer::Delay::new(std::time::Instant::now() + wait.to_std().unwrap())
                        .map_err(|e| format!("delay error: {}", e)) // TODO: mixed errors, some are acceptable for retry, other not
                        .and_then(move |_| {
                            log(sub_id, &format!("attempt at {}", time::now_utc().rfc3339()));
                            ifile.fetch_bytes(sub_id, &HTTP_CLIENT)
                        })
                })
                .inspect_err(move |e| log(sub_id, &format!("inspect err: {}", e)))
                .then(|result: Result<Vec<u8>, String>| future::ok(result)) // stream of vec --> stream of results
                .filter_map(|item: Result<Vec<u8>, String>| item.ok())
                .into_future()
                .map_err(|x: (String, _)| x.0)
                .and_then(|x: (Option<Vec<u8>>, _)| future::result(x.0.ok_or("give up, file did not appear".to_owned())))
        })
}

fn make_fetch_grid_fut(sub_id: (i64, i32), file_key: FileKey) -> Box<dyn Future<Item=std::sync::Arc<grib::GribMessage>, Error=String> + Send> {

    let icon_file = std::sync::Arc::new(IconFile::new(file_key));

    log(sub_id, &format!("fetch grid: {}...", icon_file.cache_filename()));
    let res = std::fs::File::open(icon_file.cache_filename())
        .map_err(|e| format!("file open failed: {}", e))
        .and_then(|f| {
            log(sub_id, "cache hit!");
            let mut v = Vec::new();
            std::io::BufReader::new(f)
                .read_to_end(&mut v)
                .map_err(|e| format!("file read failed: {}", e))
                .map(move |_n| v)
        });

    let fut = future::result(res) // TODO:
        .or_else(move |e: String| {
            log(sub_id, &format!("cache miss: {}", &e));
            download_grid_fut(sub_id, icon_file.clone())
                .map(move |v: Vec<u8>| {
                    let res = save_to_file(&v, icon_file.cache_filename());
                    log(sub_id, &format!("save to cache: {:?}", res));
                    v
                })
        })
        .and_then(|grib2: Vec<u8>| parse_grib2(&grib2))
        .map(|(g, _)| std::sync::Arc::new(g));

    Box::new(fut)
}

#[test]
fn simultaneous_fetch() {
    let key = FileKey {
        param: Parameter::Temperature2m,
        yyyy: 2018,
        mm: 11,
        dd: 24,
        modelrun: 15,
        timestep: 0,
    };

    let fake_id = (0, 0);

    let task = Future::join4(
        fetch_grid(fake_id, key.clone()),
        fetch_grid(fake_id, key.clone()),
        fetch_grid(fake_id, key.clone()),
        fetch_grid(fake_id, key.clone()),
    )
    .map(|_| ())
    .map_err(|e| println!("error: {}", e));

    tokio::run(task);
}

fn extract_value_impl(values: &[f32], s3: &grib::Section3, latf: f32, lonf: f32) -> Result<f32, String> {
    let lat = (latf * 1_000_000.0) as i32;
    let lon = (lonf * 1_000_000.0) as i32;

    let lon0 = if s3.lon_first > s3.lon_last { s3.lon_first - 360_000_000 } else { s3.lon_first };
    let lon1 = s3.lon_last;

    let lat0 = s3.lat_first;
    let lat1 = s3.lat_last;

    let lon_step = s3.di;
    let lat_step = s3.dj;

    let cols = s3.ni;

    if lat >= lat0 && lat <= lat1 && lon >= lon0 && lon <= lon1 {
        let i0 = (lon - lon0) as u32 / lon_step;
        let it = (lon - lon0) as u32 % lon_step;
        let i1 = if it == 0 { i0 } else { i0 + 1 };

        let j0 = (lat - lat0) as u32 / lat_step;
        let jt = (lat - lat0) as u32 % lat_step;
        let j1 = if jt == 0 { j0 } else { j0 + 1 };

        let x00 = values[(j0 * cols + i0) as usize];
        let x10 = values[(j0 * cols + i1) as usize];
        let x01 = values[(j1 * cols + i0) as usize];
        let x11 = values[(j1 * cols + i1) as usize];

        let xt = it as f32 / lon_step as f32;
        let yt = jt as f32 / lat_step as f32;

        let x = lerp(lerp(x00, x10, xt), lerp(x01, x11, xt), yt);

        Ok(x)
    } else {
        Err("target point in not in range outside".to_owned())
    }
}

fn extract_value_at(grib: &grib::GribMessage, latf: f32, lonf: f32) -> Result<f32, String> {
    if let grib::Packing::Simple {r, e, d, bits: 16} = grib.section5.packing {
        if let grib::CodedValues::Simple16Bit(ref v) = grib.section7.coded_values {
            let twop = 2f32.powf(e as f32);
            let tenp = 10f32.powf(-d as f32);
            Ok(v.iter().map(
                |x| (r + *x as f32 * twop) * tenp
            ).collect_vec())
        } else {
            Err("unsupported coded_values".to_owned())
        }
    } else {
        Err("unsupported packing".to_owned())
    }
    .and_then(|ys| extract_value_impl(&ys, &grib.section3, latf, lonf))
}

#[test]
fn test_extract() {
    let s3 = grib::Section3 {
        n_data_points: 720729,
		ni: 1097,
		nj: 657,
		lat_first: 29500000,
		lon_first: 336500000,
		lat_last: 70500000,
		lon_last: 45000000,
		di: 62500,
		dj: 62500,
    };

    let mut values = Vec::new();
    values.resize(s3.n_data_points as usize, 0.0);

    assert!(extract_value_impl(&values, &s3, 29.5, -23.5).is_ok());
    assert!(extract_value_impl(&values, &s3, 29.5,  45.0).is_ok());
    assert!(extract_value_impl(&values, &s3, 70.5, -23.5).is_ok());
    assert!(extract_value_impl(&values, &s3, 70.5,  45.0).is_ok());

    assert!(extract_value_impl(&values, &s3, 29.5 - 0.00001, -23.5 - 0.00001).is_err());
    assert!(extract_value_impl(&values, &s3, 29.5 - 0.00001,  45.0 + 0.00001).is_err());
    assert!(extract_value_impl(&values, &s3, 70.5 + 0.00001, -23.5 - 0.00001).is_err());
    assert!(extract_value_impl(&values, &s3, 70.5 + 0.00001,  45.0 + 0.00001).is_err());
}

fn lerp(a: f32, b: f32, t: f32) -> f32 {
    a * (1.0 - t) + b * t
}

fn fetch_url(sub_id: (i64, i32), url: String, http_client: &'static reqwest::async::Client) -> impl Future<Item=Vec<u8>, Error=String> {
    log(sub_id, &format!("GET {}", url));
    http_client
        .get(&url)
        .send()
        .map_err(move |e| format!("GET {} failed: {}", &url, e))
        .and_then(|resp| {
            let sc = resp.status();
            if sc == reqwest::StatusCode::OK {
                future::Either::A(
                    resp
                        .into_body()
                        .map_err(|e| format!("response body error: {}", e))
                        .fold(
                            Vec::new(),
                            |mut acc, x| { acc.extend_from_slice(&x); future::ok::<_, String>(acc) }
                        )
                )
            } else {
                future::Either::B(future::err(format!("response status is {}", sc)))
            }
        })
}

use futures::{future, Future, stream, Stream};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
enum Parameter {
    Temperature2m,
    TotalCloudCover,
    WindSpeedU10m,
    WindSpeedV10m,
    TotalAccumPrecip,
    ConvectiveSnow,
    LargeScaleSnow,
    ConvectiveRain,
    LargeScaleRain,
    SnowDepth,
}

fn icon_verify_parameter(param: Parameter, g: &grib::GribMessage) -> bool {
    let common = g.section4.product_def.common();
    match (param, common.parameter_cat, common.parameter_num) {
        (Parameter::Temperature2m, 0, 0) => true, // Temperature/Temperature(K)
        (Parameter::TotalCloudCover, 6, 1) => true, // Cloud/TotalCloudCover(%)
        (Parameter::WindSpeedU10m, 2, 2) => true, // Momentum/WindSpeedUComp(m/s)
        (Parameter::WindSpeedV10m, 2, 3) => true, // Momentum/WindSpeedVComp(m/s)
        (Parameter::TotalAccumPrecip, 1, 52) => true, // Moisture/TotalPrecipitationRate(kg/m2/s)
        (Parameter::ConvectiveSnow, 1, 55) => true, // Moisture/ConvectiveSnowfallRateWaterEquiv(kg/m2/s)
        (Parameter::LargeScaleSnow, 1, 56) => true, // Moisture/LargeScaleSnowfallRateWaterEquiv(kg/m2/s)
        (Parameter::ConvectiveRain, 1, 76) => true, // Moisture/ConvectiveRainRate(kg/m2/s)
        (Parameter::LargeScaleRain, 1, 77) => true, // Moisture/LargeScaleRainRate(kg/m2/s)
        (Parameter::SnowDepth, 1, 11) => true, // Moisture/SnowDepth(m)
        _ => false
    }
}

fn fetch_value(
    sub_id: (i64, i32),
    param: Parameter,
    lat: f32, lon: f32,
    modelrun_time: time::Tm, modelrun: u8, timestep: u8,
) -> impl Future<Item=f32, Error=String> {

    let file_key = FileKey::new(param, modelrun_time, modelrun, timestep);
    fetch_grid(sub_id, file_key)
        .and_then(move |grid| {
            if icon_verify_parameter(param, &grid) {
                future::ok(grid)
            } else {
                future::err("parameter cat/num mismatch".to_owned())
            }
        })
        .and_then(move |grid| extract_value_at(&grid, lat, lon))
        .map_err(|e| format!("fetch_value error: {}", e))
}

static ANDRIY: i64 = 54462285;
static BOTTOKEN: &'static str = include_str!("../bottoken.txt");

fn post_json(url: &str, json: String) -> impl Future<Item=(reqwest::StatusCode, Vec<u8>), Error=String> {

    HTTP_CLIENT
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(json)
        .send()
        .map_err(|e| format!("botapi post error: {}", e))
        .and_then(|resp: reqwest::async::Response| {
            let status = resp.status();
            resp.into_body().fold(
                Vec::new(),
                |mut acc, x| { acc.extend_from_slice(&x); future::ok::<_, reqwest::Error>(acc) }
            ).map(move |v| (status, v)).map_err(|e| format!("fold body error: {}", e.to_string()))
        })
        .map_err(|e| format!("botapi response error: {}", e))
}

fn tg_call<S, R>(call: &str, payload: S) -> impl Future<Item=R, Error=String>
where S: serde::Serialize, R: serde::de::DeserializeOwned {
    let url = format!("https://api.telegram.org/bot{}/{}", BOTTOKEN, call);
    let json = serde_json::to_string(&payload).unwrap();
    post_json(&url, json).and_then(|(s, body)| future::result(
        if s != reqwest::StatusCode::OK {
            Err(format!("status code: {:?}, body: {:?}", s, String::from_utf8(body)))
        } else {
            serde_json::from_reader::<_, telegram::TgResponse<R>>(std::io::Cursor::new(body))
                .map_err(|e| format!("parse response error: {}", e.to_string()))
                .and_then(|resp| resp.to_result())
        }
    ))
}

fn tg_update_widget(
    chat_id: i64, message_id: i32, text: String, reply_markup: Option<telegram::TgInlineKeyboardMarkup>, md: bool
) -> impl Future<Item=(), Error=String> {

    tg_call("editMessageText", telegram::TgEditMsg {
        chat_id, text, message_id, reply_markup,
        parse_mode: if md { Some("Markdown".to_owned()) } else { None }
    }).map(|telegram::TgMessageUltraLite {..}| ())
}

fn tg_send_widget(
    chat_id: i64, text: String, reply_to_message_id: Option<i32>, reply_markup: Option<telegram::TgInlineKeyboardMarkup>, md: bool
) -> impl Future<Item=i32, Error=String> {

    tg_call("sendMessage", telegram::TgSendMsg {
        chat_id, text, reply_to_message_id, reply_markup,
        parse_mode: if md { Some("Markdown".to_owned()) } else { None }
    }).map(|m: telegram::TgMessageUltraLite| m.message_id)
}

fn tg_edit_kb(chat_id: i64, message_id: i32, reply_markup: Option<telegram::TgInlineKeyboardMarkup>) -> impl Future<Item=(), Error=String> {
    tg_call("editMessageReplyMarkup", telegram::TgEditMsgKb {chat_id, message_id, reply_markup})
        .map(|telegram::TgMessageUltraLite {..}| ())
}

fn tg_answer_cbq(id: String, notification: Option<String>) -> impl Future<Item=(), Error=String> {
    tg_call("answerCallbackQuery", telegram::TgAnswerCBQ{callback_query_id: id, text: notification})
        .and_then(|t| if t { Ok(()) } else { Err("should return true".to_owned()) })
}

fn tg_get_updates(last: Option<i32>) -> impl Future<Item=Vec<(Option<i32>, String)>, Error=String> {
    let mut url = format!("https://api.telegram.org/bot{}/getUpdates", BOTTOKEN);
    if let Some(x) = last {
        url.push_str("?offset=");
        url.push_str(&(x+1).to_string());
    }
    HTTP_CLIENT
        .get(&url)
        .send()
        .map_err(|e| format!("botapi get error: {}", e))
        .and_then(|resp: reqwest::async::Response| {
            let status = resp.status();
            resp.into_body().fold(
                Vec::new(),
                |mut acc, x| { acc.extend_from_slice(&x); future::ok::<_, reqwest::Error>(acc) }
            ).map(move |v| (status, v)).map_err(|e| format!("fold body error: {}", e.to_string()))
        })
        .map_err(|e| format!("botapi response error: {}", e))
        .and_then(|(s, body)| future::result(
            if s == reqwest::StatusCode::OK {
                serde_json::from_slice::<telegram::TgResponse< Vec<serde_json::Value>> >(&body)
                    .map_err(|e| format!("parse updates error: {}", e.to_string()))
                    .and_then(|resp| resp.to_result())
                    .map(|v| v.iter().map(|i| (
                        i.get("update_id").and_then(|n| n.as_i64().map(|x| x as i32)),
                        i.to_string()
                    )).collect())
            } else {
                Err(format!("status code: {:?}, body: {:?}", s, String::from_utf8(body)))
            }
        ))
}

lazy_static! {
    static ref HTTP_CLIENT: reqwest::async::Client = reqwest::async::Client::new();
    static ref USER_CLICKS: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<(i64, i32), futures::sync::oneshot::Sender<String>>>> = std::sync::Arc::default();
    static ref USER_INPUTS: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<i64, futures::sync::oneshot::Sender<String>>>> = std::sync::Arc::default();
    static ref SUBS: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<(i64, i32), Sub>>> = std::sync::Arc::default();
}

fn format_lat_lon(lat: f32, lon: f32) -> String {
    format!("{:.03}°{} {:.03}°{}",
            lat.abs(), if lat > 0.0 {"N"} else {"S"},
            lon.abs(), if lat > 0.0 {"E"} else {"W"},
    )
}

const MONTH_ABBREVS: [&'static str; 12] = [
    "Січ", "Лют", "Бер", "Кві", "Тра", "Чер", "Лип", "Сер", "Вер", "Жов", "Лис", "Гру"
];

fn format_time(t: time::Tm) -> String {
    format!("{} {} {:02}:{:02} (UTC)",
            t.tm_mday,
            MONTH_ABBREVS.get(t.tm_mon as usize).unwrap_or(&"?"),
            t.tm_hour, t.tm_min
    )
}

fn format_rain_rate(mmhr: f32) -> String {
    if mmhr < 0.01 {
        None
    } else if mmhr < 2.5 {
        Some("\u{1F4A7}")
    } else if mmhr < 7.6 {
        Some("\u{1F4A7}\u{1F4A7}")
    } else {
        Some("\u{1F4A7}\u{1F4A7}\u{1F4A7}")
    }.map(|g| format!("{}{:.2}мм/год", g, mmhr)).unwrap_or("--".to_owned())
}

fn format_snow_rate(mmhr: f32) -> String {
    if mmhr < 0.01 {
        None
    } else if mmhr < 1.3 {
        Some("\u{2744}")
    } else if mmhr < 3.0 {
        Some("\u{2744}\u{2744}")
    } else if mmhr < 7.6 {
        Some("\u{2744}\u{2744}\u{2744}")
    } else {
        Some("\u{2744}\u{26A0}")
    }.map(|g| format!("{}{:.2}мм/год", g, mmhr)).unwrap_or("--".to_owned())
}

fn format_wind_dir(u: f32, v: f32) -> &'static str {
    let ws_az = (-v).atan2(-u) / 3.1415926535 * 180.0;

    if ws_az > 135.0 + 22.5 {
        "\u{2192}"
    } else if ws_az > 90.0 + 22.5 {
        "\u{2198}"
    } else if ws_az > 45.0 + 22.5 {
        "\u{2193}"
    } else if ws_az > 22.5 {
        "\u{2199}"
    } else if ws_az > -22.5 {
        "\u{2190}"
    } else if ws_az > -45.0 - 22.5 {
        "\u{2196}"
    } else if ws_az > -90.0 - 22.5 {
        "\u{2191}"
    } else if ws_az > -135.0 - 22.5 {
        "\u{2197}"
    } else {
        "\u{2192}"
    }
}

/*
fn update_stream<SF, S>(
    chat_id: i64, loc_msg_id: i32, n: usize, last: Option<i32>, sf: SF
) -> impl Future<Item=(usize, bool), Error=String>
where SF: FnOnce() -> S, S: Stream<Item=ForecastText, Error=String> {

    if let Some(last_msg_id) = last {
        future::Either::A(tg_edit_kb(chat_id, last_msg_id, None))
    } else {
        future::Either::B(future::ok( () ))
    }.and_then(move |()|
        sf().into_future().and_then(|(head, tail)|
            if let Some(ForecastText(upd)) = head {
                let keyboard = telegram::TgInlineKeyboardMarkup { inline_keyboard: vec![make_cancel_row()] };

                let f = tg_send_widget(chat_id, upd, Some(loc_msg_id), Some(keyboard), true)
                    .and_then(|msg_id| Future::select(
                        get_user_click(chat_id, msg_id),
                        update_stream(chat_id, loc_msg_id, n + 1, Some(msg_id), || tail)
                    ));
                future::Either::A(f)
            } else {
                future::Either::B(future::ok((n, false)))
            }
        )
    )
}
*/

#[test]
fn stream_with_cancel() {
    const FS: [(u64, Result<usize, &'static str>); 5] = [
        (5, Ok(1)),
        (10, Ok(2)),
        (15, Ok(3)),
        (20, Err("Err")),
        (25, Ok(4))
    ];

    let start = std::time::Instant::now();
    let s = stream::iter_ok(FS.iter().cloned())
        .and_then(move |(secs, res)|
            tokio::timer::Delay::new(start + std::time::Duration::from_secs(secs))
                .map_err(|e| format!("timer error: {}", e.to_string()))
                .and_then(move |()| future::result::<usize, &'static str>(res).map_err(|e| e.to_owned()))
        )
        .inspect(|v| println!("v -> {}", v));

    let t = s
        .fold( (), |(), _| future::ok::<_, String>( () ))
        .map_err(|e| println!("stream error: {}", e));
    
    tokio::run(t);
}

// TODO: cancelation button on updates does nothing

fn monitor_weather_wrap(sub: Sub, target_time: time::Tm) -> Box<dyn Future<Item=(usize, bool), Error=String> + Send> {
    let key = (sub.chat_id, sub.widget_message_id);
    {
        let mut hm = SUBS.lock().unwrap();
        if let Some(_prev) = hm.insert(key, sub.clone()) {
            println!("there was sub for {:?} already, override", key);
        }
    }

    let chat_id = sub.chat_id;
    let loc_msg_id = sub.location_message_id;
    let sub_id = (chat_id, loc_msg_id);

    let f = if sub.name.is_none() {
        let f = forecast_stream(sub, target_time)
            .into_future()
            .map_err(|(e, _)| e)
            .and_then(|(f, _)| f.ok_or(format!("no current forecast")))
            .inspect(move |ForecastText(upd)| log(sub_id, &format!("single update: {}", upd)))
            .and_then(move |ForecastText(upd)| tg_send_widget(chat_id, upd, Some(loc_msg_id), None, true).map(|_msg| (1, false)));
        future::Either::A(f)
    } else {
        let f = forecast_stream(sub, target_time)
            .inspect(move |ForecastText(upd)| log(sub_id, &format!("update: {}", upd)))
            .fold( (0, None, false), move |(n, last, _), ForecastText(upd)| {
                if let Some(last_msg_id) = last {
                    future::Either::A(tg_edit_kb(chat_id, last_msg_id, None))
                } else {
                    future::Either::B(future::ok( () ))
                }.and_then(move |()| {
                    let keyboard = telegram::TgInlineKeyboardMarkup { inline_keyboard: vec![make_cancel_row()] };

                    tg_send_widget(chat_id, upd, Some(loc_msg_id), Some(keyboard), true)
                    .map(move |msg_id| (n + 1, Some(msg_id), false))
                })
            })
            .and_then(move |(n, last, int)| {
                if let Some(last_msg_id) = last {
                    future::Either::A(tg_edit_kb(chat_id, last_msg_id, None))
                } else {
                    future::Either::B(future::ok( () ))
                }.and_then(move |()|
                    tg_send_widget(chat_id, format!("вичерпано ({} оновлень)", n), Some(loc_msg_id), None, false)
                        .map(move |_msg_id| (n, int))
                )
            });
        future::Either::B(f)
    }
        .inspect(move |(n, int)| log(sub_id, &format!("done: {} updates, interrupted: {}", n, int)))
        .map_err(|e| format!("subscription future error: {}", e))
        .then(move |x| {SUBS.lock().unwrap().remove(&key); future::result(x)});
    Box::new(f)
}

struct Forecast {
    temperature: f32,
    time: (time::Tm, time::Tm),
    total_cloud_cover: Option<f32>,
    wind_speed: Option<(f32, f32)>,
    total_precip_accum: Option<(f32, f32)>,
    rain_accum: Option<(f32, f32)>,
    snow_accum: Option<(f32, f32)>,
    snow_depth: Option<(f32)>,
}

fn format_forecast(name: Option<&str>, f: &Forecast) -> String {
    let interval = (f.time.1 - f.time.0).num_minutes() as f32;
    let mut result = String::new();

    if let Some(name) = name {
        result.push_str(&format!("\"{}\"\n", name));
    }
    result.push_str(&format!("_{}_\n", format_time(f.time.0)));
    result.push_str(&format!("t повітря: *{:.1}°C*\n", (10.0 * (f.temperature - 273.15)).round() / 10.0));
    if let Some(precip) = f.total_precip_accum {
        let precip_rate = ((precip.1 - precip.0) * 60.0 / interval).max(0.0);
        result.push_str(&format!("опади: *{:.02}*\n", precip_rate));
    }
    if let Some(rain) = f.rain_accum {
        let rain_rate = ((rain.1 - rain.0) * 60.0 / interval).max(0.0);
        result.push_str(&format!("дощ: *{}*\n", format_rain_rate(rain_rate)));
    }
    if let Some(snow) = f.snow_accum {
        let snow_rate = ((snow.1 - snow.0) * 60.0 / interval).max(0.0);
        result.push_str(&format!("сніг: *{}*\n", format_snow_rate(snow_rate)));
    }
    if let Some(snow_depth) = f.snow_depth {
        result.push_str(&format!("шар снігу: *{:.01}см*\n", snow_depth * 100.0));
    }
    if let Some(clouds) = f.total_cloud_cover {
        result.push_str(&format!("хмарність: *{:.0}%*\n", clouds.round()));
    }
    if let Some(wind) = f.wind_speed {
        let wind_speed = (wind.0 * wind.0 + wind.1 * wind.1).sqrt();
        result.push_str(&format!("вітер: *{} {:.1}м/с*\n", format_wind_dir(wind.0, wind.1), (10.0 * wind_speed).round() / 10.0));
    }
    result
}

struct ForecastText(String);

fn opt_wrap<FN, F, I>(b: bool, f: FN) -> impl Future<Item=Option<I>, Error=String>
where FN: FnOnce() -> F, F: Future<Item=I, Error=String> {
    if b {
        future::Either::A(f().map(|x| Some(x)))
    } else {
        future::Either::B(future::ok(None))
    }
}

fn fetch_all(
    sub_id: (i64, i32), lat: f32, lon: f32, mrt: time::Tm, mr: u8, t0: time::Tm, ts: u8, t1: time::Tm, ts1: u8,
    tcc: bool, wind: bool, precip: bool, rain: bool, snow: bool, depth: bool
) -> impl Future<Item=Forecast, Error=String> {

    future::Future::join4(
        fetch_value(sub_id, Parameter::Temperature2m, lat, lon, mrt, mr, ts),
        opt_wrap(tcc, move || fetch_value(sub_id, Parameter::TotalCloudCover, lat, lon, mrt, mr, ts)),
        opt_wrap(wind, move || future::Future::join(
            fetch_value(sub_id, Parameter::WindSpeedU10m, lat, lon, mrt, mr, ts),
            fetch_value(sub_id, Parameter::WindSpeedV10m, lat, lon, mrt, mr, ts),
        )),
        future::Future::join4(
            opt_wrap(precip, move || future::Future::join(
                fetch_value(sub_id, Parameter::TotalAccumPrecip, lat, lon, mrt, mr, ts),
                fetch_value(sub_id, Parameter::TotalAccumPrecip, lat, lon, mrt, mr, ts1),
            )),
            opt_wrap(rain, move || future::Future::join4(
                fetch_value(sub_id, Parameter::LargeScaleRain, lat, lon, mrt, mr, ts),
                fetch_value(sub_id, Parameter::LargeScaleRain, lat, lon, mrt, mr, ts1),
                fetch_value(sub_id, Parameter::ConvectiveRain, lat, lon, mrt, mr, ts),
                fetch_value(sub_id, Parameter::ConvectiveRain, lat, lon, mrt, mr, ts1),
            )),
            opt_wrap(snow, move || future::Future::join4(
                fetch_value(sub_id, Parameter::LargeScaleSnow, lat, lon, mrt, mr, ts),
                fetch_value(sub_id, Parameter::LargeScaleSnow, lat, lon, mrt, mr, ts1),
                fetch_value(sub_id, Parameter::ConvectiveSnow, lat, lon, mrt, mr, ts),
                fetch_value(sub_id, Parameter::ConvectiveSnow, lat, lon, mrt, mr, ts1),
            )),
            opt_wrap(depth, move || fetch_value(sub_id, Parameter::SnowDepth, lat, lon, mrt, mr, ts)),
        ),
    )
    .map(move |(t, otcc, owind, (oprecip, orain, osnow, odepth))| Forecast {
        temperature: t,
        total_cloud_cover: otcc,
        wind_speed: owind,
        total_precip_accum: oprecip,
        rain_accum: orain.map(|(ls0, ls1, c0, c1)| (ls0 + c0, ls1 + c1)),
        snow_accum: osnow.map(|(ls0, ls1, c0, c1)| (ls0 + c0, ls1 + c1)),
        snow_depth: odepth,
        time: (t0, t1),
    })
}

fn select_start_time<F, R>(
    sub_id: (i64, i32), target_time: time::Tm, try_func: F, try_timeout: std::time::Duration
) -> impl Future<Item=time::Tm, Error=String>
where
    F: Fn(time::Tm, u8, time::Tm, u8, time::Tm, u8) -> R,
    R: Future<Item=Forecast, Error=String>,
{
    let now = time::now_utc();
    let start_time = now - time::Duration::hours(12);
    let fs: Vec<_> = forecast_iterator(start_time, target_time, icon_modelrun_iter, icon_timestep_iter).collect();

    stream::unfold(fs, |mut fs| fs.pop().map(|x| future::ok( (x, fs) )))
        .skip_while(move |(mrt, _, _, _, _, _)| future::ok(now < *mrt))
        .and_then(move |(mrt, mr, ft, ts, ft1, ts1)| {
            log(sub_id, &format!("try {}/{:02} >> {}/{:03} .. {}{:03}", mrt.rfc3339(), mr, ft.rfc3339(), ts, ft1.rfc3339(), ts1));
            tokio::timer::Timeout::new(try_func(mrt, mr, ft, ts, ft1, ts1), try_timeout)
                .map(move |_f: Forecast| Some(mrt))
                .or_else(move |_e: tokio::timer::timeout::Error<String>| {
                    log(sub_id, &format!("took too long"));
                    future::ok(None)
                })
        })
        .filter_map(|item: Option<time::Tm>| item)
        .into_future()
        .map_err(|(e, _)| e)
        .and_then(|(f, _)| f.ok_or(format!("tried all start times")))
}

/*
run once for some target time: one fails, second succeedes (it's in the cache)

run again for the same time, cache still has unfinished requests:

[2018-12-08T22:24:25Z] (54462285, 388) try 2018-12-08T21:00:00Z/21 >> 2018-12-09T12:00:00Z/015 .. 2018-12-09T13:00:00Z016
[2018-12-08T22:24:32Z] (54462285, 388) took too long
[2018-12-08T22:24:32Z] (54462285, 388) try 2018-12-08T18:00:00Z/18 >> 2018-12-09T12:00:00Z/018 .. 2018-12-09T13:00:00Z019
[2018-12-08T22:24:32Z] (54462285, 388) want 2018-12-08T18:00:00Z/18 >> 2018-12-09T12:00:00Z/018 .. 2018-12-09T13:00:00Z019
[2018-12-08T22:24:33Z] (54462285, 388) single update: ....

QUESTION: will those requests ever finish?
*/


fn forecast_stream(sub: Sub, target_time: time::Tm) -> impl Stream<Item=ForecastText, Error=String> {
    let lat = sub.latitude;
    let lon = sub.longitude;
    let sub_id = (sub.chat_id, sub.location_message_id);

    select_start_time(
        sub_id, target_time, move |mrt, mr, ft, ts, ft1, ts1| fetch_all(
            sub_id, lat, lon, mrt, mr, ft, ts, ft1, ts1, false, false, false, true, true, true
        ), std::time::Duration::from_secs(7)
    )
        .map(move |f| {
            stream::iter_ok(forecast_iterator(f, target_time, icon_modelrun_iter, icon_timestep_iter))
                .and_then(move |(mrt, mr, ft, ts, ft1, ts1)| {
                    log(sub_id, &format!("want {}/{:02} >> {}/{:03} .. {}{:03}", mrt.rfc3339(), mr, ft.rfc3339(), ts, ft1.rfc3339(), ts1));
                    fetch_all(
                        sub_id, lat, lon, mrt, mr, ft, ts, ft1, ts1, false, false, false, true, true, true
                    )
                })
                .map(move |f| format_forecast(sub.name.as_ref().map(String::as_ref), &f))
                .inspect_err(move |e| log(sub_id, &format!("monitor stream error: {}", e)))
                .then(|result: Result<_, String>| future::ok(result)) // stream of values --> stream of results
                .filter_map(|item: Result<_, String>| item.ok()) // drop errors
                .map(|s| ForecastText(s))
        })
        .flatten_stream()
}

#[derive(Serialize, Deserialize, Clone)]
struct Sub {
    chat_id: i64,
    location_message_id: i32,
    widget_message_id: i32,
    latitude: f32,
    longitude: f32,
    name: Option<String>, // name for repeated/none if once
    target_time: String, // TODO:
}

fn log(id: (i64, i32), s: &str) {
    let msg = &format!("[{}] {:?} {}\n", time::now_utc().rfc3339(), id, s);
    //if let (0, 0) = id {
        print!("{}", msg);
    //} else {
    //    SUBS.lock().unwrap().entry(id).and_modify(|sub| sub.log.push_str(msg));
    //}
}

struct SerpanokApi {
    executor: tokio::runtime::TaskExecutor,
}

fn hresp<T>(code: u16, t: T) -> hyper::Response<hyper::Body>
where T: Into<hyper::Body> {
    hyper::Response::builder().status(code).body(t.into()).unwrap()
}

mod telegram;

const PADDING_DATA: &'static str = "na";
const CANCEL_DATA: &'static str = "xx";

fn make_cancel_row() -> Vec<telegram::TgInlineKeyboardButtonCB>{
    vec![telegram::TgInlineKeyboardButtonCB::new("скасувати".to_owned(), CANCEL_DATA.to_owned())]
}

struct UserClick {
    chat_id: i64,
    msg_id: i32,
    rx: Box<dyn Future<Item=String, Error=String> + Send>,
} 

impl UserClick {
    fn new(chat_id: i64, msg_id: i32) -> Self {
        let (tx, rx) = futures::sync::oneshot::channel::<String>();
        let mut hm = USER_CLICKS.lock().unwrap();
        if let Some(_prev) = hm.insert((chat_id, msg_id), tx) {
            println!("there was tx for clicking {}:{} already, override", chat_id, msg_id);
        }
        let f = rx.map_err(|e| format!("rx error: {}", e.to_string()));
        UserClick { chat_id, msg_id, rx: Box::new(f) }
    }
    fn click(chat_id: i64, msg_id: i32, data: String) -> Result<(), &'static str> {
            USER_CLICKS.lock().unwrap()
                .remove(&(chat_id, msg_id))
                .ok_or("unexpected click")
                .and_then(|tx| tx.send(data).map_err(|_| "receiver is no longer available"))
    }
}

impl Drop for UserClick {
    fn drop(&mut self) {
        if let Some(_) = USER_CLICKS.lock().unwrap().remove(&(self.chat_id, self.msg_id)) {
            println!("remove unused user click {}:{}", self.chat_id, self.msg_id);
        }
    }
}

impl Future for UserClick {
    type Item = String;
    type Error = String;
    fn poll(&mut self) -> futures::Poll<String, String>{
        self.rx.poll()
    }
}

struct UserInput {
    chat_id: i64,
    rx: Box<dyn Future<Item=String, Error=String> + Send>,
}

impl UserInput {
    fn new(chat_id: i64) -> Self {
        let (tx, rx) = futures::sync::oneshot::channel::<String>();
        let mut hm = USER_INPUTS.lock().unwrap();
        if let Some(_prev) = hm.insert(chat_id, tx) {
            println!("there was tx for input {} already, override", chat_id);
        }
        let f = rx.map_err(|e| format!("rx error: {}", e.to_string()));
        UserInput { chat_id, rx: Box::new(f) }
    }
    fn input(chat_id: i64, text: String) -> Result<(), &'static str> {
        USER_INPUTS.lock().unwrap()
            .remove(&chat_id)
            .ok_or("unexpected input")
            .and_then(|tx| tx.send(text).map_err(|_| "receiver is no longer available"))
    }
}

impl Drop for UserInput {
    fn drop(&mut self) {
        if let Some(_) = USER_INPUTS.lock().unwrap().remove(&self.chat_id) {
            println!("drop unused user input {}", self.chat_id);
        }
    }
}

impl Future for UserInput {
    type Item = String;
    type Error = String;
    fn poll(&mut self) -> futures::Poll<String, String>{
        self.rx.poll()
    }
}

fn process_widget(
    chat_id: i64, loc_msg_id: i32, target_lat: f32, target_lon: f32
) -> impl Future<Item=(), Error=String> {

    future::ok(format!("координати: *{}*", format_lat_lon(target_lat, target_lon)))
        .and_then(move |widget_text| {
            let mut days_map: std::collections::HashMap<String, (i32, i32, i32, Vec<Vec<Option<i32>>>)> = std::collections::HashMap::new();

            let v = time_picker(time::now_utc() - time::Duration::hours(1));
            let first = v.iter().take(3).map(|(y, m, d, ts)| {
                    let t = format!("{:02}.{:02}", d, m);
                    days_map.insert(t.clone(), (*y, *m, *d, ts.clone()));
                    telegram::TgInlineKeyboardButtonCB::new(t.clone(), t)
                }).collect();
            let second = v.iter().skip(3).take(3).map(|(y, m, d, ts)| {
                    let t = format!("{:02}.{:02}", d, m);
                    days_map.insert(t.clone(), (*y, *m, *d, ts.clone()));
                    telegram::TgInlineKeyboardButtonCB::new(t.clone(), t)
                }).collect();
            let inline_keyboard = vec![
                first,
                second,
                make_cancel_row(),
            ];

            let keyboard = telegram::TgInlineKeyboardMarkup{ inline_keyboard };

            tg_send_widget(chat_id, format!("{}\nвибери дату (utc):", widget_text), Some(loc_msg_id), Some(keyboard), true)
                .and_then(move |msg_id|
                    UserClick::new(chat_id, msg_id)
                        .map(move |data| (widget_text, msg_id, days_map.remove(&data)))
                )
        })
        .and_then(move |(mut widget_text, msg_id, build)| {
            if let Some((y, m, d, tss)) = build {
                let mut hours_map: std::collections::HashMap<String, i32> = std::collections::HashMap::new();

                let mut inline_keyboard: Vec<Vec<_>> = tss.iter().map(|r| {
                    let row: Vec<telegram::TgInlineKeyboardButtonCB> = r.iter().map(|c| {
                        match c {
                            Some(h) => {
                                let t = format!("{:02}", h);
                                hours_map.insert(t.clone(), *h);
                                telegram::TgInlineKeyboardButtonCB::new(t.clone(), t)
                            },
                            None => {
                                let t = "--".to_owned();
                                telegram::TgInlineKeyboardButtonCB::new(t, PADDING_DATA.to_owned())
                            }
                        }
                    }).collect();
                    row
                }).collect();
                inline_keyboard.push(make_cancel_row());

                let keyboard = Some(telegram::TgInlineKeyboardMarkup{ inline_keyboard });
                widget_text.push_str(&format!("\nдата: *{}-{:02}-{:02}*", y, m, d));

                let f = tg_update_widget(chat_id, msg_id, format!("{}\nвибери час (utc):", widget_text), keyboard, true)
                    .and_then(move |()|
                        UserClick::new(chat_id, msg_id)
                            .map(move |data| {
                                let build = hours_map.remove(&data).and_then(|h|
                                    time::strptime(&format!("{}-{:02}-{:02}T{:02}:00:00Z", y, m, d, h), "%FT%TZ").ok()
                                );
                                (widget_text, msg_id, build)
                            })
                    );
                future::Either::A(f)
            } else {
                future::Either::B(future::ok((widget_text, msg_id, None)))
            }
        })
        .and_then(move |(mut widget_text, msg_id, build)| {
            if let Some(target_time) = build {
                let inline_keyboard = vec![
                    vec![telegram::TgInlineKeyboardButtonCB::new("надсилати оновлення".to_owned(), "live".to_owned())],
                    vec![telegram::TgInlineKeyboardButtonCB::new("тільки поточний".to_owned(), "once".to_owned())],
                    make_cancel_row(),
                ];

                let keyboard = Some(telegram::TgInlineKeyboardMarkup{ inline_keyboard });
                widget_text.push_str(&format!("\nчас: *{:02}:00 (utc)*", target_time.tm_hour));
                let text = format!("{}\nякий прогноз цікавить:", widget_text);

                let f = tg_update_widget(chat_id, msg_id, text, keyboard, true).and_then(move |()| {
                    UserClick::new(chat_id, msg_id)
                        .map(move |data: String| {
                            match data.as_str() {
                                "live" => Some(false),
                                "once" => Some(true),
                                _ => None,
                            }
                            .map(|once| (once, target_time))
                        })
                        .map(move |build| (widget_text, msg_id, build))
                });
                future::Either::A(f)
            } else {
                future::Either::B(future::ok((widget_text, msg_id, None)))
            }
        })
        .and_then(move |(widget_text, msg_id, build)| match build {
            Some((true, target_time)) => {
                let t = (widget_text, msg_id, Some((target_time, None)));
                future::Either::A(future::Either::A(future::ok(t)))
            },
            Some((false, target_time)) => {
                let keyboard = telegram::TgInlineKeyboardMarkup { inline_keyboard: vec![make_cancel_row()] };
                let text = format!("{}\nдай назву цьому спостереженню:", widget_text);

                let f = tg_update_widget(chat_id, msg_id, text, Some(keyboard), true)
                    .and_then(move |()| Future::select(
                            UserInput::new(chat_id).map(move |name| Some((target_time, Some(name)))),
                            UserClick::new(chat_id, msg_id).map(move |_| None)
                        ).map(move |(x, _)| (widget_text, msg_id, x)).map_err(|(x, _)| x)
                    );
                future::Either::A(future::Either::B(f))
            },
            None => future::Either::B(future::ok((widget_text, msg_id, None))),
        })
        .and_then(move |(mut widget_text, msg_id, build)| {
            if let Some((target_time, name)) = build {
                if let Some(ref name) = name {
                    widget_text.push_str(&format!("\nназва: *{}*", name));
                }

                let f = tg_update_widget(chat_id, msg_id, format!("{}\nстан: *відслідковується*", widget_text), None, true)
                    .and_then(move |()| {
                        let sub = Sub {
                            chat_id, name,
                            location_message_id: loc_msg_id,
                            widget_message_id: msg_id,
                            latitude: target_lat,
                            longitude: target_lon,
                            target_time: target_time.strftime("%FT%TZ").unwrap().to_string(),
                        };
                        monitor_weather_wrap(sub, target_time).map(move |status| (widget_text, msg_id, Some(status)) )
                    });
                future::Either::A(f)
            } else {
                future::Either::B(future::ok( (widget_text, msg_id, None) ))
            }
        })
        .and_then(move |(widget_text, msg_id, build)| {
            let status = match build {
                Some( (times, false) ) => format!("виконано ({})", times),
                Some( (times, true) ) => format!("скасовано ({})", times),
                None => "скасовано".to_owned(),
            };
            tg_update_widget(chat_id, msg_id, format!("{}\nстан: *{}*", widget_text, status), None, true)
        })
        .or_else(move |e| {
            tg_send_widget(chat_id, format!("сталась помилка: `{}`", e), Some(loc_msg_id), None, true)
                .map(|_msg| ())
        })
}

static SEND_LOCATION_MSG: &'static str = "покажи локацію для якої потрібно відслідковувати погоду";
static WRONG_LOCATION_MSG: &'static str = "я можу відслідковувати тільки європейську погоду";
static UNKNOWN_COMMAND_MSG: &'static str = "я розумію лише команду /start";
static CBQ_ERROR_MSG: &'static str = "помилка";
static PADDING_BTN_MSG: &'static str = "недоcтупна опція";
static TEXT_ERROR_MSG: &'static str = "я не розумію";

// TODO: what if user deletes chat or deletes widget message??
// ANSWER: in private chats user can only delete message for himself
//         deleting whole chat still allows bot to send messages
// NOTE: turns out it is possible to delete from mobile client
// TODO2:  what if user blocks bot?

// NOTE: allow only private chat communication for now
fn process_bot(upd: SeUpdate) -> Box<dyn Future<Item=(), Error=String> + Send> {
    match upd {
        SeUpdate::PrivateChat {chat_id, user: _, update} => match update {
            SeUpdateVariant::Command(cmd) =>
                match cmd.as_str() {
                    "/start" => Box::new(tg_send_widget(chat_id, SEND_LOCATION_MSG.to_owned(), None, None, false).map(|_| ())),
                    _ => Box::new(tg_send_widget(chat_id, UNKNOWN_COMMAND_MSG.to_owned(), None, None, false).map(|_| ())),
                },
            SeUpdateVariant::Location {location, msg_id} =>
                if location.latitude > 29.5 && location.latitude < 70.5 && location.longitude > -23.5 && location.longitude < 45.0 {
                    Box::new(process_widget(chat_id, msg_id, location.latitude, location.longitude))
                } else {
                    Box::new(tg_send_widget(chat_id, WRONG_LOCATION_MSG.to_owned(), None, None, false).map(|_| ()))
                }
            SeUpdateVariant::CBQ {id, msg_id, data} =>
                if data == PADDING_DATA { // TODO: this is wrong place to check this
                    Box::new(tg_answer_cbq(id, Some(PADDING_BTN_MSG.to_owned()))
                             .map_err(|e| format!("answer stub cbq error: {}", e.to_string())))
                } else {
                    // push choice into channel for (chat, msg_id) conversation
                    let ok = UserClick::click(chat_id, msg_id, data)
                        .map_err(|e| println!("cbq(click) {}:{} error: {}", chat_id, msg_id, e))
                        .is_ok();

                    Box::new(tg_answer_cbq(id, if ok { None } else { Some(CBQ_ERROR_MSG.to_owned()) })
                             .map_err(|e| format!("answer cbq error: {}", e.to_string())))
                },
            SeUpdateVariant::Text(text) => {
                let ok = UserInput::input(chat_id, text)
                    .map_err(|e| println!("text {} error: {}", chat_id, e))
                    .is_ok();

                if ok {
                    Box::new(future::ok( () ))
                } else {
                    Box::new(tg_send_widget(chat_id, TEXT_ERROR_MSG.to_owned(), None, None, false).map(|_m| () ))
                }
            },
        },            
        SeUpdate::Other(update) =>
            Box::new(tg_send_widget(ANDRIY, format!("unsupported update:\n{:#?}", update), None, None, false).map(|_| ())),
    }
}

impl hyper::service::Service for SerpanokApi {
    type ReqBody = hyper::Body;
    type ResBody = hyper::Body;
    type Error = hyper::Error;
    type Future = Box<dyn Future<Item=hyper::Response<Self::ResBody>, Error=Self::Error> + Send>;

    fn call(&mut self, req: hyper::Request<Self::ReqBody>) -> Self::Future {
        println!("request: {} {}", req.method(), req.uri());

        match (req.method(), req.uri().path()) {
            (&hyper::Method::POST, "/bot") => {
                let exec = self.executor.clone();
                let f = req.into_body()
                    .fold(Vec::new(), |mut acc, x| {acc.extend_from_slice(&x); future::ok::<_, hyper::Error>(acc)}) // -> Future<vec, hyper>
                    .and_then(move |body| {
                        let res = serde_json::from_slice::<telegram::TgUpdate>(&body)
                            .map(|tgu| SeUpdate::from(tgu))
                            .map_err(|e| println!("TgUpdate parse error: {}", e.to_string()))
                            .map(|seu| {println!("!new update: {:?}", seu); seu});
                        if let Ok(upd) = res {
                            exec.spawn(
                                process_bot(upd).map_err(|e| println!("process bot error: {}", e))
                            );
                        }
                        future::ok( () )
                    })
                    .then(|_| future::ok(hresp(200, "")));
                Box::new(f)
            },
            (&hyper::Method::GET, "/modeltimes") => {
                let body = icon_modelrun_iter().map(|mr| {
                    format!("------- MODEL RUN {:02} ---------------------\n{:?}\n", mr, icon_timestep_iter(mr).collect_vec())
                }).fold(String::new(), |mut acc, x| {acc.push_str(&x); acc});
                Box::new(future::ok(hresp(200, body)))
            },
            (&hyper::Method::GET, "/dryrun") => {
                let start_time = time::now_utc() - time::Duration::hours(6);
                let target_time = req.uri().query().and_then(|q| time::strptime(q, "target=%FT%TZ").ok()).unwrap_or(time::now_utc());
                let mut res = String::new();
                res.push_str("----------------------------\n");
                res.push_str(&format!("start: {}\n", start_time.rfc3339()));
                res.push_str(&format!("now: {}\n", time::now_utc().rfc3339()));
                res.push_str(&format!("target: {}\n", target_time.rfc3339()));
                res.push_str("----------------------------\n");
                let body = forecast_iterator(start_time, target_time, icon_modelrun_iter, icon_timestep_iter)
                    .map(|(mrt, mr, ft, ts, ft1, ts1)| format!("* {}/{:02} >> {}/{:03}  add {}/{:03}\n",
                                                               mrt.rfc3339(), mr, ft.rfc3339(), ts, ft1.rfc3339(), ts1
                    ))
                    .fold(res, |mut acc, x| {acc.push_str(&x); acc});
                Box::new(future::ok(hresp(200, body)))
            },
            (&hyper::Method::GET, "/subs") => {
                let v: Vec<_> = SUBS.lock().unwrap().values().cloned().collect();
                let f = future::result(serde_json::to_string_pretty(&v))
                    .and_then(|json| future::ok(hresp(200, json)))
                    .or_else(|se_err| future::ok(hresp(500, se_err.to_string())));
                Box::new(f)
            },
            (&hyper::Method::POST, "/subs") => {
                let exec = self.executor.clone();
                let f = req.into_body()
                    .fold(Vec::new(), |mut acc, x| {acc.extend_from_slice(&x); future::ok::<_, hyper::Error>(acc)}) // -> Future<vec, hyper>
                    .and_then(move |body| {
                        let r = serde_json::from_slice::<Vec<Sub>>(&body)
                            .map(|ss| {
                                for s in ss {
                                    let now = time::now_utc();
                                    let target_time = time::strptime(&s.target_time, "%FT%TZ").unwrap_or(now);
                                    /*
                                    let widget_text = format!(
                                        "координати: *{}*\nдата: *{}-{:02}-{:02}*\nчас: *{}:00 (utc)*\nназва: *{}*\nстан: *відслідковується*",
                                        format_lat_lon(s.latitude, s.longitude),
                                        target_time.tm_year + 1900, target_time.tm_mon + 1, target_time.tm_mday,
                                        target_time.tm_hour,
                                        s.name
                                    );
                                    */
                                    exec.spawn(
                                        monitor_weather_wrap(s.clone(), target_time)
                                            .map(|_| ())
                                            .map_err(|err| println!("restored sub err: {}", err))
                                    );
                                }
                                (200, "registered".to_owned())
                            });
                        let p = r.unwrap_or_else(|e| (400, e.to_string()));
                        future::ok(hresp(p.0, p.1))
                    });
                Box::new(f)
            },
            (&hyper::Method::GET, "/cache") => {
                let vd: Vec<FileKey> = DISK_CACHE.lock().unwrap().keys().cloned().collect();
                let vm: Vec<FileKey> = MEM_CACHE.lock().unwrap().keys().cloned().collect();
                let f = future::result(serde_json::to_string_pretty(&(vd, vm)))
                    .and_then(|json| future::ok(hresp(200, json)))
                    .or_else(|se_err| future::ok(hresp(500, se_err.to_string())));
                Box::new(f)
            },
            (&hyper::Method::GET, "/picker") => {
                let start = req.uri().query().and_then(|q| time::strptime(q, "start=%FT%TZ").ok()).unwrap_or(time::now_utc());
                let v = time_picker(start);
                Box::new(future::ok(hresp(200, format!("start={}\n{:#?}\n", start.strftime("%FT%TZ").unwrap(), v))))
            },
            (&hyper::Method::GET, "/uis") => {
                let cs: Vec<(i64, i32)> = USER_CLICKS.lock().unwrap().keys().cloned().collect();
                let is: Vec<i64> = USER_INPUTS.lock().unwrap().keys().cloned().collect();
                let f = future::result(serde_json::to_string_pretty(&(cs, is)))
                    .and_then(|json| future::ok(hresp(200, json)))
                    .or_else(|se_err| future::ok(hresp(500, se_err.to_string())));
                Box::new(f)
            },
            _ => Box::new(future::ok(hresp(404, "[404]\n")))
        }
    }
}

fn forward_updates() -> impl Future<Item=(), Error=()> {
    stream::iter_ok::<_, String>(0..)
        .and_then(|_i|
            tokio::timer::Delay::new(std::time::Instant::now() + std::time::Duration::from_secs(3))
                .map_err(|err| format!("delay error: {}", err.to_string()))
        )
        .fold(None, |last_id, ()|
            tg_get_updates(last_id)
                .and_then(|v|
                    stream::iter_ok(v)
                        .filter_map(|(opt_id, json)| opt_id.map(move |id| (id, json)))
                        .fold(None, |max, (id, json)| {
                            println!(">>>>>>>>>>>>>>>> new update ({}):\n{}\n>>>>>>>>>>>>>>>>>>>>>", id, json);
                            post_json("http://127.0.0.1:8778/bot", json)
                                .then(move |_| future::ok::<Option<i32>, String>(max.map(|m: i32| m.max(id)).or(Some(id))))
                        })
                )
                .or_else(move |e| {
                    println!("err: {}", e);
                    future::ok::<_, String>(last_id)
                })
        )
        .map(|_| ())
        .map_err(|e| println!("poll stream error: {}", e.to_string()))
}

fn main() {

    println!("pick up cache: {} entries", DISK_CACHE.lock().unwrap().len());

    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let ex = rt.executor();

    let new_service = move || future::ok::<_, hyper::Error>(SerpanokApi {
        executor: ex.clone(),
    });

    let addr = ([127, 0, 0, 1], 8778).into();
    let server = hyper::Server::bind(&addr).serve(new_service).map_err(|e| println!("hyper error: {}", e));

    let purge_mem_cache = tokio::timer::Interval::new_interval(std::time::Duration::from_secs(60))
        .map(|_| purge_mem_cache())
        .map_err(|e| println!("purge mem interval error: {}", e.to_string()))
        .fold((), |_, _| Ok(()));

    let purge_disk_cache = tokio::timer::Interval::new_interval(std::time::Duration::from_secs(60 * 5))
        .map(|_| purge_disk_cache())
        .map_err(|e| println!("purge disk interval error: {}", e.to_string()))
        .fold((), |_, _| Ok(()));

    rt.spawn(server);
    rt.spawn(forward_updates());
    rt.spawn(purge_mem_cache);
    rt.spawn(purge_disk_cache);

    rt.shutdown_on_idle().wait().unwrap();
}

fn time_picker(start: time::Tm) -> Vec<(i32, i32, i32, Vec<Vec<Option<i32>>>)> {

    let start00 = trunc_days_utc(start);

    let groups = (0..)
        .flat_map(|d: i64| (0..4).map(move |h: i64| d*24 + h*6))
        .map(|h| start00 + time::Duration::hours(h))
        .skip_while(|t| start >= *t + time::Duration::hours(5))
        .group_by(|t| (t.tm_year, t.tm_mon, t.tm_mday));

    groups
        .into_iter()
        .map(|(d, ts)| (d.0 + 1900, d.1 + 1, d.2, ts))
        .take(6)
        .map(|(y, m, d, ts)| {
            let hs = ts
                .map(|t| (0..6)
                     .map(move |h| t + time::Duration::hours(h))
                     .map(|t| if t <= start && t < start + time::Duration::hours(120) { None } else { Some(t.tm_hour) })
                     .collect()
                )
                .collect();
             (y, m, d, hs)
        })
        .collect()
}

#[test]
fn tg_location() {
    let t = r#"
    {
      "update_id": 14180656,
      "message": {
        "message_id": 2,
        "from": {
          "id": 54462285,
          "is_bot": false,
          "first_name": "Andriy",
          "last_name": "Chyvonomys",
          "username": "chyvonomys",
          "language_code": "en-UA"
        },
        "chat": {
          "id": 54462285,
          "first_name": "Andriy",
          "last_name": "Chyvonomys",
          "username": "chyvonomys",
          "type": "private"
        },
        "date": 1541198764,
        "location": {
          "latitude": 50.425195,
          "longitude": 25.703556
        }
      }
    }
"#;

    let u = serde_json::from_str::<telegram::TgUpdate>(t).map(|u| SeUpdate::from(u));

    if let Ok(SeUpdate::PrivateChat {update: SeUpdateVariant::Location {..}, .. }) = u {
    } else {
        panic!("{:#?}, should be SeUpdate::Location", u);
    }
}


#[test]
fn tg_start() {
    let t = r#"
    {
      "update_id": 14180655,
      "message": {
        "message_id": 1,
        "from": {
          "id": 54462285,
          "is_bot": false,
          "first_name": "Andriy",
          "last_name": "Chyvonomys",
          "username": "chyvonomys"
        },
        "chat": {
          "id": 54462285,
          "first_name": "Andriy",
          "last_name": "Chyvonomys",
          "username": "chyvonomys",
          "type": "private"
        },
        "date": 1541198334,
        "text": "/start",
        "entities": [
          {
            "offset": 0,
            "length": 6,
            "type": "bot_command"
          }
        ]
      }
    }
"#;

    let u = serde_json::from_str::<telegram::TgUpdate>(t).map(|u| SeUpdate::from(u));

    if let Ok(SeUpdate::PrivateChat {update: SeUpdateVariant::Command {..}, .. }) = u {
    } else {
        panic!("{:#?}, should be SeUpdate::Command", u);
    }
}

#[test]
fn tg_cbq() {
    let t = r#"
{
  "callback_query": {
    "chat_instance": "8882132206646987846",
    "data": "11.11",
    "from": {
      "first_name": "Andriy",
      "id": 54462285,
      "is_bot": false,
      "language_code": "en-UA",
      "last_name": "Chyvonomys",
      "username": "chyvonomys"
    },
    "id": "233913736986880302",
    "message": {
      "chat": {
        "first_name": "Andriy",
        "id": 54462285,
        "last_name": "Chyvonomys",
        "type": "private",
        "username": "chyvonomys"
      },
      "date": 1541890580,
      "from": {
        "first_name": "серпанок",
        "id": 701332998,
        "is_bot": true,
        "username": "serpanok_bot"
      },
      "message_id": 115,
      "reply_to_message": {
        "chat": {
          "first_name": "Andriy",
          "id": 54462285,
          "last_name": "Chyvonomys",
          "type": "private",
          "username": "chyvonomys"
        },
        "date": 1541890579,
        "forward_date": 1541795230,
        "forward_from": {
          "first_name": "Andriy",
          "id": 54462285,
          "is_bot": false,
          "language_code": "en-UA",
          "last_name": "Chyvonomys",
          "username": "chyvonomys"
        },
        "from": {
          "first_name": "Andriy",
          "id": 54462285,
          "is_bot": false,
          "language_code": "en-UA",
          "last_name": "Chyvonomys",
          "username": "chyvonomys"
        },
        "location": {
          "latitude": 50.610482,
          "longitude": 26.341016
        },
        "message_id": 113
      },
      "text": "location: 50.610°N 26.341°E, pick a date (utc):"
    }
  },
  "update_id": 14180731
}
"#;

    let u = serde_json::from_str::<telegram::TgUpdate>(t).map(|u| SeUpdate::from(u));

    if let Ok(SeUpdate::PrivateChat {update: SeUpdateVariant::CBQ {..}, .. }) = u {
    } else {
        panic!("{:#?}, should be SeUpdate::CBQ", u);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
enum SeUpdate {
    PrivateChat {
        chat_id: i64,
        user: telegram::TgUser,
        update: SeUpdateVariant,
    },
    Other(telegram::TgUpdate),
}

#[derive(Debug)]
enum SeUpdateVariant {
    CBQ {
        id: String,
        msg_id: i32,
        data: String,
    },
    Location {
        location: telegram::TgLocation,
        msg_id: i32,
    },
    Command(String),
    Text(String),
}

impl From<telegram::TgUpdate> for SeUpdate {
    fn from(upd: telegram::TgUpdate) -> Self {
        
        match (upd.message.as_ref().and_then(|m| m.first_bot_command()), upd) {
            (None, telegram::TgUpdate {
                message: Some(telegram::TgMessage {
                    from: Some(user),
                    chat: telegram::TgChat {id: chat_id, type_: telegram::TgChatType::Private},
                    message_id: msg_id,
                    text: None,
                    entities: None,
                    location: Some(location),
                }),
                callback_query: None,
            }) => SeUpdate::PrivateChat { chat_id, user, update: SeUpdateVariant::Location {location, msg_id} },

            (None, telegram::TgUpdate {
                message: None,
                callback_query: Some(telegram::TgCallbackQuery {
                    id,
                    from: user,
                    message: Some(telegram::TgMessageLite {
                        message_id: msg_id,
                        chat: telegram::TgChat {id: chat_id, type_: telegram::TgChatType::Private},
                    }),
                    data: Some(data),
                }),
            }) => SeUpdate::PrivateChat { chat_id, user, update: SeUpdateVariant::CBQ {id, msg_id, data} },

            (None, telegram::TgUpdate {
                message: Some(telegram::TgMessage {
                    from: Some(user),
                    chat: telegram::TgChat {id: chat_id, type_: telegram::TgChatType::Private},
                    message_id: _,
                    text: Some(text),
                    entities: None,
                    location: None,
                }),
                callback_query: None,
            }) => SeUpdate::PrivateChat { chat_id, user, update: SeUpdateVariant::Text(text) },

            (Some(cmd), telegram::TgUpdate {
                message: Some(telegram::TgMessage {
                    from: Some(user),
                    chat: telegram::TgChat {id: chat_id, type_: telegram::TgChatType::Private},
                    message_id: _,
                    text: _,
                    entities: Some(_),
                    location: None,
                }),
                callback_query: None,
            }) => SeUpdate::PrivateChat { chat_id, user, update: SeUpdateVariant::Command(cmd) },

            (_, u) => SeUpdate::Other(u),
        }
    }
}

