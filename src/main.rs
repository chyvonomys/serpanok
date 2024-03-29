#![recursion_limit="256"]
use std::io::Read;
use std::sync::Arc;
use chrono::{Datelike, Timelike, TimeZone};
use serde_derive::{Serialize, Deserialize};
use lazy_static::*;
use futures::{future, Future, FutureExt, TryFutureExt, stream, StreamExt, TryStreamExt};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
enum SourceParameter {
    IconEu(icon::IconParameter),
    Gfs(gfs::GfsParameter, gfs::GfsResolution),
}

impl SourceParameter {
    fn verify_grib2(self, grib2: &grib::GribMessage) -> bool {
        match self {
            SourceParameter::IconEu(icon_param) => icon_param.verify_grib2(grib2),
            SourceParameter::Gfs(gfs_param, res) => gfs_param.verify_grib2(grib2),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataSource {
    IconEu,
    Gfs(gfs::GfsResolution),
}

impl DataSource {
    fn from_latlon(lat: f32, lon: f32) -> Option<Self> {
        if DataSource::IconEu.covers_point(lat, lon) {
            Some(DataSource::IconEu)
        } else if DataSource::Gfs(gfs::GfsResolution::Deg025).covers_point(lat, lon) {
            Some(DataSource::Gfs(gfs::GfsResolution::Deg025))
        } else {
            None
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "ICON_EU" => Some(DataSource::IconEu),
            "GFS_025" => Some(DataSource::Gfs(gfs::GfsResolution::Deg025)),
            "GFS_050" => Some(DataSource::Gfs(gfs::GfsResolution::Deg050)),
            "GFS_100" => Some(DataSource::Gfs(gfs::GfsResolution::Deg100)),
            _ => None,
        }
    }

    fn default_params(self) -> data::ParameterFlags {
        match self {
            DataSource::IconEu => data::ParameterFlags::icon(),
            DataSource::Gfs(_) => data::ParameterFlags::gfs(),
        }
    }

    fn covers_point(self, lat: f32, lon: f32) -> bool {
        match self {
            DataSource::IconEu => icon::covers_point(lat, lon),
            DataSource::Gfs(res) => gfs::covers_point(res, lat, lon),
        }
    }
    fn modelrun_iter(self) -> impl Iterator<Item=u8> {
        match self {
            DataSource::IconEu => either::Either::Left( icon::modelrun_iter() ),
            DataSource::Gfs(_) => either::Either::Right( gfs::modelrun_iter() ),
        }
    }
    fn timestep_iter(self, mr: u8) -> impl Iterator<Item=u16> {
        match self {
            DataSource::IconEu => either::Either::Left( icon::timestep_iter(mr) ),
            DataSource::Gfs(res) => either::Either::Right( gfs::timestep_iter(res) ),
        }
    }

    fn avail_all(
        self, log: Arc<TaggedLog>, timespec: data::ForecastTimeSpec, params: data::ParameterFlags
    ) -> impl Future<Output=Result<(), String>> {
        match self {
            DataSource::IconEu => data::icon_avail_all(log, timespec, params).left_future(),
            DataSource::Gfs(res) => data::gfs_avail_all(res, log, timespec).right_future(),
        }
    }

    fn fetch_all(
        self, log: Arc<TaggedLog>, lat: f32, lon: f32, timespec: data::ForecastTimeSpec, params: data::ParameterFlags
    ) -> impl Future<Output=Result<data::Forecast, String>> {
        match self {
            DataSource::IconEu => data::icon_fetch_all(log, lat, lon, timespec, params).left_future(),
            DataSource::Gfs(res) => data::gfs_fetch_all(log, lat, lon, timespec, params, res).right_future(),
        }
    }
}

impl std::fmt::Display for DataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            DataSource::IconEu => "ICON_EU",
            DataSource::Gfs(gfs::GfsResolution::Deg025) => "GFS_025",
            DataSource::Gfs(gfs::GfsResolution::Deg050) => "GFS_050",
            DataSource::Gfs(gfs::GfsResolution::Deg100) => "GFS_100",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct FileKey {
    param: SourceParameter,
    yyyy: u16,
    mm: u8,
    dd: u8,
    modelrun: u8,
    timestep: u16,
}

impl FileKey {
    #[cfg(test)]
    fn test_new() -> Self {
        Self {
            param: SourceParameter::Gfs(gfs::GfsParameter::Temperature2m, gfs::GfsResolution::Deg050),
            yyyy: 2018,
            mm: 11,
            dd: 24,
            modelrun: 15,
            timestep: 0,
        }
    }

    fn new(param: SourceParameter, dt: chrono::Date<chrono::Utc>, modelrun: u8, timestep: u16) -> Self {
        Self {
            param,
            yyyy: dt.year() as u16,
            mm: dt.month() as u8,
            dd: dt.day() as u8,
            modelrun,
            timestep,
        }
    }

    fn get_modelrun_tm(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.ymd(self.yyyy.into(), self.mm.into(), self.dd.into()).and_hms(self.modelrun.into(), 0, 0)
    }

    fn build_data_file(&self) -> Box<dyn DataFile> {
        match self.param {
            SourceParameter::IconEu(icon_param) => Box::new(icon::IconFile::new(
                icon_param, self.yyyy, self.mm, self.dd, self.modelrun, self.timestep
            )),
            SourceParameter::Gfs(gfs_param, res) => Box::new(gfs::GfsFile::new(
                gfs_param, res, self.yyyy, self.mm, self.dd, self.modelrun, self.timestep
            )),
        }
    }
}

trait DataFile: Send + Sync {
    fn cache_filename(&self) -> &str;
    fn fetch_bytes(&self, log: Arc<TaggedLog>) -> Box<dyn Future<Output=Result<Vec<u8>, String>> + Send + Unpin>;
    fn available_from(&self) -> chrono::DateTime<chrono::Utc>;
    fn available_to(&self) -> chrono::DateTime<chrono::Utc>;
    fn check_avail(&self, log: std::sync::Arc<TaggedLog>) -> Box<dyn Future<Output=Result<(), String>> + Send + Unpin>;
}

enum ExchangeSource {
    Rulya,
}

impl ExchangeSource {
    async fn fetch_rates(self, log: std::sync::Arc<TaggedLog>) -> Result<data::Rates, String> {
        match self {
            ExchangeSource::Rulya => data::rulya_fetch_rates(log).await,
        }
    }
}

mod grib;
mod telegram;
mod ui;
mod cache;
mod icon;
mod gfs;
mod data;
mod format;

pub struct TaggedLog {
    tag: String,
}

trait DebugRFC3339 {
    fn to_rfc3339_debug(&self) -> String;
}

impl DebugRFC3339 for chrono::DateTime<chrono::Utc> {
    fn to_rfc3339_debug(&self) -> String {
        self.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    }
}

impl TaggedLog {
    fn add_line(&self, s: &str) {
        println!("[{}] {} {}", chrono::Utc::now().to_rfc3339_debug(), &self.tag, s);
    }
}

fn unpack_bzip2(bytes: &[u8]) -> impl Future<Output=Result<Vec<u8>, String>> {
    let mut vec = Vec::new();
    let res = bzip2::read::BzDecoder::new(bytes)
        .read_to_end(&mut vec)
        .map(move |_sz| vec)
        .map_err(|e| format!("unpack error: {:?}", e));
    future::ready(res) // TODO:
}

fn fold_response_body(resp: hyper::Response<hyper::Body>) -> impl Future<Output=Result<(bool, Vec<u8>), String>> {
    let status = resp.status();
    resp.into_body().try_fold(
        Vec::new(),
        |mut acc, x| { acc.extend_from_slice(&x); future::ok::<_, hyper::Error>(acc) }
    )
    .map_ok(move |v| (status == hyper::StatusCode::OK || status == hyper::StatusCode::PARTIAL_CONTENT, v))
    .map_err(|e| format!("fold body error: {}", e.to_string()))
}

fn http_post_json(url: String, json: String) -> impl Future<Output=Result<(bool, Vec<u8>), String>> {
    let req = hyper::Request::post(&url)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(hyper::Body::from(json))
        .expect("POST request build failed");

    HTTP_CLIENT
        .request(req)
        .map_err(move |e| format!("POST {} failed: {}", url, e))
        .and_then(fold_response_body)
}

enum FormData {
    Field{name: &'static str, value: String},
    File{name: &'static str, filename: String, mime: &'static str, base64: String},
}

impl FormData {
    fn append_to_body(&self, body: &mut String) {
        match self {
            FormData::Field{name, value} => {
                body.push_str(&format!("Content-Disposition: form-data; name=\"{}\"\r\n", name));
                body.push_str("\r\n");
                body.push_str(&value);
                body.push_str("\r\n");
            },
            FormData::File{name, filename, mime, base64} => {
                body.push_str(&format!("Content-Disposition: form-data; name=\"{}\"; filename=\"{}\"\r\n", name, filename));
                body.push_str(&format!("Content-Type: {}\r\n", mime));
                body.push_str("Content-Transfer-Encoding: base64\r\n");
                body.push_str("\r\n");
                body.push_str(&base64);
                body.push_str("\r\n");
            },
        }
    }
}

async fn http_post_formdata(url: String, data: &[FormData]) -> Result<(bool, Vec<u8>), String> {
    let mut body = String::new();
    for d in data {
        body.push_str("-----BOUNDARY\r\n");
        d.append_to_body(&mut body);
    }
    body.push_str("-----BOUNDARY--\r\n");
    let req = hyper::Request::post(&url)
        .header(hyper::header::CONTENT_TYPE, "multipart/form-data; boundary=---BOUNDARY")
        .body(hyper::Body::from(body))
        .expect("POST request build failed");

    HTTP_CLIENT
        .request(req)
        .map_err(move |e| format!("POST {} failed: {}", url, e))
        .and_then(fold_response_body)
        .await
}

fn http_get(url: String, hs: &[(&str, &str)]) -> impl Future<Output=Result<(bool, Vec<u8>), String>> {
    let req = hs.iter()
        .fold(hyper::Request::get(&url), |builder, y| builder.header(y.0, y.1))
        .body(hyper::Body::from(""))
        .unwrap_or_else(|_| panic!("GET request build failed for {}", &url));

    HTTP_CLIENT
        .request(req)
        .map_err(move |e| format!("GET {} failed: {}", url, e))
        .and_then(fold_response_body)
}

fn http_head(url: String) -> impl Future<Output=Result<bool, String>> {
    let req = hyper::Request::head(&url)
        .body(hyper::Body::from(""))
        .unwrap_or_else(|_| panic!("HEAD request build failed for {}", &url));

    HTTP_CLIENT
        .request(req)
        .map_err(move |e| format!("HEAD {} failed: {}", url, e))
        .map_ok(|resp| resp.status() == hyper::StatusCode::OK)
}

fn fetch_url(log: Arc<TaggedLog>, url: String, hs: &[(&str, &str)]) -> impl Future<Output=Result<Vec<u8>, String>> {
    log.add_line(&format!("GET {}", url));
    http_get(url, hs).and_then(|(ok, body)|
        if ok {
            future::Either::Left(future::ok(body))
        } else {
            future::Either::Right(future::err("response status is not OK".to_string()))
        }
    )
}

fn avail_url(log: Arc<TaggedLog>, url: String) -> impl Future<Output=Result<(), String>> {
    log.add_line(&format!("HEAD {}", url));
    http_head(url).and_then(|ok|
        if ok {
            future::Either::Left(future::ok( () ))
        } else {
            future::Either::Right(future::err("response status is not OK".to_string()))
        }
    )
}

lazy_static! {
    static ref HTTP_CLIENT: hyper::client::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>, hyper::Body> = hyper::client::Client::builder()
        .build::<_, hyper::Body>(hyper_tls::HttpsConnector::new());
}

#[test]
fn stream_with_cancel() {
    const FS: [(u64, Result<usize, &'static str>); 5] = [
        (5, Ok(1)),
        (10, Ok(2)),
        (15, Ok(3)),
        (20, Err("Err")),
        (25, Ok(4))
    ];

    let start = tokio::time::Instant::now();
    let s = stream::iter(FS.iter().cloned().map(Ok))
        .and_then(move |(secs, res)|
            tokio::time::delay_until(start + std::time::Duration::from_secs(secs))
                .map(move |()| res.map_err(|e| e.to_owned()))
        )
        .inspect_ok(|v| println!("v -> {}", v));

    let t = s
        .try_fold( (), |(), _| future::ok::<_, String>( () ))
        .map_err(|e| println!("stream error: {}", e))
        .map(|_| ());
    
    tokio::spawn(t);
}

fn lookup_tz(lat: f32, lon: f32) -> chrono_tz::Tz {
    tz_search::lookup(f64::from(lat), f64::from(lon))
        .and_then(|tag| tag.parse::<chrono_tz::Tz>().ok())
        .unwrap_or(chrono_tz::Tz::UTC)
}

fn hresp<T>(code: u16, t: T, ct: &'static str) -> hyper::Response<hyper::Body>
where T: Into<hyper::Body> {
    hyper::Response::builder().status(code).header("Content-Type", ct).body(t.into()).unwrap()
}

fn serpanok_api(
    exec: tokio::runtime::Handle, req: hyper::Request<hyper::Body>, fs: ui::Features
) -> Box<dyn Future<Output=Result<hyper::Response<hyper::Body>, hyper::Error>> + Send + Unpin> {
    println!("request: {} {}", req.method(), req.uri());
    let query: &[u8] = req.uri().query().unwrap_or("").as_bytes();
    let params: std::collections::HashMap<String, String> =
        url::form_urlencoded::parse(query).into_owned().collect();

    match (req.method(), req.uri().path()) {
        (&hyper::Method::POST, "/bot") => {
            let f = req.into_body()
                .try_fold(Vec::new(), |mut acc, x| {acc.extend_from_slice(&x); future::ok::<_, hyper::Error>(acc)}) // -> Future<vec, hyper>
                .and_then(move |body| {
                    match serde_json::from_slice::<telegram::TgUpdate>(&body) {
                        Ok(tgu) => { exec.spawn(
                            ui::process_update(tgu, fs).map_err(|e| println!("process update error: {}", e)).map(|_| ())
                        ); },
                        Err(err) => println!("TgUpdate parse error: {}", err.to_string()),
                    }
                    future::ok( () )
                })
                .then(|_| future::ok(hresp(200, "", "text/plain")));
            Box::new(f)
        },
        (&hyper::Method::GET, "/modeltimes") => {
            let source = params.get("source")
                .and_then(|q| DataSource::from_str(q))
                .unwrap_or(DataSource::IconEu);
            let body = source.modelrun_iter().map(|mr| format!(
                "------- MODEL RUN {:02} ---------------------\n{:?}\n",
                mr, source.timestep_iter(mr).collect::<Vec<_>>()
            )).fold(String::new(), |mut acc, x| {acc.push_str(&x); acc});
            Box::new(future::ok(hresp(200, body, "text/plain")))
        },
        (&hyper::Method::GET, "/dryrun") => {
            let start_time = chrono::Utc::now() - chrono::Duration::hours(6);
            let target_time = params.get("target")
                .and_then(|q| chrono::DateTime::parse_from_rfc3339(q).ok())
                .map(|f| f.into())
                .unwrap_or_else(chrono::Utc::now);
            let source = params.get("source")
                .and_then(|q| DataSource::from_str(q))
                .unwrap_or(DataSource::IconEu);
            let mut res = String::new();
            res.push_str("----------------------------\n");
            res.push_str(&format!("start:  {}\n", start_time.to_rfc3339_debug()));
            res.push_str(&format!("now:    {}\n", chrono::Utc::now().to_rfc3339_debug()));
            res.push_str(&format!("target: {}\n", target_time.to_rfc3339_debug()));
            res.push_str("----------------------------\n");
            let body = data::forecast_iterator(start_time, target_time, source)
                .map(|timespec| format!("* {}\n", timespec))
                .fold(res, |mut acc, x| {acc.push_str(&x); acc});
            Box::new(future::ok(hresp(200, body, "text/plain")))
        },
        (&hyper::Method::GET, "/subs") => {
            let v: Vec<_> = ui::SUBS.lock().unwrap().values().cloned().collect();
            let f = future::ready(serde_json::to_string_pretty(&v))
                .and_then(|json| future::ok(hresp(200, json, "application/json")))
                .or_else(|se_err| future::ok(hresp(500, se_err.to_string(), "text/plain")));
            Box::new(f)
        },
        (&hyper::Method::POST, "/subs") => {
            let f = req.into_body()
                .try_fold(Vec::new(), |mut acc, x| {acc.extend_from_slice(&x); future::ok::<_, hyper::Error>(acc)}) // -> Future<vec, hyper>
                .and_then(move |body| {
                    let r = serde_json::from_slice::<Vec<ui::Sub>>(&body)
                        .map(|ss| {
                            for s in ss {
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
                                    ui::monitor_weather_wrap(s.clone(), lookup_tz(s.latitude, s.longitude))
                                        .map_ok(|_| ())
                                        .map_err(|err| println!("restored sub err: {}", err))
                                        .map(|_| ())
                                );
                            }
                            (200, "registered".to_owned())
                        });
                    let p = r.unwrap_or_else(|e| (400, e.to_string()));
                    future::ok(hresp(p.0, p.1, "application/json"))
                });
            Box::new(f)
        },
        (&hyper::Method::GET, "/cache") => {
            let f = future::ready(serde_json::to_string_pretty(&cache::stats()))
                .and_then(|json| future::ok(hresp(200, json, "application/json")))
                .or_else(|se_err| future::ok(hresp(500, se_err.to_string(), "text/plain")));
            Box::new(f)
        },
        (&hyper::Method::GET, "/picker") => {
            let start_utc = params.get("start")
                .and_then(|q| chrono::DateTime::parse_from_rfc3339(q).ok())
                .map(|f| f.into())
                .unwrap_or_else(chrono::Utc::now);
            let lat = params.get("lat").and_then(|q| q.parse::<f32>().ok()).unwrap_or(50.62f32);
            let lon = params.get("lon").and_then(|q| q.parse::<f32>().ok()).unwrap_or(26.25f32);
            let tz = lookup_tz(lat, lon);
            let days_utc = ui::time_picker(start_utc);
            let start_target = tz.from_utc_datetime(&start_utc.naive_utc());
            let days_target = ui::time_picker(start_target);

            let mut left = String::new();
            for day in days_utc {
                let (y, m, d) = day.0;
                left.push_str(&format!("{:04}-{:02}-{:02}:\n", y, m, d));
                for row in day.1 {
                    for col in row {
                        if let Some(h) = col {
                            left.push_str(&format!(" {:02} ", h.0));
                        } else {
                            left.push_str(" -- ");
                        }
                    }
                    left.push_str("\n");
                }
            }

            let mut right = String::new();
            for day in days_target {
                let (y, m, d) = day.0;
                right.push_str(&format!("{:04}-{:02}-{:02}:\n", y, m, d));
                for row in day.1 {
                    for col in row {
                        if let Some((h, t)) = col {
                            right.push_str(&format!(" {:02}({:02}/{:02})", h, t.day(), t.hour()));
                        } else {
                            right.push_str(" -- -- -- ");
                        }
                    }
                    right.push_str("\n");
                }
            }

            let mut result = format!(
                "start_utc:    {}\nstart_target: {}\nlon: {}, lat: {} tz: {}\n\n",
                start_utc.to_rfc3339(), start_target.to_rfc3339(), lon, lat, start_target.timezone().name()
            );
            let mut l = left.split('\n');
            let mut r = right.split('\n');
            loop {
                match (l.next(), r.next()) {
                    (Some(a), Some(b)) => result.push_str(&format!("{:<25} {}\n", a,  b)),
                    (Some(a), None,  ) => result.push_str(&format!("{:<25} {}\n", a, "")),
                    (None,    Some(b)) => result.push_str(&format!("{:<25} {}\n", "", b)),
                    _ => break,
                }
            }
            Box::new(future::ok(hresp(200, result, "text/plain")))
        },
        (&hyper::Method::GET, "/daily") => {
            let start_utc = params.get("start")
                .and_then(|q| chrono::DateTime::parse_from_rfc3339(q).ok())
                .map(|f| f.into())
                .unwrap_or_else(chrono::Utc::now);
            let sendh = params.get("sendh").and_then(|q| q.parse::<u8>().ok()).unwrap_or(20);
            let targeth = params.get("targeth").and_then(|q| q.parse::<u8>().ok()).unwrap_or(8);
            let lat = params.get("lat").and_then(|q| q.parse::<f32>().ok()).unwrap_or(50.62f32);
            let lon = params.get("lon").and_then(|q| q.parse::<f32>().ok()).unwrap_or(26.25f32);
            let tz = lookup_tz(lat, lon);
            let start_tz = tz.from_utc_datetime(&start_utc.naive_utc());
            let text = data::daily_iterator(start_utc, sendh, targeth, tz)
                .take(10)
                .fold(
                    format!(
                        "start_utc={}\nstart_tz={}\nschedule={:02}->{:02}\n",
                        start_utc.to_rfc3339_debug(), start_tz.to_rfc3339(),
                        sendh, targeth
                    ),
                    |mut acc, (at, ta)| {
                        acc.push_str(&tz.from_utc_datetime(&at.naive_utc()).to_rfc3339());
                        acc.push_str("/");
                        acc.push_str(&at.to_rfc3339_debug());
                        acc.push_str(" -> ");
                        acc.push_str(&tz.from_utc_datetime(&ta.naive_utc()).to_rfc3339());
                        acc.push_str("/");
                        acc.push_str(&ta.to_rfc3339_debug());
                        acc.push_str("\n");
                        acc
                    }
                );
            Box::new(future::ok(hresp(200, text, "text/plain")))
        },
        (&hyper::Method::GET, "/query") => {
            let source = params.get("source")
                .and_then(|q| DataSource::from_str(q))
                .unwrap_or(DataSource::IconEu);
            let target = params.get("target")
                .and_then(|q| chrono::DateTime::parse_from_rfc3339(q).ok())
                .map(|f| f.into())
                .unwrap_or_else(chrono::Utc::now);
            let lat = params.get("lat").and_then(|q| q.parse::<f32>().ok()).unwrap_or(50.62f32);
            let lon = params.get("lon").and_then(|q| q.parse::<f32>().ok()).unwrap_or(26.25f32);
            let tz = lookup_tz(lat, lon);
            let log = Arc::new(TaggedLog {tag: "=query=".to_owned()});
            let f = data::forecast_stream(log, lat, lon, target, source, source.default_params())
                .into_future()
                .map(|(h, _)| h)
                .then(|opt| future::ready(opt.unwrap_or_else(|| Err("empty stream".to_owned()))))
                .map_ok(move |f| format::format_forecast(&None, lat, lon, &f, tz))
                .and_then(|format::ForecastText(upd)| future::ok(hresp(200, upd, "text/plain; charset=UTF-8")))
                .or_else(|err| future::ok(hresp(500, err, "text/plain")));
            Box::new(f)
        },
        (&hyper::Method::GET, "/uis") => {
            let f = future::ready(serde_json::to_string_pretty(&ui::stats()))
                .and_then(|json| future::ok(hresp(200, json, "application/json")))
                .or_else(|se_err| future::ok(hresp(500, se_err.to_string(), "text/plain")));
            Box::new(f)
        },
        _ => Box::new(future::ok(hresp(404, "[404]\n", "text/plain")))
    }
}

fn forward_updates(url: String, interval: u64) -> impl Future<Output=()> {
    stream::iter(0..)
        .then(move |_i|
            tokio::time::sleep(std::time::Duration::from_secs(interval)).map(Ok)
        )
        .try_fold(None, move |last_id, ()|
            telegram::get_updates(last_id)
                .and_then({ let url = url.clone(); move |v| {
                    stream::iter(v)
                        .filter_map(|(opt_id, json)| future::ready(opt_id.map(move |id| (id, json))))
                        .fold(None, { move |max, (id, json)| {
                            println!(">>>>>>>>>>>>>>>> new update ({}):\n{}\n>>>>>>>>>>>>>>>>>>>>>", id, json);
                            http_post_json(url.clone(), json)
                                .then(move |_| future::ready::<Option<i32>>(max.map(|m: i32| m.max(id)).or(Some(id))))
                        }})
                        .map(Ok)
                }})
                .or_else(move |e| {
                    println!("err: {}", e);
                    future::ok::<_, String>(last_id)
                })
        )
        .map_ok(|_| ())
        .map_err(|e| println!("poll stream error: {}", e))
        .map(|_| ())
}

fn main() {
    let clmatches = clap::App::new("serpanok bot")
        .arg(clap::Arg::with_name("bot_token")
            .short('t').takes_value(true).required(true)
            .help("Telegram Bot API token (required)")
        )
        .arg(clap::Arg::with_name("bind_addr")
            .short('a').takes_value(true).default_value("127.0.0.1:8778")
            .help("Address to start HTTP server at")
        )
        .arg(clap::Arg::with_name("poll_mode")
            .short('f')
            .help("Poll Telegram Bot API for updates")
        )
        .arg(clap::Arg::with_name("enable_weather")
             .short('W')
             .help("Enable weather monitoring features")
        )
        .arg(clap::Arg::with_name("enable_exchange")
             .short('E')
             .help("Enable exchange rate features")
        )
        .arg(clap::Arg::with_name("poll_int")
            .short('i').takes_value(true).default_value("4")
            .help("Update poll interval (seconds)")
        )
        .arg(clap::Arg::with_name("mem_int")
            .short('m').takes_value(true).default_value("60")
            .help("Memory cache purge interval (seconds)")
        )
        .arg(clap::Arg::with_name("disk_int")
            .short('d').takes_value(true).default_value("300")
            .help("Disk cache purge interval (seconds)")
        )
        .arg(clap::Arg::with_name("exch_int")
            .short('e').takes_value(true).default_value("900")
            .help("Exchange rate fetch interval (seconds)")
        )
        .get_matches();
    
    use std::str::FromStr;

    std::env::set_var("BOTTOKEN", clmatches.value_of("bot_token").unwrap());
    let bind_addr = clmatches.value_of("bind_addr")
        .and_then(|s| std::net::SocketAddr::from_str(s).ok())
        .unwrap_or_else(|| std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 8778));
    let poll_mode = clmatches.is_present("poll_mode");
    let poll_int = clmatches.value_of("poll_int").and_then(|s| str::parse::<u64>(s).ok()).unwrap_or(2);
    let mem_int = clmatches.value_of("mem_int").and_then(|s| str::parse::<u64>(s).ok()).unwrap_or(60);
    let disk_int = clmatches.value_of("disk_int").and_then(|s| str::parse::<u64>(s).ok()).unwrap_or(5 * 60);
    let exch_int = clmatches.value_of("exch_int").and_then(|s| str::parse::<u64>(s).ok()).unwrap_or(15 * 60);
    let features = ui::Features {
        weather: clmatches.is_present("enable_weather"),
        exchange: clmatches.is_present("enable_exchange"),
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let exec = rt.handle().clone();
    let (shutdown_tx, shutdown_rx) = futures::channel::oneshot::channel::<()>();

    let join_handle = {
        let _guard = rt.enter();
        
        let new_service = hyper::service::make_service_fn({
            let exec = exec.clone();
            move |_| {
                let exec = exec.clone();
                let sfn = hyper::service::service_fn(move |req| serpanok_api(exec.clone(), req, features));
                future::ok::<_, hyper::Error>(sfn)
            }
        });
    
        let server = hyper::Server::bind(&bind_addr).serve(new_service).map_err(|e| println!("hyper error: {}", e));
        println!("Starting server: http://{}/", bind_addr);
        exec.spawn(server.map(|_| ()));

        if true {
            let purge_mem_cache = tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(std::time::Duration::from_secs(mem_int)))
                .map(|_| cache::purge_mem_cache())
                .fold((), |_, _| future::ready( () ));
            
            let purge_disk_cache = tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(std::time::Duration::from_secs(disk_int)))
                .map(|_| cache::purge_disk_cache())
                .fold((), |_, _| future::ready( () ));
            
            exec.spawn(purge_mem_cache);
            exec.spawn(purge_disk_cache);
        }

        if features.exchange {
            exec.spawn(data::poll_exchange_rate(exch_int));
        }

        if poll_mode {
            exec.spawn(forward_updates(format!("http://{}/bot", bind_addr), poll_int));
        }

        shutdown_rx.map(|_| ())
    };

    rt.block_on(join_handle);
    shutdown_tx.send( () ).unwrap()
}
