extern crate futures;
extern crate tokio;
extern crate chrono;
extern crate itertools;
#[macro_use] extern crate nom;
extern crate bzip2;
extern crate either;
#[macro_use] extern crate lazy_static;
extern crate hyper;
extern crate hyper_tls;
extern crate url;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;
extern crate regex;

use std::io::Read;
use chrono::{Datelike, TimeZone};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum Parameter {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct FileKey {
    param: Parameter,
    yyyy: u16,
    mm: u8,
    dd: u8,
    modelrun: u8,
    timestep: u8,
}

impl FileKey {
    #[cfg(test)]
    fn test_new() -> Self {
        Self {
            param: Parameter::Temperature2m,
            yyyy: 2018,
            mm: 11,
            dd: 24,
            modelrun: 15,
            timestep: 0,
        }
    }

    fn new(param: Parameter, dt: chrono::Date<chrono::Utc>, modelrun: u8, timestep: u8) -> Self {
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
}

mod grib;
mod telegram;
mod ui;
mod cache;
mod icon;
mod data;
mod format;

pub struct TaggedLog {
    tag: String,
}

impl TaggedLog {
    fn add_line(&self, s: &str) {
        println!("[{}] {} {}", chrono::Utc::now().to_rfc3339(), &self.tag, s);
    }
}

fn unpack_bzip2(bytes: &[u8]) -> impl Future<Item=Vec<u8>, Error=String> {
    let mut vec = Vec::new();
    let res = bzip2::read::BzDecoder::new(bytes)
        .read_to_end(&mut vec)
        .map(move |_sz| vec)
        .map_err(|e| format!("unpack error: {:?}", e));
    future::result(res) // TODO:
}

use futures::{future, Future, stream, Stream};

fn fold_response_body(resp: hyper::Response<hyper::Body>) -> impl Future<Item=(bool, Vec<u8>), Error=String> {
    let status = resp.status();
    resp.into_body().fold(
        Vec::new(),
        |mut acc, x| { acc.extend_from_slice(&x); future::ok::<_, hyper::Error>(acc) }
    ).map(move |v| (status == hyper::StatusCode::OK, v)).map_err(|e| format!("fold body error: {}", e.to_string()))
}

fn http_post_json(url: String, json: String) -> impl Future<Item=(bool, Vec<u8>), Error=String> {
    let req = hyper::Request::post(&url)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(hyper::Body::from(json))
        .expect("POST request build failed");

    HTTP_CLIENT
        .request(req)
        .map_err(move |e| format!("POST {} failed: {}", url, e))
        .and_then(fold_response_body)
        .map_err(|e| format!("botapi response error: {}", e))
}

fn http_get(url: String) -> impl Future<Item=(bool, Vec<u8>), Error=String> {
    let req = hyper::Request::get(&url)
        .body(hyper::Body::from(""))
        .expect(&format!("GET request build failed for {}", &url));

    HTTP_CLIENT
        .request(req)
        .map_err(move |e| format!("GET {} failed: {}", url, e))
        .and_then(fold_response_body)
        .map_err(|e| format!("botapi response error: {}", e))
}

fn fetch_url(log: std::sync::Arc<TaggedLog>, url: String) -> impl Future<Item=Vec<u8>, Error=String> {
    log.add_line(&format!("GET {}", url));
    http_get(url).and_then(|(ok, body)|
        if ok {
            future::Either::A(future::ok(body))
        } else {
            future::Either::B(future::err(format!("response status is not OK")))
        }
    )
}

lazy_static! {
    static ref HTTP_CLIENT: hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>, hyper::Body> = hyper::Client::builder()
        .build::<_, hyper::Body>(hyper_tls::HttpsConnector::new(4).expect("TLS init failed"));
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

struct SerpanokApi {
    executor: tokio::runtime::TaskExecutor,
}

fn hresp<T>(code: u16, t: T) -> hyper::Response<hyper::Body>
where T: Into<hyper::Body> {
    hyper::Response::builder().status(code).body(t.into()).unwrap()
}

impl hyper::service::Service for SerpanokApi {
    type ReqBody = hyper::Body;
    type ResBody = hyper::Body;
    type Error = hyper::Error;
    type Future = Box<dyn Future<Item=hyper::Response<Self::ResBody>, Error=Self::Error> + Send>;

    fn call(&mut self, req: hyper::Request<Self::ReqBody>) -> Self::Future {
        println!("request: {} {}", req.method(), req.uri());
        let query: &[u8] = req.uri().query().unwrap_or("").as_bytes();
        let params: std::collections::HashMap<String, String> =
            url::form_urlencoded::parse(query).into_owned().collect();

        match (req.method(), req.uri().path()) {
            (&hyper::Method::POST, "/bot") => {
                let exec = self.executor.clone();
                let f = req.into_body()
                    .fold(Vec::new(), |mut acc, x| {acc.extend_from_slice(&x); future::ok::<_, hyper::Error>(acc)}) // -> Future<vec, hyper>
                    .and_then(move |body| {
                        match serde_json::from_slice::<telegram::TgUpdate>(&body) {
                            Ok(tgu) => exec.spawn(ui::process_update(tgu).map_err(|e| println!("process update error: {}", e))),
                            Err(err) => println!("TgUpdate parse error: {}", err.to_string()),
                        }
                        future::ok( () )
                    })
                    .then(|_| future::ok(hresp(200, "")));
                Box::new(f)
            },
            (&hyper::Method::GET, "/modeltimes") => {
                let body = icon::icon_modelrun_iter().map(|mr| {
                    format!("------- MODEL RUN {:02} ---------------------\n{:?}\n", mr, icon::icon_timestep_iter(mr).collect::<Vec<_>>())
                }).fold(String::new(), |mut acc, x| {acc.push_str(&x); acc});
                Box::new(future::ok(hresp(200, body)))
            },
            (&hyper::Method::GET, "/dryrun") => {
                let start_time = chrono::Utc::now() - chrono::Duration::hours(6);
                let target_time = params.get("target")
                    .and_then(|q| chrono::DateTime::parse_from_rfc3339(q).ok())
                    .map(|f| f.into())
                    .unwrap_or(chrono::Utc::now());
                let mut res = String::new();
                res.push_str("----------------------------\n");
                res.push_str(&format!("start: {}\n", start_time.to_rfc3339()));
                res.push_str(&format!("now: {}\n", chrono::Utc::now().to_rfc3339()));
                res.push_str(&format!("target: {}\n", target_time.to_rfc3339()));
                res.push_str("----------------------------\n");
                let body = data::forecast_iterator(start_time, target_time, icon::icon_modelrun_iter, icon::icon_timestep_iter)
                    .map(|(mrt, mr, ft, ts, ft1, ts1)| format!("* {}/{:02} >> {}/{:03}  add {}/{:03}\n",
                                                               mrt.to_rfc3339(), mr, ft.to_rfc3339(), ts, ft1.to_rfc3339(), ts1
                    ))
                    .fold(res, |mut acc, x| {acc.push_str(&x); acc});
                Box::new(future::ok(hresp(200, body)))
            },
            (&hyper::Method::GET, "/subs") => {
                let v: Vec<_> = ui::SUBS.lock().unwrap().values().cloned().collect();
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
                                        ui::monitor_weather_wrap(s.clone())
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
                let stats = (cache::disk_stats(), cache::mem_stats());
                let f = future::result(serde_json::to_string_pretty(&stats))
                    .and_then(|json| future::ok(hresp(200, json)))
                    .or_else(|se_err| future::ok(hresp(500, se_err.to_string())));
                Box::new(f)
            },
            (&hyper::Method::GET, "/picker") => {
                let start = params.get("start")
                    .and_then(|q| chrono::DateTime::parse_from_rfc3339(q).ok())
                    .map(|f| f.into())
                    .unwrap_or(chrono::Utc::now());
                let lat = params.get("lat").and_then(|q| q.parse::<f32>().ok()).unwrap_or(50.62f32);
                let lon = params.get("lon").and_then(|q| q.parse::<f32>().ok()).unwrap_or(26.25f32);
                let days = ui::time_picker(start);
                let mut result = format!("start={}, lon={}, lat={}\n", start.to_rfc3339(), lon, lat);
                for day in days {
                    result.push_str(&format!("{:04}-{:02}-{:02}:\n", day.0, day.1, day.2));
                    for row in day.3 {
                        for col in row {
                            if let Some(h) = col {
                                result.push_str(&format!(" {:02} ", h));
                            } else {
                                result.push_str(" -- ");
                            }
                        }
                        result.push_str("\n");
                    }
                }
                Box::new(future::ok(hresp(200, result)))
            },
            (&hyper::Method::GET, "/uis") => {
                let cs: Vec<(i64, i32)> = ui::USER_CLICKS.lock().unwrap().keys().cloned().collect();
                let is: Vec<i64> = ui::USER_INPUTS.lock().unwrap().keys().cloned().collect();
                let f = future::result(serde_json::to_string_pretty(&(cs, is)))
                    .and_then(|json| future::ok(hresp(200, json)))
                    .or_else(|se_err| future::ok(hresp(500, se_err.to_string())));
                Box::new(f)
            },
            _ => Box::new(future::ok(hresp(404, "[404]\n")))
        }
    }
}

fn forward_updates(url: String) -> impl Future<Item=(), Error=()> {
    stream::iter_ok::<_, String>(0..)
        .and_then(|_i|
            tokio::timer::Delay::new(std::time::Instant::now() + std::time::Duration::from_secs(3))
                .map_err(|err| format!("delay error: {}", err.to_string()))
        )
        .fold(None, move |last_id, ()|
            telegram::get_updates(last_id)
                .and_then({ let url = url.clone(); move |v|
                    stream::iter_ok(v)
                        .filter_map(|(opt_id, json)| opt_id.map(move |id| (id, json)))
                        .fold(None, { move |max, (id, json)| {
                            println!(">>>>>>>>>>>>>>>> new update ({}):\n{}\n>>>>>>>>>>>>>>>>>>>>>", id, json);
                            http_post_json(url.clone(), json)
                                .then(move |_| future::ok::<Option<i32>, String>(max.map(|m: i32| m.max(id)).or(Some(id))))
                        }})
                })
                .or_else(move |e| {
                    println!("err: {}", e);
                    future::ok::<_, String>(last_id)
                })
        )
        .map(|_| ())
        .map_err(|e| println!("poll stream error: {}", e.to_string()))
}

fn main() {

    //println!("pick up cache: {} entries", cache::DISK_CACHE.lock().unwrap().len());

    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let ex = rt.executor();

    let new_service = move || future::ok::<_, hyper::Error>(SerpanokApi {
        executor: ex.clone(),
    });

    let addr = ([127, 0, 0, 1], 8778).into();
    let server = hyper::Server::bind(&addr).serve(new_service).map_err(|e| println!("hyper error: {}", e));
    let addr_str = addr.to_string();
    println!("---> http://{}/", addr_str);
    let purge_mem_cache = tokio::timer::Interval::new_interval(std::time::Duration::from_secs(60))
        .map(|_| cache::purge_mem_cache())
        .map_err(|e| println!("purge mem interval error: {}", e.to_string()))
        .fold((), |_, _| Ok(()));

    let purge_disk_cache = tokio::timer::Interval::new_interval(std::time::Duration::from_secs(60 * 5))
        .map(|_| cache::purge_disk_cache())
        .map_err(|e| println!("purge disk interval error: {}", e.to_string()))
        .fold((), |_, _| Ok(()));

    rt.spawn(server);

    rt.spawn(forward_updates(format!("http://{}/bot", addr_str)));
    rt.spawn(purge_mem_cache);
    rt.spawn(purge_disk_cache);

    rt.shutdown_on_idle().wait().unwrap();
}
