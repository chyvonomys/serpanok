use super::{SourceParameter, FileKey, DataSource, TaggedLog, DebugRFC3339};
use crate::cache;
use crate::icon; // TODO:
use crate::gfs;
use crate::grib;
use std::sync::Arc;
use chrono::{Timelike, TimeZone};
use futures::{stream, Stream, StreamExt, TryStreamExt, future, Future, FutureExt, TryFutureExt};
use itertools::Itertools; // tuple_windows, collect_vec
use bitflags::*;

pub struct ForecastTimeSpec {
    modelrun_time: chrono::DateTime<chrono::Utc>,
    modelrun: u8,
    timestep0_time: chrono::DateTime<chrono::Utc>,
    timestep0: u16,
    timestep1_time: chrono::DateTime<chrono::Utc>,
    timestep1: u16,
}

impl std::fmt::Display for ForecastTimeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f, "{}/{:02} >> {}/{:03} .. {}/{:03}",
            self.modelrun_time.to_rfc3339_debug(), self.modelrun,
            self.timestep0_time.to_rfc3339_debug(), self.timestep0,
            self.timestep1_time.to_rfc3339_debug(), self.timestep1,
        )
    }
}

pub fn forecast_iterator(
    start_time: chrono::DateTime<chrono::Utc>, target_time: chrono::DateTime<chrono::Utc>, source: DataSource
) -> impl Iterator<Item=ForecastTimeSpec> {

    let start_time_d = start_time.date().and_hms(0, 0, 0); // trunc to days
    let start_time_h = start_time.date().and_hms(start_time.hour(), 0, 0); // trunc to hours

    (0..)
        .flat_map(move |day| source.modelrun_iter().map(
            move |hh| (start_time_d + chrono::Duration::hours(day as i64 * 24 + hh as i64), hh)
        ))
        .tuple_windows::<(_, _)>()
        .skip_while(move |(_, t)| t.0 <= start_time_h)
        .map(|(f, _)| f)
        .take_while(move |(hhtime, _)| target_time >= *hhtime)
        .map(move |(hhtime, hh)| {
            let opt = source.timestep_iter(hh)
             .map(|ts| (hhtime + chrono::Duration::hours(ts.into()), ts))
             .tuple_windows::<(_, _)>()
             .take_while(|(f, _)| target_time >= f.0)
             .skip_while(|(_, t)| target_time >= t.0)
             .next();
            (hhtime, hh, opt)
        })
        .filter_map(|(s, mr, o)| o.map(|(x0, x1)| (s, mr, x0, x1)))
        .map(|(modelrun_time, modelrun, (timestep0_time, timestep0), (timestep1_time, timestep1))| ForecastTimeSpec {
            modelrun_time, modelrun,
            timestep0_time, timestep0,
            timestep1_time, timestep1,
        })
}

fn lerp(a: f32, b: f32, t: f32) -> f32 {
    a * (1.0 - t) + b * t
}

#[derive(Debug)]
pub enum SamplingError {
    UnsupportedScanMode,
    OutsideOfDomain,
    InvalidDomain,
}

pub fn sample_value(values: &[f32], s3: &grib::Section3, latf: f32, lonf: f32) -> Result<f32, SamplingError> {
    let lat = (latf * 1_000_000.0) as i32;
    let lon = (lonf * 1_000_000.0) as i32;

    let lon0 = if s3.lon_first > s3.lon_last { s3.lon_first - 360_000_000 } else { s3.lon_first };
    let lon1 = s3.lon_last;

    let lon = if lon < lon0 { lon + 360_000_000 } else { lon };

    let lat0 = s3.lat_first;
    let lat1 = s3.lat_last;

    let lon_step = s3.di;
    let lat_step = s3.dj;

    let cols = s3.ni;

    match s3.scan_mode {
        grib::ScanMode{col_major: false, zigzag: false, i_neg: false, j_pos: true} => {
            if lat1 > lat0 {
                if lat >= lat0 && lat <= lat1 && lon >= lon0 && lon <= lon1 {
                    let j0 = (lat - lat0) as u32 / lat_step;
                    let jt = (lat - lat0) as u32 % lat_step;
                    Ok((j0, jt))
                } else { Err(SamplingError::OutsideOfDomain) }
            } else { Err(SamplingError::InvalidDomain) }
        },
        grib::ScanMode{col_major: false, zigzag: false, i_neg: false, j_pos: false} => {
            if lat0 > lat1 {
                if lat >= lat1 && lat < lat0 && lon >= lon0 && lon <= lon1 {
                    let j0 = (lat0 - lat) as u32 / lat_step;
                    let jt = (lat0 - lat) as u32 % lat_step;
                    Ok((j0, jt))
                } else { Err(SamplingError::OutsideOfDomain) }
            } else { Err(SamplingError::InvalidDomain) }
        },
        _ => Err(SamplingError::UnsupportedScanMode),
    }
    .map(|(j0, jt)| {
        let i0 = (lon - lon0) as u32 / lon_step;
        let it = (lon - lon0) as u32 % lon_step;
        let i1 = if it == 0 { i0 } else { i0 + 1 };
        let j1 = if jt == 0 { j0 } else { j0 + 1 };

        let x00 = values[(j0 * cols + i0) as usize];
        let x10 = values[(j0 * cols + i1) as usize];
        let x01 = values[(j1 * cols + i0) as usize];
        let x11 = values[(j1 * cols + i1) as usize];

        let xt = it as f32 / lon_step as f32;
        let yt = jt as f32 / lat_step as f32;

        lerp(lerp(x00, x10, xt), lerp(x01, x11, xt), yt)
    })
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
        scan_mode: grib::ScanMode{i_neg: false, j_pos: true, col_major: false, zigzag: false},
    };

    let mut values = Vec::new();
    values.resize(s3.n_data_points as usize, 0.0);

    assert!(sample_value(&values, &s3, 29.5, -23.5).is_ok());
    assert!(sample_value(&values, &s3, 29.5,  45.0).is_ok());
    assert!(sample_value(&values, &s3, 70.5, -23.5).is_ok());
    assert!(sample_value(&values, &s3, 70.5,  45.0).is_ok());

    assert!(sample_value(&values, &s3, 29.5 - 0.00001, -23.5 - 0.00001).is_err());
    assert!(sample_value(&values, &s3, 29.5 - 0.00001,  45.0 + 0.00001).is_err());
    assert!(sample_value(&values, &s3, 70.5 + 0.00001, -23.5 - 0.00001).is_err());
    assert!(sample_value(&values, &s3, 70.5 + 0.00001,  45.0 + 0.00001).is_err());
}

// TODO: optimize, don't decode whole grid
fn extract_value_at(msg: &grib::GribMessage, lat: f32, lon: f32) -> Result<f32, String> {
    grib::decode_original_values(msg)
        .and_then(|ys| sample_value(&ys, &msg.section3, lat, lon).map_err(|e| format!("{:?}", e)))
}

fn avail_value(
    log: Arc<TaggedLog>,
    param: SourceParameter,
    modelrun: ModelrunSpec,
    timestep: u16,
) -> impl Future<Output=Result<(), String>> {
    let file_key = FileKey::new(param, modelrun.0, modelrun.1, timestep);
    cache::avail_grid(log, file_key)
        .map_err(|e| format!("avail_value error: {}", e))
}

fn fetch_value(
    log: Arc<TaggedLog>,
    param: SourceParameter,
    lat: f32, lon: f32,
    modelrun: ModelrunSpec,
    timestep: u16,
) -> impl Future<Output=Result<f32, String>> {

    let file_key = FileKey::new(param.clone(), modelrun.0, modelrun.1, timestep);
    cache::fetch_grid(log, file_key)
        .and_then(move |grid| {
            if param.verify_grib2(&grid) {
                future::ok(grid)
            } else {
                future::err("parameter cat/num mismatch".to_owned())
            }
        })
        .and_then(move |grid| future::ready(extract_value_at(&grid, lat, lon)))
        .map_err(|e| format!("fetch_value error: {}", e))
}

fn opt_wrap<FN, F, I>(b: bool, f: FN) -> impl Future<Output=Result<Option<I>, String>>
where FN: FnOnce() -> F, F: Future<Output=Result<I, String>> {
    if b {
        future::Either::Left(f().map_ok(Some))
    } else {
        future::Either::Right(future::ok(None))
    }
}

type ModelrunSpec = (chrono::Date<chrono::Utc>, u8);

use serde_derive::{Serialize, Deserialize};

bitflags! {
    #[derive(Serialize, Deserialize)]
    struct TargetParameters: u16 {
        const TEMPERATURE_2M    = 0b0001;
        const TOTAL_CLOUD_COVER = 0b0010;
        const WIND_SPEED_UV10M  = 0b0100;
        const SNOW_PRECIP_RATE  = 0b1000;
        const RAIN_PRECIP_RATE  = 0b0001_0000;
        const SNOW_DEPTH        = 0b0010_0000;
        const PRESSURE_MSL      = 0b0100_0000;
        const REL_HUMIDITY_2M   = 0b1000_0000;
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq)]
pub struct ParameterFlags {
    available: TargetParameters,
    selected: TargetParameters,
}

impl ParameterFlags {
    pub fn icon() -> Self {
        let selected = 
            TargetParameters::TEMPERATURE_2M | TargetParameters::TOTAL_CLOUD_COVER | TargetParameters::WIND_SPEED_UV10M |
            TargetParameters::SNOW_PRECIP_RATE | TargetParameters::RAIN_PRECIP_RATE;

        Self {
            available: TargetParameters::all(), selected
        }
    }

    pub fn gfs() -> Self {
        let available = TargetParameters::TEMPERATURE_2M | TargetParameters::WIND_SPEED_UV10M;

        Self {
            available, selected: available
        }
    }

    pub fn flip_tmp(mut self) -> Self { self.selected.toggle(self.available &TargetParameters::TEMPERATURE_2M); self }
    pub fn flip_tcc(mut self) -> Self { self.selected.toggle(self.available & TargetParameters::TOTAL_CLOUD_COVER); self }
    pub fn flip_wind(mut self) -> Self { self.selected.toggle(self.available & TargetParameters::WIND_SPEED_UV10M); self }
    pub fn flip_snow(mut self) -> Self { self.selected.toggle(self.available & TargetParameters::SNOW_PRECIP_RATE); self }
    pub fn flip_rain(mut self) -> Self { self.selected.toggle(self.available & TargetParameters::RAIN_PRECIP_RATE); self }
    pub fn flip_depth(mut self) -> Self { self.selected.toggle(self.available & TargetParameters::SNOW_DEPTH); self }
    pub fn flip_pmsl(mut self) -> Self { self.selected.toggle(self.available & TargetParameters::PRESSURE_MSL); self }
    pub fn flip_relhum(mut self) -> Self { self.selected.toggle(self.available & TargetParameters::REL_HUMIDITY_2M); self }

    pub fn tmp(self) -> bool { self.selected.contains(TargetParameters::TEMPERATURE_2M) }
    pub fn tcc(self) -> bool { self.selected.contains(TargetParameters::TOTAL_CLOUD_COVER) }
    pub fn wind(self) -> bool { self.selected.contains(TargetParameters::WIND_SPEED_UV10M) }
    pub fn snow(self) -> bool { self.selected.contains(TargetParameters::SNOW_PRECIP_RATE) }
    pub fn rain(self) -> bool { self.selected.contains(TargetParameters::RAIN_PRECIP_RATE) }
    pub fn depth(self) -> bool { self.selected.contains(TargetParameters::SNOW_DEPTH) }
    pub fn pmsl(self) -> bool { self.selected.contains(TargetParameters::PRESSURE_MSL) }
    pub fn relhum(self) -> bool { self.selected.contains(TargetParameters::REL_HUMIDITY_2M) }
}

pub fn gfs_avail_all(
    res: gfs::GfsResolution, log: Arc<TaggedLog>, timespec: ForecastTimeSpec
) -> impl Future<Output=Result<(), String>> {
    let mr = (timespec.modelrun_time.date(), timespec.modelrun);
    let ts = timespec.timestep0;
    let ts1 = timespec.timestep1;
    // GFS has everything in one file, so checking one parameter is enough
    let param = SourceParameter::Gfs(gfs::GfsParameter::Temperature2m, res);
    future::try_join(
        avail_value(log.clone(), param.clone(), mr, ts),
        avail_value(log, param, mr, ts1)
    ).map_ok(|_| ())
}

pub fn icon_avail_all(
    log: Arc<TaggedLog>, timespec: ForecastTimeSpec, params: ParameterFlags
) -> impl Future<Output=Result<(), String>> {

    let log1 = log.clone();
    let log2 = log.clone();
    let log4 = log.clone();
    let log5 = log.clone();
    let mr = (timespec.modelrun_time.date(), timespec.modelrun);
    let ts = timespec.timestep0;
    let ts1 = timespec.timestep1;

    use SourceParameter::IconEu;
    use icon::IconParameter::*;

    future::try_join4(
        opt_wrap(params.tmp(), { let l = log.clone(); move || avail_value(l, IconEu(Temperature2m), mr, ts)}),
        future::try_join3(
            opt_wrap(params.tcc(), { let l = log.clone(); move || avail_value(l, IconEu(TotalCloudCover), mr, ts)}),
            opt_wrap(params.relhum(), { let l = log.clone(); move || avail_value(l, IconEu(RelHumidity2m), mr, ts)}),
            opt_wrap(params.pmsl(), move || avail_value(log1, IconEu(PressureMSL), mr, ts)),
        ),
        opt_wrap(params.wind(), move || future::try_join(
            avail_value(log2.clone(), IconEu(WindSpeedU10m), mr, ts),
            avail_value(log2, IconEu(WindSpeedV10m), mr, ts),
        )),
        future::try_join3(
            opt_wrap(params.rain(), move || future::try_join4(
                avail_value(log4.clone(), IconEu(LargeScaleRain), mr, ts),
                avail_value(log4.clone(), IconEu(LargeScaleRain), mr, ts1),
                avail_value(log4.clone(), IconEu(ConvectiveRain), mr, ts),
                avail_value(log4, IconEu(ConvectiveRain), mr, ts1),
            )),
            opt_wrap(params.snow(), move || future::try_join4(
                avail_value(log5.clone(), IconEu(LargeScaleSnow), mr, ts),
                avail_value(log5.clone(), IconEu(LargeScaleSnow), mr, ts1),
                avail_value(log5.clone(), IconEu(ConvectiveSnow), mr, ts),
                avail_value(log5, IconEu(ConvectiveSnow), mr, ts1),
            )),
            opt_wrap(params.depth(), move || avail_value(log, IconEu(SnowDepth), mr, ts)),
        ),
    ).map_ok(move |_| ())
}

pub fn icon_fetch_all(
    log: Arc<TaggedLog>, lat: f32, lon: f32, timespec: ForecastTimeSpec, params: ParameterFlags
) -> impl Future<Output=Result<Forecast, String>> {

    let log1 = log.clone();
    let log2 = log.clone();
    let log4 = log.clone();
    let log5 = log.clone();
    let mr = (timespec.modelrun_time.date(), timespec.modelrun);
    let ts = timespec.timestep0;
    let ts1 = timespec.timestep1;

    use SourceParameter::IconEu;
    use icon::IconParameter::*;

    future::try_join4(
        opt_wrap(params.tmp(), { let l = log.clone(); move || fetch_value(l, IconEu(Temperature2m), lat, lon, mr, ts)}),
        future::try_join3(
            opt_wrap(params.tcc(), { let l = log.clone(); move || fetch_value(l, IconEu(TotalCloudCover), lat, lon, mr, ts)}),
            opt_wrap(params.relhum(), { let l = log.clone(); move || fetch_value(l, IconEu(RelHumidity2m), lat, lon, mr, ts)}),
            opt_wrap(params.pmsl(), move || fetch_value(log1, IconEu(PressureMSL), lat, lon, mr, ts)),
        ),
        opt_wrap(params.wind(), move || future::try_join(
            fetch_value(log2.clone(), IconEu(WindSpeedU10m), lat, lon, mr, ts),
            fetch_value(log2, IconEu(WindSpeedV10m), lat, lon, mr, ts),
        )),
        future::try_join3(
            opt_wrap(params.rain(), move || future::try_join4(
                fetch_value(log4.clone(), IconEu(LargeScaleRain), lat, lon, mr, ts),
                fetch_value(log4.clone(), IconEu(LargeScaleRain), lat, lon, mr, ts1),
                fetch_value(log4.clone(), IconEu(ConvectiveRain), lat, lon, mr, ts),
                fetch_value(log4, IconEu(ConvectiveRain), lat, lon, mr, ts1),
            )),
            opt_wrap(params.snow(), move || future::try_join4(
                fetch_value(log5.clone(), IconEu(LargeScaleSnow), lat, lon, mr, ts),
                fetch_value(log5.clone(), IconEu(LargeScaleSnow), lat, lon, mr, ts1),
                fetch_value(log5.clone(), IconEu(ConvectiveSnow), lat, lon, mr, ts),
                fetch_value(log5, IconEu(ConvectiveSnow), lat, lon, mr, ts1),
            )),
            opt_wrap(params.depth(), move || fetch_value(log, IconEu(SnowDepth), lat, lon, mr, ts)),
        ),
    )
    .map_ok(move |(ot, (otcc, orhum, opmsl), owind, (orain, osnow, odepth))| Forecast {
        temperature: ot,
        total_cloud_cover: otcc,
        rel_humidity: orhum,
        pressure_msl: opmsl,
        wind_speed: owind,
        rain_accum: orain.map(|(ls0, ls1, c0, c1)| (ls0 + c0, ls1 + c1)),
        snow_accum: osnow.map(|(ls0, ls1, c0, c1)| (ls0 + c0, ls1 + c1)),
        snow_depth: odepth,
        time: (timespec.timestep0_time, timespec.timestep1_time),
    })
}

pub fn gfs_fetch_all(
    log: Arc<TaggedLog>, lat: f32, lon: f32, timespec: ForecastTimeSpec, params: ParameterFlags, res: gfs::GfsResolution
) -> impl Future<Output=Result<Forecast, String>> {

    let log1 = log.clone();
    let mr = (timespec.modelrun_time.date(), timespec.modelrun);
    let ts = timespec.timestep0;

    use SourceParameter::Gfs;
    use gfs::GfsParameter::*;

    future::try_join(
        opt_wrap(params.tmp(), move || fetch_value(log1, Gfs(Temperature2m, res), lat, lon, mr, ts)),
        opt_wrap(params.wind(), move || future::try_join(
            fetch_value(log.clone(), Gfs(UComponentWind10m, res), lat, lon, mr, ts),
            fetch_value(log, Gfs(VComponentWind10m, res), lat, lon, mr, ts),
        ))
    )
        .map_ok(move |(ot, owind)| Forecast {
            temperature: ot,
            total_cloud_cover: None,
            rel_humidity: None,
            pressure_msl: None,
            wind_speed: owind,
            rain_accum: None,
            snow_accum: None,
            snow_depth: None,
            time: (timespec.timestep0_time, timespec.timestep1_time),
        })
}

fn select_start_time<F, R>(
    log: Arc<TaggedLog>, target_time: chrono::DateTime<chrono::Utc>, source: DataSource, try_func: F
) -> impl Future<Output=Result<chrono::DateTime<chrono::Utc>, String>>
where
    F: Fn(Arc<TaggedLog>, ForecastTimeSpec) -> R,
    R: Future<Output=Result<(), String>> + Unpin,
{
    let now = chrono::Utc::now();
    let start_time = now - chrono::Duration::hours(12);
    let fs: Vec<_> = forecast_iterator(start_time, target_time, source).collect();

    stream::unfold(fs, |mut fs| future::ready( fs.pop().map(|x| (x, fs)) ) )
        .skip_while(move |ForecastTimeSpec {modelrun_time, ..}| {
            let skip = now < *modelrun_time;
            future::ready(skip)
        })
        .then(move |timespec| {
            let mrt = timespec.modelrun_time;
            log.add_line(&format!("try {} {}", source, timespec));
            let log = log.clone();
            try_func(log.clone(), timespec)
                .then(move |v: Result<(), String>| {
                    let res = match v {
                        Ok( () ) => Some(mrt),
                        Err(s) => { log.add_line(&format!("failed with: {}", s)); None },
                    };
                    future::ready(res)
                })
        })
        .filter_map(future::ready)
        .into_future()
        .map(|(f, _)| f.ok_or_else(|| "tried all start times".to_string()))
}

pub struct Forecast {
    pub temperature: Option<f32>,
    pub time: (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>),
    pub total_cloud_cover: Option<f32>,
    pub rel_humidity: Option<f32>,
    pub pressure_msl: Option<f32>,
    pub wind_speed: Option<(f32, f32)>,
    pub rain_accum: Option<(f32, f32)>,
    pub snow_accum: Option<(f32, f32)>,
    pub snow_depth: Option<f32>,
}

pub fn forecast_stream(
    log: Arc<TaggedLog>, lat: f32, lon: f32, target_time: chrono::DateTime<chrono::Utc>, source: DataSource, params: ParameterFlags
) -> impl Stream<Item=Result<Forecast, String>> {

    select_start_time(
        log.clone(), target_time, source,
        move |log, timespec| source.avail_all(log, timespec, params)
    )
    .map_ok(move |f| {
        stream::iter(forecast_iterator(f, target_time, source))
            .then({ let log = log.clone(); move |timespec| {
                log.add_line(&format!("want {} {}", source, timespec));
                source.fetch_all(log.clone(), lat, lon, timespec, params)
            }})
            .inspect_err(move |e| log.add_line(&format!("monitor stream error: {}", e)))
            .then(future::ok) // stream of values --> stream of results
            .filter_map(|item: Result<_, String>| future::ready(item.ok())) // drop errors
    })
    .try_flatten_stream()
}

pub fn daily_iterator(
    start_time: chrono::DateTime<chrono::Utc>, h0: u8, mut h1: u8, tz: chrono_tz::Tz
) -> impl Iterator<Item=(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)> {

    let start_tz = tz.from_utc_datetime(&start_time.naive_utc());
    let start_tz_trunc = start_tz.date().and_hms(0, 0, 0);

    if h1 <= h0 {
        h1 += 24;
    }

    (0..).map(move |i| {
        let send   = (start_tz_trunc + chrono::Duration::hours(i64::from(24 * i + h0)))
            .date().and_hms_opt((h0 % 24).into(), 0, 0)
            .map(|t| chrono::DateTime::<chrono::Utc>::from_utc(t.naive_utc(), chrono::Utc));

        let target = (start_tz_trunc + chrono::Duration::hours(i64::from(24 * i + h1)))
            .date().and_hms_opt((h1 % 24).into(), 0, 0)
            .map(|t| chrono::DateTime::<chrono::Utc>::from_utc(t.naive_utc(), chrono::Utc));

        (send, target)
    }).filter_map(move |p|
        match p {
            (Some(s), Some(t)) if s > start_time => Some((s, t)),
            _ => None,
        }
    )
}

pub fn daily_forecast_stream(
    log: Arc<TaggedLog>, lat: f32, lon: f32, sendh: u8, targeth: u8, tz: chrono_tz::Tz, source: DataSource, params: ParameterFlags
) -> impl Stream<Item=Result<Forecast, String>> {
    stream::iter(daily_iterator(chrono::Utc::now(), sendh, targeth, tz))
        .then(move |(at, target)| {
            let dur = (at - chrono::Utc::now()).to_std().unwrap_or_default();
            tokio::time::sleep(dur)
                .then({ let log = log.clone(); move |()|
                    forecast_stream(log, lat, lon, target, source, params)
                        .into_future()
                        .map(|(head, _tail)| head.unwrap_or_else(|| Err("empty daily stream".to_owned())))
                })
        })
}

pub struct Rates {
    pub label: String,
    pub usd_buy: f32,
    pub usd_sell: f32,
    pub eur_buy: f32,
    pub eur_sell: f32,
}

use select;
use select::predicate::{Name, Class};
use super::fetch_url;

static RULYAURL: &'static str = "http://rulya-bank.com.ua";

pub async fn rulya_fetch_rates(log: Arc<TaggedLog>) -> Result<Rates, String> {
    let bytes: Vec<u8> = fetch_url(log, RULYAURL.to_owned(), &[]).await?;

    if let Ok(document) = select::document::Document::from_read(bytes.as_slice()) {
        if let Some(div2) = document.find(Class("col-md-3")).take(1).next() {
            if let Some(center) = div2.find(Name("center")).take(1).next() {
                let label = center.text();

                let rates: Vec<f32> = div2.find(Class("tbl"))
                    .take(4)
                    .filter_map(|e| e.text().parse().ok())
                    .collect();

                if rates.len() == 4 {
                    let rates = Rates{label, usd_buy: rates[0], usd_sell: rates[1], eur_buy: rates[2], eur_sell: rates[3]};
                    return Ok(rates);
                }
            }
        }
    }
    Err("could not parse rulya page".to_owned())
}

async fn poll_exchange_rate_impl(
    secs: u64, log: Arc<TaggedLog>,
) -> Result<std::convert::Infallible, String> {

    let conn = rusqlite::Connection::open("exchange.db")
        .map_err(|e| e.to_string())?;

    let _c = conn.execute(
        "CREATE TABLE IF NOT EXISTS rulya_usd(\
time TEXT NOT NULL PRIMARY KEY,\
buy FLOAT NOT NULL,\
sell FLOAT NOT NULL)",
        [],
    )
        .map_err(|e| e.to_string())?;

    loop {
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let rulya = rulya_fetch_rates(log.clone()).await?;
        let _i = conn.execute(
            "INSERT INTO rulya_usd(time, buy, sell) VALUES(?1, ?2, ?3)",
            [now, rulya.usd_buy.to_string(), rulya.usd_sell.to_string()],
        )
            .map_err(|e| e.to_string())?;
        let _x = tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
    }
}

pub async fn poll_exchange_rate(secs: u64) {
    let log = Arc::new(TaggedLog {tag: "poll_exch".to_owned()});

    if let Err(e) = poll_exchange_rate_impl(secs, log.clone()).await {
        log.add_line(&e);
    }
}
