use super::{Parameter, FileKey, TaggedLog, DebugRFC3339};
use crate::cache;
use crate::icon; // TODO:
use crate::grib;
use std::sync::Arc;
use chrono::Timelike;
use futures::{stream, Stream, StreamExt, TryStreamExt, future, Future, FutureExt, TryFutureExt};
use itertools::Itertools; // tuple_windows, collect_vec

pub trait Timeseries {
    fn to_u8(&self) -> u8;
}

impl Timeseries for u8 {
    fn to_u8(&self) -> u8 {
        *self
    }
}

pub fn forecast_iterator<MR, TS, TSI, MRI, MRIF, TSIF>(
    start_time: chrono::DateTime<chrono::Utc>, target_time: chrono::DateTime<chrono::Utc>, mri: MRIF, tsi: TSIF
) -> impl Iterator<Item=(chrono::DateTime<chrono::Utc>, MR, chrono::DateTime<chrono::Utc>, TS, chrono::DateTime<chrono::Utc>, TS)>
where
    MR: Timeseries + Clone,
    TS: Timeseries + Clone,
    TSI: Iterator<Item=TS>,
    MRI: Iterator<Item=MR>,
    MRIF: Fn() -> MRI,
    TSIF: Fn(MR) -> TSI,
{
    let start_time_d = start_time.date().and_hms(0, 0, 0); // trunc to days
    let start_time_h = start_time.date().and_hms(start_time.hour(), 0, 0); // trunc to hours

    (0..)
        .flat_map(move |day| mri().map(
            move |hh| (start_time_d + chrono::Duration::hours(day * 24 + i64::from(hh.to_u8())), hh)
        ))
        .tuple_windows::<(_, _)>()
        .skip_while(move |(_, t)| t.0 <= start_time_h)
        .map(|(f, _)| f)
        .take_while(move |(hhtime, _)| target_time >= *hhtime)
        .map(move |(hhtime, hh)| {
            let opt = tsi(hh.clone())
             .map(|ts| (hhtime + chrono::Duration::hours(i64::from(ts.to_u8())), ts))
             .tuple_windows::<(_, _)>()
             .take_while(|(f, _)| target_time >= f.0)
             .skip_while(|(_, t)| target_time >= t.0)
             .next();
            (hhtime, hh, opt)
        })
        .filter_map(|(s, mr, o)| o.map(|(x0, x1)| (s, mr, x0, x1)))
        .map(|(s, mr, ts, ts1)| (s, mr, ts.0, ts.1, ts1.0, ts1.1))
}

fn lerp(a: f32, b: f32, t: f32) -> f32 {
    a * (1.0 - t) + b * t
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
        Err(format!("target point {} {} is not in range {}..{}, {}..{}", lon, lat, lon0, lon1, lat0, lat1))
    }
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

fn extract_value_at(grib: &grib::GribMessage, lat: f32, lon: f32) -> Result<f32, String> {
    if let grib::Packing::Simple {r, e, d, bits: 16} = grib.section5.packing {
        if let grib::CodedValues::Simple16Bit(ref v) = grib.section7.coded_values {
            let twop = 2f32.powf(f32::from(e));
            let tenp = 10f32.powf(f32::from(-d));
            Ok(v.iter().map(
                |x| (r + f32::from(*x) * twop) * tenp
            ).collect_vec())
        } else {
            Err("unsupported coded_values".to_owned())
        }
    } else {
        Err("unsupported packing".to_owned())
    }
    .and_then(|ys| extract_value_impl(&ys, &grib.section3, lat, lon))
}

fn fetch_value(
    log: Arc<TaggedLog>,
    param: Parameter,
    lat: f32, lon: f32,
    modelrun: ModelrunSpec,
    timestep: u8,
) -> impl Future<Output=Result<f32, String>> {

    let file_key = FileKey::new(param, modelrun.0, modelrun.1, timestep);
    cache::fetch_grid(log, file_key)
        .and_then(move |grid| {
            if icon::icon_verify_parameter(param, &grid) {
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
type TimestepSpec = (chrono::DateTime<chrono::Utc>, u8);

use serde_derive::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct ParameterFlags {
    pub tcc: bool,
    pub wind: bool,
    pub precip: bool,
    pub rain: bool,
    pub snow: bool,
    pub depth: bool,
}

impl Default for ParameterFlags {
    fn default() -> Self {
        Self {
            tcc: true,
            wind: true,
            precip: false,
            rain: true,
            snow: true,
            depth: false,
        }
    }
}

fn fetch_all(
    log: Arc<TaggedLog>, lat: f32, lon: f32, mr: ModelrunSpec, t0: TimestepSpec, t1: TimestepSpec, params: ParameterFlags
) -> std::pin::Pin<Box<dyn Future<Output=Result<Forecast, String>> + Send>> {

    let log1 = log.clone();
    let log2 = log.clone();
    let log3 = log.clone();
    let log4 = log.clone();
    let log5 = log.clone();
    let ts = t0.1;
    let ts1 = t1.1;
    let f = future::try_join4(
        fetch_value(log.clone(), Parameter::Temperature2m, lat, lon, mr, ts),
        opt_wrap(params.tcc, move || fetch_value(log1, Parameter::TotalCloudCover, lat, lon, mr, ts)),
        opt_wrap(params.wind, move || future::try_join(
            fetch_value(log2.clone(), Parameter::WindSpeedU10m, lat, lon, mr, ts),
            fetch_value(log2, Parameter::WindSpeedV10m, lat, lon, mr, ts),
        )),
        future::try_join4(
            opt_wrap(params.precip, move || future::try_join(
                fetch_value(log3.clone(), Parameter::TotalAccumPrecip, lat, lon, mr, ts),
                fetch_value(log3, Parameter::TotalAccumPrecip, lat, lon, mr, ts1),
            )),
            opt_wrap(params.rain, move || future::try_join4(
                fetch_value(log4.clone(), Parameter::LargeScaleRain, lat, lon, mr, ts),
                fetch_value(log4.clone(), Parameter::LargeScaleRain, lat, lon, mr, ts1),
                fetch_value(log4.clone(), Parameter::ConvectiveRain, lat, lon, mr, ts),
                fetch_value(log4, Parameter::ConvectiveRain, lat, lon, mr, ts1),
            )),
            opt_wrap(params.snow, move || future::try_join4(
                fetch_value(log5.clone(), Parameter::LargeScaleSnow, lat, lon, mr, ts),
                fetch_value(log5.clone(), Parameter::LargeScaleSnow, lat, lon, mr, ts1),
                fetch_value(log5.clone(), Parameter::ConvectiveSnow, lat, lon, mr, ts),
                fetch_value(log5, Parameter::ConvectiveSnow, lat, lon, mr, ts1),
            )),
            opt_wrap(params.depth, move || fetch_value(log, Parameter::SnowDepth, lat, lon, mr, ts)),
        ),
    )
    .map_ok(move |(t, otcc, owind, (oprecip, orain, osnow, odepth))| Forecast {
        temperature: t,
        total_cloud_cover: otcc,
        wind_speed: owind,
        total_precip_accum: oprecip,
        rain_accum: orain.map(|(ls0, ls1, c0, c1)| (ls0 + c0, ls1 + c1)),
        snow_accum: osnow.map(|(ls0, ls1, c0, c1)| (ls0 + c0, ls1 + c1)),
        snow_depth: odepth,
        time: (t0.0, t1.0),
    });
    Box::pin(f)
}

fn select_start_time<F, R>(
    log: Arc<TaggedLog>, target_time: chrono::DateTime<chrono::Utc>, try_func: F, try_timeout: std::time::Duration
) -> impl Future<Output=Result<chrono::DateTime<chrono::Utc>, String>>
where
    F: Fn(Arc<TaggedLog>, (chrono::DateTime<chrono::Utc>, u8, chrono::DateTime<chrono::Utc>, u8, chrono::DateTime<chrono::Utc>, u8)) -> R,
    R: Future<Output=Result<Forecast, String>> + Unpin,
{
    let now = chrono::Utc::now();
    let start_time = now - chrono::Duration::hours(12);
    let fs: Vec<_> = forecast_iterator(start_time, target_time, icon::icon_modelrun_iter, icon::icon_timestep_iter).collect();

    stream::unfold(fs, |mut fs| future::ready( fs.pop().map(|x| (x, fs)) ) )
        .skip_while(move |(mrt, _, _, _, _, _)| {
            let skip = now < *mrt;
            println!("mrt={}, now={}, skip={}", mrt.to_rfc3339_debug(), now.to_rfc3339_debug(), skip);
            future::ready(skip)
        })
        .then(move |(mrt, mr, ft, ts, ft1, ts1)| {
            log.add_line(&format!("try {}/{:02} >> {}/{:03} .. {}/{:03}", mrt.to_rfc3339_debug(), mr, ft.to_rfc3339_debug(), ts, ft1.to_rfc3339_debug(), ts1));
            let log = log.clone();
            tokio::time::timeout(try_timeout, try_func(log.clone(), (mrt, mr, ft, ts, ft1, ts1)))
                .then(move |v: Result<Result<Forecast, String>, tokio::time::Elapsed>| {
                    let res = match v {
                        Ok(Ok(_f)) => Some(mrt),
                        Ok(Err(s)) => { log.add_line(&format!("failed with: {}", s)); None },
                        Err(e) => { log.add_line(&format!("took too long, {}, {:?}", e, try_timeout)); None },
                    };
                    future::ready(res)
                })
        })
        .filter_map(future::ready)
        .into_future()
        .map(|(f, _)| f.ok_or_else(|| "tried all start times".to_string()))
}

pub struct Forecast {
    pub temperature: f32,
    pub time: (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>),
    pub total_cloud_cover: Option<f32>,
    pub wind_speed: Option<(f32, f32)>,
    pub total_precip_accum: Option<(f32, f32)>,
    pub rain_accum: Option<(f32, f32)>,
    pub snow_accum: Option<(f32, f32)>,
    pub snow_depth: Option<f32>,
}

pub fn forecast_stream(
    log: Arc<TaggedLog>, lat: f32, lon: f32, target_time: chrono::DateTime<chrono::Utc>, params: ParameterFlags
) -> impl Stream<Item=Result<Forecast, String>> {

    select_start_time(
        log.clone(), target_time,
        move |log, (mrt, mr, ft, ts, ft1, ts1)| fetch_all(log, lat, lon, (mrt.date(), mr), (ft, ts), (ft1, ts1), params),
        std::time::Duration::from_secs(7)
    )
        .map_ok(move |f| {
            stream::iter(forecast_iterator(f, target_time, icon::icon_modelrun_iter, icon::icon_timestep_iter))
                .then({ let log = log.clone(); move |(mrt, mr, ft, ts, ft1, ts1)| {
                    log.add_line(&format!("want {}/{:02} >> {}/{:03} .. {}/{:03}", mrt.to_rfc3339_debug(), mr, ft.to_rfc3339_debug(), ts, ft1.to_rfc3339_debug(), ts1));
                    fetch_all(log.clone(), lat, lon, (mrt.date(), mr), (ft, ts), (ft1, ts1), params)
                }})
                .inspect_err(move |e| log.add_line(&format!("monitor stream error: {}", e)))
                .then(future::ok) // stream of values --> stream of results
                .filter_map(|item: Result<_, String>| future::ready(item.ok())) // drop errors
        })
        .try_flatten_stream()
}
