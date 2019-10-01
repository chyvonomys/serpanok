pub trait Timeseries {
    fn to_u8(&self) -> u8;
}

impl Timeseries for u8 {
    fn to_u8(&self) -> u8 {
        *self
    }
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

use itertools::Itertools; // tuple_windows

pub fn forecast_iterator<MR, TS, TSI, MRI, MRIF, TSIF>(
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

fn lerp(a: f32, b: f32, t: f32) -> f32 {
    a * (1.0 - t) + b * t
}

use grib;

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
    .and_then(|ys| extract_value_impl(&ys, &grib.section3, lat, lon))
}

use future;
use stream;
use super::{Parameter, FileKey, Future};
use cache;
use icon; // TODO:
use std::sync::Arc;

fn fetch_value(
    log: Arc<TaggedLog>,
    param: Parameter,
    lat: f32, lon: f32,
    modelrun_time: time::Tm, modelrun: u8, timestep: u8,
) -> impl Future<Item=f32, Error=String> {

    let file_key = FileKey::new(param, modelrun_time, modelrun, timestep);
    cache::fetch_grid(log, file_key)
        .and_then(move |grid| {
            if icon::icon_verify_parameter(param, &grid) {
                future::ok(grid)
            } else {
                future::err("parameter cat/num mismatch".to_owned())
            }
        })
        .and_then(move |grid| extract_value_at(&grid, lat, lon))
        .map_err(|e| format!("fetch_value error: {}", e))
}

fn opt_wrap<FN, F, I>(b: bool, f: FN) -> impl Future<Item=Option<I>, Error=String>
where FN: FnOnce() -> F, F: Future<Item=I, Error=String> {
    if b {
        future::Either::A(f().map(|x| Some(x)))
    } else {
        future::Either::B(future::ok(None))
    }
}

fn fetch_all(
    log: Arc<TaggedLog>, lat: f32, lon: f32, mrt: time::Tm, mr: u8, t0: time::Tm, ts: u8, t1: time::Tm, ts1: u8,
    tcc: bool, wind: bool, precip: bool, rain: bool, snow: bool, depth: bool
) -> Box<dyn Future<Item=Forecast, Error=String> + Send> {

    let log1 = log.clone();
    let log2 = log.clone();
    let log3 = log.clone();
    let log4 = log.clone();
    let log5 = log.clone();

    let f = future::Future::join4(
        fetch_value(log.clone(), Parameter::Temperature2m, lat, lon, mrt, mr, ts),
        opt_wrap(tcc, move || fetch_value(log1, Parameter::TotalCloudCover, lat, lon, mrt, mr, ts)),
        opt_wrap(wind, move || future::Future::join(
            fetch_value(log2.clone(), Parameter::WindSpeedU10m, lat, lon, mrt, mr, ts),
            fetch_value(log2, Parameter::WindSpeedV10m, lat, lon, mrt, mr, ts),
        )),
        future::Future::join4(
            opt_wrap(precip, move || future::Future::join(
                fetch_value(log3.clone(), Parameter::TotalAccumPrecip, lat, lon, mrt, mr, ts),
                fetch_value(log3, Parameter::TotalAccumPrecip, lat, lon, mrt, mr, ts1),
            )),
            opt_wrap(rain, move || future::Future::join4(
                fetch_value(log4.clone(), Parameter::LargeScaleRain, lat, lon, mrt, mr, ts),
                fetch_value(log4.clone(), Parameter::LargeScaleRain, lat, lon, mrt, mr, ts1),
                fetch_value(log4.clone(), Parameter::ConvectiveRain, lat, lon, mrt, mr, ts),
                fetch_value(log4, Parameter::ConvectiveRain, lat, lon, mrt, mr, ts1),
            )),
            opt_wrap(snow, move || future::Future::join4(
                fetch_value(log5.clone(), Parameter::LargeScaleSnow, lat, lon, mrt, mr, ts),
                fetch_value(log5.clone(), Parameter::LargeScaleSnow, lat, lon, mrt, mr, ts1),
                fetch_value(log5.clone(), Parameter::ConvectiveSnow, lat, lon, mrt, mr, ts),
                fetch_value(log5, Parameter::ConvectiveSnow, lat, lon, mrt, mr, ts1),
            )),
            opt_wrap(depth, move || fetch_value(log, Parameter::SnowDepth, lat, lon, mrt, mr, ts)),
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
    });
    Box::new(f)
}

use super::TaggedLog;

fn select_start_time<F, R>(
    log: Arc<TaggedLog>, target_time: time::Tm, try_func: F, try_timeout: std::time::Duration
) -> impl Future<Item=time::Tm, Error=String>
where
    F: Fn(Arc<TaggedLog>, (time::Tm, u8, time::Tm, u8, time::Tm, u8)) -> R,
    R: Future<Item=Forecast, Error=String>,
{
    let now = time::now_utc();
    let start_time = now - time::Duration::hours(12);
    let fs: Vec<_> = forecast_iterator(start_time, target_time, icon::icon_modelrun_iter, icon::icon_timestep_iter).collect();

    stream::unfold(fs, |mut fs| fs.pop().map(|x| future::ok( (x, fs) )))
        .skip_while(move |(mrt, _, _, _, _, _)| future::ok(now < *mrt))
        .and_then(move |(mrt, mr, ft, ts, ft1, ts1)| {
            log.add_line(&format!("try {}/{:02} >> {}/{:03} .. {}{:03}", mrt.rfc3339(), mr, ft.rfc3339(), ts, ft1.rfc3339(), ts1));
            let log = log.clone();
            tokio::timer::Timeout::new(try_func(log.clone(), (mrt, mr, ft, ts, ft1, ts1)), try_timeout)
                .map(move |_f: Forecast| Some(mrt))
                .or_else(move |e: tokio::timer::timeout::Error<String>| {
                    log.add_line(&format!("took too long, {}, {:?}", e, try_timeout));
                    future::ok(None)
                })
        })
        .filter_map(|item: Option<time::Tm>| item)
        .into_future()
        .map_err(|(e, _)| e)
        .and_then(|(f, _)| f.ok_or(format!("tried all start times")))
}

pub struct Forecast {
    pub temperature: f32,
    pub time: (time::Tm, time::Tm),
    pub total_cloud_cover: Option<f32>,
    pub wind_speed: Option<(f32, f32)>,
    pub total_precip_accum: Option<(f32, f32)>,
    pub rain_accum: Option<(f32, f32)>,
    pub snow_accum: Option<(f32, f32)>,
    pub snow_depth: Option<(f32)>,
}

use super::Stream;

pub fn forecast_stream(log: Arc<TaggedLog>, lat: f32, lon: f32, target_time: time::Tm) -> impl Stream<Item=Forecast, Error=String> {

    select_start_time(
        log.clone(), target_time,
        move |log, (mrt, mr, ft, ts, ft1, ts1)| fetch_all(log, lat, lon, mrt, mr, ft, ts, ft1, ts1, false, false, false, true, true, true),
        std::time::Duration::from_secs(7)
    )
        .map(move |f| {
            stream::iter_ok(forecast_iterator(f, target_time, icon::icon_modelrun_iter, icon::icon_timestep_iter))
                .and_then({ let log = log.clone(); move |(mrt, mr, ft, ts, ft1, ts1)| {
                    log.add_line(&format!("want {}/{:02} >> {}/{:03} .. {}{:03}", mrt.rfc3339(), mr, ft.rfc3339(), ts, ft1.rfc3339(), ts1));
                    fetch_all(log.clone(), lat, lon, mrt, mr, ft, ts, ft1, ts1, false, false, false, true, true, true)
                }})
                .inspect_err(move |e| log.add_line(&format!("monitor stream error: {}", e)))
                .then(|result: Result<_, String>| future::ok(result)) // stream of values --> stream of results
                .filter_map(|item: Result<_, String>| item.ok()) // drop errors
        })
        .flatten_stream()
}