use super::{SourceParameter, FileKey, DataFile, TaggedLog};
use super::{fetch_url, avail_url, unpack_bzip2};
use crate::grib;
use futures::{Future, TryFutureExt};
use serde_derive::Serialize;

pub fn covers_point(lat: f32, lon: f32) -> bool {
    lat >= 29.5 && lat <= 70.5 && lon >= -23.5 && lon <= 45.0
}

pub fn timestep_iter(mr: u8) -> impl Iterator<Item=u16> {
    if mr % 6 == 0 {
        either::Either::Left(Iterator::chain(
            0u16..78,
            (78..=120).step_by(3)
        ))
    } else {
        either::Either::Right(0u16..=30)
    }
}

pub fn modelrun_iter() -> impl Iterator<Item=u8> {
    (0u8..=21).step_by(3)
}

pub fn filename_to_filekey(filename: &str) -> Option<FileKey> {
    lazy_static::lazy_static! {
        static ref RE: regex::Regex = regex::Regex::new(
            r"^icon-eu_europe_regular-lat-lon_single-level_(\d{4})(\d{2})(\d{2})(\d{2})_(\d{3})_([0-9_A-Z]+).grib2$"
        ).unwrap();
    }

    RE.captures(filename).and_then(|cs| {
        let oyyyy = cs.get(1).and_then(|x| x.as_str().parse::<u16>().ok());
        let omm = cs.get(2).and_then(|x| x.as_str().parse::<u8>().ok());
        let odd = cs.get(3).and_then(|x| x.as_str().parse::<u8>().ok());
        let omr = cs.get(4).and_then(|x| x.as_str().parse::<u8>().ok());
        let ots = cs.get(5).and_then(|x| x.as_str().parse::<u16>().ok());
        let op = cs.get(6).map(|x| x.as_str());
        if let (Some(yyyy), Some(mm), Some(dd), Some(modelrun), Some(timestep), Some(abbrev)) = (oyyyy, omm, odd, omr, ots, op) {
            IconParameter::from_str(abbrev).map(move |param| FileKey {
                param: SourceParameter::IconEu(param),
                yyyy, mm, dd, modelrun, timestep, 
            })
        } else {
            None
        }
    })
}

pub struct IconFile {
    prefix: String,
    filename: String,
    modelrun_time: chrono::DateTime<chrono::Utc>,
}

impl IconFile {
    pub fn new(param: IconParameter, yyyy: u16, mm: u8, dd: u8, modelrun: u8, timestep: u16) -> Self {

        use chrono::offset::TimeZone;

        let paramstr = param.to_abbrev();
        let yyyymmdd = format!("{}{:02}{:02}", yyyy, mm, dd);
        let modelrun_time = chrono::Utc.with_ymd_and_hms(yyyy.into(), mm.into(), dd.into(), modelrun.into(), 0, 0).unwrap();

        Self {        
            prefix: format!("https://opendata.dwd.de/weather/nwp/icon-eu/grib/{:02}/{}/", modelrun, paramstr.to_lowercase()),
            filename: format!("icon-eu_europe_regular-lat-lon_single-level_{}{:02}_{:03}_{}.grib2", yyyymmdd, modelrun, timestep, paramstr),
            modelrun_time
        }
    }
}

impl DataFile for IconFile {
    fn cache_filename(&self) -> &str {
        &self.filename
    }

    fn fetch_bytes(&self, log: std::sync::Arc<TaggedLog>) -> Box<dyn Future<Output=Result<Vec<u8>, String>> + Send + Unpin> {
        Box::new(fetch_url(log, format!("{}{}.bz2", self.prefix, self.filename), &[])
            .and_then(|bzip2: Vec<u8>| unpack_bzip2(&bzip2))
        )
    }

    fn available_from(&self) -> chrono::DateTime<chrono::Utc> {
        self.modelrun_time + chrono::Duration::hours(2) + chrono::Duration::minutes(30)
    }

    fn available_to(&self) -> chrono::DateTime<chrono::Utc> {
        self.modelrun_time + chrono::Duration::hours(26) + chrono::Duration::minutes(30)
    }

    fn check_avail(&self, log: std::sync::Arc<TaggedLog>) -> Box<dyn Future<Output=Result<(), String>> + Send + Unpin> {
        Box::new( avail_url(log, format!("{}{}.bz2", self.prefix, self.filename)) )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum IconParameter {
    Temperature2m,
    WindSpeedU10m,
    WindSpeedV10m,
    TotalCloudCover,
    TotalAccumPrecip,
    ConvectiveSnow,
    ConvectiveRain,
    LargeScaleSnow,
    LargeScaleRain,
    SnowDepth,
    PressureMSL,
    RelHumidity2m,
}

impl IconParameter {
    fn to_abbrev(self) -> &'static str {
        match self {
            IconParameter::Temperature2m    => "T_2M",
            IconParameter::WindSpeedU10m    => "U_10M",
            IconParameter::WindSpeedV10m    => "V_10M",
            IconParameter::TotalCloudCover  => "CLCT",
            IconParameter::TotalAccumPrecip => "TOT_PREC",
            IconParameter::ConvectiveSnow   => "SNOW_CON",
            IconParameter::ConvectiveRain   => "RAIN_CON",
            IconParameter::LargeScaleSnow   => "SNOW_GSP",
            IconParameter::LargeScaleRain   => "RAIN_GSP",
            IconParameter::SnowDepth        => "H_SNOW",
            IconParameter::PressureMSL      => "PMSL",
            IconParameter::RelHumidity2m    => "RELHUM_2M",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "T_2M"     => Some(IconParameter::Temperature2m),
            "U_10M"    => Some(IconParameter::WindSpeedU10m),
            "V_10M"    => Some(IconParameter::WindSpeedV10m),
            "CLCT"     => Some(IconParameter::TotalCloudCover),
            "TOT_PREC" => Some(IconParameter::TotalAccumPrecip),
            "SNOW_CON" => Some(IconParameter::ConvectiveSnow),
            "RAIN_CON" => Some(IconParameter::ConvectiveRain),
            "SNOW_GSP" => Some(IconParameter::LargeScaleSnow),
            "RAIN_GSP" => Some(IconParameter::LargeScaleRain),
            "H_SNOW"   => Some(IconParameter::SnowDepth),
            "PMSL"     => Some(IconParameter::PressureMSL),
            "RELHUM_2M"=> Some(IconParameter::RelHumidity2m),
            _ => None
        }
    }

    // TODO: add level to verification
    pub fn verify_grib2(self, g: &grib::GribMessage) -> bool {
        let common = g.section4.product_def.common();
        match (self, common.parameter_cat, common.parameter_num) {
            (IconParameter::Temperature2m,    0, 0)  => true, // Temperature/Temperature(K)
            (IconParameter::TotalCloudCover,  6, 1)  => true, // Cloud/TotalCloudCover(%)
            (IconParameter::WindSpeedU10m,    2, 2)  => true, // Momentum/WindSpeedUComp(m/s)
            (IconParameter::WindSpeedV10m,    2, 3)  => true, // Momentum/WindSpeedVComp(m/s)
            (IconParameter::TotalAccumPrecip, 1, 52) => true, // Moisture/TotalPrecipitationRate(kg/m2/s)
            (IconParameter::ConvectiveSnow,   1, 55) => true, // Moisture/ConvectiveSnowfallRateWaterEquiv(kg/m2/s)
            (IconParameter::LargeScaleSnow,   1, 56) => true, // Moisture/LargeScaleSnowfallRateWaterEquiv(kg/m2/s)
            (IconParameter::ConvectiveRain,   1, 76) => true, // Moisture/ConvectiveRainRate(kg/m2/s)
            (IconParameter::LargeScaleRain,   1, 77) => true, // Moisture/LargeScaleRainRate(kg/m2/s)
            (IconParameter::SnowDepth,        1, 11) => true, // Moisture/SnowDepth(m)
            (IconParameter::PressureMSL,      3, 1)  => true, // Mass/PressureReducedToMSL(Pa)
            (IconParameter::RelHumidity2m,    1, 1)  => true, // Moisture/RelativeHumidity(%)
            _ => false
        }
    }
}