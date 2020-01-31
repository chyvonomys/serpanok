use super::{Parameter, FileKey, TaggedLog};
use super::{fetch_url, avail_url, unpack_bzip2};
use crate::grib;
use futures::{Future, TryFutureExt};

pub fn icon_verify_parameter(param: Parameter, g: &grib::GribMessage) -> bool {
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
        (Parameter::PressureMSL, 3, 1) => true, // Mass/PressureReducedToMSL(Pa)
        (Parameter::RelHumidity2m, 1, 1) => true, // Moisture/RelativeHumidity(%)
        _ => false
    }
}


pub fn icon_timestep_iter(mr: u8) -> impl Iterator<Item=u16> {
    if mr % 6 == 0 {
        either::Either::Left(Iterator::chain(
            0u16..78,
            (78..=120).step_by(3)
        ))
    } else {
        either::Either::Right(0u16..=30)
    }
}

pub fn icon_modelrun_iter() -> impl Iterator<Item=u8> {
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
                "PMSL"     => Some(Parameter::PressureMSL),
                "RELHUM_2M"=> Some(Parameter::RelHumidity2m),
                _ => None
            }.map(move |param| FileKey {yyyy, mm, dd, modelrun, timestep, param})
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
    pub fn new(key: FileKey) -> Self {

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
            Parameter::PressureMSL => "PMSL",
            Parameter::RelHumidity2m => "RELHUM_2M",
        };

        let yyyymmdd = format!("{}{:02}{:02}", key.yyyy, key.mm, key.dd);

        Self {        
            prefix: format!("https://opendata.dwd.de/weather/nwp/icon-eu/grib/{:02}/{}/", key.modelrun, paramstr.to_lowercase()),
            filename: format!("icon-eu_europe_regular-lat-lon_single-level_{}{:02}_{:03}_{}.grib2", yyyymmdd, key.modelrun, key.timestep, paramstr),
            modelrun_time: key.get_modelrun_tm(),
        }
    }

    pub fn cache_filename(&self) -> &str {
        &self.filename
    }

    // ICON specific: download and unpack

    pub fn fetch_bytes(&self, log: std::sync::Arc<TaggedLog>) -> impl Future<Output=Result<Vec<u8>, String>> {
        fetch_url(log, format!("{}{}.bz2", self.prefix, self.filename), &[])
            .and_then(|bzip2: Vec<u8>| unpack_bzip2(&bzip2))
    }

    pub fn check_avail(&self, log: std::sync::Arc<TaggedLog>) -> impl Future<Output=Result<(), String>> {
        avail_url(log, format!("{}{}.bz2", self.prefix, self.filename))
    }

    pub fn available_from(&self) -> chrono::DateTime<chrono::Utc> {
        self.modelrun_time + chrono::Duration::hours(2) + chrono::Duration::minutes(30)
    }

    pub fn available_to(&self) -> chrono::DateTime<chrono::Utc> {
        self.modelrun_time + chrono::Duration::hours(26) + chrono::Duration::minutes(30)
    }
}
