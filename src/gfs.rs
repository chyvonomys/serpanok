use nom::{named, do_parse, call, tag, recognize, take_until, map_res, many0, complete, digit};
use super::{FileKey, DataFile, TaggedLog, SourceParameter, fetch_url, avail_url};
use futures::{Future, FutureExt, TryFutureExt};
use serde_derive::{Serialize, Deserialize};
use crate::grib;

#[test]
fn test_parse_inventory() {
    let input = "\
        437:230938696:d=2020013000:DPT:2 m above ground:1 hour fcst:\n\
        438:231824580:d=2020013000:RH:2 m above ground:1 hour fcst:\n\
        439:232568275:d=2020013000:APTMP:2 m above ground:1 hour fcst:\n\
        440:233113593:d=2020013000:TMAX:2 m above ground:0-1 hour max fcst:\n\
        441:233939827:d=2020013000:TMIN:2 m above ground:0-1 hour min fcst:\n\
        442:234770300:d=2020013000:UGRD:10 m above ground:1 hour fcst:\n\
        ";
    let res = parse_inventory(input);
    println!("{:#?}", res);
    assert!(res.is_ok());
}

#[test]
fn test_gfs_fetch() {
    use crate::cache;
    use crate::grib;
    use crate::data;
    use futures::{future, stream, StreamExt, TryStreamExt};
    let ps = [GfsParameter::Temperature2m, GfsParameter::UComponentWind10m, GfsParameter::TotalCloudCover50mb, GfsParameter::TotalCloudCover1000mb, GfsParameter::TotalCloudCoverAvgAtm];
    let it = ps.iter().flat_map(|p| [GfsResolution::Deg100].iter().map(move |r| (*p, *r)));
    let s = stream::iter(it).then(|(param, res)| {
        let key = FileKey::new(SourceParameter::Gfs(param, res), chrono::Utc::today().pred(), 0, 3);
        let log = std::sync::Arc::new(TaggedLog{tag: "//test//".to_owned()});
        let f = cache::make_fetch_grid_fut(log, key)
            .inspect_ok(|msg| println!("{:#?}", msg))
            .map(|res| res.and_then(|msg| grib::decode_original_values(&msg).map(|vs| (msg, vs))))
            .inspect_ok(|(_, vs)| {
                let mm = vs.iter().fold(None, |acc, x| match acc {
                    None => Some((x, x)),
                    Some((mi, ma)) => Some((if mi < x { mi } else { x }, if ma > x { ma } else { x })),
                });
                println!(
                    "> {} values, minmax={:?}:\n{:?}...\n...{:?}", vs.len(), mm,
                    &vs[..std::cmp::min(10, vs.len())], &vs[std::cmp::max(0, vs.len() - 10)..]
                );
            })
            .map_ok(|(msg, vs)| {
                let lat = 50.62; let lon = 26.32;
                println!(
                    "value at lat={} lon={} is {:?}", lat, lon,
                    data::sample_value(&vs, &msg.section3, lat, lon)
                )
            })
            .map_err(|e| println!("error {}", e));
        f
    }).try_fold((), |_, x| future::ok(x));
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    assert_eq!(rt.block_on(s), Ok(()));
}

named!(parse_usize<usize>, map_res!(map_res!(digit, std::str::from_utf8), |s: &str| s.parse::<usize>()));
named!(parse_u32<u32>, map_res!(map_res!(digit, std::str::from_utf8), |s: &str| s.parse::<u32>()));
named!(parse_u16<u16>, map_res!(map_res!(digit, std::str::from_utf8), |s: &str| s.parse::<u16>()));
named!(parse_until_colon<String>, map_res!(recognize!(take_until!(":")), |s: &[u8]| String::from_utf8(s.to_vec())));

named!(parse_inv_item<InventoryItem>, do_parse!(
    idx: call!(parse_usize) >>
    tag!(":") >>
    offset: call!(parse_usize) >>
    tag!(":d=") >>
    modelrun: call!(parse_u32) >>
    tag!(":") >>
    abbrev: call!(parse_until_colon) >>
    tag!(":") >>
    level: call!(parse_until_colon) >>
    tag!(":") >>
    timedesc: call!(parse_until_colon) >>
    tag!(":\n") >>

    (InventoryItem{
        idx, offset, modelrun, abbrev, level, timedesc
    })
));

named!(parse_inventory_items<Vec<InventoryItem>>, many0!(complete!(parse_inv_item)));

#[derive(Debug)]
struct InventoryItem {
    idx: usize,
    offset: usize,
    modelrun: u32,
    abbrev: String,
    level: String,
    timedesc: String,
}

fn parse_inventory(index: &str) -> Result<Vec<InventoryItem>, String> {
    parse_inventory_items(index.as_bytes())
        .map(|(s, inv)| {println!("{:?}", std::str::from_utf8(s)); inv})
        .map_err(|e| e.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GfsResolution {
    Deg025, Deg050, Deg100
}

impl GfsResolution {
    fn abbrev(self) -> &'static str {
        match self {
            GfsResolution::Deg025 => "0p25",
            GfsResolution::Deg050 => "0p50",
            GfsResolution::Deg100 => "1p00",
        }
    }

    fn from_abbrev(s: &str) -> Option<Self> {
        match s {
            "0p25" => Some(GfsResolution::Deg025),
            "0p50" => Some(GfsResolution::Deg050),
            "1p00" => Some(GfsResolution::Deg100),
            _ => None
        }
    }
}

pub fn filename_to_filekey(filename: &str) -> Option<FileKey> {
    lazy_static::lazy_static! {
        static ref RE: regex::Regex = regex::Regex::new(
            r"^gfs.(0p25|0p50|1p00).(\d{4})-(\d{2})-(\d{2}).(\d{2}).([0-9_A-Za-z]+).(\d{3}).grib2$"
        ).unwrap();
    }

    RE.captures(filename).and_then(|cs| {
        let ores = cs.get(1).map(|x| x.as_str());
        let oyyyy = cs.get(2).and_then(|x| x.as_str().parse::<u16>().ok());
        let omm = cs.get(3).and_then(|x| x.as_str().parse::<u8>().ok());
        let odd = cs.get(4).and_then(|x| x.as_str().parse::<u8>().ok());
        let omr = cs.get(5).and_then(|x| x.as_str().parse::<u8>().ok());
        let op = cs.get(6).map(|x| x.as_str());
        let ots = cs.get(7).and_then(|x| x.as_str().parse::<u16>().ok());
        if let (Some(res), Some(yyyy), Some(mm), Some(dd), Some(modelrun), Some(p), Some(timestep)) = (ores, oyyyy, omm, odd, omr, op, ots) {
            GfsParameter::from_str(p).and_then(|param| GfsResolution::from_abbrev(res).map(|res| FileKey {
                param: SourceParameter::Gfs(param, res), yyyy, mm, dd, modelrun, timestep
            }))
        } else {
            None
        }
    })
}

pub fn covers_point(res: GfsResolution, lat: f32, lon: f32) -> bool {
    let lon = if lon < 0.0 { lon + 360.0 } else { lon };
    lat >= -90.0 && lat <= 90.0 && lon >= 0.0 && lon <= match res {
        GfsResolution::Deg025 => 359.75,
        GfsResolution::Deg050 => 359.5,
        GfsResolution::Deg100 => 359.0,
    }
}

pub fn timestep_iter(res: GfsResolution) -> impl Iterator<Item=u16> {
    match res {
        GfsResolution::Deg025 => Iterator::chain(
            0u16..120,
            (120..=384).step_by(3)
        ),
        _ => Iterator::chain(
            0u16..0,
            (0..=384).step_by(3)
        ),
    }
}

pub fn modelrun_iter() -> impl Iterator<Item=u8> {
    (0u8..=18).step_by(6)
}

pub struct GfsFile {
    param: GfsParameter,
    url: String,
    filename: String,
}

impl GfsFile {
    pub fn new(param: GfsParameter, res: GfsResolution, yyyy: u16, mm: u8, dd: u8, modelrun: u8, timestep: u16) -> Self {
        Self {
            url: format!(
                "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{:04}{:02}{:02}/{:02}/gfs.t{:02}z.pgrb2.{}.f{:03}",
                yyyy, mm, dd, modelrun, modelrun,
                res.abbrev(), timestep,
            ),
            filename: format!(
                "gfs.{}.{:04}-{:02}-{:02}.{:02}.{}.{:03}.grib2",
                res.abbrev(), yyyy, mm, dd, modelrun, param.as_str(), timestep
            ),
            param,
        }
    }
}

impl DataFile for GfsFile {
    fn cache_filename(&self) -> &str {
        &self.filename
    }

    fn fetch_bytes(&self, log: std::sync::Arc<TaggedLog>) -> Box<dyn Future<Output=Result<Vec<u8>, String>> + Send + Unpin> {
        let abbrev = self.param.gfs_abbrev();
        let level = self.param.gfs_level();
        let url = self.url.clone();
        let f = fetch_url(log.clone(), format!("{}.idx", self.url), &[])
            .map(move |res| res
                .and_then(|vec| String::from_utf8(vec)
                    .map_err(|_| ".idx is not valid utf8".to_owned())
                )
                .and_then(|s| parse_inventory(&s))
                .and_then(|xs| {
                    let mut iter = xs.iter()
                        .skip_while(|x| x.abbrev != abbrev || x.level != level);
                    let item = iter.next();
                    let next_item = iter.next();
                    println!("{:#?}\n{:#?}", item, next_item);
                    match (item, next_item) {
                        (Some(i), Some(ni)) => Ok((i.offset, ni.offset-1)),
                        _ => Err("could not find item in .idx".to_owned()),
                    }
                })
            )
            .and_then(move |(from, to)| {
                log.add_line(&format!("fetch bytes [{}-{}] ({})", from, to, to + 1 - from));
                fetch_url(log, url, &[("Range", &format!("bytes={}-{}", from, to))])
            });
        Box::new(f)
    }

    fn available_from(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now() // TODO:
    }

    fn available_to(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now() + chrono::Duration::hours(1) // TODO:
    }

    fn check_avail(&self, log: std::sync::Arc<TaggedLog>) -> Box<dyn Future<Output=Result<(), String>> + Send + Unpin> {
        Box::new( avail_url(log, format!("{}.idx", self.url)) )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum GfsParameter {
    Temperature2m,
    UComponentWind10m,
    VComponentWind10m,
    TotalCloudCover50mb,
    TotalCloudCover1000mb,
    TotalCloudCoverAvgAtm,
}

impl GfsParameter {
    fn gfs_abbrev(self) -> &'static str {
        match self {
            GfsParameter::Temperature2m => "TMP",
            GfsParameter::UComponentWind10m => "UGRD",
            GfsParameter::VComponentWind10m => "VGRD",
            GfsParameter::TotalCloudCover50mb => "TCDC",
            GfsParameter::TotalCloudCover1000mb => "TCDC",
            GfsParameter::TotalCloudCoverAvgAtm => "TCDC",
        }
    }

    fn gfs_level(self) -> &'static str {
        match self {
            GfsParameter::Temperature2m => "2 m above ground",
            GfsParameter::UComponentWind10m => "10 m above ground",
            GfsParameter::VComponentWind10m=> "10 m above ground",
            GfsParameter::TotalCloudCover50mb => "50 mb",
            GfsParameter::TotalCloudCover1000mb => "1000 mb",
            GfsParameter::TotalCloudCoverAvgAtm => "entire atmosphere",
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            GfsParameter::Temperature2m => "TMP_2m",
            GfsParameter::UComponentWind10m => "UGRD_10m",
            GfsParameter::VComponentWind10m => "VGRD_10m",
            GfsParameter::TotalCloudCover50mb => "TCDC_50mb",
            GfsParameter::TotalCloudCover1000mb => "TCDC_1000mb",
            GfsParameter::TotalCloudCoverAvgAtm => "TCDC_atm",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "TMP_2m" => Some(GfsParameter::Temperature2m),
            "UGRD_10m" => Some(GfsParameter::UComponentWind10m),
            "VGRD_10m" => Some(GfsParameter::VComponentWind10m),
            "TCDC_50mb" => Some(GfsParameter::TotalCloudCover50mb),
            "TCDC_1000mb" => Some(GfsParameter::TotalCloudCover1000mb),
            "TCDC_atm" => Some(GfsParameter::TotalCloudCoverAvgAtm),
            _ => None
        }
    }

    // TODO: add level to verification
    pub fn verify_grib2(self, g: &grib::GribMessage) -> bool {
        let common = g.section4.product_def.common();
        match (self, common.parameter_cat, common.parameter_num) {
            (GfsParameter::Temperature2m,     0, 0)  => true, // Temperature/Temperature(K)
            (GfsParameter::UComponentWind10m, 2, 2)  => true, // Momentum/WindSpeedUComp(m/s)
            (GfsParameter::VComponentWind10m, 2, 3)  => true, // Momentum/WindSpeedVComp(m/s)
            (s, a, b) => { println!("{:?}: {},{}", s, a, b); false }
        }
    }
}