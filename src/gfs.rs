use nom::{named, do_parse, call, tag, recognize, take_until, map_res, many0, complete, digit};
use super::{FileKey, DataFile, DataSource, TaggedLog, Parameter, fetch_url, avail_url};
use futures::{Future, FutureExt, TryFutureExt};

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
    let key = FileKey::new(DataSource::Gfs100, Parameter::Temperature2m, chrono::Utc::today().pred(), 0, 3);
    let log = std::sync::Arc::new(TaggedLog{tag: "//test//".to_owned()});
    let f = cache::make_fetch_grid_fut(log, key)
        .inspect_ok(|msg| println!("{:#?}", msg))
        .map(|res| res.and_then(|msg| grib::decode_original_values(&msg).map(|vs| (msg, vs))))
        .inspect_ok(|(_, vs)| {
            let mm = vs.iter().fold(None, |acc, x| match acc {
                None => Some((x, x)),
                Some((mi, ma)) => Some((if mi < x { mi } else { x }, if ma > x { ma } else { x })),
            });
            println!("{:?}\n{} values, minmax={:?}", &vs[..std::cmp::min(10, vs.len())], vs.len(), mm);
        })
        .map_ok(|(msg, vs)| println!(
            "value at lat={} lon={} is {:?}", 50.62, 26.25,
            data::extract_value_impl(&vs, &msg.section3, 50.62, 26.25)
        ))
        .map_err(|e| println!("error {}", e));
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    assert_eq!(rt.block_on(f), Ok(()));
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

pub enum GfsResolution {
    Deg025, Deg050, Deg100
}

impl GfsResolution {
    fn abbrev(&self) -> &'static str {
        match self {
            GfsResolution::Deg025 => "0p25",
            GfsResolution::Deg050 => "0p50",
            GfsResolution::Deg100 => "1p00",
        }
    }
}

pub fn filename_to_filekey(filename: &str) -> Option<FileKey> {
    lazy_static::lazy_static! {
        static ref RE: regex::Regex = regex::Regex::new(
            r"^gfs.(0p25|0p50|1p00).(\d{4})-(\d{2})-(\d{2}).(\d{2}).([0-9_A-Z]+).(\d{3}).grib2$"
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
            match p {
                "TMP"     => Some(Parameter::Temperature2m),
                _ => None
            }
            .and_then(|param|
                match res {
                    "0p25" => Some(DataSource::Gfs025),
                    "0p50" => Some(DataSource::Gfs050),
                    "1p00" => Some(DataSource::Gfs100),
                    _ => None
                }.map(|ds| (ds, param))
            )
            .map(move |(source, param)| FileKey{ source, yyyy, mm, dd, modelrun, timestep, param })
        } else {
            None
        }
    })
}

pub struct GfsFile {
    url: String,
    filename: String,
    abbrev: String,
    level: String,
    res: GfsResolution,
}

impl GfsFile {
    pub fn new(key: &FileKey, res: GfsResolution) -> Self {
        Self {
            url: format!(
                "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{:04}{:02}{:02}/{:02}/gfs.t{:02}z.pgrb2.{}.f{:03}",
                key.yyyy, key.mm, key.dd, key.modelrun, key.modelrun,
                res.abbrev(), key.timestep,
            ),
            filename: format!(
                "gfs.{}.{:04}-{:02}-{:02}.{:02}.{}.{:03}.grib2",
                res.abbrev(), key.yyyy, key.mm, key.dd, key.modelrun, "TMP", key.timestep
            ),
            abbrev: "TMP".to_owned(),
            level: "2 m above ground".to_owned(),
            res,
        }
    }

    pub fn timestep_iter(&self, _mr: u8) -> impl Iterator<Item=u16> {
        match self.res {
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
    
    pub fn modelrun_iter(&self) -> impl Iterator<Item=u8> {
        (0u8..=18).step_by(6)
    }

    pub fn check_avail(&self, log: std::sync::Arc<TaggedLog>) -> impl Future<Output=Result<(), String>> {
        avail_url(log, format!("{}.idx", self.url))
    }
}

impl DataFile for GfsFile {
    fn cache_filename(&self) -> &str {
        &self.filename
    }

    fn fetch_bytes(&self, log: std::sync::Arc<TaggedLog>) -> Box<dyn Future<Output=Result<Vec<u8>, String>> + Send + Unpin> {
        let abbrev = self.abbrev.clone();
        let level = self.level.clone();
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
                        (Some(i), Some(ni)) => Ok((i.offset, ni.offset)),
                        _ => Err(format!("could not find item in .idx")),
                    }
                })
            )
            .and_then({ let log = log.clone(); move |(from, to)| {
                log.add_line(&format!("fetch bytes {}-{} ({})", from, to, to-from));
                fetch_url(log, url, &[("Range", &format!("bytes={}-{}", from, to))])
            }});
        Box::new(f)
    }

    fn available_from(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now() // TODO:
    }

    fn available_to(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now() + chrono::Duration::hours(1) // TODO:
    }
}
