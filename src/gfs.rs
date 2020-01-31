use nom::{named, do_parse, call, tag, recognize, take_until, map_res, many0, complete, digit};
use super::{FileKey, TaggedLog, Parameter, fetch_url, avail_url};
use futures::{Future, FutureExt, TryFutureExt};
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
    let key = FileKey::new(Parameter::Temperature2m, chrono::Utc::today().pred(), 0, 1);
    let gfs = Gfs0p25::new(key);
    let log = std::sync::Arc::new(TaggedLog{tag: "test".to_owned()});
    let f = gfs.fetch_bytes(log)
        .inspect_ok(|x| println!("downloaded {} bytes", x.len()))
        .map(|res| res.and_then(|x|
            grib::parse_message(&x)
                .map(|(rem, msg)| {println!("remaining: {}", rem.len()); msg})
                .map_err(|_e| format!("grib parse error"))
        ))
        .map_ok(|_x| println!("got grib message"))
        .map_err(|e| println!("error {}", e))
        .map(|_res| ());
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(f);
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

pub struct Gfs0p25 {
    url: String,
    abbrev: String,
    level: String,
}

impl Gfs0p25 {
    pub fn new(key: FileKey) -> Self {
        Self {
            url: format!(
                "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{:04}{:02}{:02}/{:02}/gfs.t{:02}z.pgrb2.0p25.f{:03}",
                key.yyyy, key.mm, key.dd, key.modelrun, key.modelrun, key.timestep,
            ),
            abbrev: "TMP".to_owned(),
            level: "2 m above ground".to_owned(),
        }
    }

    pub fn timestep_iter(_mr: u8) -> impl Iterator<Item=u16> {
        Iterator::chain(
            0u16..120,
            (120..=384).step_by(3)
        )
    }
    
    pub fn modelrun_iter() -> impl Iterator<Item=u8> {
        (0u8..=18).step_by(6)
    }

    pub fn fetch_bytes(&self, log: std::sync::Arc<TaggedLog>) -> impl Future<Output=Result<Vec<u8>, String>> {
        let abbrev = self.abbrev.clone();
        let level = self.level.clone();
        let url = self.url.clone();
        fetch_url(log.clone(), format!("{}.idx", self.url), &[])
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
                println!("fetch bytes {}-{} ({})", from, to, to-from);
                fetch_url(log, url, &[("Range", &format!("bytes={}-{}", from, to))])
            }})
    }

    pub fn check_avail(&self, log: std::sync::Arc<TaggedLog>) -> impl Future<Output=Result<(), String>> {
        avail_url(log, format!("{}.idx", self.url))
    }
}

pub struct Gfs0p50 {
    // https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20200128/12/gfs.t12z.pgrb2.0p50.f000 (+.idx)
    // 0, 3, 6, .., 381, 384 (+3)
}

pub struct Gfs1p00 {
    // https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20200128/12/gfs.t12z.pgrb2.1p00.f000 (+.idx)
    // 0, 3, 6, .., 381, 384 (+3)
}
