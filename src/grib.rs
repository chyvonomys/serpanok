use nom::{
    named, named_args, do_parse, call, apply, verify, value,
    map, flat_map, count, alt, switch, opt,
    bits, tag, tag_bits, take, take_bits,
    be_u8, be_u16, be_u32, be_u64, be_f32
};

named!(parse_i16<i16>, map!(be_u16, |n| {
    let i = (n & 0x7FFF) as i16;
    if n & 0x8000 == 0x8000 {
        -i
    } else {
        i
    }
}));

named!(parse_i32<i32>, map!(be_u32, |n| {
    let i = (n & 0x7FFF_FFFF) as i32;
    if n & 0x8000_0000 == 0x8000_0000 {
        -i
    } else {
        i
    }
}));

#[derive(Debug)]
pub struct Section1 {
    pub tables_version: u8,
    pub ref_time: Ymdhms,
}

#[derive(Debug)]
pub struct Ymdhms {
    pub year: u16,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
}

named!(parse_ymdhms<Ymdhms>, do_parse!(
    year: call!(be_u16) >>
    month: call!(be_u8) >>
    day: call!(be_u8) >>
    hour: call!(be_u8) >>
    minute: verify!(call!(be_u8), |x| x == 0) >>
    second: verify!(call!(be_u8), |x| x == 0) >>

    (Ymdhms {
        year, month, day, hour, minute, second
    })
));

named!(parse_section1<Section1>, do_parse!(
    _length: verify!(be_u32, |n| n == 21) >>
    _section_id: tag!(&[1]) >>
    _centre: alt!(tag!(&[0, 54]) | tag!(&[0, 78]) | tag!(&[0, 7])) >>
    _sub_centre: call!(be_u16) >>
    tables_version: call!(be_u8) >>
    _local_tables_version: call!(be_u8) >>
    _ref_time_significance: tag!(&[1]) >>
    ref_time: call!(parse_ymdhms) >>
    _proc_data_prod_status: tag!(&[0]) >>
    _proc_data_type: alt!(tag!(&[1]) | tag!(&[2])) >>

    (Section1 {
        tables_version, ref_time
    })
));

struct Section2 {}

named!(parse_section2<Section2>, do_parse!(
    length: call!(be_u32) >>
    _section_id: tag!(&[2]) >>
    _data: take!(length - 5) >>

    (Section2 {})
));

#[derive(Debug)]
pub struct Section3 {
    pub n_data_points: u32,
    pub ni: u32,
    pub nj: u32,
    pub lat_first: i32,
    pub lon_first: i32,
    pub lat_last: i32,
    pub lon_last: i32,
    pub di: u32,
    pub dj: u32,
    pub scan_mode: ScanMode,
}

#[derive(Debug)]
pub struct ScanMode {
    pub i_neg: bool,
    pub j_pos: bool,
    pub col_major: bool,
    pub zigzag: bool,
}

named!(parse_scanmode<ScanMode>, bits!(do_parse!(
    i_neg: map!(take_bits!(u8, 1), |b| b == 1) >>
    j_pos: map!(take_bits!(u8, 1), |b| b == 1) >>
    col_major: map!(take_bits!(u8, 1), |b| b == 1) >>
    zigzag: map!(take_bits!(u8, 1), |b| b == 1) >>
    tag_bits!(u8, 4, 0) >>

    (ScanMode { i_neg, j_pos, col_major, zigzag })
)));

// Grid Definition Section
named!(parse_section3<Section3>, do_parse!(
    _length: verify!(be_u32, |n| n == 72) >>
    _section_id: tag!(&[3]) >>
    _grid_def_src: tag!(&[0]) >> // CodeTable 3.0
    n_data_points: call!(be_u32) >>
    _n_points_bytes: tag!(&[0]) >>
    _n_points_interpetation: tag!(&[0]) >> // CodeTable 3.11
    // Template 3.0
    _grid_def_template: tag!(&[0, 0]) >> // CodeTable 3.1
    _earth_shape: tag!(&[6]) >> // CodeTable 3.2
    _scales: alt!(tag!(&[0xFF; 15]) | tag!(&[0; 15])) >> // GFS has zeros
    ni: call!(be_u32) >>
    nj: call!(be_u32) >>
    _basic_angle: tag!(&[0; 4]) >>
    _basic_angle_subdiv: tag!(&[0xFF; 4]) >>
    lat_first: call!(parse_i32) >>
    lon_first: call!(parse_i32) >>
    _res_comp_flags: tag!(&[0b0011_0000]) >> // FlagTable 3.3
    lat_last: call!(parse_i32) >>
    lon_last: call!(parse_i32) >>
    di: call!(be_u32) >>
    dj: call!(be_u32) >>
    scan_mode: call!(parse_scanmode) >> // FlagTable 3.4
    _n_points: take!(0) >>
  
    (Section3 {
        n_data_points, ni, nj, di, dj, scan_mode,
        lat_first, lon_first, lat_last, lon_last,
    })
));

#[derive(Debug)]
pub enum ProductDef {
    Template8 {
        common: CommonProductDef,
        overall_time_interval_end: Ymdhms,
        stat_proc: StatisticalProcess,
        time_range: TimeAmount,
    },
    Template0 {
        common: CommonProductDef,
    },
}

impl ProductDef {
    pub fn common(&self) -> &CommonProductDef {
        match self {
            ProductDef::Template0 { common } => common,
            ProductDef::Template8 { common, .. } => common,
        }
    }
}

named!(parse_section4_template0<ProductDef>, do_parse!(
    _template_id: verify!(call!(be_u16), |x| x == 0) >>
    common: call!(parse_common_productdef) >>
    (ProductDef::Template0 {
        common
    })
));

// CodeTable 4.10
#[derive(Debug)]
enum StatisticalProcess {
    Average,
    Accumulation,
}

named!(parse_stat_proc<StatisticalProcess>, switch!(call!(be_u8),
    0 => value!(StatisticalProcess::Average) |
    1 => value!(StatisticalProcess::Accumulation)
));


#[derive(Debug)]
pub enum TimeAmount {
    Minutes(u32),
    Hours(u32),
    Missing,
}

named!(parse_time_amount<TimeAmount>, do_parse!(
    t: switch!(call!(be_u8), // CodeTable 4.4
        0 => map!(call!(be_u32), |x| TimeAmount::Minutes(x)) |
        1 => map!(call!(be_u32), |x| TimeAmount::Hours(x)) |
        0xFF => map!(tag!(&[0; 4]), |_| TimeAmount::Missing)
    ) >>
    (t)
));

named!(parse_section4_template8<ProductDef>, do_parse!(
    _template_id: verify!(call!(be_u16), |x| x == 8) >>
    common: call!(parse_common_productdef) >>
    overall_time_interval_end: call!(parse_ymdhms) >>
    _time_range_n: tag!(&[1]) >>
    _missing_in_stat_proc_n: tag!(&[0; 4]) >>
    stat_proc: call!(parse_stat_proc) >>
    _time_increment_type: tag!(&[2]) >> // Successive times processed have same start time of forecast, forecast time is incremented  (grib2/tables/19/4.11.table)
    time_range: call!(parse_time_amount) >>
    _time_increment: call!(parse_time_amount) >>
        
    (ProductDef::Template8 {
        common, overall_time_interval_end, stat_proc, time_range
    })
));

#[derive(Debug)]
pub struct CommonProductDef {
    pub parameter_cat: u8,
    pub parameter_num: u8,
    pub forecast_time: TimeAmount,
    pub level1: FixedSurface,
}

#[derive(Debug)]
pub struct Section4 {
    pub product_def: ProductDef,
}

named!(parse_section4<Section4>, do_parse!(
    _section_id: tag!(&[4]) >>
    _nv: tag!(&[0, 0]) >>
    product_def: alt!(call!(parse_section4_template0) | call!(parse_section4_template8)) >>

    (Section4 {
        product_def
    })
));

#[derive(Debug)]
pub enum FixedSurface {
    Isobaric(u32),
    AboveGround(u32),
    EntireAtmosphere,
    GroundOrWater,
    MeanSeaLevel,
    Missing,
}

named!(parse_fixed_surface<FixedSurface>, do_parse!(
    typ: call!(be_u8) >> // CodeTable 4.5
    surf: switch!(value!(typ),
        100 => do_parse!(_factor: tag!(&[0]) >> value: call!(be_u32) >> (FixedSurface::Isobaric(value))) |
        103 => do_parse!(_factor: tag!(&[0]) >> value: call!(be_u32) >> (FixedSurface::AboveGround(value))) |
        101 => map!(tag!(&[0; 5]), |_| FixedSurface::MeanSeaLevel) |
        10 => map!(tag!(&[0; 5]), |_| FixedSurface::EntireAtmosphere) |
        1 => map!(tag!(&[0; 5]), |_| FixedSurface::GroundOrWater) |
        0xFF => map!(alt!(tag!(&[0xFF; 5]) | tag!(&[0; 5])), |_| FixedSurface::Missing) // GFS has zero here
    ) >>

    (surf)
));

named!(parse_common_productdef<CommonProductDef>, do_parse!(
    parameter_cat: call!(be_u8) >> // CodeTable 4.1
    parameter_num: call!(be_u8) >> // CodeTable 4.2
    _gen_proc_type: tag!(&[2]) >> // CodeTable 4.3
    _bg_proc: call!(be_u8) >>
    _gen_proc_id: call!(be_u8) >>
    _cutoff_hours: tag!(&[0, 0]) >>
    _cutoff_mins: tag!(&[0]) >>
    forecast_time: call!(parse_time_amount) >>
    level1: call!(parse_fixed_surface) >>
    _level2: call!(parse_fixed_surface) >>
    
    (CommonProductDef {
        parameter_cat, parameter_num, forecast_time, level1,
    })
));

// CodeTable 5.4
#[derive(Debug)]
enum GroupSplittingMethod {
    General, // 1
}

// CodeTable 5.5
#[derive(Debug)]
enum MissingValueMgmt {
    None, // 0
}

// CodeTable 5.6
#[derive(Debug)]
enum SpatialDiffOrder {
    Second // 2
}

#[derive(Debug)]
pub enum Packing {
    Simple {r: f32, e: i16, d: i16, x2_bits: u8},
    Jpeg2000 {r: f32, e: i16, d: i16, bits: u8},
    ComplexSpatialDiff {
        r: f32, e: i16, d: i16, x1_bits: u8,
        ng: u32,
        sd_order: SpatialDiffOrder, sd_bytes: u8,
        group_width_ref: u8, group_width_bits: u8,
        group_length_ref: u32, group_length_incr: u8,
        last_group_length: u32, group_length_bits: u8,
    },
}

named!(parse_simple_packing<Packing>, do_parse!(
    _template_id: tag!(&[0, 0]) >>
    r: call!(be_f32) >>
    e: call!(parse_i16) >>
    d: call!(parse_i16) >>
    x2_bits: call!(be_u8) >>
    _field_type: tag!(&[0]) >>

    (Packing::Simple{r, e, d, x2_bits})
));

named!(parse_complex_packing_spatial_diff<Packing>, do_parse!(
    _template_id: tag!(&[0, 3]) >>
    // Template 5.0
    r: call!(be_f32) >>
    e: call!(parse_i16) >>
    d: call!(parse_i16) >>
    x1_bits: call!(be_u8) >> // 20
    _field_type: tag!(&[0]) >>
    // Template 5.2
    _group_splitting_method_used: tag!(&[1]) >>              // 22, CodeTable 5.4
    _missing_value_management_used: tag!(&[0]) >>            // 23, CodeTable 5.5
    _primary_missing_value_substitute: call!(be_u32) >>      // 24-27
    _secondary_missing_value_substitute: call!(be_u32) >>    // 28-31
    number_of_groups_of_data_values: call!(be_u32) >>        // 32-35, 'NG'
    reference_for_group_widths: call!(be_u8) >>              // 36, Note ( 12)
    number_of_bits_used_for_the_group_widths: call!(be_u8) >>// 37
    reference_for_group_lengths: call!(be_u32) >>            // 38-41, Note ( 13)
    length_increment_for_the_group_lengths: call!(be_u8) >>  // 42, Note ( 14)
    true_length_of_last_group: call!(be_u32) >>              // 43-46
    number_of_bits_for_scaled_group_lengths: call!(be_u8) >> // 47
    // Template 5.3
    _order_of_spatial_differencing: tag!(&[2]) >>            // 48, CodeTable 5.6
    _number_of_octets_extra_descriptors: tag!(&[2]) >>       // 49

    (Packing::ComplexSpatialDiff {
        r, e, d, x1_bits, ng: number_of_groups_of_data_values,
        sd_order: SpatialDiffOrder::Second, sd_bytes: 2 /* @49 */,
        group_width_ref: reference_for_group_widths,
        group_width_bits: number_of_bits_used_for_the_group_widths,
        group_length_ref: reference_for_group_lengths,
        group_length_incr: length_increment_for_the_group_lengths,
        last_group_length: true_length_of_last_group,
        group_length_bits: number_of_bits_for_scaled_group_lengths,
    })
));

named!(parse_jpeg2000_packing<Packing>, do_parse!(
    _template_id: tag!(&[0, 0x28]) >>
    r: call!(be_f32) >>
    e: call!(parse_i16) >>
    d: call!(parse_i16) >>
    bits: call!(be_u8) >>
    _field_type: tag!(&[0]) >>
    _compression_type: tag!(&[0]) >>
    _compression_ratio: tag!(&[0xFF]) >>

    (Packing::Jpeg2000{r, e, d, bits})
));

#[derive(Debug)]
pub struct Section5 {
    pub n_values: u32,
    pub packing: Packing,
}

named!(parse_section5<Section5>, do_parse!(
    _section_id: tag!(&[5]) >>
    n_values: call!(be_u32) >>
    packing: alt!(call!(parse_simple_packing) | call!(parse_jpeg2000_packing) | call!(parse_complex_packing_spatial_diff)) >>

    (Section5 { n_values, packing })
));

pub enum CodedValues {
    RawJpeg2000(Vec<u8>),
    Simple16Bit(Vec<u16>),
    Simple0Bit,
    ComplexSpatialDiff {
        h1: u32,
        h2: u32,
        hmin: i32,
        x1rle: Vec<(usize, u32)>,
        x2s: Vec<u32>,
    },
}

/*
Template 5.2 Notes
( 12) The group width is the number of bits used for every value in a group.
( 13) The group length (L) is the number of values in a group.
( 14) The essence of the complex packing method is to subdivide a field of values into NG groups,
where the values in each group have similar sizes. In this procedure, it is necessary to retain
enough information to recover the group lengths upon decoding. The NG group lengths for any given
field can be described by Ln = ref + Kn * len_inc, n = 1,NG, where ref is given by octets 38-41
and len_inc by octet 42. The NG values of K (the scaled group lengths) are stored in the Data
Section, each with the number of bits specified by octet 47. Since the last group is a special
case which may not be able to be specified by this relationship, the length of the last group is
stored in octets 43-46.
*/

/*
Template 5.3 Notes
( 1) Spatial differencing is a pre-processing before group splitting at encoding time. It is
intended to reduce the size of sufficiently smooth fields, when combined with a splitting scheme
as described in Data Representation Template 5.2. At order 1, an initial field of values f is
replaced by a new field of values g, where g1 = f1, g2 = f2 - f1, ..., gn = fn - fn-1. At order 2,
the field of values g is itself replaced by a new field of values h, where h1 = f1, h2 = f2, h3 =
g3 - g2, ..., hn = gn - gn-1. To keep values positive, the overall minimum of the resulting field
(either gmin or hmin) is removed. At decoding time, after bit string unpacking, the original
scaled values are recovered by adding the overall minimum and summing up recursively.
*/

pub fn decode_original_values(msg: &GribMessage) -> Result<Vec<f32>, String> {
    match msg {
        GribMessage{
            section5: Section5{ packing: Packing::Simple {r, e, d, x2_bits: 16}, .. },
            section7: Section7{ coded_values: CodedValues::Simple16Bit(ref x2s) },
            ..
        } => {
            let twop = 2f32.powf(f32::from(*e));
            let tenp = 10f32.powf(f32::from(-d));
            let vec = x2s.iter().map(
                |x2| (r + f32::from(*x2) * twop) * tenp
            ).collect();
            Ok(vec)
        },
        GribMessage{
            section5: Section5{ packing: Packing::Simple {r, d, x2_bits: 0, ..}, n_values },
            section7: Section7{ coded_values: CodedValues::Simple0Bit },
            ..
        } => {
            let tenp = 10f32.powf(f32::from(-d));
            let vec = vec![r * tenp; *n_values as usize];
            Ok(vec)
        },
        GribMessage{
            section5: Section5{ packing: Packing::ComplexSpatialDiff {r, e, d, ..}, .. },
            section7: Section7{ coded_values: CodedValues::ComplexSpatialDiff {h1, h2, hmin, ref x1rle, ref x2s} },
            ..
        } => {
            let hs = x2s.iter()
                .zip(x1rle.iter().flat_map(|(n, v)| std::iter::repeat(v).take(*n)))
                .map(|(x1, x2)| *x1 + *x2);
            
            let (mut fs, _, _) = hs.skip(2).fold(
                (vec![*h1 as f32, *h2 as f32], *h2 as i32, *h1 as i32),
                |(mut vec, f_, f__), h| {
                    let f = h as i32 + 2 * f_ - f__ + *hmin;
                    vec.push(f as f32);
                    (vec, f, f_)
                }
            );

            let twop = 2f32.powf(f32::from(*e));
            let tenp = 10f32.powf(f32::from(-d));

            for f in fs.iter_mut() {
                *f = (r + *f * twop) * tenp;
            }

            Ok(fs)
        },
        _ => Err("unsupported packing".to_owned()),
    }
}

pub struct Section7 {
    pub coded_values: CodedValues,
}

use std::fmt;

impl fmt::Debug for Section7 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Section7 {{ coded_values: <{}> }} ", match &self.coded_values {
            CodedValues::RawJpeg2000(v) => format!("Jpeg2000 {} bytes", v.len()),
            CodedValues::Simple16Bit(v) => format!("{} 16bit values", v.len()),
            CodedValues::Simple0Bit => "0bit values".to_owned(),
            CodedValues::ComplexSpatialDiff{ h1, h2, hmin, x1rle, x2s } => format!(
                "Complex packing + spatial differencing\nh1={}, h2={}, hmin={}\nX1={} pairs:\n{:?}...\n...{:?}\nX2={} data points:\n{:?}...\n...{:?}",
                h1, h2, hmin,
                x1rle.len(), &x1rle[..std::cmp::min(10, x1rle.len())], &x1rle[std::cmp::max(0, x1rle.len() - 10)..],
                x2s.len(), &x2s[..std::cmp::min(10, x2s.len())], &x2s[std::cmp::max(0, x2s.len() - 10)..]
            ),
        })
    }
}

named!(parse_section7_template40<Section7>, do_parse!(
    length: call!(be_u32) >>
    _section_id: tag!(&[7]) >>
    coded_values: map!(count!(be_u8, (length - 5) as usize), |v| CodedValues::RawJpeg2000(v)) >>

    (Section7 { coded_values })
));

named!(parse_section7_template0_16bit<Section7>, do_parse!(
    length: call!(be_u32) >>
    _section_id: tag!(&[7]) >>
    coded_values: map!(count!(be_u16, ((length - 5) / 2) as usize), |v| CodedValues::Simple16Bit(v)) >>

    (Section7 { coded_values })
));

named!(parse_section7_template0_0bit<Section7>, do_parse!(
    _length: verify!(call!(be_u32), |l| l == 5) >>
    _section_id: tag!(&[7]) >>

    (Section7 { coded_values: CodedValues::Simple0Bit })
));

fn octets_for(n: u32, width: u8) -> usize {
    let total = n * width as u32;
    (total / 8 + if total % 8 == 0 { 0 } else { 1 }) as usize
}

named_args!(parse_section7_template3(
    nvalues: u32, ng: u32, sd_bytes: u8, x1_bits: u8,
    group_width_ref: u8, group_width_bits: u8,
    group_length_ref: u32, group_length_incr: u8, last_group_length: u32, group_length_bits: u8
)<Section7>, do_parse!(
    length: call!(be_u32) >>
    _section_id: tag!(&[7]) >>
    ww: value!(3 * sd_bytes as usize) >>
    xx: value!(octets_for(ng, x1_bits)) >>
    yy: value!(octets_for(ng, group_width_bits)) >>
    zz: value!(octets_for(ng, group_length_bits)) >>
    nn: value!(length as usize - 5 - ww - xx - yy - zz) >>
    h1: bits!(take_bits!(u32, sd_bytes as usize * 8)) >>
    h2: bits!(take_bits!(u32, sd_bytes as usize * 8)) >>
    hmin: bits!(alt!(
        do_parse!(tag_bits!(u8, 1, 0x1) >> x: take_bits!(u32, sd_bytes as usize * 8 - 1) >> (-(x as i32))) |
        do_parse!(tag_bits!(u8, 1, 0x0) >> x: take_bits!(u32, sd_bytes as usize * 8 - 1) >> (x as i32))
    )) >>
    x1s: flat_map!(take!(xx), bits!(count!(take_bits!(u32, x1_bits as usize), ng as usize))) >>
    group_widths: flat_map!(take!(yy), bits!(count!(take_bits!(u32, group_width_bits as usize), ng as usize))) >>
    ks: flat_map!(take!(zz), bits!(count!(take_bits!(u32, group_length_bits as usize), ng as usize))) >>
    ws: value!(group_widths.iter().map(|w| group_width_ref as u32 + w).collect::<Vec<_>>()) >>
    ls: value!(ks.iter().map(|k| group_length_ref + *k * group_length_incr as u32).collect::<Vec<_>>()) >>
    verify!(value!( (x1s.len(), ls.len()) ), |(a, b)| a == b) >>
    x1rle: value!(ls.iter().zip(x1s.iter()).map(|(l, v)| (*l as usize, *v)).collect::<Vec<_>>()) >>
    groups: value!(ls.iter().zip(ws.iter()).map(|(l, w)| (*l, *w))) >> 
    verify!(value!({
        let total = ls.iter().sum();
        let x2len: u32 = ls.iter().zip(ws.iter()).map(|(a, b)| a * b).sum();
        let wsmin = ws.iter().min().copied();
        let wsmax = ws.iter().max().copied();
        let lsmin = ls.iter().min().copied();
        let lsmax = ls.iter().max().copied();
        println!(
            "ws min:{:?} max:{:?}\nls: min:{:?} max:{:?}\nitems_calc={}\nitems={}\nl={}, ng={}, rem={}, x2len={}bits, ({}bytes +{}bits)",
            wsmin, wsmax, lsmin, lsmax, total, nvalues, length, ng, nn, x2len, x2len/8, x2len%8);
        (total, ls.last().copied().unwrap_or(0))
    }), |x: (u32, u32)| x.0 == nvalues && x.1 == last_group_length) >>
    x2s: bits!(apply!(parse_bit_groups, groups)) >>

    (Section7{ coded_values: CodedValues::ComplexSpatialDiff{
        h1, h2, hmin, x1rle, x2s
    }})
));

fn parse_bit_groups<I: Iterator<Item=(u32, u32)>>(inp: (&[u8], usize), rle: I) -> nom::IResult<(&[u8], usize), Vec<u32>> {
    rle.fold(Ok( (inp, Vec::default()) ), |acc, (times, bits)| {
        acc.and_then(|(inp, mut vec)| {
            let batch = count!(inp, take_bits!(u32, bits as usize), times as usize);
            batch.map(|(rest, chunk)| (rest, {vec.extend_from_slice(&chunk); vec}))
        })
    })
}

#[derive(Debug)]
pub struct GribMessage {
    pub length: usize,
    pub section1: Section1,
    pub section3: Section3,
    pub section4: Section4,
    pub section5: Section5,
    pub section7: Section7,
}

named!(parse_section<&[u8]>, do_parse!(
    length: call!(be_u32) >>
    bytes: take!(length - 4) >>
    (bytes)
));

named!(pub parse_message<GribMessage>, do_parse!(
    tag!("GRIB") >>
    _reserved: alt!(tag!(&[0, 0]) | tag!(&[0xFF, 0xFF])) >>
    _discipline: tag!(&[0]) >> 
    _edition: tag!(&[2]) >>
    total_length: call!(be_u64) >>
    section1: call!(parse_section1) >>
    _section2: opt!(call!(parse_section2)) >>
    section3: call!(parse_section3) >>
    section4: flat_map!(call!(parse_section), call!(parse_section4)) >>
    section5: flat_map!(call!(parse_section), call!(parse_section5)) >>
    _section6: tag!(&[0, 0, 0, 6, 6, 0xFF]) >>
    section7: switch!(value!(&section5.packing),
        &Packing::Simple{ x2_bits: 16, .. } => call!(parse_section7_template0_16bit) |
        &Packing::Simple{ x2_bits: 0, .. } => call!(parse_section7_template0_0bit) |
        &Packing::ComplexSpatialDiff{
            ng, sd_bytes, x1_bits,
            group_width_ref, group_width_bits,
            group_length_ref, group_length_incr, last_group_length, group_length_bits, ..
        } => apply!(parse_section7_template3, section3.n_data_points, ng, sd_bytes, x1_bits,
            group_width_ref, group_width_bits,
            group_length_ref, group_length_incr, last_group_length, group_length_bits
        ) |
        &Packing::Jpeg2000{..} => call!(parse_section7_template40)
    ) >>
    tag!("7777") >>
        
    ( GribMessage {
        length: total_length as usize,
        section1, section3, section4, section5, section7,
    } )
));
