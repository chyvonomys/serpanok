use nom::{be_u8, be_u16, be_u32, be_u64, be_f32};

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
    _centre: alt!(tag!(&[0, 54]) | tag!(&[0, 78])) >>
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
}

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

named!(parse_section3<Section3>, do_parse!(
    _length: verify!(be_u32, |n| n == 72) >>
    _section_id: tag!(&[3]) >>
    _grid_def_src: tag!(&[0]) >>
    n_data_points: call!(be_u32) >>
    _n_points_bytes: tag!(&[0]) >>
    _n_points_interp: tag!(&[0]) >>
    _grid_def_template: tag!(&[0, 0]) >>
    _earth_shape: tag!(&[6]) >>
    _scales: tag!(&[0xFF; 15]) >>
    ni: call!(be_u32) >>
    nj: call!(be_u32) >>
    _basic_angle: tag!(&[0; 4]) >>
    _basic_angle_subdiv: tag!(&[0xFF; 4]) >>
    lat_first: call!(parse_i32) >>
    lon_first: call!(parse_i32) >>
    _res_comp_flags: tag!(&[0b00110000]) >>
    lat_last: call!(parse_i32) >>
    lon_last: call!(parse_i32) >>
    di: call!(be_u32) >>
    dj: call!(be_u32) >>
    _scan_mode: tag!(&[0b01000000]) >>
  
    (Section3 {
        n_data_points, ni, nj, di, dj,
        lat_first, lon_first, lat_last, lon_last,
    })
));


#[derive(Debug)]
pub enum ProductDef {
    Template8 {
        common: CommonProductDef,
        overall_time_interval_end: Ymdhms,
        time_range: u32,
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

named!(parse_section4_template8<ProductDef>, do_parse!(
    _template_id: verify!(call!(be_u16), |x| x == 8) >>
    common: call!(parse_common_productdef) >>
    overall_time_interval_end: call!(parse_ymdhms) >>
    _time_range_n: tag!(&[1]) >>
    _missing_in_stat_proc_n: tag!(&[0; 4]) >>
    _statistical_proc_type: tag!(&[1]) >> // Accumulation  (grib2/tables/19/4.10.table)
    _time_increment_type: tag!(&[2]) >> // Successive times processed have same start time of forecast, forecast time is incremented  (grib2/tables/19/4.11.table)
    _time_range_unit: tag!(&[0]) >> // Minute  (grib2/tables/19/4.4.table)
    time_range: call!(be_u32) >>
    _time_increment_unit: tag!(&[255]) >> // Missing  (grib2/tables/19/4.4.table)
    _time_increment: tag!(&[0; 4]) >>
        
    (ProductDef::Template8 {
        common, overall_time_interval_end, time_range
    })
));

#[derive(Debug)]
pub struct CommonProductDef {
    pub parameter_cat: u8,
    pub parameter_num: u8,
    pub time_units: u8,
    pub forecast_time: u32,
    pub level1_type: u8,
    pub level1_value: u32,
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

named!(parse_common_productdef<CommonProductDef>, do_parse!(
    parameter_cat: call!(be_u8) >>
    parameter_num: call!(be_u8) >>
    _gen_proc_type: tag!(&[2]) >>
    _bg_proc: call!(be_u8) >>
    _gen_proc_id: call!(be_u8) >>
    _cutoff_hours: tag!(&[0, 0]) >>
    _cutoff_mins: tag!(&[0]) >>
    time_units: map!(alt!(tag!(&[0]) | tag!(&[1])), |x| x[0]) >>
    forecast_time: call!(be_u32) >>
    level1_type: call!(be_u8) >>
    _level1_factor: tag!(&[0]) >>
    level1_value: call!(be_u32) >>
    _level2_type: tag!(&[0xFF]) >>
    _level2_factor: tag!(&[0xFF]) >>
    _level2_value: tag!(&[0xFF; 4]) >>
    
    (CommonProductDef {
        parameter_cat, parameter_num,
        time_units, forecast_time,
        level1_type, level1_value,
    })
));

#[derive(Debug)]
pub enum Packing {
    Simple {r: f32, e: i16, d: i16, bits: u8},
    Jpeg2000 {r: f32, e: i16, d: i16, bits: u8},
}

named!(parse_simple_packing<Packing>, do_parse!(
    _template_id: tag!(&[0, 0]) >>
    r: call!(be_f32) >>
    e: call!(parse_i16) >>
    d: call!(parse_i16) >>
    bits: call!(be_u8) >>
    _field_type: tag!(&[0]) >>

    (Packing::Simple{r, e, d, bits})
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
    packing: alt!(call!(parse_simple_packing) | call!(parse_jpeg2000_packing)) >>

    (Section5 { n_values, packing })
));

pub enum CodedValues {
    RawJpeg2000(Vec<u8>),
    Simple16Bit(Vec<u16>),
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
        })
    }
}

named_args!(parse_section7<'a>(p: &Packing)<Section7>, do_parse!(
    length: call!(be_u32) >>
    _section_id: tag!(&[7]) >>
    coded_values: flat_map!(
        take!(length - 5),
        switch!(value!(match p {
            Packing::Simple{bits: 16, ..} => true,
            _ => false,
        }),
                true => map!(count!(be_u16, ((length - 5) / 2) as usize), |v| CodedValues::Simple16Bit(v)) |
                false => map!(count!(be_u8, (length - 5) as usize),        |v| CodedValues::RawJpeg2000(v))
        )
    ) >>

    (Section7 { coded_values })
));

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
    section7: call!(parse_section7, &section5.packing) >>
    tag!("7777") >>
        
    ( GribMessage {
        length: total_length as usize,
        section1, section3, section4, section5, section7,
    } )
));
