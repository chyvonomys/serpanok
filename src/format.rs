pub fn format_lat_lon(lat: f32, lon: f32) -> String {
    format!("{:.03}°{} {:.03}°{}",
            lat.abs(), if lat > 0.0 {"N"} else {"S"},
            lon.abs(), if lat > 0.0 {"E"} else {"W"},
    )
}

const MONTH_ABBREVS: [&'static str; 12] = [
    "Січ", "Лют", "Бер", "Кві", "Тра", "Чер", "Лип", "Сер", "Вер", "Жов", "Лис", "Гру"
];

fn format_time(t: time::Tm) -> String {
    format!("{} {} {:02}:{:02} (UTC)",
            t.tm_mday,
            MONTH_ABBREVS.get(t.tm_mon as usize).unwrap_or(&"?"),
            t.tm_hour, t.tm_min
    )
}

fn format_rain_rate(mmhr: f32) -> String {
    if mmhr < 0.01 {
        None
    } else if mmhr < 2.5 {
        Some("\u{1F4A7}")
    } else if mmhr < 7.6 {
        Some("\u{1F4A7}\u{1F4A7}")
    } else {
        Some("\u{1F4A7}\u{1F4A7}\u{1F4A7}")
    }.map(|g| format!("{}{:.2}мм/год", g, mmhr)).unwrap_or("--".to_owned())
}

fn format_snow_rate(mmhr: f32) -> String {
    if mmhr < 0.01 {
        None
    } else if mmhr < 1.3 {
        Some("\u{2744}")
    } else if mmhr < 3.0 {
        Some("\u{2744}\u{2744}")
    } else if mmhr < 7.6 {
        Some("\u{2744}\u{2744}\u{2744}")
    } else {
        Some("\u{2744}\u{26A0}")
    }.map(|g| format!("{}{:.2}мм/год", g, mmhr)).unwrap_or("--".to_owned())
}

fn format_wind_dir(u: f32, v: f32) -> &'static str {
    let ws_az = (-v).atan2(-u) / 3.1415926535 * 180.0;

    if ws_az > 135.0 + 22.5 {
        "\u{2192}"
    } else if ws_az > 90.0 + 22.5 {
        "\u{2198}"
    } else if ws_az > 45.0 + 22.5 {
        "\u{2193}"
    } else if ws_az > 22.5 {
        "\u{2199}"
    } else if ws_az > -22.5 {
        "\u{2190}"
    } else if ws_az > -45.0 - 22.5 {
        "\u{2196}"
    } else if ws_az > -90.0 - 22.5 {
        "\u{2191}"
    } else if ws_az > -135.0 - 22.5 {
        "\u{2197}"
    } else {
        "\u{2192}"
    }
}

use data;

pub struct ForecastText(pub String);

pub fn format_forecast(name: Option<&str>, f: &data::Forecast) -> ForecastText {
    let interval = (f.time.1 - f.time.0).num_minutes() as f32;
    let mut result = String::new();

    if let Some(name) = name {
        result.push_str(&format!("\"{}\"\n", name));
    }
    result.push_str(&format!("_{}_\n", format_time(f.time.0)));
    result.push_str(&format!("t повітря: *{:.1}°C*\n", (10.0 * (f.temperature - 273.15)).round() / 10.0));
    if let Some(precip) = f.total_precip_accum {
        let precip_rate = ((precip.1 - precip.0) * 60.0 / interval).max(0.0);
        result.push_str(&format!("опади: *{:.02}*\n", precip_rate));
    }
    if let Some(rain) = f.rain_accum {
        let rain_rate = ((rain.1 - rain.0) * 60.0 / interval).max(0.0);
        result.push_str(&format!("дощ: *{}*\n", format_rain_rate(rain_rate)));
    }
    if let Some(snow) = f.snow_accum {
        let snow_rate = ((snow.1 - snow.0) * 60.0 / interval).max(0.0);
        result.push_str(&format!("сніг: *{}*\n", format_snow_rate(snow_rate)));
    }
    if let Some(snow_depth) = f.snow_depth {
        result.push_str(&format!("шар снігу: *{:.01}см*\n", snow_depth * 100.0));
    }
    if let Some(clouds) = f.total_cloud_cover {
        result.push_str(&format!("хмарність: *{:.0}%*\n", clouds.round()));
    }
    if let Some(wind) = f.wind_speed {
        let wind_speed = (wind.0 * wind.0 + wind.1 * wind.1).sqrt();
        result.push_str(&format!("вітер: *{} {:.1}м/с*\n", format_wind_dir(wind.0, wind.1), (10.0 * wind_speed).round() / 10.0));
    }
    ForecastText(result)
}
