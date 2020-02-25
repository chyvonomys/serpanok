use chrono::{Datelike, Timelike};

pub fn format_lat_lon(lat: f32, lon: f32) -> String {
    format!("{:.03}°{} {:.03}°{}",
            lat.abs(), if lat > 0.0 {"N"} else {"S"},
            lon.abs(), if lat > 0.0 {"E"} else {"W"},
    )
}

const MONTH_ABBREVS: [&str; 12] = [
    "Січ", "Лют", "Бер", "Кві", "Тра", "Чер", "Лип", "Сер", "Вер", "Жов", "Лис", "Гру"
];

fn format_time(t: chrono::DateTime<chrono_tz::Tz>) -> String {
    format!("{} {} {:02}:{:02}",
            t.day(),
            MONTH_ABBREVS.get(t.month0() as usize).unwrap_or(&"?"),
            t.hour(), t.minute()
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
    }.map(|g| format!("{}{:.2}мм/год", g, mmhr)).unwrap_or_else(|| "--".to_owned())
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
    }.map(|g| format!("{}{:.2}мм/год", g, mmhr)).unwrap_or_else(|| "--".to_owned())
}

fn format_wind_dir(u: f32, v: f32) -> &'static str {
    let ws_az = (-v).atan2(-u) / std::f32::consts::PI * 180.0;

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

use crate::data;

pub struct ForecastText(pub String);

pub fn format_place_link(name: &Option<String>, lat: f32, lon: f32) -> String {
    let text = name.clone().unwrap_or_else(|| format_lat_lon(lat, lon));
    format!("[{}](http://www.openstreetmap.org/?mlat={}&mlon={})", text, lat, lon)
}

pub fn format_forecast(name: &Option<String>, lat: f32, lon: f32, f: &data::Forecast, tz: chrono_tz::Tz) -> ForecastText {
    let interval = (f.time.1 - f.time.0).num_minutes() as f32;
    let mut result = String::new();

    result.push_str(&format_place_link(name, lat, lon));
    result.push('\n');
    result.push_str(&format!("_{}_\n", format_time(f.time.0.with_timezone(&tz))));
    if let Some(tmpk) = f.temperature {
        let tmpc = (10.0 * (tmpk - 273.15)).round() / 10.0;
        result.push_str(&format!("t повітря: *{:.1}°C*\n", tmpc));
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
    if let Some(relhum) = f.rel_humidity {
        result.push_str(&format!("відн. вологість: *{:.0}%*\n", relhum));
    }
    if let Some(pmsl) = f.pressure_msl {
        result.push_str(&format!("атм. тиск: *{:.0}ммHg*\n", pmsl / 133.322))
    }
    ForecastText(result)
}
