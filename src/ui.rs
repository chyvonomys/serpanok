use crate::telegram;
use crate::format;
use crate::data;
use super::{TaggedLog, DataSource, ExchangeSource, lookup_tz};
use futures::{future, Future, FutureExt, TryFutureExt, stream, StreamExt, TryStreamExt};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use chrono::{Datelike, Timelike};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref SUBS: Arc<Mutex<HashMap<(i64, i32), Sub>>> = Arc::default();
}

pub fn monitor_weather_wrap(sub: Sub, tz: chrono_tz::Tz) -> Box<dyn Future<Output=Result<usize, String>> + Send + Unpin> {
    let key = (sub.chat_id, sub.widget_message_id);
    {
        let mut hm = SUBS.lock().unwrap();
        if let Some(_prev) = hm.insert(key, sub.clone()) {
            println!("there was sub for {:?} already, override", key);
        }
    }

    let chat_id = sub.chat_id;
    let widget_msg_id = sub.widget_message_id;
    let log = Arc::new(TaggedLog {tag: format!("{}:{}", chat_id, widget_msg_id)});

    let fts = match sub.mode {
        Mode::Daily(sendh, targeth) =>
            data::daily_forecast_stream(log.clone(), sub.latitude, sub.longitude, sendh, targeth, tz, sub.source, sub.params)
                .boxed(),
        Mode::Once(target_time) =>
            data::forecast_stream(log.clone(), sub.latitude, sub.longitude, target_time, sub.source, sub.params)
                .take(1).boxed(),
        Mode::Live(target_time) =>
            data::forecast_stream(log.clone(), sub.latitude, sub.longitude, target_time, sub.source, sub.params)
                .take(1000).boxed(),
        Mode::Debug(n, d) =>
            tokio_stream::wrappers::IntervalStream::new(tokio::time::interval_at(
                tokio::time::Instant::now() + tokio::time::Duration::from_secs(d),
                tokio::time::Duration::from_secs(d)
            )).map(|_x| Ok(data::Forecast{
                temperature: None, time: (chrono::Utc::now(), chrono::Utc::now()),
                rain_accum: None, snow_accum: None, snow_depth: None, total_cloud_cover: None,
                wind_speed: None, rel_humidity: None, pressure_msl: None
            }))
                .take(n).boxed()
    };

    let forecasts = fts.map_ok(move |f| format::format_forecast(&sub.name, sub.latitude, sub.longitude, &f, tz));

    let f = loop_fn(Ok(Some( (widget_msg_id, forecasts)) ), move |x| match x {
        Ok(Some( (cancel_host, forecasts) )) =>
            futures::future::try_select(
                forecasts.into_future().then(move |(head, tail)| match head {
                    Some(Ok(format::ForecastText(upd))) =>
                        tg_send_widget(chat_id, TgText::Markdown(upd), None, None)
                            .map_ok(|new_host| Some( (new_host, tail) )).left_future(), // next ok item in stream
                    Some(Err(e)) => future::err(e).right_future(), // error in stream
                    None => future::ok(None).right_future(), // end of stream
                }),
                keyboard_input(chat_id, cancel_host, vec![vec![btn("припинити", CANCEL_DATA)]]).map_ok(|_d| None),
            )
            .map_ok(|p| p.factor_first().0)
            .map_err(|p| p.factor_first().0)
            .then(move |x| tg_edit_kb(chat_id, cancel_host, None).map(|_| x)) // compulsory, but make sure we delete that kb
            .map(Loop::Continue).left_future(),
        x => future::ready(x)
            .map(Loop::Break).right_future(),
    })
    .map_ok(|_| 0)
    .inspect_ok(move |n| log.add_line(&format!("done: {} updates", n)))
    .map_err(|e| format!("subscription future error: {}", e))
    .inspect(move |_| { SUBS.lock().unwrap().remove(&key); });

    Box::new(f)
}

/*
run once for some target time: one fails, second succeedes (it's in the cache)

run again for the same time, cache still has unfinished requests:

[2018-12-08T22:24:25Z] (54462285, 388) try 2018-12-08T21:00:00Z/21 >> 2018-12-09T12:00:00Z/015 .. 2018-12-09T13:00:00Z016
[2018-12-08T22:24:32Z] (54462285, 388) took too long
[2018-12-08T22:24:32Z] (54462285, 388) try 2018-12-08T18:00:00Z/18 >> 2018-12-09T12:00:00Z/018 .. 2018-12-09T13:00:00Z019
[2018-12-08T22:24:32Z] (54462285, 388) want 2018-12-08T18:00:00Z/18 >> 2018-12-09T12:00:00Z/018 .. 2018-12-09T13:00:00Z019
[2018-12-08T22:24:33Z] (54462285, 388) single update: ....

QUESTION: will those requests ever finish?
*/

use serde_derive::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone)]
enum Mode {
    Once(chrono::DateTime<chrono::Utc>),
    Live(chrono::DateTime<chrono::Utc>),
    Daily(u8, u8),
    Debug(usize, u64),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Sub {
    chat_id: i64,
    widget_message_id: i32,
    pub latitude: f32,
    pub longitude: f32,
    name: Option<String>, // name for place/none if coords
    mode: Mode,
    source: DataSource,
    params: data::ParameterFlags,
}

#[allow(dead_code)]
enum TgText {
    Markdown(String),
    HTML(String),
    Plain(String),
}

impl TgText {
    fn into_pair(self) -> (Option<String>, String) {
        match self {
            TgText::Markdown(x) => (Some("Markdown".to_owned()), x),
            TgText::HTML(x) => (Some("HTML".to_owned()), x),
            TgText::Plain(x) => (None, x),
        }
    }
}

fn tg_update_widget(
    chat_id: i64, message_id: i32, text: TgText, reply_markup: Option<telegram::TgInlineKeyboardMarkup>
) -> impl Future<Output=Result<(), String>> {

    let (parse_mode, text) = text.into_pair();
    telegram::tg_call("editMessageText", telegram::TgEditMsg {
        chat_id, text, message_id, reply_markup, parse_mode, disable_web_page_preview: true
    }).map_ok(|telegram::TgMessageUltraLite {..}| ())
    .or_else(|err| match err {
        telegram::TgCallError::ApiError(s) if s.starts_with("Bad Request: message is not modified") => future::ok( () ),
        x => future::err(format!("{:?}", x)),
    })
}

fn tg_send_widget(
    chat_id: i64, text: TgText, reply_to_message_id: Option<i32>, reply_markup: Option<telegram::TgInlineKeyboardMarkup>
) -> impl Future<Output=Result<i32, String>> {

    let (parse_mode, text) = text.into_pair();
    telegram::tg_call("sendMessage", telegram::TgSendMsg {
        chat_id, text, reply_to_message_id, reply_markup, parse_mode, disable_web_page_preview: true
    }).map_ok(|m: telegram::TgMessageUltraLite| m.message_id)
    .map_err(|e| format!("{:?}", e))
}

fn tg_edit_kb(chat_id: i64, message_id: i32, reply_markup: Option<telegram::TgInlineKeyboardMarkup>) -> impl Future<Output=Result<(), String>> {
    telegram::tg_call("editMessageReplyMarkup", telegram::TgEditMsgKb {chat_id, message_id, reply_markup})
        .map_ok(|telegram::TgMessageUltraLite {..}| ())
        .map_err(|e| format!("{:?}", e))
}

fn tg_answer_cbq(id: String, notification: Option<String>) -> impl Future<Output=Result<(), String>> {
    telegram::tg_call("answerCallbackQuery", telegram::TgAnswerCBQ{callback_query_id: id, text: notification})
        .map_err(|e| format!("{:?}", e))
        .and_then(|t| future::ready(if t { Ok(()) } else { Err("should return true".to_owned()) }))
}

type MsgId = (i64, i32);

lazy_static! {
    static ref USER_CLICKS: Arc<Mutex<HashMap<MsgId, futures::channel::oneshot::Sender<String>>>> = Arc::default();
    static ref USER_INPUTS: Arc<Mutex<HashMap<i64, futures::channel::oneshot::Sender<String>>>> = Arc::default();
}

#[derive(Serialize)]
pub struct UiStats {
    buttons: Vec<MsgId>,
    inputs: Vec<i64>,
}

pub fn stats() -> UiStats {
    UiStats {
        buttons: USER_CLICKS.lock().unwrap().keys().cloned().collect(),
        inputs: USER_INPUTS.lock().unwrap().keys().cloned().collect(),
    }
}

const PADDING_DATA: &str = "na";
const CANCEL_DATA: &str = "xx";

struct UserClick {
    chat_id: i64,
    msg_id: i32,
    rx: Box<dyn Future<Output=Result<String, String>> + Send + Unpin>,
} 

impl UserClick {
    fn new(chat_id: i64, msg_id: i32) -> Self {
        let (tx, rx) = futures::channel::oneshot::channel::<String>();
        let mut hm = USER_CLICKS.lock().unwrap();
        if let Some(_prev) = hm.insert((chat_id, msg_id), tx) {
            println!("there was tx for clicking {}:{} already, override", chat_id, msg_id);
        }
        let f = rx.map_err(|e| format!("rx error: {}", e.to_string()));
        UserClick { chat_id, msg_id, rx: Box::new(f) }
    }
    fn click(chat_id: i64, msg_id: i32, data: String) -> Result<(), &'static str> {
            USER_CLICKS.lock().unwrap()
                .remove(&(chat_id, msg_id))
                .ok_or("unexpected click")
                .and_then(|tx| tx.send(data).map_err(|_| "receiver is no longer available"))
    }
}

impl Drop for UserClick {
    fn drop(&mut self) {
        if USER_CLICKS.lock().unwrap().remove(&(self.chat_id, self.msg_id)).is_some() {
            println!("remove unused user click {}:{}", self.chat_id, self.msg_id);
        }
    }
}

impl Future for UserClick {
    type Output = Result<String, String>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut futures::task::Context) -> futures::task::Poll<Self::Output>{
        self.rx.poll_unpin(cx)
    }
}

struct UserInput {
    chat_id: i64,
    rx: Box<dyn Future<Output=Result<String, String>> + Send + Unpin>,
}

impl UserInput {
    fn new(chat_id: i64) -> Self {
        let (tx, rx) = futures::channel::oneshot::channel::<String>();
        let mut hm = USER_INPUTS.lock().unwrap();
        if let Some(_prev) = hm.insert(chat_id, tx) {
            println!("there was tx for input {} already, override", chat_id);
        }
        let f = rx.map_err(|e| format!("rx error: {}", e.to_string()));
        UserInput { chat_id, rx: Box::new(f) }
    }
    fn input(chat_id: i64, text: String) -> Result<(), &'static str> {
        USER_INPUTS.lock().unwrap()
            .remove(&chat_id)
            .ok_or("unexpected input")
            .and_then(|tx| tx.send(text).map_err(|_| "receiver is no longer available"))
    }
}

impl Drop for UserInput {
    fn drop(&mut self) {
        if USER_INPUTS.lock().unwrap().remove(&self.chat_id).is_some() {
            println!("drop unused user input {}", self.chat_id);
        }
    }
}

impl Future for UserInput {
    type Output = Result<String, String>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut futures::task::Context) -> futures::task::Poll<Self::Output>{
        self.rx.poll_unpin(cx)
    }
}

type Ymd = (i32, u32, u32);
type PickerCell = Option<(u8, chrono::DateTime<chrono::Utc>)>;

pub fn time_picker<TZ: chrono::TimeZone>(start: chrono::DateTime<TZ>) -> Vec<(Ymd, Vec<Vec<PickerCell>>)> {

    use itertools::Itertools; // group_by, merge_join_by
    use std::convert::TryInto;

    let start0 = start.date_naive().and_hms_opt(start.hour(), 0, 0).unwrap();

    (1..120)
        .map(|dh| start0.clone() + chrono::Duration::hours(dh))
        .group_by(|t| (t.year(), t.month(), t.day()))
        .into_iter()
        .map(|(ymd, ts)| {
            let hs = ts
                .map(|t| (t.hour().try_into().unwrap(), chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(t, chrono::Utc)))
                .group_by(|p| p.0 / 6)
                .into_iter()
                .map(|(_row, xs)| {
                    (0..6).merge_join_by(xs, |i, j| i.cmp(&(j.0 % 6))).map(|eith| {
                        match eith {
                            itertools::EitherOrBoth::Left(_) => None,
                            itertools::EitherOrBoth::Right(j) => Some(j),
                            itertools::EitherOrBoth::Both(_, j) => Some(j),
                        }
                    }).collect::<Vec<PickerCell>>()
                })
                .collect::<Vec<Vec<PickerCell>>>();
             (ymd, hs)
        })
        .collect()
}

struct WidgetState {
    lat: f32,
    lon: f32,
    name: Option<String>,
    tz: chrono_tz::Tz,
    params: data::ParameterFlags,
}

// TODO: escape names for markdown 
fn format_widget_text(state: &WidgetState, time: Option<&Mode>) -> TgText {
    let s = format!(
        "місце: {}\nчасовий пояс: _{}_{}",
        format::format_place_link(&state.name, state.lat, state.lon),
        state.tz.name().replace("_", " "), // markdown
        match time {
            Some(Mode::Once(datetime)) => {
                let local = datetime.with_timezone(&state.tz);
                format!("\nдата: *{}-{:02}-{:02}*\nчас: *{:02}:00*", local.year(), local.month(), local.day(), local.hour())
            },
            Some(Mode::Live(datetime)) => {
                let local = datetime.with_timezone(&state.tz);
                format!("\nдата: *{}-{:02}-{:02}*\nчас: *{:02}:00*", local.year(), local.month(), local.day(), local.hour())
            },
            Some(Mode::Daily(sendh, targeth)) => format!(
                "\nщодня о *{:02}:00*\nна *{:02}:00*{}",
                sendh, targeth, if sendh < targeth { " того ж дня" } else { " наступного дня" }
            ),
            Some(Mode::Debug(n, d)) => format!("\nDEBUG {} times, {}s", n, d),
            None => String::default(),
        }
    );
    println!("TEXT: {}", s);
    TgText::Markdown(s)
}

fn mode_screen(chat_id: i64, widget_msg_id: i32) -> impl Future<Output=Result<Option<Mode>, String>> {
    let kb = vec![
        vec![btn("поточний прогноз", "once")],
        vec![btn("стежити за прогнозом", "live")],
        vec![btn("прогноз щодня", "daily")],
        #[cfg(debug_assertions)]
        vec![btn("DEBUG", "dbg")],
        vec![btn("скасувати", CANCEL_DATA)]
    ];

    keyboard_input(chat_id, widget_msg_id, kb)
        .map(|data| match data {
            Ok(m) if m == "once" => Ok(Some(Mode::Once(chrono::Utc::now() + chrono::Duration::hours(1)))),
            Ok(m) if m == "live" => Ok(Some(Mode::Live(chrono::Utc::now() + chrono::Duration::hours(1)))),
            Ok(m) if m == "daily" => Ok(Some(Mode::Daily(20, 8))),
            Ok(m) if m == "dbg" => Ok(Some(Mode::Debug(1, 5))),
            Ok(m) if m == CANCEL_DATA => Ok(None),
            Ok(m) => Err(format!("unexpected mode string `{}`", m)),
            Err(e) => Err(e),
        })
}

type HourGrid = Vec<Vec<Option<(u8, chrono::DateTime<chrono::Utc>)>>>;

fn day_screen(
    chat_id: i64, widget_msg_id: i32, tz: chrono_tz::Tz
) -> impl Future<Output=Result<Option<(Ymd, HourGrid)>, String>> {

    let mut days_map: HashMap<String, (Ymd, HourGrid)> = HashMap::new();

    let v = time_picker(chrono::Utc::now().with_timezone(&tz));
    let first = v.iter().take(3).map(|(ymd, ts)| {
            let t = format!("{:02}.{:02}", ymd.2, ymd.1);
            days_map.insert(t.clone(), (*ymd, ts.clone()));
            telegram::TgInlineKeyboardButtonCB::new(t.clone(), t)
        }).collect();
    let second = v.iter().skip(3).take(3).map(|(ymd, ts)| {
            let t = format!("{:02}.{:02}", ymd.2, ymd.1);
            days_map.insert(t.clone(), (*ymd, ts.clone()));
            telegram::TgInlineKeyboardButtonCB::new(t.clone(), t)
        }).collect();
    let kb = vec![
        first,
        second,
        vec![btn("назад", CANCEL_DATA)],
    ];

    keyboard_input(chat_id, widget_msg_id, kb)
        .map_ok(move |data| days_map.remove(&data))
}

fn time_screen<T>(
    chat_id: i64, widget_msg_id: i32, tss: Vec<Vec<Option<(u8, T)>>>
) -> impl Future<Output=Result<Option<T>, String>> {

    let mut hours_map: HashMap<String, (u8, _)> = HashMap::new();

    let mut kb: Vec<Vec<_>> = tss.into_iter().map(|r| {
        let row: Vec<telegram::TgInlineKeyboardButtonCB> = r.into_iter().map(|c| {
            match c {
                Some(h) => {
                    let t = format!("{:02}", h.0);
                    hours_map.insert(t.clone(), h);
                    telegram::TgInlineKeyboardButtonCB::new(t.clone(), t)
                },
                None => {
                    telegram::TgInlineKeyboardButtonCB::new("--".to_owned(), PADDING_DATA.to_owned())
                }
            }
        }).collect();
        row
    }).collect();
    kb.push(vec![btn("назад", CANCEL_DATA)]);

    keyboard_input(chat_id, widget_msg_id, kb)
        .map_ok(move |data| hours_map.remove(&data).map(|h| h.1))
}

fn keyboard_input(
    chat_id: i64, message_id: i32, buttons: Vec<Vec<telegram::TgInlineKeyboardButtonCB>>
) -> impl Future<Output=Result<String, String>> {

    let keyboard = Some(telegram::TgInlineKeyboardMarkup{ inline_keyboard: buttons });
    tg_edit_kb(chat_id, message_id, keyboard)
        .and_then(move |()| UserClick::new(chat_id, message_id))
        .and_then(move |d| tg_edit_kb(chat_id, message_id, None).map_ok(|()| d))
}

fn btn(label: &str, data: &str) -> telegram::TgInlineKeyboardButtonCB {
    telegram::TgInlineKeyboardButtonCB::new(label.to_owned(), data.to_owned())
}

fn opt_btn(label: &str, on: bool, data: &str) -> telegram::TgInlineKeyboardButtonCB {
    telegram::TgInlineKeyboardButtonCB::new(format!("{} {}", label, if on { "\u{1F508}" } else { "\u{1F507}" }), data.to_owned())
}

fn pick_datetime(
    chat_id: i64, widget_msg_id: i32, state: &WidgetState
) -> impl Future<Output=Result<Option<chrono::DateTime<chrono::Utc>>, String>> {

    day_screen(chat_id, widget_msg_id, state.tz)
        .and_then(move |opt_next| match opt_next {
            Some((_ymd, tss)) => time_screen(chat_id, widget_msg_id, tss).left_future(),
            None => future::ok(None).right_future(),
        })
}

fn pick_timetime(
    chat_id: i64, widget_msg_id: i32,
) -> impl Future<Output=Result<Option<(u8, u8)>, String>> {

    time_screen(chat_id, widget_msg_id, make_24h_grid())
        .and_then(move |opt_from| match opt_from {
            Some(from) =>
                time_screen(chat_id, widget_msg_id, make_24h_grid())
                    .map_ok(move |opt_to| opt_to.map(|to| (from, to))).left_future(),
            None => future::ok(None).right_future(),
        })
}

fn make_24h_grid() -> Vec<Vec<Option<(u8, u8)>>> {
    (0..4).map(|row| (0..6).map(|col| row * 6 + col).map(|x| Some((x, x))).collect()).collect()
}

enum Adjust<T> {
    Changed(T),
    Same(T),
}

impl<T> Adjust<T> {
    fn either(self) -> T {
        match self {
            Self::Changed(x) => x,
            Self::Same(x) => x,
        }
    }
}

fn adjust_mode(
    chat_id: i64, widget_msg_id: i32, state: &WidgetState, mode: Mode
) -> impl Future<Output=Result<Adjust<Mode>, String>> {
    match mode {
        Mode::Once(t0) =>
            pick_datetime(chat_id, widget_msg_id, state)
                .map_ok(move |opt| match opt {
                    Some(t) => Adjust::Changed(Mode::Once(t)),
                    None => Adjust::Same(Mode::Once(t0)),
                })
                .left_future().left_future(),
        Mode::Live(t0) =>
            pick_datetime(chat_id, widget_msg_id, state)
                .map_ok(move |opt| match opt {
                    Some(t) => Adjust::Changed(Mode::Live(t)),
                    None => Adjust::Same(Mode::Live(t0)),
                })
                .right_future().left_future(),
        Mode::Daily(f0, t0) =>
            pick_timetime(chat_id, widget_msg_id)
                .map_ok(move |opt| match opt {
                    Some((f, t)) => Adjust::Changed(Mode::Daily(f, t)),
                    None => Adjust::Same(Mode::Daily(f0, t0)),
                })
                .left_future().right_future(),
        Mode::Debug(n0, d0) =>
            pick_timetime(chat_id, widget_msg_id)
                .map_ok(move |opt| match opt {
                    Some((n, d)) => Adjust::Changed(Mode::Debug(n.into(), d.into())),
                    None => Adjust::Same(Mode::Debug(n0, d0)),
                })
                .right_future().right_future(),
    }
}

fn adjust_name(chat_id: i64, widget_msg_id: i32, name: Option<String>) -> impl Future<Output=Result<Adjust<Option<String>>, String>> {

    let keyboard = telegram::TgInlineKeyboardMarkup { inline_keyboard: vec![vec![btn("залишити як є", CANCEL_DATA)]] };

    tg_edit_kb(chat_id, widget_msg_id, Some(keyboard))
        .and_then(move |()| {
            futures::future::try_select(
                UserInput::new(chat_id).map_ok(|s| Adjust::Changed(Some(s))),
                UserClick::new(chat_id, widget_msg_id).map_ok(|_d| Adjust::Same(name))
            )
            .map_ok(|p| p.factor_first().0)
            .map_err(|p| p.factor_first().0)
        })
        .and_then(move |adj| tg_edit_kb(chat_id, widget_msg_id, None).map_ok(|_| adj))
}

fn adjust_params(
    chat_id: i64, widget_msg_id: i32, params: data::ParameterFlags
) -> impl Future<Output=Result<Adjust<data::ParameterFlags>, String>> {
    
    loop_fn(Ok(params), move |res| match res {
        Ok(ps) => adjust_params_screen(chat_id, widget_msg_id, ps)
            .then(|x| future::ready(match x {
                Ok( (true, flags) ) => Loop::Break(Ok(flags)),
                Ok( (false, flags) ) => Loop::Continue(Ok(flags)),
                Err(e) => Loop::Break(Err(e)),
            })).left_future(),
        Err(e) => future::ready(Loop::Break(Err(e))).right_future(),
    })
    .map_ok(move |flags| if flags == params { Adjust::Same(flags) } else { Adjust::Changed(flags) })
}

fn adjust_params_screen(
    chat_id: i64, widget_msg_id: i32, params: data::ParameterFlags
) -> impl Future<Output=Result<(bool, data::ParameterFlags), String>> {

    let kb = vec![
        vec![opt_btn("t повітря", params.tmp(), "tmp"), opt_btn("шар снігу", params.depth(), "depth")],
        vec![opt_btn("дощ", params.rain(), "rain"), opt_btn("сніг", params.snow(), "snow")],
        vec![opt_btn("хмарність", params.tcc(), "tcc"), opt_btn("вітер", params.wind(), "wind")],
        vec![opt_btn("тиск", params.pmsl(), "pmsl"), opt_btn("вологість", params.relhum(), "relhum")],
        vec![btn("ок", "ok")],
    ];

    keyboard_input(chat_id, widget_msg_id, kb)
        .and_then(move |d| future::ready(match d.as_str() {
            "ok" => Ok((true, params)),
            "tmp" =>    Ok( (false, params.flip_tmp()) ),
            "tcc" =>    Ok( (false, params.flip_tcc()) ),
            "wind" =>   Ok( (false, params.flip_wind()) ),
            "rain" =>   Ok( (false, params.flip_rain()) ),
            "snow" =>   Ok( (false, params.flip_snow()) ),
            "depth" =>  Ok( (false, params.flip_depth()) ),
            "pmsl" =>   Ok( (false, params.flip_pmsl()) ),
            "relhum" => Ok( (false, params.flip_relhum()) ),
            x => Err(format!("unexpected param click `{}`", x)),
        }))
}

enum Loop<T> {
    Break(T),
    Continue(T),
}

impl<T> Loop<T> {
    fn map<F, U>(self, f: F) -> Loop<U> where F: FnOnce(T) -> U {
        match self {
            Self::Break(x) => Loop::Break(f(x)),
            Self::Continue(x) => Loop::Continue(f(x)),
        }
    }
}

fn loop_fn<S, FN, F>(init: S, upd: FN) -> impl Future<Output=S>
where FN: Fn(S) -> F, F: Future<Output=Loop<S>>
{
    stream::iter(0..).map(Ok)
        .try_fold(init, move |state, _| upd(state).map(|l| match l {
            Loop::Break(s) => Err(s),
            Loop::Continue(s) => Ok(s),
        }))
        .map(|x| x.unwrap_or_else(|e| e))
}

fn config_loop(
    chat_id: i64, widget_msg_id: i32, state: WidgetState, mode: Mode
) -> impl Future<Output=Result<(WidgetState, Option<Mode>), String>> {

    loop_fn(Ok( (state, Some(mode)) ), move |x| match x {
        Ok( (state, opt_mode) ) =>
            match opt_mode {
                Some(mode) => config_screen(chat_id, widget_msg_id, state, mode)
                    .map(|res| match res {
                        Ok(l) => l.map(Ok),
                        Err(e) => Loop::Break(Err(e)),
                    })
                    .left_future(),
                None => future::ready(Loop::Break(Ok( (state, None) )))
                    .right_future(),
            },
        err => future::ready(Loop::Break(err)).right_future(),
    })
}

fn config_screen(
    chat_id: i64, widget_msg_id: i32, state: WidgetState, mode: Mode
) -> impl Future<Output=Result<Loop<(WidgetState, Option<Mode>)>, String>> {

    let kb = vec![
        vec![btn("змінити дату/час", "tm")],
        vec![btn("змінити назву", "nm")],
        vec![btn("вибрати параметри", "pr")],
        vec![btn("ок", "ok"), btn("скасувати", CANCEL_DATA)],
    ];

    let text = format_widget_text(&state, Some(&mode));

    tg_update_widget(chat_id, widget_msg_id, text, None)
        .and_then(move |()| keyboard_input(chat_id, widget_msg_id, kb)
            .and_then(move |data| {
                let f: Box<dyn Future<Output=Result<Loop<(_, _)>, String>> + Unpin + Send> = match data.as_str() {
                    "tm" => Box::new(adjust_mode(chat_id, widget_msg_id, &state, mode)
                        .map_ok(|adj| Loop::Continue( (state, Some(adj.either())) ))
                    ),
                    "nm" => Box::new(adjust_name(chat_id, widget_msg_id, state.name.clone())
                        .map_ok(move |adj| Loop::Continue( (WidgetState{ name: adj.either(), ..state }, Some(mode)) ))
                    ),
                    "pr" => Box::new(adjust_params(chat_id, widget_msg_id, state.params)
                        .map_ok(move |adj| Loop::Continue( (WidgetState{ params: adj.either(), ..state }, Some(mode)) ))
                    ),
                    "ok" => Box::new(future::ok(Loop::Break( (state, Some(mode)) ))),
                    CANCEL_DATA => Box::new(future::ok(Loop::Break( (state, None) ))),
                    d => Box::new(future::err(format!("unexpected config choice `{}`", d))),
                };
                f
            })
        )
}

fn process_widget(chat_id: i64, lat: f32, lon: f32, source: DataSource, name: Option<String>) -> impl Future<Output=Result<(), String>> {

    let state = WidgetState {
        lat, lon, name, tz: lookup_tz(lat, lon), params: source.default_params()
    };

    let text = format_widget_text(&state, None);

    tg_send_widget(chat_id, text, None, None)
        .and_then(move |widget_msg_id| {
            mode_screen(chat_id, widget_msg_id)
                .and_then(move |opt_mode| match opt_mode {
                    Some(mode) => future::Either::Left(config_loop(chat_id, widget_msg_id, state, mode)),
                    None => future::Either::Right(future::ok((state, None))),
                })
                .and_then(move |(state, opt_mode)| match opt_mode {
                    Some(mode) => future::Either::Left({
                        let sub = Sub {
                            chat_id, name: state.name,
                            widget_message_id: widget_msg_id,
                            latitude: state.lat,
                            longitude: state.lon,
                            mode, source, params: state.params,
                        };
                        monitor_weather_wrap(sub, state.tz)
                    }),
                    None => future::Either::Right(future::ok(0)),
                })
                .map_ok(|_n| ())
                .map_err(move |err| format!("widget={} `{}`", widget_msg_id, err))
        })
        .or_else(move |err| {
            let report_text = TgText::Markdown(format!("error in chat={} {}", chat_id, err));
            tg_send_widget(ANDRIY, report_text, None, None)
                .then(move |_res| tg_send_widget(chat_id, TgText::Markdown("сталась помилка".to_owned()), None, None))
                .map_ok(|_| ())
        })
}

async fn process_graph_widget(chat_id: i64, source: ExchangeSource) -> Result<(), String> {

    let log = Arc::new(TaggedLog {tag: format!("{}:.", chat_id)});
    let rates = source.fetch_rates(log).await?;
    let text = format!("{}:\nUSD: {:.2}/{:.2}\nEUR: {:.2}/{:.2}", rates.label,
                       rates.usd_buy, rates.usd_sell,
                       rates.eur_buy, rates.eur_sell,
    );
/*
    let rs = vec![
        (chrono::Utc::now() - chrono::Duration::minutes(5 * 15), 2775, 2795),
        (chrono::Utc::now() - chrono::Duration::minutes(4 * 15), 2775, 2794),
        (chrono::Utc::now() - chrono::Duration::minutes(3 * 15), 2775, 2799),
        (chrono::Utc::now() - chrono::Duration::minutes(2 * 15), 2775, 2799),
        (chrono::Utc::now() - chrono::Duration::minutes(1 * 15), 2799, 2804),
        (chrono::Utc::now() - chrono::Duration::minutes(0 * 15), 2800, 2806),
    ];
    let file = format::format_exchange_graph(
        &rs,
        &(chrono::Utc::now() - chrono::Duration::minutes(6 * 15)),
        &(chrono::Utc::now() + chrono::Duration::minutes(15)))?;
    let text = file;
*/
    let _msg_id = tg_send_widget(chat_id, TgText::Plain(text), None, None).await?;
    Ok( () )
}

#[derive(Clone, Copy)]
pub struct Features {
    pub weather: bool,
    pub exchange: bool,
}

pub fn process_update(tgu: telegram::TgUpdate, fs: Features) -> impl Future<Output=Result<(), String>> {
    process_bot(SeUpdate::from(tgu), fs)
}

static ANDRIY: i64 = 54_462_285;

static USAGE_MSG: &str = "\u{1F4A1} цей бот вміє відстежувати і періодично надсилати\n\
оновлення прогнозу погоди для заданого місця та часу\n\n\
щоб почати, вкажіть місце одним із способів:\n\
\u{1F4CD} *точне розташування*\n\
`     `(меню \u{1F4CE} в мобільному додатку)\n\
\u{1F5FA} *по назві місця*\n\
`     `(ввести `@osmbot <назва>` і вибрати з списку)";

static EXCH_USAGE_MSG: &str = "kurs valut rulya bank: /rulya";

fn build_start_message(features: Features) -> String {
    let mut msg = String::new();
    if features.weather {
        msg.push_str(USAGE_MSG);
    }

    if features.exchange {
        if msg.len() > 0 {
            msg.push_str("\n\n");
        }
        msg.push_str(EXCH_USAGE_MSG);
    }
    msg
}

static WRONG_LOCATION_MSG: &str = "невірні координати місця";
static UNKNOWN_COMMAND_MSG: &str = "невідома команда";
static CBQ_ERROR_MSG: &str = "помилка";
static PADDING_BTN_MSG: &str = "недоcтупна опція";

// TODO: what if user deletes chat or deletes widget message??
// ANSWER: in private chats user can only delete message for himself
//         deleting whole chat still allows bot to send messages
// NOTE: turns out it is possible to delete from mobile client
// TODO2:  what if user blocks bot?

// NOTE: allow only private chat communication for now
async fn process_bot(upd: SeUpdate, fs: Features) -> Result<(), String> {
    match upd {
        SeUpdate::PrivateChat {chat_id, update, ..} => match update {
            SeUpdateVariant::Command(cmd) =>
                match cmd.as_str() {
                    "/start" if fs.weather => tg_send_widget(chat_id, TgText::Markdown(USAGE_MSG.to_owned()), None, None).map_ok(|_| ()).await,
                    "/debug" if fs.weather => process_widget(chat_id, 50.62, 26.25, DataSource::IconEu, Some("debug-rv".to_owned())).await,
                    "/rulya" if fs.exchange => process_graph_widget(chat_id, ExchangeSource::Rulya).await,
                    _ => tg_send_widget(chat_id, TgText::Plain(UNKNOWN_COMMAND_MSG.to_owned()), None, None).map_ok(|_| ()).await,
                },
            SeUpdateVariant::Place {latitude, longitude, name, ..} if fs.weather =>
                if let Some(source) = DataSource::from_latlon(latitude, longitude) {
                    process_widget(chat_id, latitude, longitude, source, name).await
                } else {
                    tg_send_widget(chat_id, TgText::Plain(WRONG_LOCATION_MSG.to_owned()), None, None).map_ok(|_| ()).await
                }
            SeUpdateVariant::CBQ {id, msg_id, data} =>
                if data == PADDING_DATA { // TODO: this is wrong place to check this
                    tg_answer_cbq(id, Some(PADDING_BTN_MSG.to_owned()))
                        .map_err(|e| format!("answer stub cbq error: {}", e)).await
                } else {
                    // push choice into channel for (chat, msg_id) conversation
                    let ok = UserClick::click(chat_id, msg_id, data)
                        .map_err(|e| println!("cbq(click) {}:{} error: {}", chat_id, msg_id, e))
                        .is_ok();

                    tg_answer_cbq(id, if ok { None } else { Some(CBQ_ERROR_MSG.to_owned()) })
                        .map_err(|e| format!("answer cbq error: {}", e)).await
                },
            SeUpdateVariant::Text(text) if fs.weather => {
                let ok = UserInput::input(chat_id, text)
                    .map_err(|e| println!("text {} error: {}", chat_id, e))
                    .is_ok();

                if ok {
                    Ok(())
                } else {
                    tg_send_widget(chat_id, TgText::Markdown(USAGE_MSG.to_owned()), None, None).map_ok(|_m| () ).await
                }
            },
            update => tg_send_widget(ANDRIY, TgText::Plain(format!("disabled update:\n{:#?}", update)), None, None).map_ok(|_| ()).await,
        },            
        SeUpdate::GroupChat {chat_id, update, ..} => match update {
            SeUpdateVariant::Command(cmd) =>
                match cmd.as_str() {
                    "/rulya" if fs.exchange => process_graph_widget(chat_id, ExchangeSource::Rulya).await,
                    _ => tg_send_widget(chat_id, TgText::Plain(UNKNOWN_COMMAND_MSG.to_owned()), None, None).map_ok(|_| ()).await,
                },
            _ => Ok(()), // Do nothing, we only support commands 
        },
        SeUpdate::Other(update) =>
            tg_send_widget(ANDRIY, TgText::Plain(format!("unsupported update:\n{:#?}", update)), None, None).map_ok(|_| ()).await,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
enum SeUpdate {
    PrivateChat {
        chat_id: i64,
        user: telegram::TgUser,
        update: SeUpdateVariant,
    },
    GroupChat {
        chat_id: i64,
        user: telegram::TgUser,
        update: SeUpdateVariant,
    },
    Other(telegram::TgUpdate),
}

#[derive(Debug)]
enum SeUpdateVariant {
    CBQ {
        id: String,
        msg_id: i32,
        data: String,
    },
    Place {
        latitude: f32,
        longitude: f32,
        name: Option<String>,
        msg_id: i32,
    },
    Command(String),
    Text(String),
}

fn parse_osm_url(url: &str) -> Option<(f32, f32)> {
    lazy_static! {
        static ref LAT_RE: regex::Regex = regex::Regex::new(r"www\.openstreetmap\.org/.*[?&]mlat=(-?\d+.\d+)").unwrap();
        static ref LON_RE: regex::Regex = regex::Regex::new(r"www\.openstreetmap\.org/.*[?&]mlon=(-?\d+.\d+)").unwrap();
    }
    Some(())
        .and_then(| _ | LAT_RE.captures(url).and_then(|cs| cs.get(1).and_then(|m| m.as_str().parse::<f32>().ok())))
        .and_then(|lat| LON_RE.captures(url).and_then(|cs| cs.get(1).and_then(|m| m.as_str().parse::<f32>().ok())).map(|lon| (lat, lon)))
}

fn parse_osm_title(h: &str) -> Option<&str> {
    lazy_static! {
        static ref NAME_RE: regex::Regex = regex::Regex::new(r"^Tags for (.+) $").unwrap();
    }
    NAME_RE.captures(h).and_then(|cs| cs.get(1).map(|m| m.as_str()))
}

#[test]
fn test_osm_url() {
    let url = "http://www.openstreetmap.org/?minlat=50.4718774&maxlat=50.5518774&minlon=25.6526259&maxlon=25.6526259&mlat=50.5118774&mlon=25.6126259";
    assert_eq!(parse_osm_url(url), Some((50.5118774f32, 25.6126259f32)));
}

impl From<telegram::TgUpdate> for SeUpdate {
    fn from(upd: telegram::TgUpdate) -> Self {

        use telegram::TgMessageEntityType;

        let osm_coords = upd.message.as_ref()
            .and_then(|m| m.get_text_links().find_map(|(label, url)| if label == "Map" { Some(url) } else { None }))
            .and_then(|url| parse_osm_url(url));
        
        let osm_name = upd.message.as_ref()
            .and_then(|m| m.get_entities_of_type(TgMessageEntityType::Bold).next())
            .and_then(|h| parse_osm_title(h.as_str()).map(|x| x.to_owned()));

        let first_command = upd.message.as_ref().and_then(|m| m.get_entities_of_type(TgMessageEntityType::BotCommand).next());
        match (first_command, osm_name, osm_coords, upd) {
            // Location/venue
            (None, None, None, telegram::TgUpdate {
                message: Some(telegram::TgMessage {
                    from: Some(user),
                    chat: telegram::TgChat {id: chat_id, type_: telegram::TgChatType::Private},
                    message_id: msg_id,
                    text: None,
                    entities: None,
                    location: Some(location),
                    venue,
                }),
                callback_query: None,
            }) => SeUpdate::PrivateChat { chat_id, user, update: SeUpdateVariant::Place {
                latitude: location.latitude,
                longitude: location.longitude,
                name: venue.map(|v| v.title),
                msg_id,
            } },
            // CBQ
            (None, None, None, telegram::TgUpdate {
                message: None,
                callback_query: Some(telegram::TgCallbackQuery {
                    id,
                    from: user,
                    message: Some(telegram::TgMessageLite {
                        message_id: msg_id,
                        chat: telegram::TgChat {id: chat_id, type_: telegram::TgChatType::Private},
                    }),
                    data: Some(data),
                }),
            }) => SeUpdate::PrivateChat { chat_id, user, update: SeUpdateVariant::CBQ {id, msg_id, data} },
            // Text from user
            (None, None, None, telegram::TgUpdate {
                message: Some(telegram::TgMessage {
                    from: Some(user),
                    chat: telegram::TgChat {id: chat_id, type_: telegram::TgChatType::Private},
                    text: Some(text),
                    entities: None,
                    location: None,
                    ..
                }),
                callback_query: None,
            }) => SeUpdate::PrivateChat { chat_id, user, update: SeUpdateVariant::Text(text) },
            // OSM place card
            (None, name, Some((latitude, longitude)), telegram::TgUpdate {
                message: Some(telegram::TgMessage {
                    from: Some(user),
                    message_id: msg_id,
                    chat: telegram::TgChat {id: chat_id, type_: telegram::TgChatType::Private},
                    location: None,
                    ..
                }),
                callback_query: None,
            }) => SeUpdate::PrivateChat {
                chat_id, user, update: SeUpdateVariant::Place{latitude, longitude, name, msg_id}
            },
            // Command
            (Some(cmd_full), None, None, telegram::TgUpdate {
                message: Some(telegram::TgMessage {
                    from: Some(user),
                    chat: telegram::TgChat {id: chat_id, type_: chat_type},
                    entities: Some(_),
                    location: None,
                    ..
                }),
                callback_query: None,
            }) => {
                // remove trailing @botname
                let cmd = cmd_full.split('@').next().unwrap_or("/start").to_owned();
                match chat_type {
                    telegram::TgChatType::Private => SeUpdate::PrivateChat { chat_id, user, update: SeUpdateVariant::Command(cmd) },
                    _ => SeUpdate::GroupChat { chat_id, user, update: SeUpdateVariant::Command(cmd) },
                }
            },
            // Other
            (_, _, _, u) => SeUpdate::Other(u),
        }
    }
}
