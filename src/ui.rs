use crate::telegram;
use crate::format;
use crate::data;
use super::{TaggedLog, lookup_tz};
use futures::{future, Future, FutureExt, TryFutureExt, stream, Stream, StreamExt, TryStreamExt};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use chrono::{Datelike, Timelike};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref SUBS: Arc<Mutex<HashMap<(i64, i32), Sub>>> = Arc::default();
}

fn iter_cancel<S, I, SE, CE, CFn, CFt>(init: I, mut s: S, mut cf: CFn) -> impl Stream<Item=Result<I, SE>> where
    I: Clone,
    CFn: FnMut(Option<I>, I) -> CFt + 'static,
    CFt: Future<Output=Result<(), CE>> + Unpin,
    S: Stream<Item=Result<I, SE>> + Unpin,
{
    let mut cancel_fut: CFt = cf(None, init.clone());
    let mut prev = Some(init);

    stream::poll_fn(
        move |cx| -> futures::task::Poll<Option<Result<I, SE>>> {
            match cancel_fut.poll_unpin(cx) {
                futures::task::Poll::Ready(Ok(())) => futures::task::Poll::Ready(None),
                futures::task::Poll::Pending | futures::task::Poll::Ready(Err(_)) => {
                    let res = s.poll_next_unpin(cx);
                    if let futures::task::Poll::Ready(Some(Ok(ref i))) = res {
                        cancel_fut = cf(prev.clone(), i.clone());
                        prev = Some(i.clone());
                    }
                    res
                }
            }
        }
    )
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
    let loc_msg_id = sub.location_message_id;
    let widget_msg_id = sub.widget_message_id;
    let log = Arc::new(TaggedLog {tag: format!("{}:{}", chat_id, loc_msg_id)});

    let fts = match sub.mode {
        Mode::Daily(_sendh, _targeth) => future::Either::Left(stream::empty()),
        Mode::Once(target_time) => future::Either::Right(
            data::forecast_stream(log.clone(), sub.latitude, sub.longitude, target_time).take(1)
        ),
        Mode::Live(target_time) => future::Either::Right(
            data::forecast_stream(log.clone(), sub.latitude, sub.longitude, target_time).take(1000)
        ),
    };

    let f = iter_cancel(
        widget_msg_id,
        fts
            .map_ok(move |f| format::format_forecast(&sub.name, sub.latitude, sub.longitude, &f, tz))
            .and_then(move |format::ForecastText(upd)| tg_send_widget(chat_id, TgText::Markdown(upd), None, None)),
        move |remove_id, add_id| {
            if let Some(msg_id) = remove_id {
                future::Either::Left(tg_edit_kb(chat_id, msg_id, None))
            } else {
                future::Either::Right(future::ok( () ))
            }
            .and_then(move |()| {
                let keyboard = telegram::TgInlineKeyboardMarkup {
                    inline_keyboard: vec![make_custom_cancel_row("припинити")]
                };
                tg_edit_kb(chat_id, add_id, Some(keyboard))
                    .and_then(move |()| {
                        UserClick::new(chat_id, add_id)
                            .and_then(move |_| tg_edit_kb(chat_id, add_id, None))
                    })
            })
        },
    )
        .try_fold(0, |n, _| future::ok::<_, String>(n+1))
        .inspect_ok(move |n| log.add_line(&format!("done: {} updates", n)))
        .map_err(|e| format!("subscription future error: {}", e))
        .then(move |x| {SUBS.lock().unwrap().remove(&key); future::ready(x)});

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
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Sub {
    chat_id: i64,
    location_message_id: i32,
    widget_message_id: i32,
    pub latitude: f32,
    pub longitude: f32,
    name: Option<String>, // name for place/none if coords
    mode: Mode,
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
}

fn tg_send_widget(
    chat_id: i64, text: TgText, reply_to_message_id: Option<i32>, reply_markup: Option<telegram::TgInlineKeyboardMarkup>
) -> impl Future<Output=Result<i32, String>> {

    let (parse_mode, text) = text.into_pair();
    telegram::tg_call("sendMessage", telegram::TgSendMsg {
        chat_id, text, reply_to_message_id, reply_markup, parse_mode, disable_web_page_preview: true
    }).map_ok(|m: telegram::TgMessageUltraLite| m.message_id)
}

fn tg_edit_kb(chat_id: i64, message_id: i32, reply_markup: Option<telegram::TgInlineKeyboardMarkup>) -> impl Future<Output=Result<(), String>> {
    telegram::tg_call("editMessageReplyMarkup", telegram::TgEditMsgKb {chat_id, message_id, reply_markup})
        .map_ok(|telegram::TgMessageUltraLite {..}| ())
}

fn tg_answer_cbq(id: String, notification: Option<String>) -> impl Future<Output=Result<(), String>> {
    telegram::tg_call("answerCallbackQuery", telegram::TgAnswerCBQ{callback_query_id: id, text: notification})
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

fn make_cancel_row() -> Vec<telegram::TgInlineKeyboardButtonCB>{
    vec![telegram::TgInlineKeyboardButtonCB::new("скасувати".to_owned(), CANCEL_DATA.to_owned())]
}

fn make_custom_cancel_row(text: &str) -> Vec<telegram::TgInlineKeyboardButtonCB>{
    vec![telegram::TgInlineKeyboardButtonCB::new(text.to_owned(), CANCEL_DATA.to_owned())]
}

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
/*
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
*/
type Ymd = (i32, u32, u32);
type PickerCell = Option<(u32, chrono::DateTime<chrono::Utc>)>;

pub fn time_picker<TZ: chrono::TimeZone>(start: chrono::DateTime<TZ>) -> Vec<(Ymd, Vec<Vec<PickerCell>>)> {

    use itertools::Itertools; // group_by, merge_join_by
    
    let start0 = start.date().and_hms(start.hour(), 0, 0);

    (1..120)
        .map(|dh| start0.clone() + chrono::Duration::hours(dh))
        .group_by(|t| (t.year(), t.month(), t.day()))
        .into_iter()
        .map(|(ymd, ts)| {
            let hs = ts
                .map(|t| (t.hour(), chrono::DateTime::<chrono::Utc>::from_utc(t.naive_utc(), chrono::Utc)))
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
    time: Option<Mode>,
}

impl WidgetState {
    fn format(&self) -> String {
        format!(
            "місце: {}\nчасовий пояс: _{}_{}",
            format::format_place_link(&self.name, self.lat, self.lon),
            self.tz.name(),
            match self.time {
                Some(Mode::Once(datetime)) => {
                    let local = datetime.with_timezone(&self.tz);
                    format!("\nдата: *{}-{:02}-{:02}*\nчас: *{:02}:00*", local.year(), local.month(), local.day(), local.hour())
                },
                Some(Mode::Live(datetime)) => {
                    let local = datetime.with_timezone(&self.tz);
                    format!("\nдата: *{}-{:02}-{:02}*\nчас: *{:02}:00*", local.year(), local.month(), local.day(), local.hour())
                },
                Some(Mode::Daily(sendh, targeth)) => format!(
                    "\nщодня о *{:02}:00*\nна *{:02}:00*{}",
                    sendh, targeth, if sendh < targeth { " того ж дня" } else { " наступного дня" }
                ),
                None => String::default(),
            }
        )
    }
}

fn process_mode_screen(chat_id: i64, widget_msg_id: i32, state: WidgetState) -> impl Future<Output=Result<Option<Mode>, String>> {
    let keyboard = telegram::TgInlineKeyboardMarkup{ inline_keyboard: vec![
        vec![telegram::TgInlineKeyboardButtonCB::new("поточний прогноз".to_owned(), "once".to_owned())],
        vec![telegram::TgInlineKeyboardButtonCB::new("стежити за прогнозом".to_owned(), "live".to_owned())],
        vec![telegram::TgInlineKeyboardButtonCB::new("прогноз щодня".to_owned(), /*"daily"*/PADDING_DATA.to_owned())],
        make_cancel_row(),
    ]};

    tg_update_widget(chat_id, widget_msg_id, TgText::Markdown(state.format()), Some(keyboard))
        .and_then(move |()|
            UserClick::new(chat_id, widget_msg_id)
                .map(|data| match data {
                    Ok(m) if m == "once" => Ok(Some(Mode::Once(chrono::Utc::now() + chrono::Duration::hours(1)))),
                    Ok(m) if m == "live" => Ok(Some(Mode::Live(chrono::Utc::now() + chrono::Duration::hours(1)))),
                    Ok(m) if m == "daily" => Ok(Some(Mode::Daily(20, 8))),
                    Ok(m) if m == CANCEL_DATA => Ok(None),
                    Ok(m) => Err(format!("unexpected mode string `{}`", m)),
                    Err(e) => Err(e),
                })
        )
}

fn process_widget(
    chat_id: i64, loc_msg_id: i32, target_lat: f32, target_lon: f32, name: Option<String>
) -> impl Future<Output=Result<(), String>> {

    let tz = lookup_tz(target_lat, target_lon);

    let widget_text0 = format!(
        "місце: {}\nчасовий пояс: _{}_",
        format::format_place_link(&name, target_lat, target_lon),
        tz.name()
    );

    future::ok(widget_text0)
        .and_then(move |widget_text| {
            type HourGrid = Vec<Vec<Option<(u32, chrono::DateTime<chrono::Utc>)>>>;
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
            let inline_keyboard = vec![
                first,
                second,
                make_cancel_row(),
            ];

            let keyboard = telegram::TgInlineKeyboardMarkup{ inline_keyboard };

            tg_send_widget(chat_id, TgText::Markdown(format!("{}\nоберіть дату:", widget_text)), None, Some(keyboard))
                .and_then(move |msg_id|
                    UserClick::new(chat_id, msg_id)
                        .map_ok(move |data| (widget_text, msg_id, days_map.remove(&data)))
                )
        })
        .and_then(move |(mut widget_text, msg_id, build)| {
            if let Some((ymd, tss)) = build {
                let mut hours_map: HashMap<String, (u32, _)> = HashMap::new();

                let mut inline_keyboard: Vec<Vec<_>> = tss.iter().map(|r| {
                    let row: Vec<telegram::TgInlineKeyboardButtonCB> = r.iter().map(|c| {
                        match c {
                            Some(h) => {
                                let t = format!("{:02}", h.0);
                                hours_map.insert(t.clone(), *h);
                                telegram::TgInlineKeyboardButtonCB::new(t.clone(), t)
                            },
                            None => {
                                let t = "--".to_owned();
                                telegram::TgInlineKeyboardButtonCB::new(t, PADDING_DATA.to_owned())
                            }
                        }
                    }).collect();
                    row
                }).collect();
                inline_keyboard.push(make_cancel_row());

                let keyboard = Some(telegram::TgInlineKeyboardMarkup{ inline_keyboard });
                widget_text.push_str(&format!("\nдата: *{}-{:02}-{:02}*", ymd.0, ymd.1, ymd.2));

                let f = tg_update_widget(chat_id, msg_id, TgText::Markdown(format!("{}\nоберіть час:", widget_text)), keyboard)
                    .and_then(move |()|
                        UserClick::new(chat_id, msg_id)
                            .map_ok(move |data| {
                                let build = hours_map.remove(&data).map(|h| h.1);
                                (widget_text, msg_id, build)
                            })
                    );
                future::Either::Left(f)
            } else {
                future::Either::Right(future::ok((widget_text, msg_id, None)))
            }
        })
        .and_then(move |(mut widget_text, msg_id, build)| {
            if let Some(target_time) = build {
                let inline_keyboard = vec![
                    vec![telegram::TgInlineKeyboardButtonCB::new("так, надсилати оновлення".to_owned(), "live".to_owned())],
                    vec![telegram::TgInlineKeyboardButtonCB::new("ні, тільки поточний".to_owned(), "once".to_owned())],
                    make_cancel_row(),
                ];

                let keyboard = Some(telegram::TgInlineKeyboardMarkup{ inline_keyboard });
                widget_text.push_str(&format!("\nчас: *{:02}:00*", target_time.with_timezone(&tz).hour()));
                let text = format!("{}\nбажаєте отримувати періодичні оновлення прогнозу?", widget_text);

                let f = tg_update_widget(chat_id, msg_id, TgText::Markdown(text), keyboard).and_then(move |()| {
                    UserClick::new(chat_id, msg_id)
                        .map_ok(move |data: String| {
                            match data.as_str() {
                                "live" => Some(Mode::Live(target_time)),
                                "once" => Some(Mode::Once(target_time)),
                                _ => None,
                            }
                        })
                        .map_ok(move |build| (widget_text, msg_id, build))
                });
                future::Either::Left(f)
            } else {
                future::Either::Right(future::ok((widget_text, msg_id, None)))
            }
        })
        /*
        .and_then(move |(widget_text, msg_id, build)| match build {
            Some((true, target_time)) => {
                let t = (widget_text, msg_id, Some((target_time, None)));
                future::Either::Left(future::Either::Left(future::ok(t)))
            },
            Some((false, target_time)) => {
                let keyboard = telegram::TgInlineKeyboardMarkup { inline_keyboard: vec![make_cancel_row()] };
                let text = format!("{}\nдайте назву цьому спостереженню:", widget_text);

                let f = tg_update_widget(chat_id, msg_id, TgText::Markdown(text), Some(keyboard))
                    .and_then(move |()| {
                        futures::future::try_select(
                            UserInput::new(chat_id).map_ok(move |name| Some((target_time, Some(name)))),
                            UserClick::new(chat_id, msg_id).map_ok(move |_| None)
                        )
                            .map_ok(move |e| (widget_text, msg_id, e.factor_first().0))
                            .map_err(|e| e.factor_first().0)
                    });
                future::Either::Left(future::Either::Right(f))
            },
            None => future::Either::Right(future::ok((widget_text, msg_id, None))),
        })
        */
        .map_ok(move |(widget_text, msg_id, build)| {
            (widget_text, msg_id, build.map(|mode| (name, mode)))
        })
        .and_then(move |(widget_text, msg_id, build)| {
            if let Some((name, mode)) = build {

                let f = tg_update_widget(chat_id, msg_id, TgText::Markdown(format!("{}\nстан: *у роботі*", widget_text)), None)
                    .and_then(move |()| {
                        let sub = Sub {
                            chat_id, name,
                            location_message_id: loc_msg_id,
                            widget_message_id: msg_id,
                            latitude: target_lat,
                            longitude: target_lon,
                            mode,
                        };
                        monitor_weather_wrap(sub, tz).map_ok(move |status| (widget_text, msg_id, Some(status)) )
                    });
                future::Either::Left(f)
            } else {
                future::Either::Right(future::ok( (widget_text, msg_id, None) ))
            }
        })
        .and_then(move |(widget_text, msg_id, build)| {
            let status = match build {
                Some(times) => format!("виконано ({})", times),
                None => "скасовано".to_owned(),
            };
            tg_update_widget(chat_id, msg_id, TgText::Markdown(format!("{}\nстан: *{}*", widget_text, status)), None)
        })
        .or_else(move |e| {
            tg_send_widget(chat_id, TgText::Markdown("сталась помилка".to_owned()), None, None).then(move |_| {
            tg_send_widget(ANDRIY, TgText::Markdown(format!("error in {}:{}: `{}`", chat_id, loc_msg_id, e)), None, None)
                .map_ok(|_msg| ())
            })
        })
}

pub fn process_update(tgu: telegram::TgUpdate) -> Box<dyn Future<Output=Result<(), String>> + Send + Unpin> {
    process_bot(SeUpdate::from(tgu))
}

static ANDRIY: i64 = 54_462_285;

static USAGE_MSG: &str = "\u{1F4A1} цей бот вміє відстежувати і періодично надсилати\n\
оновлення прогнозу погоди для заданого місця та часу\n\n\
щоб почати, вкажіть місце одним із способів:\n\
\u{1F4CD} *точне розташування*\n\
`     `(меню \u{1F4CE} в мобільному додатку)\n\
\u{1F5FA} *по назві місця*\n\
`     `(ввести `@osmbot <назва>` і вибрати з списку)";
static WRONG_LOCATION_MSG: &str = "я можу відстежувати тільки європейську погоду";
static UNKNOWN_COMMAND_MSG: &str = "я розумію лише команду /start";
static CBQ_ERROR_MSG: &str = "помилка";
static PADDING_BTN_MSG: &str = "недоcтупна опція";

// TODO: what if user deletes chat or deletes widget message??
// ANSWER: in private chats user can only delete message for himself
//         deleting whole chat still allows bot to send messages
// NOTE: turns out it is possible to delete from mobile client
// TODO2:  what if user blocks bot?

// NOTE: allow only private chat communication for now
fn process_bot(upd: SeUpdate) -> Box<dyn Future<Output=Result<(), String>> + Send + Unpin> {
    match upd {
        SeUpdate::PrivateChat {chat_id, update, ..} => match update {
            SeUpdateVariant::Command(cmd) =>
                match cmd.as_str() {
                    "/start" => Box::new(tg_send_widget(chat_id, TgText::Markdown(USAGE_MSG.to_owned()), None, None).map_ok(|_| ())),
                    _ => Box::new(tg_send_widget(chat_id, TgText::Plain(UNKNOWN_COMMAND_MSG.to_owned()), None, None).map_ok(|_| ())),
                },
            SeUpdateVariant::Place {latitude, longitude, name, msg_id} =>
                if latitude > 29.5 && latitude < 70.5 && longitude > -23.5 && longitude < 45.0 {
                    Box::new(process_widget(chat_id, msg_id, latitude, longitude, name))
                } else {
                    Box::new(tg_send_widget(chat_id, TgText::Plain(WRONG_LOCATION_MSG.to_owned()), None, None).map_ok(|_| ()))
                }
            SeUpdateVariant::CBQ {id, msg_id, data} =>
                if data == PADDING_DATA { // TODO: this is wrong place to check this
                    Box::new(tg_answer_cbq(id, Some(PADDING_BTN_MSG.to_owned()))
                             .map_err(|e| format!("answer stub cbq error: {}", e.to_string())))
                } else {
                    // push choice into channel for (chat, msg_id) conversation
                    let ok = UserClick::click(chat_id, msg_id, data)
                        .map_err(|e| println!("cbq(click) {}:{} error: {}", chat_id, msg_id, e))
                        .is_ok();

                    Box::new(tg_answer_cbq(id, if ok { None } else { Some(CBQ_ERROR_MSG.to_owned()) })
                             .map_err(|e| format!("answer cbq error: {}", e.to_string())))
                },
            SeUpdateVariant::Text(_) => {
                /*
                let ok = UserInput::input(chat_id, text)
                    .map_err(|e| println!("text {} error: {}", chat_id, e))
                    .is_ok();
                */
                let ok = false;

                if ok {
                    Box::new(future::ok( () ))
                } else {
                    Box::new(tg_send_widget(chat_id, TgText::Markdown(USAGE_MSG.to_owned()), None, None).map_ok(|_m| () ))
                }
            },
        },            
        SeUpdate::Other(update) =>
            Box::new(tg_send_widget(ANDRIY, TgText::Plain(format!("unsupported update:\n{:#?}", update)), None, None).map_ok(|_| ())),
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

        let osm_info = osm_coords.and_then(|coords| osm_name.map(|name| (name, coords)));

        let first_command = upd.message.as_ref().and_then(|m| m.get_entities_of_type(TgMessageEntityType::BotCommand).next().map(|s| s.to_owned()));
        match (first_command, osm_info, upd) {
            (None, None, telegram::TgUpdate {
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

            (None, None, telegram::TgUpdate {
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

            (None, None, telegram::TgUpdate {
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

            (None, Some((name, (latitude, longitude))), telegram::TgUpdate {
                message: Some(telegram::TgMessage {
                    from: Some(user),
                    message_id: msg_id,
                    chat: telegram::TgChat {id: chat_id, type_: telegram::TgChatType::Private},
                    location: None,
                    ..
                }),
                callback_query: None,
            }) => SeUpdate::PrivateChat {
                chat_id, user, update: SeUpdateVariant::Place{latitude, longitude, name: Some(name), msg_id}
            },

            (Some(cmd), None, telegram::TgUpdate {
                message: Some(telegram::TgMessage {
                    from: Some(user),
                    chat: telegram::TgChat {id: chat_id, type_: telegram::TgChatType::Private},
                    entities: Some(_),
                    location: None,
                    ..
                }),
                callback_query: None,
            }) => SeUpdate::PrivateChat { chat_id, user, update: SeUpdateVariant::Command(cmd) },

            (_, _, u) => SeUpdate::Other(u),
        }
    }
}
