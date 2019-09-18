use telegram;
use super::{Future, Stream};
use super::TaggedLog;
use future;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

// TODO: cancelation button on updates does nothing
lazy_static! {
    pub static ref SUBS: Arc<Mutex<HashMap<(i64, i32), Sub>>> = Arc::default();
}

pub fn monitor_weather_wrap(sub: Sub) -> Box<dyn Future<Item=(usize, bool), Error=String> + Send> {
    let key = (sub.chat_id, sub.widget_message_id);
    {
        let mut hm = SUBS.lock().unwrap();
        if let Some(_prev) = hm.insert(key, sub.clone()) {
            println!("there was sub for {:?} already, override", key);
        }
    }

    let chat_id = sub.chat_id;
    let loc_msg_id = sub.location_message_id;
    let log = Arc::new(TaggedLog {tag: format!("{}:{}", chat_id, loc_msg_id)});
    let once = sub.name.is_none();
    let fts = data::forecast_stream(log.clone(), sub.latitude, sub.longitude, sub.target_time.0)
        .map(move |f| format::format_forecast(sub.name.as_ref().map(String::as_ref), &f))
        .inspect({
            let log = log.clone();
            move |format::ForecastText(upd)| log.add_line(&format!("{}: {}", if once {"single update"} else {"update"}, upd))
        });

    let f = if once {
        let f = fts
            .into_future()
            .map_err(|(e, _)| e)
            .and_then(|(f, _)| f.ok_or(format!("no current forecast")))
            .and_then(move |format::ForecastText(upd)| tg_send_widget(chat_id, upd, Some(loc_msg_id), None, true).map(|_msg| (1, false)));
        future::Either::A(f)
    } else {
        let f = fts
            .fold( (0, None, false), move |(n, last, _), format::ForecastText(upd)| {
                if let Some(last_msg_id) = last {
                    future::Either::A(tg_edit_kb(chat_id, last_msg_id, None))
                } else {
                    future::Either::B(future::ok( () ))
                }.and_then(move |()| {
                    let keyboard = telegram::TgInlineKeyboardMarkup { inline_keyboard: vec![make_cancel_row()] };

                    tg_send_widget(chat_id, upd, Some(loc_msg_id), Some(keyboard), true)
                    .map(move |msg_id| (n + 1, Some(msg_id), false))
                })
            })
            .and_then(move |(n, last, int)| {
                if let Some(last_msg_id) = last {
                    future::Either::A(tg_edit_kb(chat_id, last_msg_id, None))
                } else {
                    future::Either::B(future::ok( () ))
                }.and_then(move |()|
                    tg_send_widget(chat_id, format!("вичерпано ({} оновлень)", n), Some(loc_msg_id), None, false)
                        .map(move |_msg_id| (n, int))
                )
            });
        future::Either::B(f)
    }
        .inspect(move |(n, int)| log.add_line(&format!("done: {} updates, interrupted: {}", n, int)))
        .map_err(|e| format!("subscription future error: {}", e))
        .then(move |x| {SUBS.lock().unwrap().remove(&key); future::result(x)});
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

#[derive(Clone)]
struct Timestamp(time::Tm);

use serde::ser::{Serialize, Serializer};

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer
    {
        time::strftime("%FT%TZ", &self.0).unwrap().serialize(serializer)
    }
}

use serde::de::{Deserialize, Deserializer};

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Timestamp, D::Error>
    where D: Deserializer<'de>
    {
        <&str>::deserialize(deserializer).and_then(|s| {
            time::strptime(&s, "%FT%TZ")
                .map(|tm| Timestamp(tm))
                .map_err(|pe| serde::de::Error::custom(pe))
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Sub {
    chat_id: i64,
    location_message_id: i32,
    widget_message_id: i32,
    latitude: f32,
    longitude: f32,
    name: Option<String>, // name for repeated/none if once
    target_time: Timestamp,
}

fn tg_update_widget(
    chat_id: i64, message_id: i32, text: String, reply_markup: Option<telegram::TgInlineKeyboardMarkup>, md: bool
) -> impl Future<Item=(), Error=String> {

    telegram::tg_call("editMessageText", telegram::TgEditMsg {
        chat_id, text, message_id, reply_markup,
        parse_mode: if md { Some("Markdown".to_owned()) } else { None }
    }).map(|telegram::TgMessageUltraLite {..}| ())
}

pub fn tg_send_widget(
    chat_id: i64, text: String, reply_to_message_id: Option<i32>, reply_markup: Option<telegram::TgInlineKeyboardMarkup>, md: bool
) -> impl Future<Item=i32, Error=String> {

    telegram::tg_call("sendMessage", telegram::TgSendMsg {
        chat_id, text, reply_to_message_id, reply_markup,
        parse_mode: if md { Some("Markdown".to_owned()) } else { None }
    }).map(|m: telegram::TgMessageUltraLite| m.message_id)
}

pub fn tg_edit_kb(chat_id: i64, message_id: i32, reply_markup: Option<telegram::TgInlineKeyboardMarkup>) -> impl Future<Item=(), Error=String> {
    telegram::tg_call("editMessageReplyMarkup", telegram::TgEditMsgKb {chat_id, message_id, reply_markup})
        .map(|telegram::TgMessageUltraLite {..}| ())
}

fn tg_answer_cbq(id: String, notification: Option<String>) -> impl Future<Item=(), Error=String> {
    telegram::tg_call("answerCallbackQuery", telegram::TgAnswerCBQ{callback_query_id: id, text: notification})
        .and_then(|t| if t { Ok(()) } else { Err("should return true".to_owned()) })
}

lazy_static! {
    pub static ref USER_CLICKS: Arc<Mutex<HashMap<(i64, i32), futures::sync::oneshot::Sender<String>>>> = Arc::default();
    pub static ref USER_INPUTS: Arc<Mutex<HashMap<i64, futures::sync::oneshot::Sender<String>>>> = Arc::default();
}

const PADDING_DATA: &'static str = "na";
const CANCEL_DATA: &'static str = "xx";

pub fn make_cancel_row() -> Vec<telegram::TgInlineKeyboardButtonCB>{
    vec![telegram::TgInlineKeyboardButtonCB::new("скасувати".to_owned(), CANCEL_DATA.to_owned())]
}

struct UserClick {
    chat_id: i64,
    msg_id: i32,
    rx: Box<dyn Future<Item=String, Error=String> + Send>,
} 

impl UserClick {
    fn new(chat_id: i64, msg_id: i32) -> Self {
        let (tx, rx) = futures::sync::oneshot::channel::<String>();
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
        if let Some(_) = USER_CLICKS.lock().unwrap().remove(&(self.chat_id, self.msg_id)) {
            println!("remove unused user click {}:{}", self.chat_id, self.msg_id);
        }
    }
}

impl Future for UserClick {
    type Item = String;
    type Error = String;
    fn poll(&mut self) -> futures::Poll<String, String>{
        self.rx.poll()
    }
}

struct UserInput {
    chat_id: i64,
    rx: Box<dyn Future<Item=String, Error=String> + Send>,
}

impl UserInput {
    fn new(chat_id: i64) -> Self {
        let (tx, rx) = futures::sync::oneshot::channel::<String>();
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
        if let Some(_) = USER_INPUTS.lock().unwrap().remove(&self.chat_id) {
            println!("drop unused user input {}", self.chat_id);
        }
    }
}

impl Future for UserInput {
    type Item = String;
    type Error = String;
    fn poll(&mut self) -> futures::Poll<String, String>{
        self.rx.poll()
    }
}

use data;
use format;

fn process_widget(
    chat_id: i64, loc_msg_id: i32, target_lat: f32, target_lon: f32
) -> impl Future<Item=(), Error=String> {

    future::ok(format!("координати: *{}*", format::format_lat_lon(target_lat, target_lon)))
        .and_then(move |widget_text| {
            let mut days_map: HashMap<String, (i32, i32, i32, Vec<Vec<Option<i32>>>)> = HashMap::new();

            let v = data::time_picker(time::now_utc() - time::Duration::hours(1));
            let first = v.iter().take(3).map(|(y, m, d, ts)| {
                    let t = format!("{:02}.{:02}", d, m);
                    days_map.insert(t.clone(), (*y, *m, *d, ts.clone()));
                    telegram::TgInlineKeyboardButtonCB::new(t.clone(), t)
                }).collect();
            let second = v.iter().skip(3).take(3).map(|(y, m, d, ts)| {
                    let t = format!("{:02}.{:02}", d, m);
                    days_map.insert(t.clone(), (*y, *m, *d, ts.clone()));
                    telegram::TgInlineKeyboardButtonCB::new(t.clone(), t)
                }).collect();
            let inline_keyboard = vec![
                first,
                second,
                make_cancel_row(),
            ];

            let keyboard = telegram::TgInlineKeyboardMarkup{ inline_keyboard };

            tg_send_widget(chat_id, format!("{}\nвибери дату (utc):", widget_text), Some(loc_msg_id), Some(keyboard), true)
                .and_then(move |msg_id|
                    UserClick::new(chat_id, msg_id)
                        .map(move |data| (widget_text, msg_id, days_map.remove(&data)))
                )
        })
        .and_then(move |(mut widget_text, msg_id, build)| {
            if let Some((y, m, d, tss)) = build {
                let mut hours_map: HashMap<String, i32> = HashMap::new();

                let mut inline_keyboard: Vec<Vec<_>> = tss.iter().map(|r| {
                    let row: Vec<telegram::TgInlineKeyboardButtonCB> = r.iter().map(|c| {
                        match c {
                            Some(h) => {
                                let t = format!("{:02}", h);
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
                widget_text.push_str(&format!("\nдата: *{}-{:02}-{:02}*", y, m, d));

                let f = tg_update_widget(chat_id, msg_id, format!("{}\nвибери час (utc):", widget_text), keyboard, true)
                    .and_then(move |()|
                        UserClick::new(chat_id, msg_id)
                            .map(move |data| {
                                let build = hours_map.remove(&data).and_then(|h|
                                    time::strptime(&format!("{}-{:02}-{:02}T{:02}:00:00Z", y, m, d, h), "%FT%TZ").ok()
                                );
                                (widget_text, msg_id, build)
                            })
                    );
                future::Either::A(f)
            } else {
                future::Either::B(future::ok((widget_text, msg_id, None)))
            }
        })
        .and_then(move |(mut widget_text, msg_id, build)| {
            if let Some(target_time) = build {
                let inline_keyboard = vec![
                    vec![telegram::TgInlineKeyboardButtonCB::new("надсилати оновлення".to_owned(), "live".to_owned())],
                    vec![telegram::TgInlineKeyboardButtonCB::new("тільки поточний".to_owned(), "once".to_owned())],
                    make_cancel_row(),
                ];

                let keyboard = Some(telegram::TgInlineKeyboardMarkup{ inline_keyboard });
                widget_text.push_str(&format!("\nчас: *{:02}:00 (utc)*", target_time.tm_hour));
                let text = format!("{}\nякий прогноз цікавить:", widget_text);

                let f = tg_update_widget(chat_id, msg_id, text, keyboard, true).and_then(move |()| {
                    UserClick::new(chat_id, msg_id)
                        .map(move |data: String| {
                            match data.as_str() {
                                "live" => Some(false),
                                "once" => Some(true),
                                _ => None,
                            }
                            .map(|once| (once, target_time))
                        })
                        .map(move |build| (widget_text, msg_id, build))
                });
                future::Either::A(f)
            } else {
                future::Either::B(future::ok((widget_text, msg_id, None)))
            }
        })
        .and_then(move |(widget_text, msg_id, build)| match build {
            Some((true, target_time)) => {
                let t = (widget_text, msg_id, Some((target_time, None)));
                future::Either::A(future::Either::A(future::ok(t)))
            },
            Some((false, target_time)) => {
                let keyboard = telegram::TgInlineKeyboardMarkup { inline_keyboard: vec![make_cancel_row()] };
                let text = format!("{}\nдай назву цьому спостереженню:", widget_text);

                let f = tg_update_widget(chat_id, msg_id, text, Some(keyboard), true)
                    .and_then(move |()| Future::select(
                            UserInput::new(chat_id).map(move |name| Some((target_time, Some(name)))),
                            UserClick::new(chat_id, msg_id).map(move |_| None)
                        ).map(move |(x, _)| (widget_text, msg_id, x)).map_err(|(x, _)| x)
                    );
                future::Either::A(future::Either::B(f))
            },
            None => future::Either::B(future::ok((widget_text, msg_id, None))),
        })
        .and_then(move |(mut widget_text, msg_id, build)| {
            if let Some((target_time, name)) = build {
                if let Some(ref name) = name {
                    widget_text.push_str(&format!("\nназва: *{}*", name));
                }

                let f = tg_update_widget(chat_id, msg_id, format!("{}\nстан: *відслідковується*", widget_text), None, true)
                    .and_then(move |()| {
                        let sub = Sub {
                            chat_id, name,
                            location_message_id: loc_msg_id,
                            widget_message_id: msg_id,
                            latitude: target_lat,
                            longitude: target_lon,
                            target_time: Timestamp(target_time),
                        };
                        monitor_weather_wrap(sub).map(move |status| (widget_text, msg_id, Some(status)) )
                    });
                future::Either::A(f)
            } else {
                future::Either::B(future::ok( (widget_text, msg_id, None) ))
            }
        })
        .and_then(move |(widget_text, msg_id, build)| {
            let status = match build {
                Some( (times, false) ) => format!("виконано ({})", times),
                Some( (times, true) ) => format!("скасовано ({})", times),
                None => "скасовано".to_owned(),
            };
            tg_update_widget(chat_id, msg_id, format!("{}\nстан: *{}*", widget_text, status), None, true)
        })
        .or_else(move |e| {
            tg_send_widget(chat_id, format!("сталась помилка: `{}`", e), Some(loc_msg_id), None, true)
                .map(|_msg| ())
        })
}

pub fn process_update(tgu: telegram::TgUpdate) -> Box<dyn Future<Item=(), Error=String> + Send> {
    process_bot(SeUpdate::from(tgu))
}

static ANDRIY: i64 = 54462285;

static SEND_LOCATION_MSG: &'static str = "покажи локацію для якої потрібно відслідковувати погоду";
static WRONG_LOCATION_MSG: &'static str = "я можу відслідковувати тільки європейську погоду";
static UNKNOWN_COMMAND_MSG: &'static str = "я розумію лише команду /start";
static CBQ_ERROR_MSG: &'static str = "помилка";
static PADDING_BTN_MSG: &'static str = "недоcтупна опція";
static TEXT_ERROR_MSG: &'static str = "я не розумію";

// TODO: what if user deletes chat or deletes widget message??
// ANSWER: in private chats user can only delete message for himself
//         deleting whole chat still allows bot to send messages
// NOTE: turns out it is possible to delete from mobile client
// TODO2:  what if user blocks bot?

// NOTE: allow only private chat communication for now
fn process_bot(upd: SeUpdate) -> Box<dyn Future<Item=(), Error=String> + Send> {
    match upd {
        SeUpdate::PrivateChat {chat_id, user: _, update} => match update {
            SeUpdateVariant::Command(cmd) =>
                match cmd.as_str() {
                    "/start" => Box::new(tg_send_widget(chat_id, SEND_LOCATION_MSG.to_owned(), None, None, false).map(|_| ())),
                    _ => Box::new(tg_send_widget(chat_id, UNKNOWN_COMMAND_MSG.to_owned(), None, None, false).map(|_| ())),
                },
            SeUpdateVariant::Location {location, msg_id} =>
                if location.latitude > 29.5 && location.latitude < 70.5 && location.longitude > -23.5 && location.longitude < 45.0 {
                    Box::new(process_widget(chat_id, msg_id, location.latitude, location.longitude))
                } else {
                    Box::new(tg_send_widget(chat_id, WRONG_LOCATION_MSG.to_owned(), None, None, false).map(|_| ()))
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
            SeUpdateVariant::Text(text) => {
                let ok = UserInput::input(chat_id, text)
                    .map_err(|e| println!("text {} error: {}", chat_id, e))
                    .is_ok();

                if ok {
                    Box::new(future::ok( () ))
                } else {
                    Box::new(tg_send_widget(chat_id, TEXT_ERROR_MSG.to_owned(), None, None, false).map(|_m| () ))
                }
            },
        },            
        SeUpdate::Other(update) =>
            Box::new(tg_send_widget(ANDRIY, format!("unsupported update:\n{:#?}", update), None, None, false).map(|_| ())),
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
    Location {
        location: telegram::TgLocation,
        msg_id: i32,
    },
    Command(String),
    Text(String),
}

impl From<telegram::TgUpdate> for SeUpdate {
    fn from(upd: telegram::TgUpdate) -> Self {
        
        match (upd.message.as_ref().and_then(|m| m.first_bot_command()), upd) {
            (None, telegram::TgUpdate {
                message: Some(telegram::TgMessage {
                    from: Some(user),
                    chat: telegram::TgChat {id: chat_id, type_: telegram::TgChatType::Private},
                    message_id: msg_id,
                    text: None,
                    entities: None,
                    location: Some(location),
                }),
                callback_query: None,
            }) => SeUpdate::PrivateChat { chat_id, user, update: SeUpdateVariant::Location {location, msg_id} },

            (None, telegram::TgUpdate {
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

            (None, telegram::TgUpdate {
                message: Some(telegram::TgMessage {
                    from: Some(user),
                    chat: telegram::TgChat {id: chat_id, type_: telegram::TgChatType::Private},
                    message_id: _,
                    text: Some(text),
                    entities: None,
                    location: None,
                }),
                callback_query: None,
            }) => SeUpdate::PrivateChat { chat_id, user, update: SeUpdateVariant::Text(text) },

            (Some(cmd), telegram::TgUpdate {
                message: Some(telegram::TgMessage {
                    from: Some(user),
                    chat: telegram::TgChat {id: chat_id, type_: telegram::TgChatType::Private},
                    message_id: _,
                    text: _,
                    entities: Some(_),
                    location: None,
                }),
                callback_query: None,
            }) => SeUpdate::PrivateChat { chat_id, user, update: SeUpdateVariant::Command(cmd) },

            (_, u) => SeUpdate::Other(u),
        }
    }
}

#[test]
fn tg_location() {
    let t = r#"
    {
      "update_id": 14180656,
      "message": {
        "message_id": 2,
        "from": {
          "id": 54462285,
          "is_bot": false,
          "first_name": "Andriy",
          "last_name": "Chyvonomys",
          "username": "chyvonomys",
          "language_code": "en-UA"
        },
        "chat": {
          "id": 54462285,
          "first_name": "Andriy",
          "last_name": "Chyvonomys",
          "username": "chyvonomys",
          "type": "private"
        },
        "date": 1541198764,
        "location": {
          "latitude": 50.425195,
          "longitude": 25.703556
        }
      }
    }
"#;

    let u = serde_json::from_str::<telegram::TgUpdate>(t).map(|u| SeUpdate::from(u));

    if let Ok(SeUpdate::PrivateChat {update: SeUpdateVariant::Location {..}, .. }) = u {
    } else {
        panic!("{:#?}, should be SeUpdate::Location", u);
    }
}

#[test]
fn tg_start() {
    let t = r#"
    {
      "update_id": 14180655,
      "message": {
        "message_id": 1,
        "from": {
          "id": 54462285,
          "is_bot": false,
          "first_name": "Andriy",
          "last_name": "Chyvonomys",
          "username": "chyvonomys"
        },
        "chat": {
          "id": 54462285,
          "first_name": "Andriy",
          "last_name": "Chyvonomys",
          "username": "chyvonomys",
          "type": "private"
        },
        "date": 1541198334,
        "text": "/start",
        "entities": [
          {
            "offset": 0,
            "length": 6,
            "type": "bot_command"
          }
        ]
      }
    }
"#;

    let u = serde_json::from_str::<telegram::TgUpdate>(t).map(|u| SeUpdate::from(u));

    if let Ok(SeUpdate::PrivateChat {update: SeUpdateVariant::Command {..}, .. }) = u {
    } else {
        panic!("{:#?}, should be SeUpdate::Command", u);
    }
}

#[test]
fn tg_cbq() {
    let t = r#"
{
  "callback_query": {
    "chat_instance": "8882132206646987846",
    "data": "11.11",
    "from": {
      "first_name": "Andriy",
      "id": 54462285,
      "is_bot": false,
      "language_code": "en-UA",
      "last_name": "Chyvonomys",
      "username": "chyvonomys"
    },
    "id": "233913736986880302",
    "message": {
      "chat": {
        "first_name": "Andriy",
        "id": 54462285,
        "last_name": "Chyvonomys",
        "type": "private",
        "username": "chyvonomys"
      },
      "date": 1541890580,
      "from": {
        "first_name": "серпанок",
        "id": 701332998,
        "is_bot": true,
        "username": "serpanok_bot"
      },
      "message_id": 115,
      "reply_to_message": {
        "chat": {
          "first_name": "Andriy",
          "id": 54462285,
          "last_name": "Chyvonomys",
          "type": "private",
          "username": "chyvonomys"
        },
        "date": 1541890579,
        "forward_date": 1541795230,
        "forward_from": {
          "first_name": "Andriy",
          "id": 54462285,
          "is_bot": false,
          "language_code": "en-UA",
          "last_name": "Chyvonomys",
          "username": "chyvonomys"
        },
        "from": {
          "first_name": "Andriy",
          "id": 54462285,
          "is_bot": false,
          "language_code": "en-UA",
          "last_name": "Chyvonomys",
          "username": "chyvonomys"
        },
        "location": {
          "latitude": 50.610482,
          "longitude": 26.341016
        },
        "message_id": 113
      },
      "text": "location: 50.610°N 26.341°E, pick a date (utc):"
    }
  },
  "update_id": 14180731
}
"#;

    let u = serde_json::from_str::<telegram::TgUpdate>(t).map(|u| SeUpdate::from(u));

    if let Ok(SeUpdate::PrivateChat {update: SeUpdateVariant::CBQ {..}, .. }) = u {
    } else {
        panic!("{:#?}, should be SeUpdate::CBQ", u);
    }
}
