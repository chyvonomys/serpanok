use serde_derive::{Serialize, Deserialize};
use super::{http_get, http_post_json};
use futures::{future, Future, TryFutureExt};

lazy_static::lazy_static! {
    static ref BOTTOKEN: String = std::env::var("BOTTOKEN").expect("BOTTOKEN env");
}

#[derive(Debug)]
pub enum TgChatType {
    Private,
    Group,
    Supergroup,
    Channel,
}

impl<'de> serde::de::Deserialize<'de> for TgChatType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: serde::de::Deserializer<'de>
    {
        String::deserialize(deserializer)
            .and_then(|s| match s.as_str() {
                "private" => Ok(TgChatType::Private),
                "group" => Ok(TgChatType::Group),
                "supergroup" => Ok(TgChatType::Supergroup),
                "channel" => Ok(TgChatType::Channel),
                _ => Err(serde::de::Error::custom(format!("unsupported chat type string `{}`", s))),
            })
    }
}

#[derive(Deserialize, Debug)]
pub struct TgChat {
    pub id: i64,
    #[serde(rename = "type")] pub type_: TgChatType,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Debug)]
pub struct TgResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
}

impl<T> TgResponse<T> {
    pub fn into_result(self) -> Result<T, String> {
        if self.ok {
            self.result.ok_or_else(|| "result field is missing".to_owned())
        } else {
            Err(self.description.unwrap_or_else(|| "description field is missing".to_owned()))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Debug)]
pub struct TgUser {
    pub id: i32,
    pub first_name: String,
    pub last_name: Option<String>,
    pub username: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TgMessageEntityType {
    Mention,
    Hashtag,
    Cashtag,
    BotCommand,
    Url,
    Email,
    PhoneNumber,
    Bold,
    Italic,
    Code,
    Pre,
    TextLink,
    TextMention,
}

impl<'de> serde::de::Deserialize<'de> for TgMessageEntityType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: serde::de::Deserializer<'de>
    {
        String::deserialize(deserializer)
            .and_then(|s| match s.as_str() {
                "mention" => Ok(TgMessageEntityType::Mention),
                "hashtag" => Ok(TgMessageEntityType::Hashtag),
                "cashtag" => Ok(TgMessageEntityType::Cashtag),
                "bot_command" => Ok(TgMessageEntityType::BotCommand),
                "url" => Ok(TgMessageEntityType::Url),
                "email" => Ok(TgMessageEntityType::Email),
                "phone_number" => Ok(TgMessageEntityType::PhoneNumber),
                "bold" => Ok(TgMessageEntityType::Bold),
                "italic" => Ok(TgMessageEntityType::Italic),
                "code" => Ok(TgMessageEntityType::Code),
                "pre" => Ok(TgMessageEntityType::Pre),
                "text_link" => Ok(TgMessageEntityType::TextLink),
                "text_mention" => Ok(TgMessageEntityType::TextMention),
                _ => Err(serde::de::Error::custom(format!("unsupported message entity type string `{}`", s))),
            })
    }
}

#[derive(Deserialize, Debug)]
pub struct TgMessageEntity {
    #[serde(rename = "type")] type_: TgMessageEntityType,
    pub offset: usize,
    pub length: usize,
    url: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Debug)]
pub struct TgLocation {
    pub latitude: f32,
    pub longitude: f32,
}

#[derive(Deserialize, Debug)]
pub struct TgVenue {
    pub title: String,
}

#[derive(Deserialize, Debug)]
pub struct TgMessage {
    pub from: Option<TgUser>,
    pub chat: TgChat,
    pub message_id: i32,
    pub text: Option<String>,
    pub entities: Option<Vec<TgMessageEntity>>,
    pub location: Option<TgLocation>,
    pub venue: Option<TgVenue>,
}

impl TgMessage {

    // NOTE: entity offsets are in UTF-16 codepoints
    pub fn extract_entity(&self, e: &TgMessageEntity) -> Option<String> {
        self.text.as_ref().and_then(|t| {
            let v: Vec<u16> = t.encode_utf16().skip(e.offset).take(e.length).collect();
            String::from_utf16(&v).ok()
        })
    }
/*
    pub fn first_bot_command(&self) -> Option<String> {
        self.entities.as_ref().and_then(|es| {
            es
                .iter()
                .find(|e| e.type_ == TgMessageEntityType::BotCommand)
                .and_then(|e| self.extract_entity(e))
        })
    }
*/
    pub fn get_text_links<'m>(&'m self) -> impl Iterator<Item=(String, &'m str)> + 'm {
        self.entities.iter().map(move |es| 
            es.iter().filter_map(move |x| {
                /*x.type_ == TgMessageEntityType::TextLink*/
                x.url.as_ref().map(|s| s.as_str()).and_then(|url| self.extract_entity(x).map(|t| (t, url)))
            })
        ).flatten()
    }

    pub fn get_entities_of_type<'m>(&'m self, typ: TgMessageEntityType) -> impl Iterator<Item=String> + 'm {
        self.entities.iter().map(move |es| 
            es.iter().filter_map(move |x| {
                (if x.type_ == typ { Some(x) } else { None })
                    .and_then(|x| self.extract_entity(x))
            })
        ).flatten()
    }
}

#[derive(Deserialize, Debug)]
pub struct TgMessageLite {
    pub message_id: i32,
    pub chat: TgChat,
}

#[derive(Deserialize, Debug)]
pub struct TgMessageUltraLite {
    pub message_id: i32,
}

#[derive(Deserialize, Debug)]
pub struct TgCallbackQuery {
    pub id: String,
    pub from: TgUser,
    pub message: Option<TgMessageLite>,
    pub data: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct TgUpdate {
    pub message: Option<TgMessage>,
    pub callback_query: Option<TgCallbackQuery>,
}

#[derive(Serialize)]
pub struct TgSendMsg {
    pub chat_id: i64,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")] pub reply_to_message_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")] pub parse_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub reply_markup: Option<TgInlineKeyboardMarkup>,
    pub disable_web_page_preview: bool,
}

#[derive(Serialize)]
pub struct TgEditMsg {
    pub chat_id: i64,
    pub message_id: i32,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")] pub parse_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub reply_markup: Option<TgInlineKeyboardMarkup>,
    pub disable_web_page_preview: bool,
}

#[derive(Serialize)]
pub struct TgEditMsgKb {
    pub chat_id: i64,
    pub message_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")] pub reply_markup: Option<TgInlineKeyboardMarkup>,
}

#[derive(Serialize)]
pub struct TgAnswerCBQ {
    pub callback_query_id: String,
    #[serde(skip_serializing_if = "Option::is_none")] pub text: Option<String>, // notification text
}

#[derive(Serialize, Debug)]
pub struct TgInlineKeyboardMarkup {
    pub inline_keyboard: Vec<Vec<TgInlineKeyboardButtonCB>>,
}

#[derive(Serialize, Debug)]
pub struct TgInlineKeyboardButtonCB {
    text: String,
    callback_data: String,
}

impl TgInlineKeyboardButtonCB {
    pub fn new(text: String, callback_data: String) -> Self {
        Self {text, callback_data}
    }
}

pub fn get_updates(last: Option<i32>) -> impl Future<Output=Result<Vec<(Option<i32>, String)>, String>> {
    let mut url = format!("https://api.telegram.org/bot{}/getUpdates", BOTTOKEN.as_str());
    if let Some(x) = last {
        url.push_str("?offset=");
        url.push_str(&(x+1).to_string());
    }

    http_get(url).and_then(|(ok, body)| future::ready(
        if ok {
            serde_json::from_slice::<TgResponse< Vec<serde_json::Value>> >(&body)
                .map_err(|e| format!("parse updates error: {}", e.to_string()))
                .and_then(|resp| resp.into_result())
                .map(|v| v.iter().map(|i| (
                    i.get("update_id").and_then(|n| n.as_i64().map(|x| x as i32)),
                    i.to_string()
                )).collect())
        } else {
            Err(format!("ok: {:?}, body: {:?}", ok, String::from_utf8(body)))
        }
    ))
}

pub fn tg_call<S, R>(call: &'static str, payload: S) -> impl Future<Output=Result<R, String>>
where S: serde::Serialize, R: serde::de::DeserializeOwned {
    let url = format!("https://api.telegram.org/bot{}/{}", BOTTOKEN.as_str(), call);
    let json = serde_json::to_string(&payload).unwrap();
    http_post_json(url, json).and_then(|(ok, body)| future::ready(
        if ok {
            serde_json::from_reader::<_, TgResponse<R>>(std::io::Cursor::new(body))
                .map_err(|e| format!("parse response error: {}", e.to_string()))
                .and_then(|resp| resp.into_result())
        } else {
            Err(format!("ok: {:?}, body: {:?}", ok, String::from_utf8(body)))
        }
    ))
}
