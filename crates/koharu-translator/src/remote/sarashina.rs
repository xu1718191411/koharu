// https://docs.sarashina.ai/api-reference/translate

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use koharu_secrets::ExposeSecret;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use super::send_json;
use crate::{Error, Language, Model, Provider, Result, TranslationRequest};

const URL: &str = "https://api.sarashina.ai/v1/translate";
// https://docs.sarashina.ai/rate-limits lists 6 RPM for sarashina3-translate.
const DEFAULT_REQUESTS_PER_MINUTE: u32 = 6;
const RATE_WINDOW: Duration = Duration::from_secs(60);

// Send times of recent requests. The quota belongs to the API key rather than to
// one translation call, so every call in the process shares this window.
static SENT: Mutex<VecDeque<Instant>> = Mutex::const_new(VecDeque::new());

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, specta::Type)]
#[serde(default)]
pub struct SarashinaConfig {
    pub requests_per_minute: u32,
}

impl Default for SarashinaConfig {
    fn default() -> Self {
        Self {
            requests_per_minute: DEFAULT_REQUESTS_PER_MINUTE,
        }
    }
}

pub(super) async fn models() -> Result<Vec<Model>> {
    Ok(if koharu_secrets::get("sarashina")?.is_some() {
        vec![Model::service(Provider::Sarashina, "Sarashina")]
    } else {
        Vec::new()
    })
}

pub(super) async fn translate(
    client: &Client,
    config: &SarashinaConfig,
    request: &TranslationRequest,
) -> Result<Vec<String>> {
    if config.requests_per_minute == 0 {
        return Err(anyhow!("sarashina requests per minute must be at least 1").into());
    }
    let api_key =
        koharu_secrets::get("sarashina")?.context("sarashina API key is not configured")?;
    let target = language(request.target_language).ok_or(Error::UnsupportedLanguage {
        provider: "sarashina",
        language: request.target_language,
    })?;
    // The API only translates between Japanese and English and requires an
    // explicit source, so an unspecified source is the other language of the pair.
    let source = match request.source_language {
        Some(language) => self::language(language).ok_or(Error::UnsupportedSourceLanguage {
            provider: "sarashina",
            language,
        })?,
        None if target == "ja" => "en",
        None => "ja",
    };
    // The endpoint accepts one text per request and has no context parameter.
    let mut translations = Vec::with_capacity(request.segments.len());
    for text in &request.segments {
        wait_for_quota(config.requests_per_minute).await;
        let response: Response = send_json(
            "sarashina",
            client
                .post(URL)
                .bearer_auth(api_key.expose_secret())
                .json(&Request {
                    text,
                    source_lang: source,
                    target_lang: target,
                }),
        )
        .await?;
        translations.push(response.translated_text);
    }
    Ok(translations)
}

// The API counts requests in fixed 60-second windows and rejects bursts past the
// quota, so no 60-second span may contain more than `limit` sends.
async fn wait_for_quota(limit: u32) {
    let limit = limit as usize;
    let mut sent = SENT.lock().await;
    loop {
        let now = Instant::now();
        while sent
            .front()
            .is_some_and(|time| now.duration_since(*time) >= RATE_WINDOW)
        {
            sent.pop_front();
        }
        if sent.len() < limit {
            sent.push_back(now);
            return;
        }
        let oldest = sent[sent.len() - limit];
        tokio::time::sleep_until((oldest + RATE_WINDOW).into()).await;
    }
}

#[derive(Serialize)]
struct Request<'a> {
    text: &'a str,
    source_lang: &'static str,
    target_lang: &'static str,
}

#[derive(Deserialize)]
struct Response {
    translated_text: String,
}

fn language(language: Language) -> Option<&'static str> {
    match language {
        Language::Japanese => Some("ja"),
        Language::English => Some("en"),
        _ => None,
    }
}
