pub mod url;

use ::url::Url;
use std::time::Duration;

pub use url::{AdmissionError, AdmittedUrl, CrawlKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CrawlJobId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OriginId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FetchId(pub u64);

// added serde::Serialize, to serialize into a JSON.
// Duration format becomes something like {"secs": 10, "nanos": 0}
#[derive(Debug, Clone, serde::Serialize)]
pub struct CrawlConfig {
    pub seeds: Vec<Url>,
    pub allowed_hosts: Vec<String>,
    pub allowed_path_prefixes: Vec<String>,
    pub max_pages: u32,
    pub max_depth: u32,
    pub global_concurrency: usize,
    pub per_origin_delay: Duration,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_body_bytes: usize,
    pub user_agent: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UrlState {
    Pending,
    Fetching,
    Complete,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    OutOfScope,
    Duplicate,
    MaxDepthReached,
    MaxPagesReached,
    InvalidUrl,
    UnsupportedMediaType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchErrorKind {
    Network,
    Timeout,
    HttpError(u16),
    BodyTooLarge,
    InvalidRedirect,
    ParseError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryDecision {
    RetryNow,
    RetryAfter(Duration),
    DoNotRetry,
}

#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub url: Url,
    pub url_id: UrlId,
    pub origin_id: OriginId,
    pub depth: u32,
    pub attempt: u32,
}

#[derive(Debug, Clone)]
pub struct FetchResponseMetadata {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub duration: Duration,
    pub bytes_download: usize,
}

#[derive(Debug, Clone)]
pub struct ExtractedPage {
    pub url: Url,
    pub url_id: UrlId,
    pub status: u16,
    pub title: Option<String>,
    pub text: String,
    pub links: Vec<DiscoveredLink>,
}

#[derive(Debug, Clone)]
pub struct DiscoveredLink {
    pub source_url_id: UrlId,
    pub target_url: Url,
    pub anchor_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Origin {
    pub host: String,
}

impl Origin {
    pub fn from_url(url: &Url) -> Option<Self> {
        url.host_str().map(|h: &str| Self {
            host: h.trim_end_matches('.').to_ascii_lowercase(),
        })
    }
}

impl CrawlConfig {
    pub fn is_in_scope(&self, url: &Url) -> bool {
        if !matches!(url.scheme(), "http" | "https") {
            return false;
        }

        let Some(host) = url.host_str() else {
            return false;
        };

        let host: String = host.trim_end_matches('.').to_ascii_lowercase();
        let host_ok: bool = self
            .allowed_hosts
            .iter()
            .any(|h| h.trim_end_matches('.').eq_ignore_ascii_case(&host));
        let path_ok: bool = self.allowed_path_prefixes.is_empty()
            || self.allowed_path_prefixes.iter().any(|p| {
                url.path() == p
                    || (p == "/" && url.path().starts_with('/'))
                    || url.path().starts_with(&format!("{p}/"))
            });

        host_ok && path_ok
    }
}
