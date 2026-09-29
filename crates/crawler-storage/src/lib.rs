use std::convert::TryFrom;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crawler_core::{CrawlJobId, OriginId, UrlId};
use serde_json::Value;

use sqlx::{
    Row,
    postgres::{PgPool, PgPoolOptions},
};

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("databse id cannot be represented by the domain id: {0}")]
    InvalidId(i64),

    #[error("invalid databse state: {0}")]
    InvalidState(String),
}

fn crawl_job_id(value: i64) -> Result<CrawlJobId, StorageError> {
    u64::try_from(value)
        .map(CrawlJobId)
        .map_err(|_| StorageError::InvalidId(value))
}

fn origin_id(value: i64) -> Result<OriginId, StorageError> {
    u64::try_from(value)
        .map(OriginId)
        .map_err(|_| StorageError::InvalidId(value))
}

fn url_id(value: i64) -> Result<UrlId, StorageError> {
    u64::try_from(value)
        .map(UrlId)
        .map_err(|_| StorageError::InvalidId(value))
}

fn database_id(value: u64) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|_| StorageError::InvalidId(i64::MAX))
}

pub struct PgRepository {
    pool: PgPool,
}

impl PgRepository {
    pub async fn connect(database_url: &str) -> Result<Self, StorageError> {
        let pool: sqlx::Pool<sqlx::Postgres> = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;

        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self) -> Result<(), StorageError> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[derive(Debug, Clone)]
pub struct OriginInput {
    pub scheme: String,
    pub host: String,
    pub port: i32,
    pub origin_key: String,
}

#[derive(Debug, Clone)]
pub struct NewUrl {
    pub job_id: CrawlJobId,
    pub origin_id: OriginId,
    pub normalized_url: String,
    pub fetch_url: String,
    pub depth: i32,
    pub priority: i32,
    pub discovery_source_url_id: Option<UrlId>,
}

#[derive(Debug, Clone)]
pub struct NewFetchAttempt {
    pub url_id: UrlId,
    pub attempt_number: i32,
    pub worker_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub requested_url: String,
    pub final_url: Option<String>,
    pub redirect_chain: Value,
    pub http_status: Option<i32>,
    pub content_type: Option<String>,
    pub encoded_bytes: Option<i64>,
    pub decoded_bytes: Option<i64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub body_hash: Option<String>,
    pub content_hash: Option<String>,
    pub duration_ms: Option<i64>,
    pub error_kind: Option<String>,
    pub error_detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PageUpsert {
    pub url_id: UrlId,
    pub final_url: Option<String>,
    pub canonical_url: Option<String>,
    pub title: Option<String>,
    pub language: Option<String>,
    pub extracted_text: Option<String>,
    pub text_storage_ref: Option<String>,
    pub content_hash: Option<String>,
    pub parser_version: String,
}

#[derive(Debug, Clone)]
pub struct LinkInput {
    pub source_url_id: UrlId,
    pub target_normalized_url: String,
    pub raw_target: String,
    pub relation_flags: Vec<String>,
    pub anchor_text: Option<String>,
}

#[async_trait]
pub trait JobRepository {
    async fn create_job(&self, name: &str, config: &Value) -> Result<CrawlJobId, StorageError>;
}

#[async_trait]
pub trait FrontierRepository {
    async fn ensure_origin(&self, origin: &OriginInput) -> Result<OriginId, StorageError>;
    async fn enqueue_url(&self, url: &NewUrl) -> Result<Option<UrlId>, StorageError>;
}

#[async_trait]
pub trait ResultRepository {
    async fn record_fetch_attempt(&self, attempt: &NewFetchAttempt) -> Result<(), StorageError>;
    async fn upsert_page(&self, page: &PageUpsert) -> Result<(), StorageError>;
    async fn insert_link(&self, link: &LinkInput) -> Result<(), StorageError>;
}

#[async_trait]
impl JobRepository for PgRepository {
    async fn create_job(&self, name: &str, config: &Value) -> Result<CrawlJobId, StorageError> {
        let row: sqlx::postgres::PgRow = sqlx::query(
            r#"
            INSERT INTO crawl_jobs (name, config)
            VALUES ($1, $2)
            RETURNING id
            "#,
        )
        .bind(name)
        .bind(config)
        .fetch_one(&self.pool)
        .await?;

        crawl_job_id(row.try_get("id")?)
    }
}

#[async_trait]
impl FrontierRepository for PgRepository {
    async fn ensure_origin(&self, origin: &OriginInput) -> Result<OriginId, StorageError> {
        let row: sqlx::postgres::PgRow = sqlx::query(
            r#"
            INSERT INTO origins (scheme, host, port, origin_key)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (scheme, host, port) DO UPDATE
                SET updated_at = origins.updated_at
            RETURNING id
            "#,
        )
        .bind(&origin.scheme)
        .bind(&origin.host)
        .bind(origin.port)
        .bind(&origin.origin_key)
        .fetch_one(&self.pool)
        .await?;

        origin_id(row.try_get("id")?)
    }

    async fn enqueue_url(&self, url: &NewUrl) -> Result<Option<UrlId>, StorageError> {
        let job_id: i64 = database_id(url.job_id.0)?;
        let origin_id: i64 = database_id(url.origin_id.0)?;

        let source_id: Option<i64> = url
            .discovery_source_url_id
            .map(|value: UrlId| database_id(value.0))
            .transpose()?;

        let row: Option<sqlx::postgres::PgRow> = sqlx::query(
            r#"
            INSERT INTO urls (
                job_id,
                origin_id,
                normalized_url,
                fetch_url,
                depth,
                priority,
                discovery_source_url_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (job_id, normalized_url) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(job_id)
        .bind(origin_id)
        .bind(&url.normalized_url)
        .bind(&url.fetch_url)
        .bind(url.depth)
        .bind(url.priority)
        .bind(source_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row: sqlx::postgres::PgRow| url_id(row.try_get("id")?))
            .transpose()
    }
}

#[async_trait]
impl ResultRepository for PgRepository {
    async fn record_fetch_attempt(&self, attempt: &NewFetchAttempt) -> Result<(), StorageError> {
        let url_id: i64 = database_id(attempt.url_id.0)?;

        sqlx::query(
            r#"
            INSERT INTO fetch_attempts (
                url_id,
                attempt_number,
                worker_id,
                started_at,
                finished_at,
                requested_url,
                final_url,
                redirect_chain,
                http_status,
                content_type,
                encoded_bytes,
                decoded_bytes,
                etag,
                last_modified,
                body_hash,
                content_hash,
                duration_ms,
                error_kind,
                error_detail
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19
            )
            ON CONFLICT (url_id, attempt_number) DO NOTHING
            "#,
        )
        .bind(url_id)
        .bind(attempt.attempt_number)
        .bind(&attempt.worker_id)
        .bind(attempt.started_at)
        .bind(attempt.finished_at)
        .bind(&attempt.requested_url)
        .bind(&attempt.final_url)
        .bind(&attempt.redirect_chain)
        .bind(attempt.http_status)
        .bind(&attempt.content_type)
        .bind(attempt.encoded_bytes)
        .bind(attempt.decoded_bytes)
        .bind(&attempt.etag)
        .bind(&attempt.last_modified)
        .bind(&attempt.body_hash)
        .bind(&attempt.content_hash)
        .bind(attempt.duration_ms)
        .bind(&attempt.error_kind)
        .bind(&attempt.error_detail)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn upsert_page(&self, page: &PageUpsert) -> Result<(), StorageError> {
        let url_id: i64 = database_id(page.url_id.0)?;

        sqlx::query(
            r#"
            INSERT INTO pages (
                url_id,
                final_url,
                canonical_url,
                title,
                language,
                extracted_text,
                text_storage_ref,
                content_hash,
                parser_version,
                last_changed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
            ON CONFLICT (url_id) DO UPDATE SET
                final_url = EXCLUDED.final_url,
                canonical_url = EXCLUDED.canonical_url,
                title = EXCLUDED.title,
                language = EXCLUDED.language,
                extracted_text = EXCLUDED.extracted_text,
                text_storage_ref = EXCLUDED.text_storage_ref,
                content_hash = EXCLUDED.content_hash,
                parser_version = EXCLUDED.parser_version,
                last_changed_at = now(),
                updated_at = now()
            "#,
        )
        .bind(url_id)
        .bind(&page.final_url)
        .bind(&page.canonical_url)
        .bind(&page.title)
        .bind(&page.language)
        .bind(&page.extracted_text)
        .bind(&page.text_storage_ref)
        .bind(&page.content_hash)
        .bind(&page.parser_version)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn insert_link(&self, link: &LinkInput) -> Result<(), StorageError> {
        let source_url_id: i64 = database_id(link.source_url_id.0)?;

        sqlx::query(
            r#"
            INSERT INTO links (
                source_url_id,
                target_normalized_url,
                raw_target,
                relation_flags,
                anchor_text
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (source_url_id, target_normalized_url)
            DO UPDATE SET last_seen_at = now()
            "#,
        )
        .bind(source_url_id)
        .bind(&link.target_normalized_url)
        .bind(&link.raw_target)
        .bind(&link.relation_flags)
        .bind(&link.anchor_text)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
