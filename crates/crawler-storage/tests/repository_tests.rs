use chrono::{TimeZone, Utc};
use crawler_core::{CrawlJobId, OriginId};

use crawler_storage::{
    FrontierRepository, JobRepository, LinkInput, NewFetchAttempt, NewUrl, OriginInput,
    PgRepository, ResultRepository,
};

use serde_json::json;
use sqlx::PgPool;

#[sqlx::test(migrations = "../../migrations")]
async fn repository_should_create_job_and_duplicate_urls(pool: PgPool) {
    let repository: PgRepository = PgRepository::from_pool(pool);
    let job_id: CrawlJobId = repository
        .create_job("test job", &json!({"max_pages": 100}))
        .await
        .unwrap();

    let origin_id: OriginId = repository
        .ensure_origin(&OriginInput {
            scheme: "https".to_string(),
            host: "example.com".to_string(),
            port: 443,
            origin_key: "https://example.com:443".to_string(),
        })
        .await
        .unwrap();

    let url: NewUrl = NewUrl {
        job_id,
        origin_id,
        normalized_url: "https://example.com/".to_string(),
        fetch_url: "http://example.com/".to_string(),
        depth: 0,
        priority: 0,
        discovery_source_url_id: None,
    };

    let first: Option<crawler_core::UrlId> = repository.enqueue_url(&url).await.unwrap();
    let second: Option<crawler_core::UrlId> = repository.enqueue_url(&url).await.unwrap();

    assert!(first.is_some());
    assert!(second.is_none());
}

#[sqlx::test(migrations = "../../migrations")]
async fn repository_should_persist_fetch_attempt_timestamps(pool: PgPool) {
    let repository: PgRepository = PgRepository::from_pool(pool);
    let job_id: CrawlJobId = repository
        .create_job("timestamp test", &json!({}))
        .await
        .unwrap();

    let origin_id: OriginId = repository
        .ensure_origin(&OriginInput {
            scheme: "https".to_string(),
            host: "example.com".to_string(),
            port: 443,
            origin_key: "https://example.com:443".to_string(),
        })
        .await
        .unwrap();

    let url_id: crawler_core::UrlId = repository
        .enqueue_url(&NewUrl {
            job_id,
            origin_id,
            normalized_url: "https://example.com/".to_string(),
            fetch_url: "https://example.com/".to_string(),
            depth: 0,
            priority: 0,
            discovery_source_url_id: None,
        })
        .await
        .unwrap()
        .unwrap();

    let started_at = Utc
        .timestamp_opt(1_700_000_000, 123_000_000)
        .single()
        .unwrap();
    let finished_at = Utc
        .timestamp_opt(1_700_000_005, 456_000_000)
        .single()
        .unwrap();

    repository
        .record_fetch_attempt(&NewFetchAttempt {
            url_id,
            attempt_number: 1,
            worker_id: "worker-1".to_string(),
            started_at,
            finished_at: Some(finished_at),
            requested_url: "https://example.com/".to_string(),
            final_url: Some("https://example.com/".to_string()),
            redirect_chain: json!([]),
            http_status: Some(200),
            content_type: Some("text/html".to_string()),
            encoded_bytes: Some(128),
            decoded_bytes: Some(128),
            etag: None,
            last_modified: None,
            body_hash: None,
            content_hash: None,
            duration_ms: Some(5_000),
            error_kind: None,
            error_detail: None,
        })
        .await
        .unwrap();

    let stored: (chrono::DateTime<Utc>, Option<chrono::DateTime<Utc>>) =
        sqlx::query_as("SELECT started_at, finished_at FROM fetch_attempts WHERE url_id = $1")
            .bind(url_id.0 as i64)
            .fetch_one(repository.pool())
            .await
            .unwrap();

    assert_eq!(stored, (started_at, Some(finished_at)));
}

#[sqlx::test(migrations = "../../migrations")]
async fn repository_should_deduplicate_identical_links(pool: PgPool) {
    let repository: PgRepository = PgRepository::from_pool(pool);
    let job_id: CrawlJobId = repository
        .create_job("link test", &json!({}))
        .await
        .unwrap();

    let origin_id: OriginId = repository
        .ensure_origin(&OriginInput {
            scheme: "https".to_string(),
            host: "example.com".to_string(),
            port: 443,
            origin_key: "https://example.com:443".to_string(),
        })
        .await
        .unwrap();

    let source_id: crawler_core::UrlId = repository
        .enqueue_url(&NewUrl {
            job_id,
            origin_id,
            normalized_url: "https://example.com/".to_string(),
            fetch_url: "http://example.com/".to_string(),
            depth: 0,
            priority: 0,
            discovery_source_url_id: None,
        })
        .await
        .unwrap()
        .unwrap();

    let link: LinkInput = LinkInput {
        source_url_id: source_id,
        target_normalized_url: "https://example.com/about".to_string(),
        raw_target: "/about".to_string(),
        relation_flags: Vec::new(),
        anchor_text: Some("About".to_string()),
    };

    repository.insert_link(&link).await.unwrap();
    repository.insert_link(&link).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM links WHERE source_url_id = $1")
        .bind(source_id.0 as i64)
        .fetch_one(repository.pool())
        .await
        .unwrap();

    assert_eq!(count, 1);
}
