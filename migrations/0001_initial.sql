CREATE TABLE crawl_jobs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'CREATED',
    config_version INTEGER NOT NULL DEFAULT 1,
    config JSONB NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,

    CONSTRAINT crawl_jobs_state_check
        CHECK (state IN (
            'CREATED',
            'RUNNING',
            'PAUSED',
            'COMPLETED',
            'FAILED',
            'CANCELLED'
        ))
);

CREATE TABLE origins (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scheme TEXT NOT NULL CHECK (scheme IN ('http', 'https')),
    host TEXT NOT NULL,
    port INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
    origin_key TEXT NOT NULL UNIQUE,

    robots_body TEXT,
    robots_status INTEGER,
    robots_fetched_at TIMESTAMPTZ,
    robots_expires_at TIMESTAMPTZ,
    robots_etag TEXT,
    robots_last_modified TEXT,

    next_fetch_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_status INTEGER,
    adaptive_delay_ms BIGINT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (scheme, host, port)
);

CREATE TABLE urls (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES crawl_jobs(id) ON DELETE CASCADE,
    origin_id BIGINT NOT NULL REFERENCES origins(id),

    normalized_url TEXT NOT NULL,
    fetch_url TEXT NOT NULL,
    depth INTEGER NOT NULL CHECK (depth >= 0),
    priority INTEGER NOT NULL DEFAULT 0,

    discovery_source_url_id BIGINT REFERENCES urls(id),
    state TEXT NOT NULL DEFAULT 'PENDING',

    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT,
    lease_token UUID,
    lease_expires_at TIMESTAMPTZ,

    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    terminal_reason TEXT,

    discovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    leased_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT urls_state_check
        CHECK (state IN (
            'PENDING',
            'LEASED',
            'RETRY_WAIT',
            'COMPLETE',
            'TERMINAL_FAILED',
            'SKIPPED'
        )),

    UNIQUE (job_id, normalized_url)
);

CREATE INDEX urls_available_idx ON urls (
    job_id, 
    state, 
    available_at, 
    priority, 
    id
);

CREATE INDEX urls_lease_expiry_idx ON urls (
    lease_expires_at
)
WHERE lease_expires_at IS NOT NULL;

CREATE TABLE fetch_attempts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url_id BIGINT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    worker_id TEXT NOT NULL,

    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,

    requested_url TEXT NOT NULL,
    final_url TEXT,
    redirect_chain JSONB NOT NULL DEFAULT '[]'::jsonb,

    http_status INTEGER,
    content_type TEXT,
    encoded_bytes BIGINT,
    decoded_bytes BIGINT,

    etag TEXT,
    last_modified TEXT,
    body_hash TEXT,
    content_hash TEXT,

    duration_ms BIGINT,
    error_kind TEXT,
    error_detail TEXT,

    UNIQUE (url_id, attempt_number)
);

CREATE INDEX fetch_attempts_url_idx ON fetch_attempts (
    url_id, 
    attempt_number
);

CREATE TABLE pages (
    url_id BIGINT PRIMARY KEY REFERENCES urls(id) ON DELETE CASCADE,

    final_url TEXT,
    canonical_url TEXT,
    title TEXT,
    language TEXT,
    extracted_text TEXT,
    text_storage_ref TEXT,

    content_hash TEXT,
    parser_version TEXT NOT NULL,
    last_changed_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE links (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_url_id BIGINT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,

    target_normalized_url TEXT NOT NULL,
    raw_target TEXT NOT NULL,
    relation_flags TEXT[] NOT NULL DEFAULT '{}',
    anchor_text TEXT,

    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (source_url_id, target_normalized_url)
);

CREATE INDEX links_source_idx ON links (
    source_url_id
);