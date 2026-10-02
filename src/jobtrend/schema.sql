-- job_platform_db 스키마 (CREATE ... IF NOT EXISTS / CREATE OR REPLACE — 몇 번 실행해도 안전, DROP 없음)
--   원본  : raw_listing_page(목록 요청 1건 = 1행), raw_posting_detail(상세 1건 = 1행)
--   정제  : job_posting(플랫폼 공고 1건 = 1행), posting_role(공고↔직무), posting_skill(기술·역량 키워드)
--   관측  : crawl_run(실행 로그), posting_snapshot(10분 관측), listing_metric(실행×원천×질의 재고·완전성)
--   통합  : job_canonical / posting_canonical(플랫폼 간 같은 공고 묶음 — 중복 게시 출처 표기)
--   품질  : quality_check
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS source_state (
  platform TEXT PRIMARY KEY,
  robots_status INTEGER,
  robots_allowed BOOLEAN NOT NULL DEFAULT false,
  checked_at TIMESTAMPTZ,
  reason TEXT,
  blocked_until TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS crawl_run (
  run_key      TEXT PRIMARY KEY,                     -- Airflow run_id | 'nb__…'(노트북)
  run_kind     TEXT NOT NULL CHECK (run_kind IN ('scheduled','manual','notebook')),
  slot_ts      TIMESTAMPTZ NOT NULL,                 -- scheduled: 틱 시각 / 그 외: 10분 내림
  code_version TEXT NOT NULL,
  plan         JSONB NOT NULL DEFAULT '{}',          -- (원천, 질의)별 scan_kind·사유
  started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at  TIMESTAMPTZ,
  status       TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running','success','partial','failed')),
  summary      JSONB NOT NULL DEFAULT '{}',
  note         TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_crawl_run_sched_slot ON crawl_run (slot_ts) WHERE run_kind = 'scheduled';

CREATE TABLE IF NOT EXISTS raw_listing_page (            -- [원본] 목록 요청 1건 = 1행(실패도 1행으로 남김)
  raw_id         BIGSERIAL PRIMARY KEY,
  run_key        TEXT NOT NULL REFERENCES crawl_run(run_key),
  platform       TEXT NOT NULL,
  query_key      TEXT NOT NULL,                      -- 'BE' | 'DA' | 'DE' | 'IT'(혼합 목록)
  job_group      TEXT NOT NULL,                      -- 'BE' | 'DA' | 'DE' | 'MIX'
  page_no        SMALLINT NOT NULL CHECK (page_no >= 1),
  scan_kind      TEXT NOT NULL CHECK (scan_kind IN ('light','full')),
  request_url    TEXT NOT NULL,                      -- 인증키 제거본
  outcome        TEXT NOT NULL CHECK (outcome IN ('ok','empty','parse_error','http_error','network_error',
                   'rate_limited','blocked','circuit_open','skipped_deadline','skipped_budget')),
  http_status    SMALLINT,
  attempts       SMALLINT NOT NULL DEFAULT 0,        -- 실제로 보낸 요청 수
  retry_trace    JSONB NOT NULL DEFAULT '[]',        -- [{attempt, status, wait_s}]
  waited_ms      INT NOT NULL DEFAULT 0,
  elapsed_ms     INT,
  fetched_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  n_items        SMALLINT,
  reported_total INT,                                -- 사이트가 표시한 총건수(수집 시점 1차 파싱)
  has_next       BOOLEAN,
  item_ids       TEXT[],                             -- 이 페이지의 공고 ID(포화 판정·기준선용)
  body_format    TEXT CHECK (body_format IN ('json','html','xml')),
  body           TEXT,                               -- 목록 영역 원문(scrub 후), 실패 시 NULL
  body_sha256    CHAR(64),
  error          TEXT,
  CONSTRAINT uq_raw_unit UNIQUE (run_key, platform, query_key, page_no)
);
CREATE INDEX IF NOT EXISTS ix_raw_platform_query ON raw_listing_page (platform, query_key, fetched_at);

CREATE TABLE IF NOT EXISTS raw_posting_detail (          -- [원본] 상세 1건 = 1행(신규 공고만 1회)
  platform     TEXT NOT NULL,
  posting_id   TEXT NOT NULL,
  run_key      TEXT REFERENCES crawl_run(run_key),
  request_url  TEXT NOT NULL,
  outcome      TEXT NOT NULL,
  http_status  SMALLINT,
  fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  body_format  TEXT,
  body         TEXT,                                 -- 필요한 필드만 남긴 JSON(담당자·본문 원문 제외)
  body_sha256  CHAR(64),
  error        TEXT,
  PRIMARY KEY (platform, posting_id)
);

CREATE TABLE IF NOT EXISTS job_posting (                 -- [정제 마스터] 자연 키 = 플랫폼이 발급한 공고 번호
  platform         TEXT NOT NULL,
  posting_id       TEXT NOT NULL,
  company_name     TEXT NOT NULL,
  company_key      TEXT NOT NULL,                    -- ㈜·(주)·주식회사·공백 등 제거(17번 normalize_company_name 확장)
  title            TEXT NOT NULL,
  title_key        TEXT NOT NULL,                    -- 플랫폼 간 같은 공고 판정용 제목 키
  url              TEXT NOT NULL,
  location_raw     TEXT,
  sido             TEXT,
  career_raw       TEXT,
  career_type      TEXT,                             -- new | exp | new_or_exp | any | unknown
  career_min_yr    SMALLINT,
  career_max_yr    SMALLINT,
  education        TEXT,
  employment_type  TEXT,
  posted_at        TIMESTAMPTZ,                      -- 첫 관측에서 고정(상대 표기는 구간 중앙값)
  posted_lo        TIMESTAMPTZ,
  posted_hi        TIMESTAMPTZ,
  posted_precision TEXT NOT NULL DEFAULT 'none' CHECK (posted_precision IN ('second','minute','hour','day','none')),
  apply_start_date DATE,
  deadline_at      TIMESTAMPTZ,
  deadline_raw     TEXT,
  deadline_kind    TEXT NOT NULL DEFAULT 'unknown' CHECK (deadline_kind IN ('date','always','until_filled','unknown')),
  tags_raw         TEXT[] NOT NULL DEFAULT '{}',
  extra            JSONB NOT NULL DEFAULT '{}',
  first_seen_at    TIMESTAMPTZ NOT NULL,
  last_seen_at     TIMESTAMPTZ NOT NULL,
  attrs_sha        CHAR(40) NOT NULL,
  last_raw_id      BIGINT REFERENCES raw_listing_page(raw_id),
  PRIMARY KEY (platform, posting_id)
);
CREATE INDEX IF NOT EXISTS ix_posting_company ON job_posting (company_key);
CREATE INDEX IF NOT EXISTS ix_posting_title_trgm ON job_posting USING gin (title_key gin_trgm_ops);

CREATE TABLE IF NOT EXISTS posting_role (                -- 공고 ↔ 직무(다대다): 요청한 코드(code) 또는 제목 분류(keyword)
  platform      TEXT NOT NULL,
  posting_id    TEXT NOT NULL,
  job_group     TEXT NOT NULL CHECK (job_group IN ('BE','DA','DE')),
  assign_method TEXT NOT NULL CHECK (assign_method IN ('code','keyword')),
  title_match   BOOLEAN,                             -- 제목 키워드 2차 분류와 일치하는가(직무 순도 점검용)
  first_seen_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (platform, posting_id, job_group),
  FOREIGN KEY (platform, posting_id) REFERENCES job_posting (platform, posting_id)
);

CREATE TABLE IF NOT EXISTS posting_snapshot (            -- [관측] 이 run에서 이 직무 목록에 이 공고가 보였다
  run_key      TEXT NOT NULL REFERENCES crawl_run(run_key),
  platform     TEXT NOT NULL,
  job_group    TEXT NOT NULL,
  posting_id   TEXT NOT NULL,
  query_key    TEXT NOT NULL,
  page_no      SMALLINT NOT NULL,
  rank_in_list SMALLINT NOT NULL,
  raw_id       BIGINT NOT NULL REFERENCES raw_listing_page(raw_id),
  observed_at  TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (run_key, platform, job_group, posting_id),
  FOREIGN KEY (platform, posting_id) REFERENCES job_posting (platform, posting_id)
);
CREATE INDEX IF NOT EXISTS ix_snap_posting ON posting_snapshot (platform, posting_id);

CREATE TABLE IF NOT EXISTS listing_metric (              -- run × 원천 × 질의 1행: 재고(사이트 총건수)·스캔 완전성
  run_key        TEXT NOT NULL REFERENCES crawl_run(run_key),
  platform       TEXT NOT NULL,
  query_key      TEXT NOT NULL,
  job_group      TEXT NOT NULL,
  scan_kind      TEXT NOT NULL,
  observed_at    TIMESTAMPTZ NOT NULL,
  reported_total INT,
  pages_ok       SMALLINT NOT NULL,
  pages_failed   SMALLINT NOT NULL,
  pages_needed   SMALLINT,
  is_complete    BOOLEAN NOT NULL,                   -- 마지막 페이지까지 실패 없이 받았는가
  items_parsed   INT NOT NULL,
  items_unique   INT NOT NULL,
  new_on_page1   SMALLINT,                           -- = 페이지 크기면 '포화'
  status         TEXT NOT NULL CHECK (status IN ('ok','partial','failed','skipped')),
  PRIMARY KEY (run_key, platform, query_key)
);

CREATE TABLE IF NOT EXISTS job_canonical (               -- [통합] 플랫폼을 넘어 같은 공고 1건 = 1행
  canonical_id BIGSERIAL PRIMARY KEY,
  company_key  TEXT NOT NULL,
  company_name TEXT NOT NULL,
  title        TEXT NOT NULL,
  title_key    TEXT NOT NULL,
  n_postings   INT NOT NULL,
  n_sources    INT NOT NULL,
  resolved_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS posting_canonical (
  platform     TEXT NOT NULL,
  posting_id   TEXT NOT NULL,
  canonical_id BIGINT NOT NULL REFERENCES job_canonical(canonical_id),
  match_method TEXT NOT NULL CHECK (match_method IN ('exact','trgm','singleton')),
  similarity   NUMERIC(4,3),
  PRIMARY KEY (platform, posting_id),
  FOREIGN KEY (platform, posting_id) REFERENCES job_posting (platform, posting_id)
);
CREATE INDEX IF NOT EXISTS ix_pc_canonical ON posting_canonical (canonical_id);

CREATE TABLE IF NOT EXISTS posting_skill (               -- 기술 스택·역량 키워드(공고당 키워드 1행)
  platform     TEXT NOT NULL,
  posting_id   TEXT NOT NULL,
  skill        TEXT NOT NULL,
  skill_group  TEXT NOT NULL,
  source_field TEXT NOT NULL CHECK (source_field IN ('tag','title','detail')),
  PRIMARY KEY (platform, posting_id, skill),
  FOREIGN KEY (platform, posting_id) REFERENCES job_posting (platform, posting_id)
);

CREATE TABLE IF NOT EXISTS quality_check (
  run_key    TEXT NOT NULL REFERENCES crawl_run(run_key),
  check_name TEXT NOT NULL,
  platform   TEXT NOT NULL DEFAULT '*',
  dimension  TEXT NOT NULL,
  metric     NUMERIC,
  threshold  NUMERIC,
  severity   TEXT NOT NULL CHECK (severity IN ('ok','warning','critical')),
  detail     JSONB NOT NULL DEFAULT '{}',
  checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_key, check_name, platform)
);

-- 분석 범위 뷰: EDA는 이 뷰만 씁니다(scheduled run, 2026-10-02 09:00~20:00 KST) → 20:00 이후 재실행해도 수치 고정
CREATE OR REPLACE VIEW v_run_scope AS
  SELECT run_key, slot_ts, slot_ts AT TIME ZONE 'Asia/Seoul' AS slot_kst, started_at, finished_at, status, code_version,
         EXTRACT(EPOCH FROM started_at - slot_ts) / 60 AS lag_min
  FROM crawl_run
  WHERE run_kind = 'scheduled' AND status IN ('success','partial')
    AND slot_ts BETWEEN TIMESTAMPTZ '2026-10-02 09:00+09' AND TIMESTAMPTZ '2026-10-02 20:00+09';
CREATE OR REPLACE VIEW v_listing_scope AS
  SELECT m.*, r.slot_ts, r.slot_kst, r.lag_min FROM listing_metric m JOIN v_run_scope r USING (run_key);
CREATE OR REPLACE VIEW v_snapshot_scope AS
  SELECT s.*, r.slot_ts, r.slot_kst, r.lag_min FROM posting_snapshot s JOIN v_run_scope r USING (run_key);

-- 모든 공고(누락 없이) + 중복 게시 출처: canonical 1건 = 1행
CREATE OR REPLACE VIEW v_job_all AS
  SELECT c.canonical_id, c.company_name, c.title, c.n_postings, c.n_sources,
         ARRAY_AGG(DISTINCT p.platform ORDER BY p.platform)                          AS sources,
         ARRAY(SELECT DISTINCT r.job_group FROM posting_role r JOIN posting_canonical x
                 ON x.platform = r.platform AND x.posting_id = r.posting_id
               WHERE x.canonical_id = c.canonical_id ORDER BY 1)                    AS job_groups,
         JSONB_AGG(JSONB_BUILD_OBJECT('platform', p.platform, 'posting_id', p.posting_id, 'url', p.url,
                   'posted_at', p.posted_at, 'deadline_at', p.deadline_at, 'deadline_kind', p.deadline_kind,
                   'first_seen_at', p.first_seen_at, 'last_seen_at', p.last_seen_at)
                   ORDER BY p.platform, p.posting_id)                                 AS source_links,
         MIN(p.posted_at) AS first_posted_at, MIN(p.first_seen_at) AS first_seen_at, MAX(p.last_seen_at) AS last_seen_at,
         ARRAY(SELECT DISTINCT sk.skill FROM posting_skill sk JOIN posting_canonical x
                 ON x.platform=sk.platform AND x.posting_id=sk.posting_id
               WHERE x.canonical_id=c.canonical_id ORDER BY 1) AS skills
  FROM job_canonical c
  JOIN posting_canonical pc ON pc.canonical_id = c.canonical_id
  JOIN job_posting p ON p.platform = pc.platform AND p.posting_id = pc.posting_id
  GROUP BY c.canonical_id;

-- 플랫폼 공고 1건 = 1행 + 같은 공고가 함께 올라간 다른 플랫폼
CREATE OR REPLACE VIEW v_posting_all AS
  SELECT p.platform, p.posting_id, p.company_name, p.title, p.url, pc.canonical_id, c.n_sources,
         ARRAY(SELECT DISTINCT o.platform FROM posting_canonical o
               WHERE o.canonical_id = pc.canonical_id AND o.platform <> p.platform ORDER BY 1) AS also_posted_on
  FROM job_posting p
  LEFT JOIN posting_canonical pc ON pc.platform = p.platform AND pc.posting_id = p.posting_id
  LEFT JOIN job_canonical c ON c.canonical_id = pc.canonical_id;

-- 원천 쌍별 공통 공고 수와 Jaccard(중복 게시 히트맵)
CREATE OR REPLACE VIEW v_platform_overlap AS
  WITH s AS (SELECT DISTINCT canonical_id, platform FROM posting_canonical),
       n AS (SELECT platform, COUNT(*) AS n FROM s GROUP BY platform)
  SELECT a.platform AS platform_a, b.platform AS platform_b, COUNT(*) AS shared,
         ROUND(COUNT(*)::numeric / NULLIF(na.n + nb.n - COUNT(*), 0), 4) AS jaccard
  FROM s a JOIN s b ON a.canonical_id = b.canonical_id AND a.platform < b.platform
  JOIN n na ON na.platform = a.platform JOIN n nb ON nb.platform = b.platform
  GROUP BY a.platform, b.platform, na.n, nb.n;

CREATE OR REPLACE VIEW v_posting_scope AS
  SELECT p.* FROM job_posting p WHERE EXISTS (
    SELECT 1 FROM v_snapshot_scope s WHERE s.platform=p.platform AND s.posting_id=p.posting_id);

CREATE OR REPLACE VIEW v_job_scope AS
  SELECT j.* FROM v_job_all j WHERE EXISTS (
    SELECT 1 FROM posting_canonical x JOIN v_posting_scope p USING(platform,posting_id)
    WHERE x.canonical_id=j.canonical_id);

CREATE OR REPLACE VIEW v_posting_status AS
  WITH latest AS (
    SELECT DISTINCT ON (platform,query_key) platform,query_key,run_key,slot_ts
    FROM v_listing_scope WHERE is_complete ORDER BY platform,query_key,slot_ts DESC),
  seen AS (SELECT DISTINCT platform,posting_id FROM latest JOIN posting_snapshot USING(run_key,platform,query_key)),
  full_platform AS (SELECT platform,max(slot_ts) last_full_at FROM latest GROUP BY platform)
  SELECT p.platform,p.posting_id,
    CASE WHEN s.posting_id IS NOT NULL THEN true
         WHEN f.last_full_at IS NULL OR p.first_seen_at>f.last_full_at THEN NULL
         ELSE false END AS is_open,
    f.last_full_at FROM v_posting_scope p LEFT JOIN seen s USING(platform,posting_id)
    LEFT JOIN full_platform f USING(platform);
