-- Watermark table DDL (Phase 1 contract)
CREATE TABLE IF NOT EXISTS capture_insights.meta_refresh_watermarks (
  pipeline_name text PRIMARY KEY,
  last_modified_to timestamptz NOT NULL,
  overlap_days integer NOT NULL DEFAULT 7,
  updated_at timestamptz NOT NULL DEFAULT now()
);
