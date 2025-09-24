-- Progress chunk tracking table DDL (Phase 1 contract)
CREATE TABLE IF NOT EXISTS capture_insights.meta_chunk_progress (
  id bigserial PRIMARY KEY,
  pipeline_name text NOT NULL,
  window_start date NOT NULL,
  window_end date NOT NULL,
  chunk_index integer NOT NULL,
  job_id uuid NOT NULL,
  correlation_id uuid NOT NULL,
  status text NOT NULL CHECK (status IN ('pending','in_progress','success','failed')),
  rows_staged bigint,
  rows_deduped bigint,
  archive_path_rel text,
  archive_sha256 text,
  error_class text,
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(pipeline_name, window_start, window_end, chunk_index)
);
