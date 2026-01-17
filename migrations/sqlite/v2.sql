-- Query results persistence table

CREATE TABLE results (
    id TEXT PRIMARY KEY,
    parquet_path TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_results_created_at ON results(created_at);
