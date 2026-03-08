CREATE TABLE IF NOT EXISTS frontier(
  key TEXT PRIMARY KEY,
  stage TEXT NOT NULL,
  payload BLOB NOT NULL,
  state TEXT NOT NULL CHECK(state IN(
    'pending',
    'reserved',
    'acked',
    'terminal_failed',
    'dropped',
    'canceled'
  )),
  lease_id TEXT,
  lease_until INTEGER,
  error TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  attempt INTEGER NOT NULL CHECK(attempt >= 1),
  hops INTEGER NOT NULL CHECK(hops >= 0)
);
CREATE INDEX IF NOT EXISTS idx_frontier_state_lease_until
ON frontier(
  state,
  lease_until
);
