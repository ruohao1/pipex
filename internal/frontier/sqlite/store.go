package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/ruohao1/pipex/internal/frontier"
)

type DurableStore[T any] struct {
	db     *sql.DB
	now    func() time.Time
	encode func(T) ([]byte, error)
	decode func([]byte) (T, error)

	schemaReady atomic.Bool
}

//go:embed schema.sql
var schemaDDL string

func NewDurableStore[T any](db *sql.DB) *DurableStore[T] {
	return &DurableStore[T]{
		db:     db,
		now:    time.Now,
		encode: func(v T) ([]byte, error) { return json.Marshal(v) },
		decode: func(b []byte) (T, error) {
			var v T
			err := json.Unmarshal(b, &v)
			return v, err
		},
	}
}

func (s *DurableStore[T]) InitSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, schemaDDL); err != nil {
		return fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	s.schemaReady.Store(true)
	return nil
}

func (s *DurableStore[T]) ensureSchema() error {
	if !s.schemaReady.Load() {
		return ErrSchemaNotInitialized
	}
	return nil
}

func (s *DurableStore[T]) Enqueue(ctx context.Context, e frontier.Entry[T]) (created bool, err error) {
	if err := s.ensureSchema(); err != nil {
		return false, err
	}
	if e.Key == "" {
		return false, fmt.Errorf("%w: empty key", ErrInvalidKey)
	}
	if e.Stage == "" {
		return false, fmt.Errorf("%w: empty stage", ErrInvalidStage)
	}
	if e.Hops < 0 {
		return false, fmt.Errorf("%w: negative hops", ErrInvalidHops)
	}
	if e.Attempt < 1 {
		return false, fmt.Errorf("%w: attempt must be >= 1", ErrInvalidAttempt)
	}

	inputBytes, err := s.encode(e.Input)
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrEncoding, err)
	}
	now := s.now().UnixMilli()
	res, err := s.db.ExecContext(ctx, `
	INSERT INTO frontier (
    key, stage, payload, hops, attempt, state,
    lease_id, lease_until, error, created_at, updated_at
  ) VALUES (?, ?, ?, ?, ?, 'pending', NULL, NULL, NULL, ?, ?)
  ON CONFLICT(key) DO NOTHING;
	`, e.Key, e.Stage, inputBytes, e.Hops, e.Attempt, now, now)
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDatabase, err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	if n == 0 {
		return false, nil
	}
	return true, nil
}

func (s *DurableStore[T]) Reserve(ctx context.Context, max int, leaseTTL time.Duration) ([]frontier.Entry[T], []frontier.Lease, error) {
	if err := s.ensureSchema(); err != nil {
		return nil, nil, err
	}
	if max <= 0 {
		return nil, nil, fmt.Errorf("%w: max must be >= 1", ErrInvalidBatchSize)
	}
	if leaseTTL <= 0 {
		return nil, nil, fmt.Errorf("%w: leaseTTL must be > 0", ErrInvalidLeaseTTL)
	}

	// write transaction to atomically select and update entries
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
	SELECT key, stage, payload, hops, attempt
	FROM frontier
	WHERE state = 'pending' AND (lease_until IS NULL OR lease_until <= ?)
	ORDER BY created_at ASC, key ASC
	LIMIT ?;
	`, s.now().UnixMilli(), max)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	defer rows.Close()

	entries := make([]frontier.Entry[T], 0, max)
	leases := make([]frontier.Lease, 0, max)

	for rows.Next() {
		var (
			key          string
			stage        string
			payloadBytes []byte
			hops         int
			attempt      int
		)
		err := rows.Scan(&key, &stage, &payloadBytes, &hops, &attempt)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", ErrDatabase, err)
		}

		leaseID := uuid.NewString()
		leaseUntil := s.now().Add(leaseTTL).UnixMilli()
		res, err := tx.ExecContext(ctx, `
	 UPDATE frontier
	 SET state = 'reserved', lease_id = ?, lease_until = ?, updated_at = ?
	 WHERE key=? AND state = 'pending';
	 `, leaseID, leaseUntil, s.now().UnixMilli(), key)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", ErrDatabase, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", ErrDatabase, err)
		}
		if n == 0 {
			continue // row changed by another transaction, skip
		}
		input, err := s.decode(payloadBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", ErrDecoding, err)
		}
		entries = append(entries, frontier.Entry[T]{
			Key:     key,
			Stage:   stage,
			Input:   input,
			Hops:    hops,
			Attempt: attempt,
		})
		leases = append(leases, frontier.Lease{
			Key:   key,
			ID:    leaseID,
			Until: time.UnixMilli(leaseUntil),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrDatabase, err)
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	if len(entries) == 0 {
		return []frontier.Entry[T]{}, []frontier.Lease{}, nil
	}
	return entries, leases, nil
}

func (s *DurableStore[T]) Ack(ctx context.Context, key, leaseID string) error {
	if err := s.ensureSchema(); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("%w: empty key", ErrInvalidKey)
	}
	if leaseID == "" {
		return fmt.Errorf("%w: empty leaseID", ErrInvalidLeaseID)
	}

	res, err := s.db.ExecContext(ctx, `
	UPDATE frontier
	SET state = 'acked', updated_at = ?, lease_id = NULL, lease_until = NULL
	WHERE key = ? AND lease_id = ? AND state = 'reserved';
	`, s.now().UnixMilli(), key, leaseID)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: no matching entry found for key=%s and leaseID=%s", ErrNotFound, key, leaseID)
	}
	return nil
}

func (s *DurableStore[T]) Retry(ctx context.Context, key, leaseID string, cause error, nextVisibleAt time.Time) error {
	if err := s.ensureSchema(); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("%w: empty key", ErrInvalidKey)
	}
	if leaseID == "" {
		return fmt.Errorf("%w: empty leaseID", ErrInvalidLeaseID)
	}

	errText := ""
	if cause != nil {
		errText = cause.Error()
	}

	// Requeue with delayed visibility window carried in lease_until.
	// Reserve query should only pick rows where lease_until <= now.
	res, err := s.db.ExecContext(ctx, `
  	UPDATE frontier
  	SET state = 'pending',
  	    attempt = attempt + 1,
  	    lease_id = NULL,
  	    lease_until = ?,   -- next visible time
  	    error = ?,
  	    updated_at = ?
  	WHERE key = ? AND lease_id = ? AND state = 'reserved';
  	`, nextVisibleAt.UnixMilli(), errText, s.now().UnixMilli(), key, leaseID)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: no matching entry found for key=%s and leaseID=%s", ErrNotFound, key, leaseID)
	}
	return nil
}

func (s *DurableStore[T]) MarkTerminalFailed(ctx context.Context, key, leaseID string, cause error) error {
	if err := s.ensureSchema(); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("%w: empty key", ErrInvalidKey)
	}
	if leaseID == "" {
		return fmt.Errorf("%w: empty leaseID", ErrInvalidLeaseID)
	}
	if cause == nil {
		return fmt.Errorf("%w: nil cause", ErrInvalidCause)
	}

	res, err := s.db.ExecContext(ctx, `
	UPDATE frontier
	SET state = 'terminal_failed', lease_id = NULL, lease_until = NULL, error = ?, updated_at = ?
	WHERE key = ? AND lease_id = ? AND state = 'reserved';
	`, cause.Error(), s.now().UnixMilli(), key, leaseID)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: no matching entry found for key=%s and leaseID=%s", ErrNotFound, key, leaseID)
	}
	return nil
}

func (s *DurableStore[T]) RequeueExpired(ctx context.Context, now time.Time, limit int) (int, error) {
	if err := s.ensureSchema(); err != nil {
		return 0, err
	}
	if limit <= 0 {
		return 0, fmt.Errorf("%w: limit must be >= 1", ErrInvalidLimit)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
  		SELECT key
  		FROM frontier
  		WHERE state = 'reserved' AND lease_until <= ?
  		ORDER BY lease_until ASC, key ASC
  		LIMIT ?;
  	`, now.UnixMilli(), limit)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	defer rows.Close()

	keys := make([]string, 0, limit)
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return 0, fmt.Errorf("%w: %w", ErrDatabase, err)
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	if len(keys) == 0 {
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("%w: %w", ErrDatabase, err)
		}
		return 0, nil
	}

	updated := 0
	for _, k := range keys {
		res, err := tx.ExecContext(ctx, `
  			UPDATE frontier
  			SET state = 'pending', lease_id = NULL, lease_until = NULL, updated_at = ?
  			WHERE key = ? AND state = 'reserved' AND lease_until <= ?;
  		`, s.now().UnixMilli(), k, now.UnixMilli())
		if err != nil {
			return 0, fmt.Errorf("%w: %w", ErrDatabase, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("%w: %w", ErrDatabase, err)
		}
		updated += int(n)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrDatabase, err)
	}
	return updated, nil
}
