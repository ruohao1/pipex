package sqlite

import "errors"

var (
	ErrNotFound       = errors.New("not found")
	ErrConflict       = errors.New("conflict")
	ErrInvalidKey     = errors.New("invalid key")
	ErrInvalidHops    = errors.New("invalid hops")
	ErrInvalidAttempt = errors.New("invalid attempt")
	ErrInvalidStage   = errors.New("invalid stage")
	ErrInvalidLease   = errors.New("invalid lease")
	ErrEncoding       = errors.New("encoding error")
	ErrDecoding       = errors.New("decoding error")
	ErrInvalidCause   = errors.New("invalid cause")

	ErrDatabase = errors.New("database error")

	ErrInvalidBatchSize     = errors.New("invalid batch size")
	ErrInvalidLeaseTTL      = errors.New("invalid lease TTL")
	ErrInvalidLeaseID       = errors.New("invalid lease ID")
	ErrInvalidLimit         = errors.New("invalid limit")
	ErrSchemaNotInitialized = errors.New("schema not initialized")
)
