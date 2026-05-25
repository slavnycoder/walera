// Package walconn — named-type wrappers for PostgreSQL connections (and the
// DSN strings that build them) that cross provider boundaries in the
// composition root.
//
// Wire (Phase 5) identifies providers by Go type identity on every parameter
// and return value; without distinct named types here, two providers that
// both return *pgx.Conn — or both demand a `string` DSN — would collide
// (wire-gen: "multiple bindings for *pgx.Conn" / colliding string inputs).
// AdminConn wraps the non-replication admin connection used by health
// checks, lag sampling, and shutdown; ReplicationConn wraps the raw
// *pgconn.PgConn used by pglogrepl. AdminDSN and ReplicationDSN are the
// matching named-string types for the constructor *inputs* so the
// disambiguation guarantee covers both sides of every provider boundary.
//
// IMPORTANT — named pointer types do NOT inherit method sets. Per Go spec
// §"Method sets", a defined pointer type (`type AdminConn *pgx.Conn`) has
// an EMPTY method set; method promotion applies only to defined non-pointer
// types and to embedded fields. Calling `adminConn.QueryRow(...)` fails to
// compile with `adminConn.QueryRow undefined`. Every consumer therefore
// converts at the boundary:
//
//	func myHelper(adminConn walconn.AdminConn) {
//	    conn := (*pgx.Conn)(adminConn)
//	    conn.QueryRow(...)
//	}
//
// This mirrors the explicit-cast-at-boundary discipline applied to the
// named string types here and in internal/wal (wal.SlotName,
// wal.PublicationName, walconn.AdminDSN, walconn.ReplicationDSN — all
// underlying-kind string, cast via `string(value)` at every pgconn /
// pglogrepl / fmt site). The pointer cast and the string cast obey the same
// rule: the named type wins at provider boundaries, the underlying kind is
// taken explicitly at the consumption site.
//
// Constructors:
//   - NewAdminConn(ctx, dsn AdminDSN) — wraps pgx.Connect; returns AdminConn.
//   - NewReplicationConn(ctx, dsn ReplicationDSN) — wraps pgconn.Connect;
//     returns ReplicationConn. Exported for Phase 5's wire ProviderSet; the
//     internal/wal package's inline pgconn.Connect in reader.runOnce stays
//     in place to preserve reconnect semantics.
//
// The package has zero behavior beyond the constructors — no logging, no
// retries, no nil-checks. Those live in the consumers. PII-safety guarantee
// (spec §10.5 / CLAUDE.md): walconn cannot log DSN strings because it has
// no logger import.
package walconn

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// AdminConn is the named-type wrapper around the non-replication admin
// connection. Used by lag sampling, health checks, bootstrap helpers, and
// shutdown. Distinct from any other *pgx.Conn that a future provider may
// return so wire-gen can disambiguate by Go type identity.
type AdminConn *pgx.Conn

// ReplicationConn is the named-type wrapper around the raw replication
// connection consumed by pglogrepl. Distinct from any other *pgconn.PgConn
// for the same wire-disambiguation reason.
type ReplicationConn *pgconn.PgConn

// AdminDSN is the named-type wrapper around the non-replication admin
// connection string. The wire-disambiguation guarantee (Phase 5) covers
// constructor *inputs* as well as outputs: NewAdminConn and NewReplicationConn
// both take a DSN-shaped string, and wire-gen identifies providers by Go type
// identity on every parameter. Without distinct named DSN types, wire would
// see two providers that both demand `string` and refuse to build. Underlying
// kind is `string`; pgx.Connect / pgconn.ParseConfig accept the value via a
// one-call `string(dsn)` cast at the boundary. SECURITY: contains database
// credentials — never log; redact via internal/config.RedactDSN at log sites.
type AdminDSN string

// ReplicationDSN is the named-type wrapper around the replication connection
// string. See AdminDSN doc for the wire-disambiguation rationale; the two
// types are intentionally distinct so wire-gen cannot bind a ReplicationDSN
// value into an AdminDSN provider parameter (or vice versa) at the
// composition root. SECURITY: contains database credentials — never log;
// redact via internal/config.RedactDSN at log sites.
type ReplicationDSN string

// NewAdminConn wraps pgx.Connect and returns the typed AdminConn. The error
// is returned unwrapped so callers see the same error chain pgx.Connect
// produced. The dsn parameter is wal.AdminDSN (named string) so Phase 5's
// wire.Build can disambiguate the two DSN providers by Go type identity.
func NewAdminConn(ctx context.Context, dsn AdminDSN) (AdminConn, error) {
	conn, err := pgx.Connect(ctx, string(dsn))
	return AdminConn(conn), err
}

// NewReplicationConn wraps pgconn.Connect and returns the typed
// ReplicationConn. Exported for Phase 5's wire ProviderSet; internal/wal's
// reader.runOnce continues to call pgconn.Connect inline to preserve its
// reconnect / error-wrapping semantics verbatim. The dsn parameter is
// walconn.ReplicationDSN (named string) so wire.Build cannot collide with
// NewAdminConn on a shared `string` input.
func NewReplicationConn(ctx context.Context, dsn ReplicationDSN) (ReplicationConn, error) {
	conn, err := pgconn.Connect(ctx, string(dsn))
	return ReplicationConn(conn), err
}
