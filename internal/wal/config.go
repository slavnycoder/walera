// Package wal — config.go owns the typed configuration struct consumed by
// the Reader plus the per-package LoadConfig that knows how to unmarshal and
// validate the "wal." subtree of the root koanf instance.
//
// The package owns its config end-to-end so that no upward edge from
// `internal/wal` to a composition layer exists. The aggregate AppConfig in
// cmd/cdc-sse calls LoadConfig once at startup.
package wal

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/knadh/koanf/v2"

	"github.com/walera/walera/internal/config"
	"github.com/walera/walera/internal/walconn"
)

// pgIdentRe matches the PostgreSQL unquoted-identifier shape (NAMEDATALEN-1 = 63).
// Used to validate publication_name and slot_name_prefix before they reach
// pgoutput PluginArgs / CREATE_REPLICATION_SLOT DDL.
var pgIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// SlotName is the named type for a PostgreSQL replication slot name as
// constructed by Config.NewSlotName. It crosses provider boundaries (wal →
// composition root → lag sampler) so it must be a distinct Go type — wire
// ProviderSets in Phase 5 cannot disambiguate two providers that both return
// bare `string`. The underlying kind is `string`; runtime bytes that reach
// pglogrepl DDL are identical to those of an untyped string of the same
// content. Conversion to `string` at the pglogrepl call sites is explicit
// (`string(slotName)`) so the cast is locally visible.
type SlotName string

// PublicationName is the named type for the DBA-owned PostgreSQL publication
// name as it crosses provider boundaries (koanf → wal.Config → pglogrepl
// PluginArgs). Same rationale as SlotName: distinguishable Go identity for
// the future wire ProviderSet without altering the wire-format identifier
// bytes.
type PublicationName string

// pgQualifiedTableRe matches a schema-qualified PostgreSQL table identifier
// (`schema.table`). Used to validate wal.bootstrap.tables entries.
var pgQualifiedTableRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}\.[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// Config is the typed configuration consumed by wal.Reader. Field names map
// to koanf keys under the "wal." prefix.
type Config struct {
	// PostgresDSN is the non-replication admin connection string.
	// Mandatory. Used for startup checks; must NOT route through PgBouncer.
	// Typed as walconn.AdminDSN so Phase 5's wire.Build can disambiguate
	// the admin and replication DSN providers by Go type identity (DI-07).
	// Not koanf-populated from the wal subtree: the app composition layer
	// derives it from the top-level database.url via DeriveDSNs.
	PostgresDSN walconn.AdminDSN `koanf:"-"`

	// ReplicationDSN is the replication connection string. Mandatory.
	// Must include replication=database and connect directly to PostgreSQL.
	// SECURITY: contains database credentials — never log.
	// Typed as walconn.ReplicationDSN for the same wire-disambiguation
	// reason as PostgresDSN.
	// Not koanf-populated from the wal subtree: the app composition layer
	// derives it from the top-level database.url via DeriveDSNs.
	ReplicationDSN walconn.ReplicationDSN `koanf:"-"`

	// PublicationName is the DBA-owned publication. Mandatory.
	PublicationName PublicationName `koanf:"publication_name"`

	// SlotNamePrefix prefixes the temporary replication slot name.
	// Default: "walera".
	SlotNamePrefix string `koanf:"slot_name_prefix"`

	// SlotHeadroomMin is the minimum number of free replication slots before
	// a startup warning is emitted. Default: 2.
	SlotHeadroomMin int `koanf:"slot_headroom_min"`

	// NaiveTimestampAssumeUTC controls interpretation of TIMESTAMP WITHOUT
	// TIME ZONE. When true (default), naive timestamps are treated as UTC.
	NaiveTimestampAssumeUTC bool `koanf:"naive_timestamp_assume_utc"`

	// Reconnect controls the reconnect loop's backoff curve and reset
	// behaviour.
	Reconnect ReconnectConfig `koanf:"reconnect"`

	// LagSampleInterval is the cadence at which lag_sampler.go polls
	// pg_wal_lsn_diff against the admin connection. Default 5s.
	LagSampleInterval time.Duration `koanf:"lag_sample_interval"`

	// Bootstrap controls publication-bootstrap policy on Walera startup.
	Bootstrap BootstrapConfig `koanf:"bootstrap"`
}

// ReconnectConfig captures the reconnect loop knobs.
type ReconnectConfig struct {
	// ResetAfterSuccessDuration is the minimum elapsed time inside runOnce
	// that "earns" a reset of the attempt counter back to 0. Default 60s —
	// a 60s+ healthy run resets backoff so the next transient blip starts
	// at 1s again.
	ResetAfterSuccessDuration time.Duration `koanf:"reset_after_success_duration"`
}

// BootstrapConfig captures the publication-bootstrap policy. See
// LoadConfig for the allowed Mode values and validation rules.
type BootstrapConfig struct {
	// Mode is one of "auto" | "verify" | "off". Default "auto".
	Mode string `koanf:"mode"`

	// Tables is the explicit schema-qualified list of tables to include in
	// the publication when Mode=="auto" creates it. Empty list falls back
	// to `FOR ALL TABLES`.
	Tables []string `koanf:"tables"`

	// CreateRoles, when true, makes Mode=="auto" idempotently create the
	// `walera_repl` and `walera_admin` roles if they do not already exist.
	// Intended for dev/testbench convenience; in production prefer
	// DBA-owned roles created out-of-band.
	CreateRoles bool `koanf:"create_roles"`
}

// NewSlotName returns the temporary replication slot name for the given host
// and PID as a typed SlotName. Format:
// <slot_name_prefix>_<sanitized_hostname>_<pid>. PostgreSQL replication slot
// identifiers must match [a-z0-9_]+; hostname runes outside that set are
// rewritten to '_'. The returned value carries the wal.SlotName named type so
// it is distinguishable from a bare string at provider boundaries (DI-05).
func (w Config) NewSlotName(hostname string, pid int) SlotName {
	var b strings.Builder
	b.Grow(len(hostname))
	for _, r := range hostname {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		default:
			b.WriteByte('_')
		}
	}
	return SlotName(fmt.Sprintf("%s_%s_%d", w.SlotNamePrefix, b.String(), pid))
}

// ApplyDefaults registers the wal.* defaults on the supplied koanf instance.
// Invoked by cmd/cdc-sse before YAML/env loading so that the defaults survive
// when neither source supplies a value.
func ApplyDefaults(k *koanf.Koanf) {
	_ = k.Set("wal.publication_name", "walera_pub")
	_ = k.Set("wal.bootstrap.mode", "auto")
	_ = k.Set("wal.bootstrap.tables", []string{})
	_ = k.Set("wal.bootstrap.create_roles", false)
	_ = k.Set("wal.slot_name_prefix", "walera")
	_ = k.Set("wal.slot_headroom_min", 2)
	_ = k.Set("wal.naive_timestamp_assume_utc", true)
	_ = k.Set("wal.reconnect.reset_after_success_duration", "60s")
	_ = k.Set("wal.lag_sample_interval", "5s")
}

// LoadConfig unmarshals the "wal" subtree from k into a Config and runs all
// wal-specific validation. Returns a wrapped error listing every violation
// when validation fails.
func LoadConfig(k *koanf.Koanf) (Config, error) {
	var cfg Config
	if err := k.UnmarshalWithConf("wal", &cfg, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return Config{}, fmt.Errorf("wal config: unmarshal: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("wal config: %w", err)
	}
	return cfg, nil
}

// DeriveDSNs turns the single operator-supplied base DSN (top-level
// database.url / WALERA_DATABASE_URL) into the typed admin and replication
// connection strings. The base URL IS the admin DSN; the replication DSN is
// the same URL with the replication=database runtime parameter added to the
// query string (sslmode and any other params preserved). The base MUST NOT
// already carry replication=database — that would open the admin connection
// in replication mode and break ordinary queries. The PG role behind the DSN
// must hold the REPLICATION attribute; that is a runtime requirement enforced
// by PostgreSQL at START_REPLICATION, not here.
func DeriveDSNs(base string) (walconn.AdminDSN, walconn.ReplicationDSN, error) {
	if base == "" {
		return "", "", errors.New("database.url is required")
	}
	if _, err := pgconn.ParseConfig(base); err != nil {
		return "", "", config.FormatError(
			"database.url",
			config.RedactDSN(base),
			"DSN parse failed: "+err.Error(),
			"see docs/operations.md#configuration",
		)
	}
	// pgconn.ParseConfig silently strips the replication runtime parameter,
	// so guard against it with a substring check on the raw base value.
	if strings.Contains(base, "replication=database") {
		return "", "", config.FormatError(
			"database.url",
			config.RedactDSN(base),
			"must not contain replication=database (the replication DSN is derived automatically)",
			"remove replication=database from database.url",
		)
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", "", config.FormatError(
			"database.url",
			config.RedactDSN(base),
			"DSN parse failed: "+err.Error(),
			"see docs/operations.md#configuration",
		)
	}
	q := u.Query()
	q.Set("replication", "database")
	u.RawQuery = q.Encode()
	return walconn.AdminDSN(base), walconn.ReplicationDSN(u.String()), nil
}

// Validate enforces the wal-package invariants.
func (c Config) Validate() error {
	var errs []error
	if c.PublicationName != "" && !pgIdentRe.MatchString(string(c.PublicationName)) {
		errs = append(errs, config.FormatError(
			"wal.publication_name",
			string(c.PublicationName),
			"must match PG identifier regex [A-Za-z_][A-Za-z0-9_]{0,62}",
			"choose an unquoted PostgreSQL identifier",
		))
	}
	if c.SlotNamePrefix != "" && !pgIdentRe.MatchString(c.SlotNamePrefix) {
		errs = append(errs, config.FormatError(
			"wal.slot_name_prefix",
			c.SlotNamePrefix,
			"must match PG identifier regex [A-Za-z_][A-Za-z0-9_]{0,62}",
			"choose an unquoted PostgreSQL identifier",
		))
	}
	switch c.Bootstrap.Mode {
	case "auto", "verify", "off":
		// ok
	case "":
		errs = append(errs, errors.New("wal.bootstrap.mode is required (one of: auto, verify, off)"))
	default:
		errs = append(errs, config.FormatError(
			"wal.bootstrap.mode",
			c.Bootstrap.Mode,
			"must be one of: auto, verify, off",
			"see docs/operations.md#configuration",
		))
	}
	for i, t := range c.Bootstrap.Tables {
		if !pgQualifiedTableRe.MatchString(t) {
			errs = append(errs, config.FormatError(
				fmt.Sprintf("wal.bootstrap.tables[%d]", i),
				t,
				"must be schema-qualified `schema.table` with each segment matching "+pgIdentRe.String(),
				"qualify each table as `<schema>.<table>`",
			))
		}
	}
	// Combination layer (D-12 layer 3): slot_name_prefix MUST NOT equal
	// publication_name. Both end up as PG identifiers in DDL; reusing the
	// publication name as the slot prefix produces confusing pg_stat_*
	// rows and risks collision with any DBA-created slot named after the
	// publication.
	if c.SlotNamePrefix != "" && c.SlotNamePrefix == string(c.PublicationName) {
		errs = append(errs, config.FormatError(
			"wal.slot_name_prefix vs wal.publication_name",
			c.SlotNamePrefix,
			"slot_name_prefix must differ from publication_name",
			"choose a distinct prefix (default: walera)",
		))
	}
	if c.Reconnect.ResetAfterSuccessDuration <= 0 {
		errs = append(errs, errors.New("wal.reconnect.reset_after_success_duration must be > 0"))
	}
	if c.LagSampleInterval <= 0 {
		errs = append(errs, errors.New("wal.lag_sample_interval must be > 0"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %w", errors.Join(errs...))
	}
	return nil
}
