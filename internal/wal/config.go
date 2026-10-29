package wal

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/walera/walera/internal/config"
	"github.com/walera/walera/internal/walconn"
)

var pgIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

type SlotName string

type PublicationName string

var pgQualifiedTableRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}\.[A-Za-z_][A-Za-z0-9_]{0,62}$`)

type Config struct {
	PostgresDSN walconn.AdminDSN

	ReplicationDSN walconn.ReplicationDSN

	PublicationName PublicationName

	// Tables (WALERA_TABLES) is the FOR TABLE list used to create a missing
	// publication and to cross-check an existing one.
	Tables []string

	SlotNamePrefix string

	SlotHeadroomMin int

	NaiveTimestampAssumeUTC bool

	Reconnect ReconnectConfig
}

type ReconnectConfig struct {
	ResetAfterSuccessDuration time.Duration
}

// Defaults are Walera's WAL settings. Only PublicationName and Tables are
// configurable; the DSNs come from WALERA_DATABASE_URL.
func Defaults() Config {
	return Config{
		PublicationName:         "walera_pub",
		SlotNamePrefix:          "walera",
		SlotHeadroomMin:         2,
		NaiveTimestampAssumeUTC: true,
		Reconnect:               ReconnectConfig{ResetAfterSuccessDuration: 60 * time.Second},
	}
}

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

func DeriveDSNs(base string) (walconn.AdminDSN, walconn.ReplicationDSN, error) {
	if base == "" {
		return "", "", errors.New("WALERA_DATABASE_URL is required")
	}
	adminURL, err := url.Parse(base)
	if err != nil {
		return "", "", config.FormatError(
			"WALERA_DATABASE_URL",
			config.RedactDSN(base),
			"DSN parse failed: invalid URL",
			"see docs/operations.md#configuration",
		)
	}
	adminQuery := adminURL.Query()
	for key := range adminQuery {
		if strings.EqualFold(key, "replication") {
			delete(adminQuery, key)
		}
	}
	adminURL.RawQuery = adminQuery.Encode()
	admin := adminURL.String()

	if _, err := pgconn.ParseConfig(base); err != nil {
		return "", "", config.FormatError(
			"WALERA_DATABASE_URL",
			config.RedactDSN(base),
			"DSN parse failed: "+err.Error(),
			"see docs/operations.md#configuration",
		)
	}
	if _, err := pgconn.ParseConfig(admin); err != nil {
		return "", "", config.FormatError(
			"WALERA_DATABASE_URL",
			config.RedactDSN(admin),
			"DSN parse failed: "+err.Error(),
			"see docs/operations.md#configuration",
		)
	}
	u := *adminURL
	q := u.Query()
	q.Set("replication", "database")
	u.RawQuery = q.Encode()
	return walconn.AdminDSN(admin), walconn.ReplicationDSN(u.String()), nil
}

func (c Config) Validate() error {
	var errs []error
	if c.PublicationName != "" && !pgIdentRe.MatchString(string(c.PublicationName)) {
		errs = append(errs, config.FormatError(
			"WALERA_PUBLICATION",
			"",
			"must match PG identifier regex [A-Za-z_][A-Za-z0-9_]{0,62}",
			"choose an unquoted PostgreSQL identifier",
		))
	}
	if c.SlotNamePrefix != "" && !pgIdentRe.MatchString(c.SlotNamePrefix) {
		errs = append(errs, fmt.Errorf("wal: slot name prefix %q is not a PG identifier", c.SlotNamePrefix))
	}
	for i, t := range c.Tables {
		if !pgQualifiedTableRe.MatchString(t) {
			errs = append(errs, config.FormatError(
				fmt.Sprintf("WALERA_TABLES entry %d", i+1),
				"",
				"must be schema-qualified `schema.table` with each segment matching "+pgIdentRe.String(),
				"qualify each table as `<schema>.<table>`",
			))
		}
	}
	if c.SlotNamePrefix != "" && c.SlotNamePrefix == string(c.PublicationName) {
		errs = append(errs, config.FormatError(
			"WALERA_PUBLICATION",
			"",
			fmt.Sprintf("must differ from the replication slot prefix %q", c.SlotNamePrefix),
			"choose another publication name",
		))
	}
	if c.Reconnect.ResetAfterSuccessDuration <= 0 {
		errs = append(errs, errors.New("wal: reconnect reset duration must be > 0"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %w", errors.Join(errs...))
	}
	return nil
}
