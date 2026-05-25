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

var pgIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

type SlotName string

type PublicationName string

var pgQualifiedTableRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}\.[A-Za-z_][A-Za-z0-9_]{0,62}$`)

type Config struct {
	PostgresDSN walconn.AdminDSN `koanf:"-"`

	ReplicationDSN walconn.ReplicationDSN `koanf:"-"`

	PublicationName PublicationName `koanf:"publication_name"`

	SlotNamePrefix string `koanf:"slot_name_prefix"`

	SlotHeadroomMin int `koanf:"slot_headroom_min"`

	NaiveTimestampAssumeUTC bool `koanf:"naive_timestamp_assume_utc"`

	Reconnect ReconnectConfig `koanf:"reconnect"`

	LagSampleInterval time.Duration `koanf:"lag_sample_interval"`

	Bootstrap BootstrapConfig `koanf:"bootstrap"`
}

type ReconnectConfig struct {
	ResetAfterSuccessDuration time.Duration `koanf:"reset_after_success_duration"`
}

type BootstrapConfig struct {
	Mode string `koanf:"mode"`

	Tables []string `koanf:"tables"`

	CreateRoles bool `koanf:"create_roles"`
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

func DeriveDSNs(base string) (walconn.AdminDSN, walconn.ReplicationDSN, error) {
	if base == "" {
		return "", "", errors.New("database.url is required")
	}
	adminURL, err := url.Parse(base)
	if err != nil {
		return "", "", config.FormatError(
			"database.url",
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
			"database.url",
			config.RedactDSN(base),
			"DSN parse failed: "+err.Error(),
			"see docs/operations.md#configuration",
		)
	}
	if _, err := pgconn.ParseConfig(admin); err != nil {
		return "", "", config.FormatError(
			"database.url",
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
