package app

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/walconn"
)

var pgRoleNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

type bootstrapConfig struct {
	Mode            string
	PublicationName string
	Tables          []string
	CreateRoles     bool
	ReplicationDSN  string
	PostgresDSN     string
}

func verifyPGPrereqs(ctx context.Context, adminConn walconn.AdminConn, logger zerolog.Logger) error {

	conn := (*pgx.Conn)(adminConn)
	readPGSetting := func(name string) (string, error) {
		var setting string
		if err := conn.QueryRow(ctx,
			"SELECT setting FROM pg_settings WHERE name = $1",
			name,
		).Scan(&setting); err != nil {
			return "", fmt.Errorf("failed to query pg_settings for prereq verification (%s): %w", name, err)
		}
		return setting, nil
	}
	failPGSetting := func(name, setting, required string) error {
		return fmt.Errorf(
			"PostgreSQL setting %q does not meet Walera's prerequisites (actual=%q required=%q) — edit postgresql.conf and restart the server",
			name, setting, required,
		)
	}
	verifyAtLeastOne := func(name string) error {
		setting, err := readPGSetting(name)
		if err != nil {
			return err
		}

		n, err := strconv.Atoi(setting)
		if err != nil || n < 1 {
			return failPGSetting(name, setting, ">= 1")
		}
		logger.Info().Str("setting", name).Str("value", setting).Msg("postgres prereq verified")
		return nil
	}
	setting, err := readPGSetting("wal_level")
	if err != nil {
		return err
	}
	if setting != "logical" {
		return failPGSetting("wal_level", setting, "logical")
	}
	logger.Info().Str("setting", "wal_level").Str("value", setting).Msg("postgres prereq verified")
	if err := verifyAtLeastOne("max_replication_slots"); err != nil {
		return err
	}
	if err := verifyAtLeastOne("max_wal_senders"); err != nil {
		return err
	}
	return nil
}

func verifyReplicationRole(ctx context.Context, adminConn walconn.AdminConn, logger zerolog.Logger) error {

	conn := (*pgx.Conn)(adminConn)
	var (
		rolname        string
		rolreplication bool
		rolsuper       bool
	)
	if err := conn.QueryRow(ctx,
		"SELECT rolname, rolreplication, rolsuper FROM pg_roles WHERE rolname = current_user",
	).Scan(&rolname, &rolreplication, &rolsuper); err != nil {
		return fmt.Errorf("failed to verify role replication attribute: %w", err)
	}
	if !rolreplication && !rolsuper {
		return fmt.Errorf(
			"PostgreSQL role %q lacks the REPLICATION attribute, which is a required prerequisite "+
				"for logical replication — grant it with `ALTER ROLE %s REPLICATION;` (or connect as a "+
				"superuser); see docs/operations.md#required-runtime",
			rolname, rolname,
		)
	}
	logger.Info().
		Str("role", rolname).
		Bool("replication", rolreplication).
		Bool("superuser", rolsuper).
		Msg("postgres prereq verified")
	return nil
}

func bootstrapPublication(ctx context.Context, adminConn walconn.AdminConn, cfg bootstrapConfig, logger zerolog.Logger) error {
	conn := (*pgx.Conn)(adminConn)
	switch cfg.Mode {
	case "off":
		logger.Info().
			Str("publication", cfg.PublicationName).
			Msg("publication check skipped (bootstrap.mode=off)")
	case "verify":
		var tableCount int
		err := conn.QueryRow(ctx,
			"SELECT count(*) FROM pg_publication_tables WHERE pubname = $1",
			cfg.PublicationName,
		).Scan(&tableCount)
		if err != nil {
			return fmt.Errorf("publication existence check failed (%s): %w", cfg.PublicationName, err)
		}
		if tableCount == 0 {
			return fmt.Errorf("publication %q exists but has no tables — create publication and add tables", cfg.PublicationName)
		}
		logger.Info().
			Str("publication", cfg.PublicationName).
			Int("table_count", tableCount).
			Msg("publication check passed")
	case "auto":

		if cfg.CreateRoles {
			bootstrapEnsureRole(ctx, walconn.AdminConn(conn), cfg.ReplicationDSN, true, logger)
			bootstrapEnsureRole(ctx, walconn.AdminConn(conn), cfg.PostgresDSN, false, logger)
		}

		var exists bool
		err := conn.QueryRow(ctx,
			"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
			cfg.PublicationName,
		).Scan(&exists)
		if err != nil {
			return fmt.Errorf("publication existence check failed (%s): %w", cfg.PublicationName, err)
		}
		if exists {

			var tableCount int
			if err := conn.QueryRow(ctx,
				"SELECT count(*) FROM pg_publication_tables WHERE pubname = $1",
				cfg.PublicationName,
			).Scan(&tableCount); err != nil {
				return fmt.Errorf("publication existence check failed (%s): %w", cfg.PublicationName, err)
			}
			if tableCount == 0 {
				logger.Warn().
					Str("publication", cfg.PublicationName).
					Msg("publication exists with zero tables; nothing to stream yet — clients will see no events until tables are added")
			} else {
				logger.Info().
					Str("publication", cfg.PublicationName).
					Int("table_count", tableCount).
					Msg("publication check passed")
			}
			if len(cfg.Tables) > 0 {
				bootstrapVerifyTables(ctx, walconn.AdminConn(conn), cfg.PublicationName, cfg.Tables, logger)
			}
		} else {

			var ddl, mode string
			if len(cfg.Tables) > 0 {
				ddl = fmt.Sprintf(
					"CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert, update, delete')",
					cfg.PublicationName,
					strings.Join(cfg.Tables, ", "),
				)
				mode = "FOR TABLE"
			} else {
				ddl = fmt.Sprintf(
					"CREATE PUBLICATION %s FOR ALL TABLES WITH (publish = 'insert, update, delete')",
					cfg.PublicationName,
				)
				mode = "FOR ALL TABLES"
			}
			if _, err := conn.Exec(ctx, ddl); err != nil {
				return fmt.Errorf("failed to create publication %q for bootstrap: %w", cfg.PublicationName, err)
			}
			logger.Info().
				Str("publication", cfg.PublicationName).
				Str("mode", mode).
				Int("table_count", len(cfg.Tables)).
				Msg("publication created (bootstrap.mode=auto)")
		}
	default:

		panic(fmt.Sprintf("unreachable: invalid bootstrap.mode %q passed validation", cfg.Mode))
	}
	return nil
}

func checkSlotHeadroom(ctx context.Context, adminConn walconn.AdminConn, headroomMin int, slotName string, logger zerolog.Logger) {
	conn := (*pgx.Conn)(adminConn)
	var maxSlots, usedSlots int
	if err := conn.QueryRow(ctx,
		"SELECT setting::int FROM pg_settings WHERE name = 'max_replication_slots'",
	).Scan(&maxSlots); err != nil {
		logger.Warn().Err(err).Msg("could not query max_replication_slots; skipping headroom check")
		return
	}
	if err := conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_replication_slots",
	).Scan(&usedSlots); err != nil {
		logger.Warn().Err(err).Msg("could not query pg_replication_slots; skipping headroom check")
		return
	}
	freeSlots := maxSlots - usedSlots
	if freeSlots < headroomMin {
		logger.Warn().
			Str("slot", slotName).
			Int("max_replication_slots", maxSlots).
			Int("used_slots", usedSlots).
			Int("free_slots", freeSlots).
			Int("slot_headroom_min", headroomMin).
			Msg("WARNING: low replication slot headroom — risk of slot exhaustion")
		return
	}
	logger.Info().
		Int("free_slots", freeSlots).
		Int("slot_headroom_min", headroomMin).
		Msg("slot headroom check passed")
}

func bootstrapEnsureRole(ctx context.Context, adminConn walconn.AdminConn, dsn string, isReplication bool, logger zerolog.Logger) {
	conn := (*pgx.Conn)(adminConn)
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		logger.Warn().Err(err).
			Bool("replication", isReplication).
			Msg("bootstrap.create_roles: could not parse DSN for role provisioning; skipping")
		return
	}
	username := u.User.Username()
	password, hasPassword := u.User.Password()
	if username == "" || !hasPassword {

		logger.Warn().
			Bool("replication", isReplication).
			Msg("bootstrap.create_roles: DSN lacks username or password; cannot provision role")
		return
	}

	if !pgRoleNameRe.MatchString(username) {
		logger.Warn().
			Str("role", username).
			Bool("replication", isReplication).
			Msg("bootstrap.create_roles: DSN username is not a valid PostgreSQL identifier; skipping")
		return
	}
	var exists bool
	if err := conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $1)",
		username,
	).Scan(&exists); err != nil {
		logger.Warn().Err(err).
			Str("role", username).
			Msg("bootstrap.create_roles: pg_roles probe failed; skipping role creation")
		return
	}
	if exists {
		logger.Info().Str("role", username).Msg("bootstrap.create_roles: role already exists; skipping")
		return
	}
	attrs := "LOGIN"
	if isReplication {
		attrs = "LOGIN REPLICATION"
	}

	if strings.ContainsAny(password, "\\\x00") {
		logger.Warn().
			Bool("replication", isReplication).
			Str("role", username).
			Msg("bootstrap.create_roles: password contains backslash or NUL; skipping (provision the role manually with a dollar-quoted PASSWORD literal)")
		return
	}
	escaped := strings.ReplaceAll(password, "'", "''")
	ddl := fmt.Sprintf("CREATE ROLE %s WITH %s PASSWORD '%s'", username, attrs, escaped)
	if _, err := conn.Exec(ctx, ddl); err != nil {
		logger.Warn().Err(err).
			Str("role", username).
			Msg("bootstrap.create_roles: CREATE ROLE failed; proceeding (operator may have pre-provisioned role under a different name)")
		return
	}
	logger.Info().Str("role", username).
		Bool("replication", isReplication).
		Msg("bootstrap.create_roles: role created")

	if !isReplication {
		if _, err := conn.Exec(ctx, fmt.Sprintf("GRANT pg_monitor TO %s", username)); err != nil {
			logger.Warn().Err(err).
				Str("role", username).
				Msg("bootstrap.create_roles: GRANT pg_monitor failed; metrics queries may degrade")
		}
	}
}

func bootstrapVerifyTables(ctx context.Context, adminConn walconn.AdminConn, publication string, want []string, logger zerolog.Logger) {
	conn := (*pgx.Conn)(adminConn)
	rows, err := conn.Query(ctx,
		"SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1",
		publication,
	)
	if err != nil {
		logger.Warn().Err(err).Str("publication", publication).
			Msg("bootstrap: could not enumerate publication tables for verification")
		return
	}
	defer rows.Close()
	have := make(map[string]struct{})
	for rows.Next() {
		var qname string
		if err := rows.Scan(&qname); err != nil {
			logger.Warn().Err(err).Msg("bootstrap: pg_publication_tables row scan failed")
			return
		}
		have[qname] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		logger.Warn().Err(err).Msg("bootstrap: pg_publication_tables iteration failed")
		return
	}
	wantSet := make(map[string]struct{}, len(want))
	var missing []string
	for _, t := range want {
		wantSet[t] = struct{}{}
		if _, ok := have[t]; !ok {
			missing = append(missing, t)
		}
	}
	var extra []string
	for t := range have {
		if _, ok := wantSet[t]; !ok {
			extra = append(extra, t)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) == 0 && len(extra) == 0 {
		logger.Info().Str("publication", publication).
			Int("table_count", len(want)).
			Msg("bootstrap: publication table list matches wal.bootstrap.tables")
		return
	}
	ev := logger.Warn().Str("publication", publication)
	if len(missing) > 0 {
		ev = ev.Strs("missing_from_publication", missing)
	}
	if len(extra) > 0 {
		ev = ev.Strs("extra_in_publication", extra)
	}
	ev.Msg("bootstrap: publication membership differs from wal.bootstrap.tables; reconcile manually via ALTER PUBLICATION")
}
