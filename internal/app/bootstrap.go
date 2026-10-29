package app

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"
)

type bootstrapConfig struct {
	PublicationName string
	Tables          []string
}

type bootstrapDB interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func verifyPGPrereqs(ctx context.Context, conn bootstrapDB, logger zerolog.Logger) error {

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

func verifyReplicationRole(ctx context.Context, conn bootstrapDB, logger zerolog.Logger) error {
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
				"superuser); see docs/postgres.md#replication-role",
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

// lookupPublication reports whether the publication exists and whether it
// was created FOR ALL TABLES.
func lookupPublication(ctx context.Context, conn bootstrapDB, name string) (exists, allTables bool, err error) {
	err = conn.QueryRow(ctx,
		"SELECT puballtables FROM pg_publication WHERE pubname = $1",
		name,
	).Scan(&allTables)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("publication existence check failed (%s): %w", name, err)
	}
	return true, allTables, nil
}

func errAllTablesPublication(name string) error {
	return fmt.Errorf(
		"publication %q is FOR ALL TABLES, which streams every table in the database — "+
			"recreate it with an explicit table list: DROP PUBLICATION %s; CREATE PUBLICATION %s FOR TABLE <list>",
		name, name, name,
	)
}

// publicationHasSchemas reports whether the publication includes whole
// schemas (FOR TABLES IN SCHEMA, PG 15+). PG 14 has no such publications.
func publicationHasSchemas(ctx context.Context, conn bootstrapDB, name string) (bool, error) {
	var version int
	if err := conn.QueryRow(ctx,
		"SELECT current_setting('server_version_num')::int",
	).Scan(&version); err != nil {
		return false, fmt.Errorf("server version check failed: %w", err)
	}
	if version < 150000 {
		return false, nil
	}
	var has bool
	if err := conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication_namespace pn JOIN pg_publication p ON p.oid = pn.pnpubid WHERE p.pubname = $1)",
		name,
	).Scan(&has); err != nil {
		return false, fmt.Errorf("publication schema check failed (%s): %w", name, err)
	}
	return has, nil
}

func errSchemaPublication(name string) error {
	return fmt.Errorf(
		"publication %q includes FOR TABLES IN SCHEMA, which streams every table in the schema — "+
			"recreate it with an explicit list: DROP PUBLICATION %s; CREATE PUBLICATION %s FOR TABLE <list>",
		name, name, name,
	)
}

// bootstrapPublication verifies an existing publication or, when it is
// missing and WALERA_TABLES lists tables, creates it FOR TABLE. Publications
// that stream more than a table list are refused.
func bootstrapPublication(ctx context.Context, conn bootstrapDB, cfg bootstrapConfig, logger zerolog.Logger) error {
	exists, allTables, err := lookupPublication(ctx, conn, cfg.PublicationName)
	if err != nil {
		return err
	}
	if !exists {
		if len(cfg.Tables) == 0 {
			return fmt.Errorf(
				"publication %q does not exist — create it (CREATE PUBLICATION %s FOR TABLE <list>) or set WALERA_TABLES so Walera creates it",
				cfg.PublicationName, cfg.PublicationName,
			)
		}
		ddl := fmt.Sprintf(
			"CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert, update, delete')",
			cfg.PublicationName, strings.Join(cfg.Tables, ", "),
		)
		if _, err := conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("failed to create publication %q: %w", cfg.PublicationName, err)
		}
		logger.Info().Str("publication", cfg.PublicationName).Int("table_count", len(cfg.Tables)).Msg("publication created")
		return nil
	}
	if allTables {
		return errAllTablesPublication(cfg.PublicationName)
	}
	if hasSchemas, err := publicationHasSchemas(ctx, conn, cfg.PublicationName); err != nil {
		return err
	} else if hasSchemas {
		return errSchemaPublication(cfg.PublicationName)
	}
	var tableCount int
	if err := conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_publication_tables WHERE pubname = $1",
		cfg.PublicationName,
	).Scan(&tableCount); err != nil {
		return fmt.Errorf("publication table count failed (%s): %w", cfg.PublicationName, err)
	}
	if tableCount == 0 {
		return fmt.Errorf("publication %q has no tables — add tables with ALTER PUBLICATION %s ADD TABLE <list>", cfg.PublicationName, cfg.PublicationName)
	}
	logger.Info().Str("publication", cfg.PublicationName).Int("table_count", tableCount).Msg("publication check passed")
	if len(cfg.Tables) > 0 {
		bootstrapVerifyTables(ctx, conn, cfg.PublicationName, cfg.Tables, logger)
	}
	return nil
}

func checkSlotHeadroom(ctx context.Context, conn bootstrapDB, headroomMin int, slotName string, logger zerolog.Logger) {
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

func bootstrapVerifyTables(ctx context.Context, conn bootstrapDB, publication string, want []string, logger zerolog.Logger) {
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
			Msg("bootstrap: publication table list matches WALERA_TABLES")
		return
	}
	ev := logger.Warn().Str("publication", publication)
	if len(missing) > 0 {
		ev = ev.Strs("missing_from_publication", missing)
	}
	if len(extra) > 0 {
		ev = ev.Strs("extra_in_publication", extra)
	}
	ev.Msg("bootstrap: publication membership differs from WALERA_TABLES; reconcile manually via ALTER PUBLICATION")
}
