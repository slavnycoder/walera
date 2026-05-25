package walconn

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type AdminConn *pgx.Conn

type ReplicationConn *pgconn.PgConn

type AdminDSN string

type ReplicationDSN string

func NewAdminConn(ctx context.Context, dsn AdminDSN) (AdminConn, error) {
	conn, err := pgx.Connect(ctx, string(dsn))
	return AdminConn(conn), err
}

func NewReplicationConn(ctx context.Context, dsn ReplicationDSN) (ReplicationConn, error) {
	conn, err := pgconn.Connect(ctx, string(dsn))
	return ReplicationConn(conn), err
}
