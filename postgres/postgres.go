package postgres

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	sqlds "github.com/whyrusleeping/sql-datastore"
)

type PostgresOptions struct {
	Host     string
	User     string
	Password string
	Database string
}

func (opts *PostgresOptions) Create() (*sqlds.SQLDatastore, error) {
	opts.setDefaults()

	fmtstr := "postgres://%s:%s@%s/%s?sslmode=disable"
	constr := fmt.Sprintf(fmtstr, opts.User, opts.Password, opts.Host, opts.Database)
	db, err := sql.Open("postgres", constr)
	if err != nil {
		return nil, err
	}

	return sqlds.NewSqlDatastore(db), nil
}

func (opts *PostgresOptions) setDefaults() {
	if opts.Host == "" {
		opts.Host = "127.0.0.1"
	}

	if opts.User == "" {
		opts.User = "postgres"
	}

	if opts.Database == "" {
		opts.Database = "datastore"
	}
}
