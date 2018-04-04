package postgres

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" //postgres driver
	"github.com/whyrusleeping/sql-datastore"
)

// Options are the postgres datastore options, reexported here for convenience.
type PostgresOptions struct {
	Host     string
	User     string
	Password string
	Database string
}

type PostgresQueries struct {
}

func (PostgresQueries) Delete() string {
	return `DELETE FROM blocks WHERE key = $1`
}

func (PostgresQueries) Exists() string {
	return `SELECT exists(SELECT 1 FROM blocks WHERE key=$1)`
}

func (PostgresQueries) Get() string {
	return `SELECT data FROM blocks WHERE key = $1`
}

func (PostgresQueries) Put() string {
	return `INSERT INTO blocks (key, data) SELECT $1, $2 WHERE NOT EXISTS ( SELECT key FROM blocks WHERE key = $1)`
}

func (PostgresQueries) Query() string {
	return `SELECT key, data FROM blocks`
}

func (PostgresQueries) Prefix() string {
	return " WHERE key LIKE '%s%%' ORDER BY key"
}

func (PostgresQueries) Limit() string {
	return " LIMIT %d"
}

func (PostgresQueries) Offset() string {
	return " OFFSET %d"
}

// Create returns a datastore connected to postgres
func (opts *PostgresOptions) Create() (*sqlds.Datastore, error) {
	opts.setDefaults()

	fmtstr := "postgres://%s:%s@%s/%s?sslmode=disable"
	constr := fmt.Sprintf(fmtstr, opts.User, opts.Password, opts.Host, opts.Database)
	db, err := sql.Open("postgres", constr)
	if err != nil {
		return nil, err
	}
	return sqlds.NewDatastore(db, PostgresQueries{}), nil
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
