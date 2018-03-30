package sqlds

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" //postgres driver
)

// Options are the postgres datastore options, reexported here for convenience.
type Options struct {
	Host     string
	User     string
	Password string
	Database string
}

// Create returns a datastore connected to postgres
func (opts *Options) Create() (*datastore, error) {
	opts.setDefaults()

	fmtstr := "postgres://%s:%s@%s/%s?sslmode=disable"
	constr := fmt.Sprintf(fmtstr, opts.User, opts.Password, opts.Host, opts.Database)
	db, err := sql.Open("postgres", constr)
	if err != nil {
		return nil, err
	}

	return NewDatastore(db), nil
}

func (opts *Options) setDefaults() {
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
