package sqlds

import (
	"database/sql"
	"fmt"

	ds "github.com/jbenet/go-datastore"
	dsq "github.com/jbenet/go-datastore/query"
)

type SQLDatastore struct {
	db *sql.DB
}

func NewSqlDatastore(db *sql.DB) *SQLDatastore {
	return &SQLDatastore{db}
}

func (d *SQLDatastore) Put(k ds.Key, val interface{}) error {
	data, ok := val.([]byte)
	if !ok {
		return fmt.Errorf("data was not a []byte")
	}

	_, err := d.db.Exec("INSERT INTO blocks (key, data) SELECT $1, $2 WHERE NOT EXISTS ( SELECT key FROM blocks WHERE key = $1);", k.String(), data)
	if err != nil {
		return err
	}

	return nil
}

func (d *SQLDatastore) Get(k ds.Key) (interface{}, error) {
	row := d.db.QueryRow("SELECT data from blocks where key=$1", k.String())

	var out []byte
	switch err := row.Scan(&out); err {
	case sql.ErrNoRows:
		return nil, ds.ErrNotFound
	case nil:
		return out, nil
	default:
		return nil, err
	}
}

func (d *SQLDatastore) Has(k ds.Key) (bool, error) {
	q := "SELECT key FROM blocks WHERE key = $1;"
	row := d.db.QueryRow(q, k.String())
	switch err := row.Scan(); err {
	case sql.ErrNoRows:
		return false, nil
	case nil:
		return true, nil
	default:
		return false, err
	}

}

func (d *SQLDatastore) Delete(k ds.Key) error {
	q := "DELETE FROM blocks WHERE key = $1;"
	row := d.db.QueryRow(q, k.String())
	switch err := row.Scan(); err {
	case sql.ErrNoRows:
		return ds.ErrNotFound
	case nil:
		return nil
	default:
		return err
	}
}

func (d *SQLDatastore) Query(q dsq.Query) (dsq.Results, error) {
	rows, err := d.db.Query("SELECT key, data FROM blocks")
	if err != nil {
		return nil, err
	}

	resch := make(chan dsq.Result)
	go func() {
		defer close(resch)

		for rows.Next() {

			var key string
			var out []byte
			err := rows.Scan(&key, &out)
			resch <- dsq.Result{
				Error: err,
				Entry: dsq.Entry{
					Key:   key,
					Value: out,
				},
			}

			if err != nil {
				return
			}
		}
	}()

	return dsq.ResultsWithChan(q, resch), nil
}

var _ ds.Datastore = (*SQLDatastore)(nil)
