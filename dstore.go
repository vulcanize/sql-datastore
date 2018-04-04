package sqlds

import (
	"database/sql"

	"fmt"
	ds "gx/ipfs/QmXRKBQA4wXP7xWbFiZsR1GP4HV6wMDQ1aWFxZZ4uBcPX9/go-datastore"
	dsq "gx/ipfs/QmXRKBQA4wXP7xWbFiZsR1GP4HV6wMDQ1aWFxZZ4uBcPX9/go-datastore/query"
	"log"
)

const (
	postgresPut    = `INSERT INTO blocks (key, data) SELECT $1, $2 WHERE NOT EXISTS ( SELECT key FROM blocks WHERE key = $1)`
	postgresQuery  = `SELECT key, data FROM blocks`
	postgresDelete = `DELETE FROM blocks WHERE key = $1`
	postgresGet    = `SELECT data FROM blocks WHERE key = $1`
	postgresExists = `SELECT exists(SELECT 1 FROM blocks WHERE key=$1)`
)

type datastore struct {
	db *sql.DB
}

// NewDatastore returns a new postgres datastore
func NewDatastore(db *sql.DB) *datastore {
	return &datastore{db}
}

type postgresBatch struct {
	db  *sql.DB
	txn *sql.Tx
}

func (b postgresBatch) Put(key ds.Key, val interface{}) error {
	data, ok := val.([]byte)
	if !ok {
		return ds.ErrInvalidType
	}
	_, err := b.txn.Exec(postgresPut, key.String(), data)
	if err != nil {
		b.txn.Rollback()
	}
	return err
}

func (b postgresBatch) Delete(key ds.Key) error {
	_, err := b.txn.Exec(postgresDelete, key.String())
	if err != nil {
		b.txn.Rollback()
	}
	return err
}

func (b postgresBatch) Commit() error {
	return b.txn.Commit()
}

func (d *datastore) Batch() (ds.Batch, error) {
	txn, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	return &postgresBatch{
		db:  d.db,
		txn: txn,
	}, nil
}

func (d *datastore) Close() error {
	return d.db.Close()
}

func (d *datastore) Delete(key ds.Key) error {
	result, err := d.db.Exec(postgresDelete, key.String())
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ds.ErrNotFound
	}
	return nil
}

func (d *datastore) Get(key ds.Key) (value interface{}, err error) {
	row := d.db.QueryRow(postgresGet, key.String())

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

func (d *datastore) Has(key ds.Key) (exists bool, err error) {
	fmt.Println(d.db.Stats().OpenConnections)
	row := d.db.QueryRow(postgresExists, key.String())

	switch err := row.Scan(&exists); err {
	case sql.ErrNoRows:
		return exists, nil
	case nil:
		return exists, nil
	default:
		return exists, err
	}
}

func (d *datastore) Put(key ds.Key, value interface{}) error {
	data, ok := value.([]byte)
	if !ok {
		return ds.ErrInvalidType
	}

	_, err := d.db.Exec(postgresPut, key.String(), data)
	if err != nil {
		return err
	}

	return nil
}

func (d *datastore) Query(q dsq.Query) (dsq.Results, error) {
	raw, err := d.RawQuery(q)
	if err != nil {
		return nil, err
	}
	for _, f := range q.Filters {
		raw = dsq.NaiveFilter(raw, f)
	}
	for _, o := range q.Orders {
		raw = dsq.NaiveOrder(raw, o)
	}
	return raw, nil
}

func (d *datastore) RawQuery(q dsq.Query) (dsq.Results, error) {
	var rows *sql.Rows
	var err error
	if q.Prefix != "" {
		rows, err = QueryWithParams(d, q)
	} else {
		rows, err = d.db.Query(postgresQuery)
	}

	if err != nil {
		return nil, err
	}

	var entries []dsq.Entry
	defer rows.Close()

	for rows.Next() {
		var key string
		var out []byte
		err := rows.Scan(&key, &out)
		if err != nil {
			log.Fatal("Error reading rows from query")
		}
		entry := dsq.Entry{
			Key:   key,
			Value: out,
		}
		entries = append(entries, entry)
	}
	results := dsq.ResultsWithEntries(q, entries)
	return results, nil
}

// QueryWithParams applies prefix, limit, and offset params in pg query
func QueryWithParams(d *datastore, q dsq.Query) (*sql.Rows, error) {
	var qNew = postgresQuery
	if q.Prefix != "" {
		qNew += fmt.Sprintf(" WHERE key LIKE '%s%%' ORDER BY key", q.Prefix)
	}
	if q.Limit != 0 {
		qNew += fmt.Sprintf(" LIMIT %d", q.Limit)
	}
	if q.Offset != 0 {
		qNew += fmt.Sprintf(" OFFSET %d", q.Offset)
	}
	return d.db.Query(qNew)

}

var _ ds.Datastore = (*datastore)(nil)
