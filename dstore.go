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

	//fmt.Println(pq.CopyIn("blocks", "key", "data"))
	_, err := d.db.Exec("INSERT INTO blocks (key, data) SELECT $1, $2 WHERE NOT EXISTS ( SELECT key FROM blocks WHERE key = $1);", k.Bytes(), data)
	if err != nil {
		return err
	}

	return nil
}

func (d *SQLDatastore) Get(k ds.Key) (interface{}, error) {
	row := d.db.QueryRow("SELECT data from blocks where key=$1", k.Bytes())

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
	row := d.db.QueryRow(q, k.Bytes())
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
	row := d.db.QueryRow(q, k.Bytes())
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
	panic("not yet implemented")
}

/*
func main() {
	db, err := sql.Open("postgres", "postgres://postgres:mysecretpassword@172.17.0.2/ipfs?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	sds := &SQLDatastore{db}

	dskey := ds.NewKey("thisisatest")
	err = sds.Put(dskey, []byte("ipfsipfsipfsipfs and stuff too like cats"))
	if err != nil {
		log.Fatal(err)
	}

	val, err := sds.Get(dskey)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(val.([]byte)))

	fmt.Println(sds.Get(ds.NewKey("bitches")))
	fmt.Println(sds.Has(ds.NewKey("bitches")))
}
*/

var _ ds.Datastore = (*SQLDatastore)(nil)
