package main

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql" // ....
	_ "github.com/lib/pq"              // ...
)

//New ..
func newStore(db *sql.DB) *transport {
	return &transport{
		db:       db,
		lines:    make([]lineOfLogType, 0),
		exitChan: getExitSignalsChannel(),
	}
}

func newDB(typedb, databaseURL string) (*sql.DB, error) {
	db, err := sql.Open(typedb, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
