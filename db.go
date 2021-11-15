package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"strings"

	migrate "github.com/golang-migrate/migrate/v4"
	bindata "github.com/golang-migrate/migrate/v4/source/go_bindata"
	"github.com/jmoiron/sqlx" // TODO: research alternatives. Ex.: https://github.com/go-reform/reform
	"github.com/jmoiron/sqlx/reflectx"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/percona/qan-api2/migrations"
)

const (
	databaseNotExistErrorCode = 81
)

// NewDB return updated db.
func NewDB(dsn string, maxIdleConns, maxOpenConns int) *sqlx.DB {
	db, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		log.Fatalf(err.Error())
	}

	// TODO: find solution with better performance
	db.Mapper = reflectx.NewMapperTagFunc("json", strings.ToUpper, func(value string) string {
		if strings.Contains(value, ",") {
			return strings.Split(value, ",")[0]
		}
		return value
	})

	db.SetConnMaxLifetime(0)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetMaxOpenConns(maxOpenConns)

	if err := runMigrations(db.DB, dsn); err != nil {
		log.Fatal("Migrations: ", err)
	}
	log.Println("Migrations applied.")
	return db
}

func createDB(dsn string) error {
	log.Println("Creating database")
	clickhouseURL, err := url.Parse(dsn)
	if err != nil {
		return err
	}
	q := clickhouseURL.Query()
	databaseName := q.Get("database")
	q.Set("database", "default")

	clickhouseURL.RawQuery = q.Encode()

	defaultDB, err := sqlx.Connect("mysql", clickhouseURL.String())
	if err != nil {
		return err
	}
	defer defaultDB.Close()

	result, err := defaultDB.Exec(fmt.Sprintf(`CREATE DATABASE %s ENGINE = Ordinary`, databaseName))
	if err != nil {
		log.Printf("Result: %v", result)
		return err
	}
	log.Println("Database was created")
	return nil
	// The qan-api2 will exit after creating the database, it'll be restarted by supervisor
}

func runMigrations(instance *sql.DB, dsn string) error {
	s := bindata.Resource(migrations.AssetNames(), migrations.Asset)

	d, err := bindata.WithInstance(s)
	if err != nil {
		return err
	}
	log.Println("dsn: ", "mysql://"+dsn)
	m, err := migrate.NewWithSourceInstance("go-bindata", d, "mysql://"+dsn)
	if err != nil {
		return err
	}

	// run up to the latest migration
	err = m.Up()
	if err == migrate.ErrNoChange {
		return nil
	}
	return err
	// sourceInstance, err := bindata.WithInstance(bindata.Resource(migrations.AssetNames(), migrations.Asset))
	// if err != nil {
	// 	// handler err
	// }
	// targetInstance, err := mysql.WithInstance(instance /* *sql.DB */, new(mysql.Config))
	// if err != nil {
	// 	// handler err
	// }
	// m, err := migrate.NewWithInstance("go-bindata", sourceInstance, "mysql", targetInstance)
	// if err != nil {
	// 	// handler err
	// }
	// // run up to the latest migration
	// err = m.Up()
	// if err == migrate.ErrNoChange {
	// 	return nil
	// }
	// return err
}

// DropOldPartition drops number of days old partitions of pmm.metrics in ClickHouse.
func DropOldPartition(db *sqlx.DB, days uint) {
	partitions := []string{}
	const query = `
		SELECT DISTINCT partition
		FROM system.parts
		WHERE toUInt32(partition) < toYYYYMMDD(now() - toIntervalDay(?)) ORDER BY partition
	`
	err := db.Select(
		&partitions,
		query,
		days,
	)
	if err != nil {
		log.Printf("Select %d days old partitions of system.parts. Result: %v, Error: %v", days, partitions, err)
		return
	}
	for _, part := range partitions {
		result, err := db.Exec(fmt.Sprintf(`ALTER TABLE metrics DROP PARTITION %s`, part))
		log.Printf("Drop %s partitions of metrics. Result: %v, Error: %v", part, result, err)
	}
}
