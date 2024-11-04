package db

import (
	"database/sql"
	"github.com/clong1995/go-config"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

var datasource *sql.DB

func init() {
	ds := config.Value("DATASOURCE")
	var err error
	datasource, err = sql.Open("mysql", ds)
	if err != nil {
		log.Fatalln(err)
	}

	datasource.SetMaxOpenConns(100)
	datasource.SetMaxIdleConns(10)
	if err = datasource.Ping(); err != nil {
		log.Println(err)
		return
	}
	log.Printf("[MySQL] conn %s\n", ds)
}

// Tx 事物
func Tx(handle func(tx *sql.Tx) (err error)) (err error) {
	//开启事物
	tx, err := datasource.Begin()
	if err != nil {
		log.Println(err)
		return
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Println(rollbackErr)
			}
		} else {
			if commitErr := tx.Commit(); commitErr != nil {
				log.Println(commitErr)
			}
		}
	}()

	if err = handle(tx); err != nil {
		log.Println(err)
		return err
	}
	return
}

// Query 查询
func Query(query string, args ...any) (rows *sql.Rows, err error) {
	if rows, err = datasource.Query(query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

// QueryRow 查询一条
func QueryRow(query string, args ...any) (row *sql.Row) {
	row = datasource.QueryRow(query, args...)
	return
}

// Exec 执行
func Exec(query string, args ...any) (result sql.Result, err error) {
	if result, err = datasource.Exec(query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}
