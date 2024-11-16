package db

import (
	"database/sql"
	"fmt"
	"github.com/clong1995/go-config"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"reflect"
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

// TxExec 事物内执行
func TxExec(tx *sql.Tx, query string, args ...any) (result sql.Result, err error) {
	if result, err = tx.Exec(query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

// QueryScan 查询并扫描
func QueryScan[T any](query string, args ...any) (res []T, err error) {
	rows, err := datasource.Query(query, args...)
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	if res, err = scan[T](rows); err != nil {
		log.Println(err)
		return
	}
	return
}

// TxQueryScan 事物内查询并扫描
func TxQueryScan[T any](tx *sql.Tx, query string, args ...any) (res []T, err error) {
	rows, err := tx.Query(query, args...)
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	if res, err = scan[T](rows); err != nil {
		log.Println(err)
		return
	}
	return
}

func scan[T any](rows *sql.Rows) (res []T, err error) {
	columns, err := rows.Columns()
	if err != nil {
		log.Println(err)
		return
	}

	var obj T
	objType := reflect.TypeOf(obj)
	if objType.NumField() != len(columns) {
		err = fmt.Errorf(`columns len = %d, objType len = %d`, len(columns), objType.NumField())
		log.Println(err)
		return
	}

	objValue := reflect.ValueOf(&obj).Elem()

	fieldPointers := make([]any, len(columns))

	for i := range fieldPointers {
		fieldPointers[i] = objValue.Field(i).Addr().Interface()
	}
	for rows.Next() {
		if err = rows.Scan(fieldPointers...); err != nil {
			log.Println(err)
			return
		}
		res = append(res, obj)
	}

	if err = rows.Err(); err != nil {
		log.Println(err)
		return
	}

	return
}
