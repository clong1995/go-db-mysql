package db

import (
	"database/sql"
	"log"
)

// PrepareStmtTx 附带事物的预编译SQL批量执行
func PrepareStmtTx(query string, handle func(stmtTx *sql.Stmt) (err error)) (err error) {
	if err = Tx(func(tx *sql.Tx) (err error) {
		stmt, err := tx.Prepare(query)
		if err != nil {
			log.Println(err)
			return
		}
		defer func() {
			_ = stmt.Close()
		}()

		if err = handle(stmt); err != nil {
			log.Println(err)
			return
		}

		return
	}); err != nil {
		log.Println(err)
		return
	}
	return
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

// Query 查询
func Query(query string, args ...any) (rows *sql.Rows, err error) {
	if rows, err = datasource.Query(query, args...); err != nil {
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

// TxQuery 事物内查询
func TxQuery(tx *sql.Tx, query string, args ...any) (rows *sql.Rows, err error) {
	rows, err = tx.Query(query, args...)
	if err != nil {
		log.Println(err)
		return
	}
	return
}

// TxQueryRow 事物内查询
func TxQueryRow(tx *sql.Tx, query string, args ...any) (row *sql.Row) {
	row = tx.QueryRow(query, args...)
	return
}
