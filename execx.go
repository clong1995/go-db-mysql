package db

import (
	"database/sql"
	"log"
)

// PrepareStmtTxX 附带事物的预编译SQL批量执行
func PrepareStmtTxX(query string, handle func(stmtTx *sql.Stmt) (err error)) (err error) {
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

// TxX 事物
func TxX(db string, handle func(tx *sql.Tx) (err error)) (err error) {
	//开启事物
	tx, err := datasourceX[db].Begin()
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

// QueryRowX 查询一条
func QueryRowX(db, query string, args ...any) (row *sql.Row) {
	row = datasourceX[db].QueryRow(query, args...)
	return
}

// ExecX 执行
func ExecX(db, query string, args ...any) (result sql.Result, err error) {
	if result, err = datasourceX[db].Exec(query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

// TxExecX 事物内执行
func TxExecX(tx *sql.Tx, query string, args ...any) (result sql.Result, err error) {
	if result, err = tx.Exec(query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

// QueryX 查询
func QueryX(db, query string, args ...any) (rows *sql.Rows, err error) {
	if rows, err = datasourceX[db].Query(query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

// QueryScanX 查询并扫描
func QueryScanX[T any](db, query string, args ...any) (res []T, err error) {
	rows, err := datasourceX[db].Query(query, args...)
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

// TxQueryScanX 事物内查询并扫描
func TxQueryScanX[T any](tx *sql.Tx, query string, args ...any) (res []T, err error) {
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

// TxQueryX 事物内查询
func TxQueryX(tx *sql.Tx, query string, args ...any) (rows *sql.Rows, err error) {
	rows, err = tx.Query(query, args...)
	if err != nil {
		log.Println(err)
		return
	}
	return
}

// TxQueryRowX 事物内查询
func TxQueryRowX(tx *sql.Tx, query string, args ...any) (row *sql.Row) {
	row = tx.QueryRow(query, args...)
	return
}
