package db

import (
	"database/sql"
	"fmt"
	"github.com/clong1995/go-config"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strconv"
	"time"
)

var datasource *sql.DB

func init() {
	ds := config.Value("DATASOURCE")
	if ds == "" {
		return
	}

	var err error
	datasource, err = sql.Open("mysql", ds)
	if err != nil {
		log.Fatalln(err)
	}

	num, err := strconv.Atoi(config.Value("MAXCONNS"))
	if err != nil {
		log.Fatalln(err)
		return
	}

	datasource.SetMaxOpenConns(num)
	datasource.SetConnMaxLifetime(time.Hour)
	if err = datasource.Ping(); err != nil {
		log.Println(err)
		return
	}
	log.Printf("[MySQL] conn %s\n", ds)
}

func Close() {
	err := datasource.Close()
	if err != nil {
		log.Println(err)
		return
	}
	fmt.Println("[PostgreSQL] db exited!")
}
