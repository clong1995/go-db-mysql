package db

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/clong1995/go-config"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var datasourceX map[string]*sql.DB

func init() {
	dsx := config.Value("DATASOURCEX")
	if dsx == "" {
		return
	}
	arr := strings.Split(dsx, ",")
	re := regexp.MustCompile(`/([^/?]+)\?`)
	datasourceX = make(map[string]*sql.DB)
	for _, v := range arr {
		var dbName string
		match := re.FindStringSubmatch(v)
		if len(match) > 1 {
			dbName = match[1]
		} else {
			err := errors.New("dbx database name is invalid")
			log.Fatalln(err)
		}

		var err error
		datasource, err = sql.Open("mysql", v)
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
		datasourceX[dbName] = datasource
		log.Printf("[MySQL] conn %s\n", v)
	}

}

func CloseX() {
	for _, v := range datasourceX {
		err := v.Close()
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("[PostgreSQL] db exited!")
	}
}
