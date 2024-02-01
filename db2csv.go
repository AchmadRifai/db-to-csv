package dbtocsv

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"runtime/debug"

	arrayutils "github.com/AchmadRifai/array-utils"
	sqlutils "github.com/AchmadRifai/sql-utils"
)

func QueryToCsv(db *sql.DB, path, query string, args ...any) {
	rows, cols := sqlutils.DbSelect(db, query, args...)
	f, err := os.Create(path)
	defer fileClose(f)
	if err != nil {
		panic(err)
	}
	writer := csv.NewWriter(f)
	defer writer.Flush()
	writer.Write(cols)
	data := arrayutils.Map(rows, func(row map[string]interface{}, i int) []string {
		var line []string
		for _, key := range cols {
			line = append(line, fmt.Sprint(row[key]))
		}
		return line
	})
	writer.WriteAll(data)
}

func fileClose(f *os.File) {
	normalError()
	if f != nil {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}
}

func normalError() {
	if r := recover(); r != nil {
		log.Println("", r)
		log.Println("", string(debug.Stack()))
	}
}
