package main

import (
	"archive/zip"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/text/encoding/korean"
)

func main() {
	fmt.Println("csv export")

	config := readConfig()
	if config == nil {
		fmt.Println("load config fail !!!")
		return
	}

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Pw, config.Host, config.Database))
	defer db.Close()
	if err != nil {
		fmt.Println("Mysql connection fail !!! [%s]", err)
		return
	}

	logfile := fmt.Sprintf("csv_export_log_%s.txt", time.Now().Format("2006_01_02_15_04_05"))
	log, err := os.Create(logfile)
	if err != nil {
		return
	}

	zipfiles := make([]string, len(config.Tables))
	for i, table := range config.Tables {
		columnInfos, err := getDBTableColumnsInfo(db, table)
		if err != nil {
			var errLog = fmt.Sprintf("columns info table:%s \r\n", table)
			log.WriteString("\r\n")
			log.WriteString(errLog)
			continue
		}

		query, err := generateSelectQuery(columnInfos, table)
		if err != nil {
			var errLog = fmt.Sprintf("generate select query table:%s i:%d\r\n", table, i)
			log.WriteString("\r\n")
			log.WriteString(errLog)
			continue
		}
		//log.WriteString(query)

		rows, err := db.Query(query)
		cols, _ := rows.Columns()
		defer rows.Close()
		if err != nil {
			var errLog = fmt.Sprintf("%s fail err:%s \r\n", query, err)
			log.WriteString("\r\n")
			log.WriteString(errLog)
			continue
		}

		dataIndex := 0
		datas := make(map[int]([]string))
		for rows.Next() {
			columns := make([]interface{}, len(cols))
			columnPointers := make([]interface{}, len(cols))
			for colindex, _ := range columns {
				columnPointers[colindex] = &columns[colindex]
			}
			err := rows.Scan(columnPointers...)
			if err != nil {
				var errLog = fmt.Sprintf("scan fail err:%s \r\n", err)
				log.WriteString("\r\n")
				log.WriteString(errLog)
			}

			// columnNames := make(map[string]interface{})
			// for colindex, colName := range cols {
			// 	val := columnPointers[colindex].(*interface{})
			// 	columnNames[colName] = *val
			// 	valStr := fmt.Sprintf("%s", columnNames[colName])
			// 	if *val == nil {
			// 		valStr = "nil"
			// 	}
			// 	fmt.Printf("%s-[%s]\r\n", colName, valStr)
			// }
			rowStringArr := make([]string, len(cols))
			for colIndex, _ := range cols {
				//if columnInfos[colIndex].Type
				val := columnPointers[colIndex].(*interface{})
				valStr := fmt.Sprintf("%s", *val)
				if *val == nil {
					valStr = ""
				}
				rowStringArr[colIndex] = valStr
			}
			datas[dataIndex] = rowStringArr
			//fmt.Println(datas[dataIndex])

			dataIndex++
		}

		//create csv file
		fileName := fmt.Sprintf("%s_.csv", table)
		saveCSVFile(fileName, cols, datas)

		// create anci file
		fileNameA := fmt.Sprintf("%s.csv", table)
		encodingfileUtf8ToAnsi(fileName, fileNameA)

		// delete utf8 file
		os.Remove(fileName)
		//fmt.Println(datas)
		zipfiles[i] = fileNameA
	}

	info, _ := log.Stat()
	if info.Size() <= 10 {
		log.Close()
		os.Remove(logfile)
	}

	err = ZipFiles("result.zip", zipfiles)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, file := range zipfiles {
		os.Remove(file)
	}
}

// json config info
type Config struct {
	Host     string   `json:"host"`
	User     string   `json:"user"`
	Pw       string   `json:"pw"`
	Database string   `json:"database"`
	Tables   []string `json:"tables"`
	Ready    []string `json:"ready"`
}

func readConfig() *Config {
	file, err := os.Open("conf.json")
	if err != nil {
		return nil
	}

	var config Config

	jsonParser := json.NewDecoder(file)
	if err := jsonParser.Decode(&config); err != nil {
		return nil
	}

	//fmt.Println(config)
	return &config
}

// mysql column info
type ColumnInfo struct {
	Field      string
	Type       string
	Collation  sql.NullString
	Null       string
	Key        string
	Default    sql.NullString
	Extra      string
	Privileges string
	Comment    string
}

func getDBTableColumnsInfo(db *sql.DB, tablename string) ([]ColumnInfo, error) {
	column := make([]ColumnInfo, 100)

	res2, err := db.Query(fmt.Sprintf("SHOW FULL COLUMNS FROM %s", tablename))
	if err != nil {
		fmt.Println("show columns query fail !!!")
		return nil, err
	}

	columCount := 0
	for i := 0; res2.Next(); i++ {

		res2.Scan(&column[i].Field, &column[i].Type, &column[i].Collation, &column[i].Null,
			&column[i].Key, &column[i].Default, &column[i].Extra, &column[i].Privileges, &column[i].Comment)

		//		fmt.Printf("%s, %s, %s, %s, %s, %s, %s, %s, %s\r\n", column[i].Field, column[i].Type,
		//			column[i].Collation.String, column[i].Null, column[i].Key, column[i].Default.String, column[i].Extra,
		//			column[i].Privileges, column[i].Comment)
		columCount++
	}

	return column[:columCount], nil
}

// generate select query
func generateSelectQuery(columnInfo []ColumnInfo, tablename string) (string, error) {
	var query string

	query = "SELECT "

	for i := 0; i < len(columnInfo); i++ {
		query += columnInfo[i].Field

		if i < len(columnInfo)-1 {
			query += ", "
		}
	}

	query += " FROM "
	query += tablename
	query += ";"

	//fmt.Println(query)
	return query, nil
}

func saveCSVFile(filename string, columns []string, datas map[int]([]string)) {

	csvfile, err := os.Create(filename)
	defer csvfile.Close()
	if err != nil {
		return
	}

	csvwriter := csv.NewWriter(csvfile)
	csvwriter.Write(columns)
	//csvwriter.Write(column)

	for i := 0; i < len(datas); i++ {
		csvwriter.Write(datas[i])
	}
	csvwriter.Flush()
}

func encodingfileUtf8ToAnsi(filename string, resultfilename string) {
	d, err := os.Create(resultfilename)
	defer d.Close()
	if err != nil {
		return
	}

	f, err := os.Open(filename)
	defer f.Close()

	if err != nil {
		fmt.Println("file open errer")
		return
	}

	r := korean.EUCKR.NewEncoder().Writer(d)
	io.Copy(r, f)
}

func ZipFiles(filename string, files []string) error {

	newfile, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer newfile.Close()

	zipWriter := zip.NewWriter(newfile)
	defer zipWriter.Close()

	// Add files to zip
	for _, file := range files {

		zipfile, err := os.Open(file)
		if err != nil {
			return err
		}
		defer zipfile.Close()

		// Get the file information
		info, err := zipfile.Stat()
		if err != nil {
			return err
		}

		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}

		// Change to deflate to gain better compression
		// see http://golang.org/pkg/archive/zip/#pkg-constants
		header.Method = zip.Deflate

		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}
		_, err = io.Copy(writer, zipfile)
		if err != nil {
			return err
		}
	}
	return nil
}
