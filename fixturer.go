package fixturer

import (
	"io/ioutil"
	"log"
	"strings"

	yaml "gopkg.in/yaml.v2"
	"os"
	"database/sql"
	"fmt"
	"bitbucket.org/lazadaweb/squirrel"
)

// EsSyncService represents elastic search sync service
type IFixturer interface {
	InitFixtures() error
	LoadCsvFixtures() error
}

// EsSyncService represents elastic search sync service
type Fixturer struct {
	db *sql.DB
	schema string
	fixturesPathYml string
	fixturesPathCsv string
	databaseSuffix string
}

// NewEsSyncServer create and returns new instance of sync service
//example dbConf root:222333@tcp(127.0.0.1:3306)/lazada_catalog?loc=Local&parseTime=true&interpolateParams=true
func NewFixturer(dbConf, schema, fixturesPathYml string, fixturesPathCsv string, databaseSuffix string) IFixturer {
	db, err := sql.Open("mysql", dbConf)

	if err != nil {
		fmt.Println(err)
	}

	return &Fixturer{
		db : db,
		schema : schema,
		fixturesPathYml : fixturesPathYml,
		fixturesPathCsv : fixturesPathCsv,
		databaseSuffix : databaseSuffix,
	}
}

// InitFixtures load and import test fixtures to test database
func (this *Fixturer)InitFixtures() error {
consoleLog("InitFixtures", 0)
	_, err := this.db.Exec("SET FOREIGN_KEY_CHECKS=0")
	if err != nil {
		return err
	}
	defer this.db.Exec("SET FOREIGN_KEY_CHECKS=1")

	err = this.recreateDatabase()
	if err != nil {
		return err
	}

	err = this.createTables()
	if err != nil {
		return err
	}

	err = this.initYmlFixtures()
	if err != nil {
		return err
	}

	err = this.generateCsvFixtures()
	if err != nil {
		return err
	}

	return nil
}


func (this *Fixturer)recreateDatabase() error {
	var dbName string
	err := this.db.QueryRow("select database() as dbName").Scan(&dbName)
	if err != nil {
		return err
	}

	log.Printf("Drop %s", dbName)
	if _, err := this.db.Exec("DROP DATABASE IF EXISTS " + dbName); err != nil {
		return err
	}

	log.Printf("Create %s", dbName)
	if _, err := this.db.Exec("CREATE DATABASE IF NOT EXISTS " + dbName); err != nil {
		return err
	}

	log.Printf("Use %s", dbName)
	if _, err := this.db.Exec("USE " + dbName); err != nil {
		return err
	}

	return nil
}


func (this *Fixturer)createTables() error {
	consoleLog("createTables", 1)
	if file, err := ioutil.ReadFile(this.schema); err == nil {
		queries := strings.Split(string(file), ";")

		for i := range queries {
			query := strings.TrimSpace(queries[i])
			if len(query) == 0 {
				continue
			}
			if _, err := this.db.Exec(query); err != nil {
				fmt.Printf("#%v\n", this.db)
				return err
			}
		}
	} else {
		return err
	}

	return nil
}

func (this *Fixturer)initYmlFixtures() error {
	var err error
	_, err = this.db.Exec("SET FOREIGN_KEY_CHECKS=0")
	if err != nil {
		log.Println(err)
		return err
	}
	defer this.db.Exec("SET FOREIGN_KEY_CHECKS=1")

	insertQueries, err := this.getInsertQueriesFromYml()

	for _, insertQuerie := range insertQueries {
		queryString,queryValues,queryError := insertQuerie.ToSql()

		if queryError != nil {
			fmt.Println(queryError)
		}

		if _, err := this.db.Exec(queryString, queryValues...); err != nil {
			return err
		}
	}

	return nil
}

func (this *Fixturer) getInsertQueriesFromYml() ([]*squirrel.InsertBuilder, error){
	var err error
	insertQueries := []*squirrel.InsertBuilder{}

	files, _ := ioutil.ReadDir(this.fixturesPathYml)

	chData := make(chan *squirrel.InsertBuilder)
	filesCount := cap(files)
	chEnd := make(chan bool)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	for _, f := range files {
		go func(f os.FileInfo) {
			filename := f.Name()
			if strings.HasSuffix(filename, ".yml") == false {
				return
			}
			data := make([]map[string]interface{}, 0, 10)

			y, _ := ioutil.ReadFile(this.fixturesPathYml + "/" + filename)

			if err := yaml.Unmarshal(y, &data); err != nil {
				log.Printf("Cant't read fixture %q. Origin error: %v", filename, err)
				log.Println(err)
			}

			tableName := strings.TrimSuffix(filename, ".yml")

			for _, item := range data {
				keys := make([]string, 0, len(item))
				values := make([]interface{}, 0, len(item))
				for k, v := range item {
					keys = append(keys, k)
					values = append(values, v)
				}

				qb := squirrel.Insert(tableName).Columns(keys...).Values(values...)

				chData <- qb
			}
			chEnd <- true
			return
		}(f)
	}

	i:= 0
	for {
		select {
		case insertQuerie := <-chData:
			insertQueries = append(insertQueries, insertQuerie)
		case endData := <-chEnd:
			if endData == true {
				i++;
				if i == filesCount{
					return  insertQueries, nil
				}
			}
		}
//		fmt.Println("Eto counter :", endData, " HE PaBNo :", filesCount)
	}

	return  insertQueries, nil
}

func (this *Fixturer)loadYmlFixtures(filename string) error {

	data := make([]map[string]interface{}, 0, 10)

	y, _ := ioutil.ReadFile(this.fixturesPathYml + "/" + filename)

	if err := yaml.Unmarshal(y, &data); err != nil {
		log.Printf("Cant't read fixture %q. Origin error: %v", filename, err)
		return err
	}

	tableName := strings.TrimSuffix(filename, ".yml")

	for _, item := range data {
		keys := make([]string, 0, len(item))
		values := make([]interface{}, 0, len(item))
		for k, v := range item {
			keys = append(keys, k)
			values = append(values, v)
		}

		qb := squirrel.Insert(tableName).Columns(keys...).Values(values...)
		queryString,queryValues,queryError := qb.ToSql()

		if queryError != nil {
			fmt.Println(queryError)
		}

		if _, err := this.db.Exec(queryString, queryValues...); err != nil {
			return err
		}
	}

	return nil
}

func (this *Fixturer)generateCsvFixtures() error {
	consoleLog("generateCsvFixtures", 3)
	suiteCsvFixturesPath := this.fixturesPathCsv + "/" + this.databaseSuffix

	info, _ := os.Stat(suiteCsvFixturesPath)

	if info != nil {
		os.RemoveAll(suiteCsvFixturesPath)
	}

	err := os.MkdirAll(suiteCsvFixturesPath, 0777)
	if err != nil {
		return err

	}

	err = os.Chmod(suiteCsvFixturesPath, 0777)
	if err != nil {
		return err
	}

	files, _ := ioutil.ReadDir(this.fixturesPathYml)
	for _, f := range files {
		tableName := strings.TrimSuffix(f.Name(), ".yml")
		outFile := suiteCsvFixturesPath + "/" + tableName + ".csv"
		query := "SELECT * FROM " + tableName + " INTO OUTFILE '" + outFile + "'"
		_, err = this.db.Exec(query)

		if err != nil {
			return err
		}
	}

	return nil
}

func (this *Fixturer)LoadCsvFixtures() error {
	consoleLog("LoadCsvFixtures", 4)
	var err error
	_, err = this.db.Exec("SET FOREIGN_KEY_CHECKS=0")
	if err != nil {
		return err
	}
	defer this.db.Exec("SET FOREIGN_KEY_CHECKS=1")

	suiteCsvFixturesPath := this.fixturesPathCsv + "/" + this.databaseSuffix

	files, _ := ioutil.ReadDir(suiteCsvFixturesPath)

	if cap(files) == 0 {
		consoleLog("Csv Fixtures not load !!!!", 5)
	}

	for _, f := range files {
		filename := f.Name()
		if strings.HasSuffix(filename, ".csv") == false {
			continue
		}

		tableName := strings.TrimSuffix(filename, ".csv")

		truncateQuery := "TRUNCATE " + tableName
		_, err := this.db.Exec(truncateQuery)

		if err != nil {
			log.Println(err)
			return err
		}

		loadQuery := "LOAD DATA INFILE '" + suiteCsvFixturesPath + "/" + filename + "' INTO TABLE " + tableName
		_, err = this.db.Exec(loadQuery)

		if err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func consoleLog(param string, colorNum int64) {
	color := []string{
		"\033[0;31m",
		"\033[0;32m",
		"\033[0;33m",
		"\033[0;34m",
		"\033[0;35m",
		"\033[0;36m",
		"\033[0;37m",
		"\033[0;38m",
		"\033[0;39m",
		"\033[0;41m",
		"\033[0;42m",
	}
	fmt.Println(color[colorNum] + param + "\033[0m")
}