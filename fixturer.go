package fixturer

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"

	"bitbucket.org/lazadaweb/squirrel"
	"flag"
	yaml "gopkg.in/yaml.v2"
)

type IFixturer interface {
	RecreateDatabaseWithSchemaAndImportFixtures() error
	RecreateDatabase() error
	LoadDbSchema() error
	ImportFixtures() error

	SetInsertGoroutinesCnt(int) IFixturer
}

type Fixturer struct {
	db                  *sql.DB
	dbConf              string
	schema              string
	fixturesPathYml     string
	recreateDatabase    bool
	dbName              string
	dbParams            string
	insertGoroutinesCnt int
}

type insertQuery struct {
	qb   *squirrel.InsertBuilder
	file string
}

const (
	InsertChannelCapacity      = 1000
	InsertGoroutinesDefaultCnt = 20
)

var (
	finishedTablseNames = []string{}
	finishedParsedDirs  = map[string]struct{}{}
	insertMap           = map[string]*squirrel.InsertBuilder{}
	recreateDatabase    = flag.Bool("recreateDatabase", true, "Do i need to recreate the database? default - true")
)

// NewFixturer create and returns new instance of &Fixturer.
// example dbConf root:222333@tcp(127.0.0.1:3306)/
func NewFixturer(dbConf, schema, fixturesPathYml, dbName, dbParams string) IFixturer {
	return &Fixturer{
		db:               nil,
		dbConf:           dbConf,
		schema:           schema,
		fixturesPathYml:  fixturesPathYml,
		recreateDatabase: *recreateDatabase,
		dbName:           dbName,
		dbParams:         dbParams,

		insertGoroutinesCnt: InsertGoroutinesDefaultCnt,
	}
}

// SetInsertGoroutinesCnt sets count of goroutines to perform table inserts.
func (this *Fixturer) SetInsertGoroutinesCnt(cnt int) IFixturer {
	if cnt < 1 {
		panic("Insert goroutines count must be > 1.")
	}
	this.insertGoroutinesCnt = cnt
	return this
}

func (this *Fixturer) RecreateDatabaseWithSchemaAndImportFixtures() error {

	if this.recreateDatabase == true {
		if err := this.RecreateDatabase(); err != nil {
			return err
		}
		if err := this.LoadDbSchema(); err != nil {
			return err
		}
	}
	return this.ImportFixtures()
}

// InitFixtures load and import test fixtures to test database
func (this *Fixturer) ImportFixtures() error {
	files, err := this.getYmlFilesList(this.fixturesPathYml)
	if err != nil {
		return err
	}

	if err := this.ensureDbConnected(); err != nil {
		return err
	}
	defer this.ensureDbDisconnected()

	if err := this.importYmlFixtures(files); err != nil {
		return err
	}

	return nil
}

// RecreateDatabase drops existing database and creates a clean one.
func (this *Fixturer) RecreateDatabase() error {

	// this.db is not used because this.db must be connected to the existing database that might not exists at the moment.
	db, err := sql.Open("mysql", this.dbConf)

	if err != nil {
		return err
	}
	log.Printf("Drop database %s", this.dbName)
	if _, err := db.Exec("DROP DATABASE IF EXISTS " + this.dbName); err != nil {
		return err
	}
	log.Printf("Create database %s", this.dbName)
	if _, err := db.Exec("CREATE DATABASE " + this.dbName); err != nil {
		return err
	}
	db.Close()

	return nil
}

// The return value of the function is intentionally left []os.FileInfo (but not []string)
// for the case when more file info needed.
func (this *Fixturer) getYmlFilesList(path string) ([]os.FileInfo, error) {

	files, err := ioutil.ReadDir(this.fixturesPathYml)
	if err != nil {
		return nil, err
	}

	var resultSlice []os.FileInfo
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".yml") {
			continue
		}

		resultSlice = append(resultSlice, file)
	}

	return resultSlice, nil
}

func (this *Fixturer) importYmlFixtures(files []os.FileInfo) error {
	// The caller of the function must ensureDbConnected() and ensureDbDisconnected() afterwards.

	log.Println("Import YML fixtures")
	var mutex = &sync.Mutex{}

	mutex.Lock()
	if _, find := finishedParsedDirs[this.fixturesPathYml]; find {
		this.loadParsedData()
		mutex.Unlock()
		return nil
	}

	mutex.Unlock()

	this.pushInsertQueriesFromYmlToChannel(files)

	finishedParsedDirs[this.fixturesPathYml] = struct{}{}

	return this.loadParsedData()
}

func (this *Fixturer) loadParsedData() error {

	if _, err := this.db.Exec("SET FOREIGN_KEY_CHECKS=0"); err != nil {
		return err
	}
	defer this.db.Exec("SET FOREIGN_KEY_CHECKS=1")

	for _, tableName := range finishedTablseNames {
		_, err := this.db.Exec("TRUNCATE " + tableName)
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	tx, err := this.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, query := range insertMap {
		queryString, queryValues, err := query.ToSql()

		if err != nil {
			fmt.Println(err)
		}

		if _, err := tx.Exec(queryString, queryValues...); err != nil {
			fmt.Println(err)
		}
	}
	if err := tx.Commit(); err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (this *Fixturer) pushInsertQueriesFromYmlToChannel(files []os.FileInfo) {
	var wg sync.WaitGroup
	wg.Add(len(files))

	tablesNames := []string{}
	var mutex = &sync.Mutex{}

	for _, f := range files {
		go func(f os.FileInfo) {
			defer wg.Done()

			filename := f.Name()
			if strings.HasSuffix(filename, ".yml") == false {
				return
			}
			data := make([]map[string]interface{}, 0, 10)

			y, _ := ioutil.ReadFile(this.fixturesPathYml + "/" + filename)

			if err := yaml.Unmarshal(y, &data); err != nil {
				log.Printf("Cant't read fixture %q. Origin error: %v", filename, err)
			}

			tableName := strings.TrimSuffix(filename, ".yml")
			mutex.Lock()
			tablesNames = append(tablesNames, tableName)
			mutex.Unlock()

			allKeysMap := map[string]struct{}{}
			for _, item := range data {
				for k := range item {
					allKeysMap[k] = struct{}{}
				}
			}

			allKeys := make([]string, 0, len(allKeysMap))

			for k := range allKeysMap {
				allKeys = append(allKeys, k)
			}

			qb := squirrel.Insert(tableName).Columns(allKeys...)

			for _, item := range data {
				qb.AddMap(item)
			}

			mutex.Lock()
			insertMap[filename] = qb
			mutex.Unlock()

			return
		}(f)
	}

	wg.Wait()

	mutex.Lock()
	finishedTablseNames = tablesNames
	mutex.Unlock()
	return
}

func (this *Fixturer) ensureDbConnected() error {
	if this.db != nil {
		return nil
	}
	dsn := this.dbConf + this.dbName
	if this.dbParams != "" {
		dsn += "?" + this.dbParams
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(this.insertGoroutinesCnt)
	db.SetMaxIdleConns(this.insertGoroutinesCnt)
	if err := db.Ping(); err != nil {
		return err
	}
	this.db = db
	return nil
}

func (this *Fixturer) ensureDbDisconnected() {
	// Ignore error.
	_ = this.db.Close()
	this.db = nil
}

func (this *Fixturer) LoadDbSchema() error {
	log.Println("Load database schema")

	if err := this.ensureDbConnected(); err != nil {
		return err
	}

	tx, err := this.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err = tx.Exec("SET FOREIGN_KEY_CHECKS=0"); err != nil {
		return err
	}
	defer tx.Exec("SET FOREIGN_KEY_CHECKS=1")

	if file, err := ioutil.ReadFile(this.schema); err == nil {
		queries := strings.Split(string(file), ";")

		for i := range queries {
			query := strings.TrimSpace(queries[i])
			if len(query) == 0 {
				continue
			}
			if _, err := tx.Exec(query); err != nil {
				return err
			}
		}
		return tx.Commit()
	} else {
		return err
	}
}
