package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql" // ....
	_ "github.com/lib/pq"              // ...
	log "github.com/sirupsen/logrus"
)

type configType struct {
	fileLog     string
	PIDFileName string
	userDB      string
	passDB      string
	hostDB      string
	nameDB      string
	typedb      string
	lastDay     string
	lastDate    string
	LogLevel    string
	numProxy    int
	numLines    int
	startTime   time.Time
	endTime     time.Time
	lineAdded   int
	lineRead    int
}

type storeType struct {
	db *sql.DB
	sync.Mutex
	lines []lineOfLogType
}

type lineOfLogType struct {
	date string
	// dealy       string
	ipaddress   string
	httpstatus  string
	sizeInBytes string
	method      string
	siteName    string
	login       string
	mime        string
}

var (
	config configType
	// line   lineOfLogType
)

func init() {

	flag.StringVar(&config.typedb, "typedb", "mysql", `Type of DB: 
		'mysql' - MySQL, 
		'postgres' - PostgreSQL`)
	flag.StringVar(&config.fileLog, "log", "/var/log/squid/access.log", "Squid log file")
	flag.StringVar(&config.userDB, "u", "root", "User of DB")
	flag.StringVar(&config.passDB, "p", "", "Password of DB")
	flag.StringVar(&config.hostDB, "h", "localhost", "host of DB")
	flag.StringVar(&config.nameDB, "n", "squidreport2", "name of DB")
	flag.IntVar(&config.numLines, "nl", 1000, "Number of lines")
	flag.IntVar(&config.numProxy, "np", 1, "Number of proxy")
	flag.StringVar(&config.LogLevel, "loglevel", "debug", "Level log:")
	flag.StringVar(&config.PIDFileName, "pid", "/run/go-fetch.pid", "Patch to PID File")
	// flag.IntVar(&config.ttl, "ttl", 300, "Defines the time after which data from the database will be updated in seconds")
	flag.Parse()

	lvl, err := log.ParseLevel(config.LogLevel)
	if err != nil {
		log.Errorf("Error in determining the level of logs (%v). Installed by default = Info", config.LogLevel)
		lvl, _ = log.ParseLevel("info")
	}
	log.SetLevel(lvl)

	log.Debugf("Config: %#v",
		config)

	if config.typedb != "mysql" && config.typedb != "postgres" {
		log.Fatal("Error. typedb must be 'mysql' or 'postgres'.")
	}
	if config.userDB == "" {
		log.Fatal("Error. Username must be specified.")
	}
}

func main() {
	config.startTime = time.Now()
	log.Info("go-fetch | Init started")

	if err := CheckPIDFile(config.PIDFileName); err != nil {
		log.Fatal(err)
	}
	if err := writePID(config.PIDFileName); err != nil {
		log.Fatal(err)
	}

	// fmt.Printf("\n%v - Start All Job.\n", config.startTime.Format("2006-01-02 15:04:05.000"))

	// dsn := "user:password@(host_bd)/dbname"
	// db, err := sql.Open("mysql", dsn)
	databaseURL := fmt.Sprintf("%v:%v@(%v)/%v", config.userDB, config.passDB, config.hostDB, config.nameDB)
	db, err := newDB(config.typedb, databaseURL)
	if err != nil {
		log.Fatalf("%v", err)
	}
	defer db.Close()

	store := newStore(db)

	config.lastDate = store.readLastDate(config.numProxy)

	config.lastDay = store.readLastDay(config.numProxy)
	log.Infof("config.lastDate: %v, config.lastDay: %v", config.lastDate, config.lastDay)

	err0 := store.prepareDB(config.lastDay, config.numProxy)
	if err0 != nil {
		log.Fatal("Error delete old data", err)
	}

	// fmt.Printf("config.lastDate:%v, config.lastDay:%v\n", config.lastDate, config.lastDay)

	file, err := os.Open(config.fileLog)
	if err != nil {
		log.Fatal("Error opening squid log file", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	err2 := store.squidLog2DBbyLine(scanner, &config)
	if err2 != nil {
		log.Fatalf("%v", err)
	}

	numStart := 0

	err3 := store.writeToDBTech(&config, numStart, config.lineAdded)
	if err3 != nil {
		log.Fatalf("%v", err)
		os.Exit(1)
	}

	if err := os.Remove(config.PIDFileName); err != nil {
		log.Errorf("Error remove file(%v):%v", config.PIDFileName, err)
	}

}

func CheckPIDFile(filename string) error {
	// Просмотреть инфу о файле
	if stat, err := os.Stat(filename); err != nil {
		// Если его нет - запуститься
		if os.IsNotExist(err) {
			return nil
		}

		// Если время более 15 минут - удалить этот файл и запустить прогу
	} else if time.Since(stat.ModTime()) > 15*time.Minute {

		if err := os.Remove(filename); err != nil {
			log.Errorf("Error remove file(%v):%v", filename, err)
		}
		if err := writePID(filename); err != nil {
			return err
		}

		return nil
		// Если он есть и время его изменения менее 15 минут - не запускться.

	}
	return fmt.Errorf("go-fetch | already running")
}

func writePID(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("Error open file(%v):%v", filename, err)
	}
	defer file.Close()
	_, err2 := file.Write([]byte(fmt.Sprint(os.Getpid())))
	if err2 != nil {
		return fmt.Errorf("Error write file=(%v), data=(%v):%v", filename, os.Getpid(), err)
	}
	return nil
}

// #clear last date in table with data.
func (s *storeType) prepareDB(lastDay string, numProxy int) error {
	_, err := s.db.Exec("delete from scsq_quicktraffic where date>? and numproxy=?", lastDay, numProxy)
	if err != nil {
		return err
	}

	// #clear temptable to be sure, that table have no strange data before import.
	_, err2 := s.db.Exec("delete from scsq_temptraffic where numproxy=?", numProxy)
	if err2 != nil {
		return err
	}

	return nil
}

func (s *storeType) squidLog2DBbyLine(scanner *bufio.Scanner, cfg *configType) error {
	// fmt.Printf("\n")
	for scanner.Scan() { // Проходим по всему файлу до конца
		cfg.lineRead = cfg.lineRead + 1
		// cfg.lineAdded = cfg.lineAdded + 1
		// fmt.Printf("\rAttempt to add a line: %v - ", cfg.lineAdded)

		line := scanner.Text() // получем текст из линии
		if line == "" {
			continue
		}
		line = replaceQuotes(line)

		lineOut, err := s.parseLineToStruct(line)
		if err != nil {
			log.Errorf("%v\n", err)
			// fmt.Printf("%v\n", err)
			continue
		}

		if cfg.lastDate > lineOut.date {
			log.Trace("line too old\r")
			// fmt.Printf("line too old\r")
			continue
		}

		err2 := s.writeLineToDB(lineOut, config.numProxy)
		if err2 != nil {
			log.Errorf("Write error(%v)\n", err2)
			// fmt.Printf("Write error(%v)\n", err2)
			continue
		}
		cfg.lineAdded = cfg.lineAdded + 1

		fmt.Printf("\r%v go-fetch | Lines read: %v, lines added: %v.", time.Now().Format("2006/01/02 15:04:05"), cfg.lineRead, cfg.lineAdded)

	}
	fmt.Printf("\n")
	// fmt.Printf("\r%v\r", strings.Repeat(" ", 80))
	if err := scanner.Err(); err != nil {
		log.Errorf("%v\n", err)
		return err
	}
	return nil
}

func replaceQuotes(lineOld string) string {
	lineNew := strings.ReplaceAll(lineOld, "'", "&quot")
	line := strings.ReplaceAll(lineNew, `"`, "&quot")
	return line
}

func (s *storeType) parseLineToStruct(line string) (lineOfLogType, error) {
	var lineOut lineOfLogType
	valueArray := strings.Fields(line) // разбиваем на поля через пробел
	if len(valueArray) == 0 {          // проверяем длину строки, чтобы убедиться что строка нормально распарсилась\её формат
		return lineOut, fmt.Errorf("Error, string is empty") // если это не так то следующая линия
	}
	// if config.lastDate <= valueArray[0] {
	// 	return lineOut, fmt.Errorf("This is line already in DB")
	// }
	lineOut.date = valueArray[0]
	lineOut.ipaddress = valueArray[2]
	lineOut.httpstatus = valueArray[3]
	lineOut.sizeInBytes = valueArray[4]
	lineOut.method = valueArray[5]
	lineOut.siteName = valueArray[6]
	lineOut.login = valueArray[7]
	lineOut.mime = valueArray[9]
	return lineOut, nil
}

func (s *storeType) writeLineToDB(lineOut lineOfLogType, numOfProxy int) error {
	stmt, err := s.db.Prepare("INSERT INTO scsq_temptraffic (date,ipaddress,httpstatus,sizeinbytes,site,login,method,mime, numproxy) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	v := lineOut
	_, err2 := stmt.Exec(v.date, v.ipaddress, v.httpstatus, v.sizeInBytes, v.siteName, v.login, v.method, v.mime, numOfProxy)
	if err2 != nil {
		return err
	}
	return nil
}

func (s *storeType) readLastDay(numOfProxy int) string {
	row := s.db.QueryRow(`select unix_timestamp(from_unixtime(max(date),'%Y-%m-%d')) from scsq_quicktraffic where numproxy=?`, numOfProxy)
	result := ""
	err2 := row.Scan(&result)
	if err2 != nil {
		return ""
	}

	return result
}

func (s *storeType) readLastDate(numOfProxy int) string {
	row := s.db.QueryRow(`select max(date) from scsq_traffic where numproxy=?`, numOfProxy)
	result := ""
	err := row.Scan(&result)
	if err != nil {
		return ""
	}

	return result
}

func (s *storeType) writeToDBTech(cfg *configType, numStart, numEnd int) error {
	lastDay := cfg.lastDay
	numOfProxy := cfg.numProxy
	lineRead := cfg.lineRead
	lineAdded := cfg.lineAdded

	t := printTime("Start filling httpstatus, ", cfg.startTime)
	if _, err := s.db.Exec("INSERT INTO scsq_httpstatus (name) (select tmp.httpstatus from (select distinct httpstatus FROM scsq_temptraffic) as tmp left outer join scsq_httpstatus on tmp.httpstatus=scsq_httpstatus.name where scsq_httpstatus.name is null);"); err != nil {
		log.Errorf("Error filling httpstatus: %v", err)
	}

	t = printTime("Start filling scsq_ipaddress, ", t)
	if _, err := s.db.Exec("insert into scsq_ipaddress (name) (select tmp.ipaddress from (select distinct ipaddress from scsq_temptraffic) as tmp left outer join scsq_ipaddress on tmp.ipaddress=scsq_ipaddress.name where scsq_ipaddress.name is null);"); err != nil {
		log.Errorf("Error filling scsq_ipaddress: %v", err)
	}

	t = printTime("Start filling scsq_logins, ", t)
	if _, err := s.db.Exec("insert into scsq_logins (name) (select tmp.login from (select distinct login from scsq_temptraffic) as tmp left outer join scsq_logins on tmp.login=scsq_logins.name where scsq_logins.name is null);"); err != nil {
		log.Errorf("Error filling scsq_logins: %v", err)
	}

	t = printTime("Start filling scsq_traffic, ", t)
	if _, err := s.db.Exec(`insert into scsq_traffic (date,ipaddress,login,httpstatus,sizeinbytes,site,method,mime,numproxy) select date,tmp.id,scsq_logins.id,scsq_httpstatus.id,sizeinbytes,site,method,mime,numproxy from scsq_temptraffic
	LEFT JOIN (select id,name from scsq_ipaddress
	RIGHT JOIN (select distinct ipaddress from scsq_temptraffic) as tt ON scsq_ipaddress.name=tt.ipaddress) as tmp ON scsq_temptraffic.ipaddress=tmp.name
	LEFT JOIN scsq_logins ON scsq_temptraffic.login=scsq_logins.name
	LEFT JOIN scsq_httpstatus ON scsq_temptraffic.httpstatus=scsq_httpstatus.name
	WHERE numproxy=?`, numOfProxy); err != nil {
		log.Errorf("Error filling scsq_traffic: %v", err)
	}

	t = printTime("Start delete from scsq_temptraffic, ", t)
	if _, err := s.db.Exec(`delete from scsq_temptraffic where numproxy=?`, numOfProxy); err != nil {
		log.Errorf("Error deleting from scsq_temptraffic: %v", err)
	}

	// Starting update scsq_quicktraffic
	t = printTime("Start filling scsq_quicktraffic, ", t)

	if _, err := s.db.Exec(`insert into scsq_quicktraffic (date,login,ipaddress,sizeinbytes,site,httpstatus,par, numproxy)
	SELECT date, tmp2.login, tmp2.ipaddress, sum(tmp2.sizeinbytes), tmp2.st, tmp2.httpstatus, 1, ?
	FROM (SELECT case when (SUBSTRING_INDEX(site,'/',1) REGEXP '^(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?')  
		then SUBSTRING_INDEX(SUBSTRING_INDEX(site,'/',1),'.',-2)
		else SUBSTRING_INDEX(site,'/',1) 
		end as st, sizeinbytes, date, login, ipaddress, httpstatus
	FROM scsq_traffic
	where date>? and numproxy=?
 	) as tmp2
 	GROUP BY CRC32(tmp2.st),FROM_UNIXTIME(date,'%Y-%m-%d-%H'),login,ipaddress,httpstatus
	ORDER BY NULL;
	`, numOfProxy, lastDay, numOfProxy); err != nil {
		log.Errorf("Error filling scsq_quicktraffic: %v", err)
	}

	// update2 scsq_quicktraffic
	t = printTime("Start update2 scsq_quicktraffic, ", t)
	if _, err := s.db.Exec(`insert into scsq_quicktraffic (date,login,ipaddress,sizeinbytes,site,par, numproxy)
	SELECT tmp2.date, '0', '0', tmp2.sums, tmp2.st, 2, ?
	FROM (SELECT case
		when (SUBSTRING_INDEX(site,'/',1) REGEXP '^(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?')  
			then SUBSTRING_INDEX(SUBSTRING_INDEX(site,'/',1),'.',-2)
			else SUBSTRING_INDEX(site,'/',1) 
		end as st, 
	sum(sizeinbytes) as sums, date
	FROM scsq_traffic
	where date>? and numproxy=?
	GROUP BY FROM_UNIXTIME(date,'%Y-%m-%d-%H'),crc32(st),date,site
	) as tmp2
	ORDER BY NULL;
	`, numOfProxy, lastDay, numOfProxy); err != nil {
		log.Errorf("Error updating scsq_quicktraffic:%v", err)
	}

	t = printTime("Start filling scsq_logtable, ", t)
	cfg.endTime = time.Now()
	// #fill scsq_logtable
	if _, err := s.db.Exec(`insert into scsq_logtable (datestart,dateend,message) VALUES (?, ?, ?);`,
		cfg.startTime.Unix(), cfg.endTime.Unix(), fmt.Sprintf("%v entries read, of which new %v added", lineRead, lineAdded)); err != nil {
		log.Errorf("Error with filling scsq_logtable: %v", err)
	}

	_ = printTime("", t)
	// fmt.Printf("\n")
	log.Infof("go-fetch | execution time:%.8v", time.Since(cfg.startTime))

	return nil
}

func printTime(text string, t time.Time) time.Time {
	log.Infof("execution time:%v\n%v %v", time.Since(t), time.Now().Format("2006-01-02 15:04:05.000"), text)
	// fmt.Printf("\texecution time:%v\n%v %v", time.Since(t), time.Now().Format("2006-01-02 15:04:05.000"), text)
	return time.Now()
}
