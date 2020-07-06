package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql" // ....
	_ "github.com/lib/pq"              // ...
)

type configType struct {
	fileLog   string
	userDB    string
	passDB    string
	hostDB    string
	nameDB    string
	typedb    string
	lastDay   string
	lastDate  string
	logLevel  int
	numProxy  int
	numLines  int
	startTime time.Time
	endTime   time.Time
	lineAdded int
	lineRead  int
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
	flag.StringVar(&config.fileLog, "log", "/var/log/squid/access2.log", "Squid log file")
	flag.StringVar(&config.userDB, "u", "root", "User of DB")
	flag.StringVar(&config.passDB, "p", "", "Password of DB")
	flag.StringVar(&config.hostDB, "h", "localhost", "host of DB")
	flag.StringVar(&config.nameDB, "n", "squidreport2", "name of DB")
	flag.IntVar(&config.numLines, "nl", 1000, "Number of lines")
	flag.IntVar(&config.numProxy, "np", 1, "Number of proxy")
	flag.IntVar(&config.logLevel, "debug", 0, `Level log: 
		0 - silent, 
		1 - error, start and end, 
		2 - error, start and end, warning, 
		3 - error, start and end, warning, info,
		4 - error, start and end, warning, access granted and denided, request from squid `)
	// flag.IntVar(&config.ttl, "ttl", 300, "Defines the time after which data from the database will be updated in seconds")
	flag.Parse()
	if (config.typedb != "mysql") || (config.typedb != "postgres") {
		chkM("Error. typedb must be 'mysql' or 'postgres'.", nil)
	}
	if config.userDB == "" {
		chkM("Error. Username must be specified.", nil)
	}
	if config.logLevel > 4 {
		config.logLevel = 4
	}
	if config.logLevel != 0 {
		log.SetFlags(log.Ldate | log.Ltime)
		toLog(config.logLevel, 1, "go-fetch | Init started")
		fl, err := os.OpenFile(config.fileLog, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		chkM(fmt.Sprintf("Error opening to write log file (%v): ", config.fileLog), err)
		defer fl.Close()
		log.SetOutput(fl)
	} // If logLevel not specified - silent mode
}

func main() {
	config.startTime = time.Now()
	fmt.Printf("\n%v - Start All Job.\n", config.startTime.Format("2006-01-02T15:04:05.000"))

	// dsn := "user:password@(host_bd)/dbname"
	// db, err := sql.Open("mysql", dsn)
	databaseURL := fmt.Sprintf("%v:%v@(%v)/%v", config.userDB, config.passDB, config.hostDB, config.nameDB)
	db, err := newDB(config.typedb, databaseURL)
	chk(err)
	defer db.Close()

	store := newStore(db)

	config.lastDate = store.readLastDate(config.numProxy)

	config.lastDay = store.readLastDay(config.numProxy)

	err0 := store.prepareDB(config.lastDay, config.numProxy)
	if err0 != nil {
		chkM("Error delete old data", err)
	}

	// fmt.Printf("config.lastDate:%v, config.lastDay:%v\n", config.lastDate, config.lastDay)

	file, err := os.Open(config.fileLog)
	if err != nil {
		chkM("Error opening squid log file", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	err2 := store.squidLog2DBbyLine(scanner, &config)
	chk(err2)

	numStart := 0
	numEnd := config.numLines

	err3 := store.writeToDBTech(&config, numStart, numEnd)
	chk(err3)
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
	fmt.Printf("\n")
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
			toLog(cfg.logLevel, 2, fmt.Sprintf("%v\n", err))
			// fmt.Printf("%v\n", err)
			continue
		}

		if cfg.lastDate >= lineOut.date {
			toLog(cfg.logLevel, 3, "line too old\r")
			// fmt.Printf("line too old\r")
			continue
		}

		err2 := s.writeLineToDB(lineOut, config.numProxy)
		if err2 != nil {
			toLog(cfg.logLevel, 2, fmt.Sprintf("Write error(%v)\n", err2))
			// fmt.Printf("Write error(%v)\n", err2)
			continue
		}
		cfg.lineAdded = cfg.lineAdded + 1

		fmt.Printf("Lines read: %v, lines added: %v.", cfg.lineRead, cfg.lineAdded)

	}
	// fmt.Printf("\n")
	if err := scanner.Err(); err != nil {
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
	// t := time.Now()

	t := printTime("Start filling httpstatus, ", cfg.startTime)
	if _, err := s.db.Exec("INSERT INTO scsq_httpstatus (name) (select tmp.httpstatus from (select distinct httpstatus FROM scsq_temptraffic) as tmp left outer join scsq_httpstatus on tmp.httpstatus=scsq_httpstatus.name where scsq_httpstatus.name is null);"); err != nil {
		fmt.Printf("Error: %v", err)
	}

	t = printTime("Start filling scsq_ipaddress, ", t)
	if _, err := s.db.Exec("insert into scsq_ipaddress (name) (select tmp.ipaddress from (select distinct ipaddress from scsq_temptraffic) as tmp left outer join scsq_ipaddress on tmp.ipaddress=scsq_ipaddress.name where scsq_ipaddress.name is null);"); err != nil {
		fmt.Printf("Error: %v", err)
	}

	t = printTime("Start filling scsq_logins, ", t)
	if _, err := s.db.Exec("insert into scsq_logins (name) (select tmp.login from (select distinct login from scsq_temptraffic) as tmp left outer join scsq_logins on tmp.login=scsq_logins.name where scsq_logins.name is null);"); err != nil {
		fmt.Printf("Error: %v", err)
	}

	t = printTime("Start filling scsq_traffic, ", t)
	if _, err := s.db.Exec(`insert into scsq_traffic (date,ipaddress,login,httpstatus,sizeinbytes,site,method,mime,numproxy) select date,tmp.id,scsq_logins.id,scsq_httpstatus.id,sizeinbytes,site,method,mime,numproxy from scsq_temptraffic
	LEFT JOIN (select id,name from scsq_ipaddress
	RIGHT JOIN (select distinct ipaddress from scsq_temptraffic) as tt ON scsq_ipaddress.name=tt.ipaddress) as tmp ON scsq_temptraffic.ipaddress=tmp.name
	LEFT JOIN scsq_logins ON scsq_temptraffic.login=scsq_logins.name
	LEFT JOIN scsq_httpstatus ON scsq_temptraffic.httpstatus=scsq_httpstatus.name
	WHERE numproxy=?`, numOfProxy); err != nil {
		fmt.Printf("Error: %v", err)
	}

	t = printTime("Start delete from scsq_temptraffic, ", t)
	if _, err := s.db.Exec(`delete from scsq_temptraffic where numproxy=?`, numOfProxy); err != nil {
		fmt.Printf("Error: %v", err)
	}

	// Starting update scsq_quicktraffic
	t = printTime("Start filling scsq_quicktraffic, ", t)

	if _, err := s.db.Exec(`insert into scsq_quicktraffic (date,login,ipaddress,sizeinbytes,site,httpstatus,par, numproxy)
	SELECT 
	date,
	tmp2.login,
	tmp2.ipaddress,
	sum(tmp2.sizeinbytes),
	tmp2.st,
	tmp2.httpstatus,
	1,
	?

	FROM (SELECT 
	case

		when (SUBSTRING_INDEX(site,'/',1) REGEXP '^(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?')  
			then SUBSTRING_INDEX(SUBSTRING_INDEX(site,'/',1),'.',-2)
			else SUBSTRING_INDEX(site,'/',1) 
		end as st, 
	sizeinbytes,
	date,
	login,
	ipaddress,
	httpstatus
	FROM scsq_traffic
	where date>? and numproxy=?

	) as tmp2

	GROUP BY CRC32(tmp2.st),FROM_UNIXTIME(date,'%Y-%m-%d-%H'),login,ipaddress,httpstatus
	ORDER BY NULL;
	`, numOfProxy, lastDay, numOfProxy); err != nil {
		fmt.Printf("Error 3: %v", err)
	}

	// update2 scsq_quicktraffic
	t = printTime("Start update2 scsq_quicktraffic, ", t)
	if _, err := s.db.Exec(`insert into scsq_quicktraffic (date,login,ipaddress,sizeinbytes,site,par, numproxy)
	SELECT 
	tmp2.date,
	'0',
	'0',
	tmp2.sums,
	tmp2.st,
	2,
	?
	
	FROM (SELECT 
	case
	
		when (SUBSTRING_INDEX(site,'/',1) REGEXP '^(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?')  
			then SUBSTRING_INDEX(SUBSTRING_INDEX(site,'/',1),'.',-2)
			else SUBSTRING_INDEX(site,'/',1) 
		end as st, 
	sum(sizeinbytes) as sums,
	date
	FROM scsq_traffic
	where date>? and numproxy=?
	
	GROUP BY FROM_UNIXTIME(date,'%Y-%m-%d-%H'),crc32(st),date,site

	) as tmp2
	
	
	ORDER BY NULL;
	`, numOfProxy, lastDay, numOfProxy); err != nil {
		fmt.Printf("Error 4: %v", err)
	}

	t = printTime("Start filling scsq_logtable, ", t)
	cfg.endTime = time.Now()
	// #fill scsq_logtable
	if _, err := s.db.Exec(`insert into scsq_logtable (datestart,dateend,message) VALUES (?,?, ?);`,
		cfg.startTime, cfg.endTime, fmt.Sprintf("%v records read, %v records added", lineRead, lineAdded)); err != nil {
		fmt.Printf("Error 5: %v", err)
	}

	_ = printTime("", t)
	fmt.Printf(" The execution time of All Job: %v\n", time.Since(cfg.startTime))

	return nil
}

func printTime(text string, t time.Time) time.Time {
	fmt.Printf("\texecution time:%v\n%v %v", time.Since(t), time.Now().Format("2006-01-02T15:04:05.000"), text)
	return time.Now()
}
