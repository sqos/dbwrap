package dbwrap

import (
	"database/sql"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"log"
	"os"
	"sync"
	"time"
)

var (
	defaultDb = &DbMgt{
		Log: log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lshortfile),
	}
)

type AssociationFunc func(*gorm.DB) *gorm.DB

type DbMgt struct {
	Host      string
	Port      string
	User      string
	Password  string
	Name      string
	Ssl       bool
	CharSet   string // default 'utf8'
	ParseTime string // value is 'True' and 'False', default 'True'
	Loc       string // default 'Local'
	Path      string
	Log       *log.Logger

	dbDriver        string
	db              *gorm.DB
	models          []interface{}
	associationFunc []AssociationFunc
	lock            sync.Mutex
}

func GetEnv(env, defaultEnv string) string {
	value := os.Getenv(env)
	if len(value) == 0 {
		value = defaultEnv
	}
	return value
}

func (c *DbMgt) SetPgParam(host, port, user, password, name string, ssl bool) *DbMgt {
	c.Host, c.Port = host, port
	c.User, c.Password = user, password
	c.Name = name
	c.Ssl = ssl
	c.dbDriver = "postgres"
	return c
}

func (c *DbMgt) SetMysqlParam(host, port, user, password, name, charset, loc string, parseTime bool) *DbMgt {
	c.Host, c.Port = host, port
	c.User, c.Password = user, password
	c.Name = name
	c.CharSet = charset
	c.Loc = loc
	if parseTime {
		c.ParseTime = "True"
	} else {
		c.ParseTime = "False"
	}
	c.dbDriver = "mysql"
	return c
}

func (c *DbMgt) SetSqlite3Param(path string) *DbMgt {
	c.Path = path
	c.dbDriver = "sqlite3"
	return c
}

func (c *DbMgt) SetMssqlParam(host, port, user, password, name string) *DbMgt {
	c.Host, c.Port = host, port
	c.User, c.Password = user, password
	c.Name = name
	c.dbDriver = "mssql"
	return c
}

func (c *DbMgt) constructPgArgs() string {
	var p string
	if len(c.Host) > 0 {
		p += "host=" + c.Host
	}
	if len(c.Port) > 0 {
		p += " port=" + c.Port
	}
	if len(c.User) > 0 {
		p += " user=" + c.User
	}
	if len(c.Password) > 0 {
		p += " password=" + c.Password
	}
	if len(c.Name) > 0 {
		p += " dbname=" + c.Name
	}
	if c.Ssl {
		p += " sslmode=enable"
	} else {
		p += " sslmode=disable"
	}
	return p
}

func (c *DbMgt) constructMysqlArgs() string {
	return fmt.Sprintf(
		"%s:%s@(%s:%s)/%s?charset=%s&parseTime=%s&loc=%s",
		c.User, c.Password, c.Host, c.Port, c.CharSet, c.ParseTime, c.Loc,
	)
}

func (c *DbMgt) constructSqlite3Args() string {
	return c.Path
}

func (c *DbMgt) constructMssqlArgs() string {
	return fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s", c.User, c.Password, c.Host, c.Port, c.Name)
}

func (c *DbMgt) Db() *gorm.DB {
	return c.db
}

func (c *DbMgt) open(driver, args string) error {
	db, err := gorm.Open(driver, args)
	if err == nil {
		c.db = db
	}
	return err
}

func (c *DbMgt) Open() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.db != nil {
		return nil
	}
	switch c.dbDriver {
	case "mysql":
		return c.open(c.dbDriver, c.constructMysqlArgs())
	case "postgres":
		return c.open(c.dbDriver, c.constructPgArgs())
	case "sqlite3":
		return c.open(c.dbDriver, c.constructSqlite3Args())
	case "mssql":
		return c.open(c.dbDriver, c.constructMssqlArgs())
	default:
		return fmt.Errorf("not support driver %v", c.dbDriver)
	}
}

func (c *DbMgt) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.db == nil {
		return nil
	}
	return c.db.Close()
}

func (c *DbMgt) Register(models ...interface{}) *DbMgt {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.models = append(c.models, models...)
	return c
}

func (c *DbMgt) RegisterAssociationFunc(funcs ...AssociationFunc) *DbMgt {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.associationFunc = append(c.associationFunc, funcs...)
	return c
}

func (c *DbMgt) OpenUntilOk(retryInterval time.Duration) bool {
	if err := c.Open(); err == nil {
		return true
	}
	for range time.NewTicker(retryInterval).C {
		if err := c.Open(); err == nil {
			return true
		} else {
			if c.Log != nil {
				c.Log.Println(err)
			}
		}
	}
	return true
}

func (c *DbMgt) CreateTables(models ...interface{}) *DbMgt {
	c.models = append(c.models, models...)
	for _, err := range c.db.AutoMigrate(c.models...).GetErrors() {
		if err == nil {
			continue
		}
		c.db.Close()
		panic(err)
	}
	for _, fc := range c.associationFunc {
		if fc != nil {
			fc(c.db)
		}
	}
	return c
}

func (c *DbMgt) OpenUntilOkAndCreateTables(retryInterval time.Duration, models ...interface{}) *DbMgt {
	if c.OpenUntilOk(retryInterval) {
		c.CreateTables(models...)
	} else {
		panic("open db failed")
	}
	return c
}

func (c *DbMgt) CommonDB() *sql.DB {
	if db, ok := c.Db().CommonDB().(*sql.DB); ok {
		return db
	} else {
		return nil
	}
}

func (c *DbMgt) SetLogger(l gorm.Logger) {
	c.Db().SetLogger(l)
}

func DefaultDbDbMgt() *DbMgt {
	return defaultDb
}

func SetDbParam(host, port, user, password, name string, ssl bool) *DbMgt {
	return SetPgParam(host, port, user, password, name, ssl)
}

func SetMysqlParam(host, port, user, password, name, charset, loc string, parseTime bool) *DbMgt {
	return defaultDb.SetMysqlParam(host, port, user, password, name, charset, loc, parseTime)
}

func SetPgParam(host, port, user, password, name string, ssl bool) *DbMgt {
	return defaultDb.SetPgParam(host, port, user, password, name, ssl)
}

func SetSqlite3Param(path string) *DbMgt {
	return defaultDb.SetSqlite3Param(path)
}

func SetMssqlParam(host, port, user, password, name string) *DbMgt {
	return defaultDb.SetMssqlParam(host, port, user, password, name)
}

func Db() *gorm.DB {
	return defaultDb.Db()
}

func Open() (err error) {
	return defaultDb.Open()
}

func Close() error {
	return defaultDb.Close()
}

func Register(models ...interface{}) *DbMgt {
	return defaultDb.Register(models...)
}

func RegisterAssociationFunc(funcs ...AssociationFunc) *DbMgt {
	return defaultDb.RegisterAssociationFunc(funcs...)
}

func OpenUntilOk(retryInterval time.Duration) bool {
	return defaultDb.OpenUntilOk(retryInterval)
}

func CreateTables(models ...interface{}) *DbMgt {
	return defaultDb.CreateTables(models...)
}

func OpenUntilOkAndCreateTables(retryInterval time.Duration, models ...interface{}) *DbMgt {
	return defaultDb.OpenUntilOkAndCreateTables(retryInterval, models...)
}

func IsRecordNotFoundError(err error) bool {
	return gorm.IsRecordNotFoundError(err)
}

func CommonDB() *sql.DB {
	return defaultDb.CommonDB()
}

func SetLogger(l gorm.Logger) {
	defaultDb.SetLogger(l)
}
