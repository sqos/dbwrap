package db

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"log"
	"os"
	"sync"
	"time"
)

var (
	defaultDb = &DbMgt{}
)

type AssociationFunc func(*gorm.DB) *gorm.DB

type DbMgt struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
	Ssl      bool

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

func (c *DbMgt) SetDbParam(host, port, user, password, name string, ssl bool) *DbMgt {
	c.Host, c.Port = host, port
	c.User, c.Password = user, password
	c.Name = name
	c.Ssl = ssl
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

func (c *DbMgt) Db() *gorm.DB {
	return c.db
}

func (c *DbMgt) Open() (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.db != nil {
		return nil
	}
	c.db, err = gorm.Open("postgres", c.constructPgArgs())
	return
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
		if err := c.Open(); err != nil {
			log.Println("open db error, retry after %v, %v\n", retryInterval, err)
			return false
		}
		break
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

func DefaultDbDbMgt() *DbMgt {
	return defaultDb
}

func SetDbParam(host, port, user, password, name string, ssl bool) *DbMgt {
	defaultDb.SetDbParam(host, port, user, password, name, ssl)
	return defaultDb
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