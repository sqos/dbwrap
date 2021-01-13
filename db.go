package dbwrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var defaultDb = New(false, nil)

type AssociationFunc func(*gorm.DB) *gorm.DB

type DbMgt struct {
	debug           bool
	openFunc        func(dsn string) gorm.Dialector
	dsn             string
	models          []interface{}
	associationFunc []AssociationFunc

	log  logger.Interface
	cfg  *gorm.Config
	db   *gorm.DB
	lock sync.Mutex
}

func (c *DbMgt) SetDbParam(host, port, user, password, name string, ssl bool) *DbMgt {
	return c.SetPgParam(host, port, user, password, name, ssl)
}

func (c *DbMgt) SetMysqlParam(host, port, user, password, name, charset, loc string, parseTime bool) *DbMgt {
	if len(charset) <= 0 {
		charset = "utf8"
	}
	if len(loc) <= 0 {
		loc = "Local"
	}
	parsetime := "False"
	if parseTime {
		parsetime = "True"
	}
	c.openFunc = mysql.Open
	c.dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s&parseTime=%s&loc=%s", user, password, host, port, name, charset, parsetime, loc)
	return c
}

func (c *DbMgt) SetPgParam(host, port, user, password, name string, ssl bool) *DbMgt {
	c.openFunc = postgres.Open
	c.dsn += "host=" + host
	if len(port) > 0 {
		c.dsn += " port=" + port
	}
	if len(user) > 0 {
		c.dsn += " user=" + user
	}
	if len(password) > 0 {
		c.dsn += " password=" + password
	}
	if len(name) > 0 {
		c.dsn += " dbname=" + name
	}
	if ssl {
		c.dsn += " sslmode=enable"
	} else {
		c.dsn += " sslmode=disable"
	}
	return c
}

func (c *DbMgt) SetSqlite3Param(path string) *DbMgt {
	c.openFunc, c.dsn = sqlite.Open, path
	return c
}

func (c *DbMgt) SetSqlServerParam(host, port, user, password, name string) *DbMgt {
	c.openFunc = sqlserver.Open
	c.dsn = fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s", user, password, host, port, name)
	return c
}

func (c *DbMgt) Db() *gorm.DB {
	if c.debug {
		return c.db.Debug()
	} else {
		return c.db
	}
}

func (c *DbMgt) Open() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.db != nil {
		return nil
	}
	db, err := gorm.Open(c.openFunc(c.dsn), c.cfg)
	if err == nil {
		if sqlDB, err := db.DB(); err != nil {
			return err
		} else if err = sqlDB.Ping(); err != nil {
			return err
		}
		c.db = db
	}
	return err
}

func (c *DbMgt) close() error {
	if c.db == nil {
		return nil
	}
	if sqlDb, err := c.db.DB(); err != nil {
		return err
	} else {
		return sqlDb.Close()
	}
}

func (c *DbMgt) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.close()
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
			c.log.Error(nil, err.Error())
		}
	}
	return true
}

func (c *DbMgt) CreateTables(models ...interface{}) *DbMgt {
	c.models = append(c.models, models...)
	if err := c.db.AutoMigrate(c.models...); err != nil {
		c.close()
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
	c.OpenUntilOk(retryInterval)
	c.CreateTables(models...)
	return c
}

func (c *DbMgt) DropTableIfExists(models ...interface{}) *DbMgt {
	if err := c.Db().Migrator().DropTable(models...); err != nil && c.log != nil {
		c.log.Error(nil, err.Error())
	}
	return c
}

func (c *DbMgt) OpenUntilOkAndDropTableIfExistsThenCreateTables(retryInterval time.Duration, models ...interface{}) *DbMgt {
	c.OpenUntilOk(retryInterval)
	c.DropTableIfExists(models...).CreateTables(models...)
	return c
}

func (c *DbMgt) CommonDB() *sql.DB {
	if db, err := c.Db().DB(); err == nil {
		return db
	} else {
		return nil
	}
}

func (c *DbMgt) Keepalive(ctx context.Context, interval time.Duration) {
	tick := time.NewTicker(interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if db := c.CommonDB(); db != nil {
				if err := db.Ping(); err != nil && c.log != nil {
					c.log.Error(nil, err.Error())
				}
			}
			break
		}
	}
}

func DefaultDbMgt() *DbMgt {
	return defaultDb
}

func SetDbParam(host, port, user, password, name string, ssl bool) *DbMgt {
	return defaultDb.SetDbParam(host, port, user, password, name, ssl)
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

func SetSqlServerParam(host, port, user, password, name string) *DbMgt {
	return defaultDb.SetSqlServerParam(host, port, user, password, name)
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

func OpenUntilOkAndDropTableIfExistsThenCreateTables(retryInterval time.Duration, models ...interface{}) *DbMgt {
	return defaultDb.OpenUntilOkAndDropTableIfExistsThenCreateTables(retryInterval, models...)
}

func IsRecordNotFoundError(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound)
}

func CommonDB() *sql.DB {
	return defaultDb.CommonDB()
}

func Keepalive(ctx context.Context, interval time.Duration) {
	defaultDb.Keepalive(ctx, interval)
}

func New(debug bool, cfg *gorm.Config) *DbMgt {
	mgt := &DbMgt{debug: debug, cfg: cfg}
	if mgt.cfg == nil {
		mgt.cfg = &gorm.Config{
			PrepareStmt: true,
		}
	}
	mgt.log = logger.Default
	if debug {
		logger.Default = logger.New(
			log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lshortfile),
			logger.Config{SlowThreshold: 200 * time.Millisecond, LogLevel: logger.Warn, Colorful: true},
		)
		if mgt.cfg.Logger == nil {
			mgt.cfg.Logger = logger.Default
		}
	} else {
		logger.Default = logger.Discard
	}
	return mgt
}
