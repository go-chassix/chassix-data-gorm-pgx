package data_gorm_pgx

import (
	"database/sql"
	"errors"
	"sync"
	"time"

	pg "gorm.io/driver/postgres"
	"gorm.io/gorm"

	gormx "c5x.io/data-gorm"
	"c5x.io/logx"
)

var log = logx.New().Category("data-gorm").Component("pgx")

var (
	connOnce sync.Once
)

func init() {
	gormx.RegisterDriver(gormx.DriverPostgres, new(PostgresProvider))
}

type PostgresProvider struct {
}

//Connect impl DatabaseProvider for gorm postgres
func (p PostgresProvider) Connect(config *gormx.DatabaseConfig) (*gorm.DB, error) {
	if config.Dialect == gormx.DriverPostgres {
		if sqlDB, err := sql.Open("postgres", config.DSN); err == nil {
			if db, err := gorm.Open(pg.New(pg.Config{Conn: sqlDB}), &gorm.Config{
				Logger: gormx.DefaultLogger(&config.Logger),
			}); err == nil {
				if config.MaxIdle > 0 {
					sqlDB.SetMaxIdleConns(config.MaxIdle)
				}
				if config.MaxOpen > 0 && config.MaxOpen > config.MaxIdle {
					sqlDB.SetMaxOpenConns(100)
				}
				if config.MaxLifetime > 0 {
					sqlDB.SetConnMaxLifetime(time.Duration(config.MaxLifetime) * time.Second)
				}
				return db, nil
			}
		} else {
			log.Errorf("connect db failed: error=%s", err.Error())
		}
		return nil, errors.New("open DB failed")
	}
	return nil, errors.New("driver is not postgres")
}
