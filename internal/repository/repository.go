package repository

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/zapgorm2"

	"github.com/glebarez/sqlite"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const ctxTxKey = "TxKey"

type Repository struct {
	db *gorm.DB
	//rdb    *redis.Client
	//mongo  *mongo.Client
	logger *log.Logger
}

func NewRepository(
	logger *log.Logger,
	db *gorm.DB,
	// rdb *redis.Client,
	//
	//	mongo *mongo.Client,
) *Repository {
	return &Repository{
		db: db,
		//rdb:    rdb,
		//mongo:  mongo,
		logger: logger,
	}
}

type Transaction interface {
	Transaction(ctx context.Context, fn func(ctx context.Context) error) error
}

func NewTransaction(r *Repository) Transaction {
	return r
}

// DB return tx
// If you need to create a Transaction, you must call DB(ctx) and Transaction(ctx,fn)
func (r *Repository) DB(ctx context.Context) *gorm.DB {
	v := ctx.Value(ctxTxKey)
	if v != nil {
		if tx, ok := v.(*gorm.DB); ok {
			return tx
		}
	}
	return r.db.WithContext(ctx)
}

func (r *Repository) Transaction(ctx context.Context, fn func(ctx context.Context) error) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		ctx = context.WithValue(ctx, ctxTxKey, tx)
		return fn(ctx)
	})
}

func NewDB(conf *viper.Viper, l *log.Logger) *gorm.DB {
	var (
		db  *gorm.DB
		err error
	)

	logger := zapgorm2.New(l.Logger)
	driver := conf.GetString("data.db.user.driver")
	dsn := conf.GetString("data.db.user.dsn")

	// GORM doc: https://gorm.io/docs/connecting_to_the_database.html
	switch driver {
	case "postgres":
		db, err = gorm.Open(postgres.New(postgres.Config{
			DSN:                  dsn,
			PreferSimpleProtocol: true, // disables implicit prepared statement usage
		}), &gorm.Config{
			Logger: logger,
		})
	case "sqlite3":
		dbPath := strings.Split(dsn, "?")[0]
		if dbPath != ":memory:" {
			dir := filepath.Dir(dbPath)
			if dir != "." && dir != "/" && dir != "" {
				if err := os.MkdirAll(dir, 0755); err != nil {
					panic(fmt.Errorf("create sqlite3 db dir %s failed: %w", dir, err))
				}
			}
		}
		db, err = gorm.Open(sqlite.Open(dsn), &gorm.Config{
			Logger: logger,
		})
	default:
		panic("unknown db driver")
	}
	if err != nil {
		panic(err)
	}
	db = db.Debug()

	// Connection Pool config
	sqlDB, err := db.DB()
	if err != nil {
		panic(err)
	}
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)
	return db
}

// RedisConfig 统一 Redis 配置，从 viper 解析，避免各处重复读取。
type RedisConfig struct {
	Addr         string
	Password     string
	DB           int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
	MinIdleConns int
}

// LoadRedisConfig 从 viper 加载 Redis 配置，仅在 data.redis 存在时返回非 nil。
func LoadRedisConfig(conf *viper.Viper) *RedisConfig {
	if !conf.IsSet("data.redis") {
		return nil
	}
	cfg := &RedisConfig{
		Addr:         conf.GetString("data.redis.addr"),
		Password:     conf.GetString("data.redis.password"),
		DB:           conf.GetInt("data.redis.db"),
		ReadTimeout:  conf.GetDuration("data.redis.read_timeout"),
		WriteTimeout: conf.GetDuration("data.redis.write_timeout"),
		PoolSize:     conf.GetInt("data.redis.pool_size"),
		MinIdleConns: conf.GetInt("data.redis.min_idle_conns"),
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 10 * runtime.GOMAXPROCS(0)
	}
	return cfg
}

// ToOptions 将 RedisConfig 转换为 go-redis Options。
func (c *RedisConfig) ToOptions() *redis.Options {
	opts := &redis.Options{
		Addr:         c.Addr,
		Password:     c.Password,
		DB:           c.DB,
		PoolSize:     c.PoolSize,
		MinIdleConns: c.MinIdleConns,
	}
	if c.ReadTimeout > 0 {
		opts.ReadTimeout = c.ReadTimeout
	}
	if c.WriteTimeout > 0 {
		opts.WriteTimeout = c.WriteTimeout
	}
	return opts
}

// NewRedis 创建共享的 *redis.Client 单例，带健康检查和 cleanup 函数。
// cfg 为 nil 时返回 (nil, func() {}, nil)，调用方可据此做 graceful degradation。
func NewRedis(cfg *RedisConfig) (*redis.Client, func(), error) {
	if cfg == nil {
		return nil, func() {}, nil
	}

	rdb := redis.NewClient(cfg.ToOptions())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Printf("Warning: redis ping failed: %v. Degrading to memory mode.\n", err)
		return nil, func() {}, nil
	}

	cleanup := func() {
		_ = rdb.Close()
	}

	return rdb, cleanup, nil
}
func NewMongo(conf *viper.Viper) (*mongo.Client, func(), error) {
	// https://www.mongodb.com/zh-cn/docs/drivers/go/current/
	uri := conf.GetString("data.mongo.uri")
	client, err := mongo.Connect(context.TODO(), options.Client().
		ApplyURI(uri))
	if err != nil {
		panic(fmt.Sprintf("mongo client error: %s", err.Error()))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Ping(ctx, nil)
	if err != nil {
		panic(fmt.Sprintf("mongo ping error: %s", err.Error()))
	}

	return client, func() {
		err = client.Disconnect(ctx)
		if err != nil {
			panic(fmt.Sprintf("mongo disconnect error: %s", err.Error()))
		}
	}, err
}
