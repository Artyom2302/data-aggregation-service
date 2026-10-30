package config

import (
	"fmt"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type WorkerPool struct {
	Size      int `yaml:"size"`
	TasksBuf  int `yaml:"tasks-buf"`
	ErrorsBuf int `yaml:"errors-buf"`
}

func (wp *WorkerPool) validate() error {
	if wp.Size <= 0 {
		return fmt.Errorf("worker pool size must be greater than zero")
	}
	if wp.TasksBuf <= 0 {
		return fmt.Errorf("worker pool tasks buffer must be greater than zero")
	}
	if wp.ErrorsBuf <= 0 {
		return fmt.Errorf("worker pool errors buffer must be greater than zero")
	}
	return nil
}

type Database struct {
	Dsn            string `yaml:"dsn" env:"DSN_URL"`
	MigrationsPath string `yaml:"migrations-path"`
}

func (d *Database) validate() error {
	if d.Dsn == "" {
		return fmt.Errorf("dsn is required")
	}
	if d.MigrationsPath == "" {
		return fmt.Errorf("migrations-path is required")
	}
	return nil
}

type HTTP struct {
	Addr              string        `yaml:"addr" env:"HTTP_ADDR" env-default:":8080"`
	ReadHeaderTimeout time.Duration `yaml:"read-header-timeout" env-default:"5s"`
	ReadTimeout       time.Duration `yaml:"read-timeout" env-default:"10s"`
	WriteTimeout      time.Duration `yaml:"write-timeout" env-default:"10s"`
	IdleTimeout       time.Duration `yaml:"idle-timeout" env-default:"60s"`
	HandlerTimeout    time.Duration `yaml:"handler-timeout" env-default:"5s"`
}

func (h *HTTP) validate() error {
	if h.ReadHeaderTimeout <= 0 {
		return fmt.Errorf("http read-header-timeout must be greater than zero")
	}
	if h.ReadTimeout <= 0 {
		return fmt.Errorf("http read-timeout must be greater than zero")
	}
	if h.WriteTimeout <= 0 {
		return fmt.Errorf("http write-timeout must be greater than zero")
	}
	if h.IdleTimeout <= 0 {
		return fmt.Errorf("http idle-timeout must be greater than zero")
	}
	if h.HandlerTimeout <= 0 {
		return fmt.Errorf("http handler-timeout must be greater than zero")
	}
	if h.HandlerTimeout >= h.WriteTimeout {
		return fmt.Errorf("http handler-timeout must be less than write-timeout")
	}
	return nil
}

type GRPC struct {
	Addr string `yaml:"addr" env:"GRPC_ADDR" env-default:":9090"`
}

type Kafka struct {
	Brokers []string `yaml:"brokers" env:"KAFKA_BROKERS"`
	Topic   string   `yaml:"topic" env:"KAFKA_TOPIC"`
	GroupID string   `yaml:"group-id" env:"KAFKA_GROUP_ID"`
}

func (k *Kafka) validate() error {
	if len(k.Brokers) == 0 {
		return fmt.Errorf("kafka brokers are required")
	}
	if k.Topic == "" {
		return fmt.Errorf("kafka topic is required")
	}
	if k.GroupID == "" {
		return fmt.Errorf("kafka group-id is required")
	}
	return nil
}

type Config struct {
	WorkerPool WorkerPool `yaml:"worker-pool"`
	Database   Database   `yaml:"database"`
	Kafka      Kafka      `yaml:"kafka"`
	HTTP       HTTP       `yaml:"http"`
	GRPC       GRPC       `yaml:"grpc"`

	ShutdownTimeout time.Duration `yaml:"shutdown-timeout" env-default:"20s"`
}

func (cfg *Config) Validate() error {
	if err := cfg.WorkerPool.validate(); err != nil {
		return err
	}
	if err := cfg.Database.validate(); err != nil {
		return err
	}
	if err := cfg.Kafka.validate(); err != nil {
		return err
	}
	if err := cfg.HTTP.validate(); err != nil {
		return err
	}
	if cfg.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown-timeout must be greater than zero")
	}
	return nil
}

func Load() (*Config, error) {
	configPath := os.Getenv("CONFIG_PATH")

	if configPath == "" {
		return nil, fmt.Errorf("CONFIG_PATH environment variable not set")
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("file does not exist: %s", configPath)
	}

	var cfg Config
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		return nil, fmt.Errorf("unable to read config: %w", err)
	}

	return &cfg, nil
}
