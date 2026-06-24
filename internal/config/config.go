package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config encapsulates all config required by the application.
type Config struct {
	Database struct {
		FilePath string `json:"filePath"`
	} `json:"database"`

	HttpServer struct {
		Addr           string   `json:"addr"`
		AllowedOrigins []string `json:"allowedOrigins"`
		// Read here: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Access-Control-Max-Age
		CorsMaxAgeSec int `json:"corsMaxAgeSec"`
	} `json:"httpServer"`

	Kafka struct {
		Brokers          []string `json:"brokers"`
		Username         string   `json:"username"`
		Password         string   `json:"password"`
		CACertPath       string   `json:"caCertPath"`
		ValidationsTopic string   `json:"validationsTopic"`
	} `json:"kafka"`

	Logger struct {
		// Leave empty for stdout logging.
		FilePath string `json:"filePath"`
		Level    string `json:"level"`
		Pretty   bool   `json:"pretty"`
	} `json:"logger"`

	Ripple struct {
		Addr string `json:"addr"`
	} `json:"ripple"`

	ValidationStreamProcessor struct {
		MaxBatchSize      int `json:"maxBatchSize"`
		AutoFlushDelaySec int `json:"autoFlushDelaySec"`
	} `json:"validationStreamProcessor"`

	DatabaseKafkaSynchronizer struct {
		MaxBatchSize    int `json:"maxBatchSize"`
		PollIntervalSec int `json:"pollIntervalSec"`
	} `json:"databaseKafkaSynchronizer"`
}

// Load config from the given JSON file.
func Load(jsonPath string) (Config, error) {
	content, err := os.ReadFile(jsonPath)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file at %s because: %w", jsonPath, err)
	}

	var config Config
	if err := json.Unmarshal(content, &config); err != nil {
		return Config{}, fmt.Errorf("failed to unmarshal config file at %s because: %w", jsonPath, err)
	}

	if err := validate(config); err != nil {
		return Config{}, fmt.Errorf("config is invalid: %w", err)
	}

	return config, nil
}

// validate the loaded config.
func validate(conf Config) error {
	if conf.Database.FilePath == "" {
		return fmt.Errorf("database.filePath is required")
	}

	if conf.HttpServer.Addr == "" {
		return fmt.Errorf("httpServer.addr is required")
	}
	if len(conf.HttpServer.AllowedOrigins) == 0 {
		return fmt.Errorf("httpServer.allowedOrigins are required")
	}
	if conf.HttpServer.CorsMaxAgeSec < 1 {
		return fmt.Errorf("httpServer.corsMaxAgeSec is required")
	}

	if len(conf.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers are required")
	}
	if conf.Kafka.ValidationsTopic == "" {
		return fmt.Errorf("kafka.validationsTopic is required")
	}

	if conf.Logger.Level == "" {
		return fmt.Errorf("logger.level is required")
	}

	if conf.Ripple.Addr == "" {
		return fmt.Errorf("ripple.addr is required")
	}

	if conf.ValidationStreamProcessor.MaxBatchSize < 1 {
		return fmt.Errorf("validationStreamProcessor.maxBatchSize is required")
	}
	if conf.ValidationStreamProcessor.AutoFlushDelaySec < 1 {
		return fmt.Errorf("validationStreamProcessor.autoFlushDelaySec is required")
	}

	if conf.DatabaseKafkaSynchronizer.MaxBatchSize < 1 {
		return fmt.Errorf("databaseKafkaSynchronizer.maxBatchSize is required")
	}
	if conf.DatabaseKafkaSynchronizer.PollIntervalSec < 1 {
		return fmt.Errorf("databaseKafkaSynchronizer.pollIntervalSec is required")
	}

	return nil
}
