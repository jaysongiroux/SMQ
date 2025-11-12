package testutils

import "github.com/jaysongiroux/smq/internal/config"

func CreateTestConfig() *config.Config {
	cfg := &config.Config{}
	cfg.LogLevel = "info"
	return cfg
}
