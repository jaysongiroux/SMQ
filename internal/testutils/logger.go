package testutils

import "github.com/jaysongiroux/smq/internal/logger"

func CreateTestLogger() *logger.Logger {
	return logger.New("test", CreateTestConfig())
}
