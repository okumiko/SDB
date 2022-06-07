package logger

func Errorf(format string, v ...interface{}) {
	_log.Errorf(format, v...)
}

func Fatalf(format string, v ...interface{}) {
	_log.Fatalf(format, v...)
}

func Warn(v ...interface{}) {
	_log.Warn(v...)
}
