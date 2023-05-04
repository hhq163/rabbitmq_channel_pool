package base

import (
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var Log *Logger

const (
	MAX_FILE_SIZE = 100 * 1024 * 1024 //100M
)

//Logger 异步日志
type Logger struct {
	warn    bool
	info    bool
	tformat func() string
	file    chan interface{}
}

//创建异步日志
func LogInit(level string, buf int) error {
	Log = &Logger{tformat: format}

	File, _ := os.OpenFile("server.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)
	if File != nil {
		FileInfo, err := File.Stat()
		if err != nil {
			return err
		}
		mode := strings.Split(FileInfo.Mode().String(), "-")
		if strings.Contains(mode[1], "w") {
			strChan := make(chan interface{}, buf)
			Log.file = strChan
			go func() {
				for msg := range strChan {
					fileInfo, _ := File.Stat()
					if fileInfo.Size() > MAX_FILE_SIZE {
						newName := time.Now().Format("2006-01-02") + "-" + strconv.Itoa(time.Now().Hour()) + "_server.log"
						log.Println("file newName=", newName)
						err := os.Rename("server.log", newName)
						if err != nil { //rename失败
							log.Println("rename server.log failed err:", err)
						}

						File, err = os.Create("server.log")
						if err != nil {
							log.Println("Create server.log failed err:", err)
						}
					}

					fmt.Fprintln(File, msg)
				}
			}()
			defer func() {
				for len(strChan) > 0 {
					time.Sleep(1e9)
				}
			}()
		} else {
			return errors.New("can't write.")
		}
	}
	switch level {
	case "Warn":
		Log.warn = true
		return nil
	case "Info":
		Log.warn = true
		Log.info = true
		return nil
	}
	return errors.New("level must be Warn or Info.")
}

func (log *Logger) Fatal(info ...interface{}) {

	if log.file != nil {
		log.file <- fmt.Sprintln("Fatal", log.tformat(), info)
	}
	os.Exit(1)
}

//Error 错误级别
func (log *Logger) Error(info ...interface{}) {
	if log.file != nil {
		log.file <- fmt.Sprintln("Error", log.tformat(), info)
	}
}

func (log *Logger) Warn(info ...interface{}) {
	if log.file != nil {
		log.file <- fmt.Sprintln("Warn", log.tformat(), info)
	}
}
func (log *Logger) Info(info ...interface{}) {
	if log.file != nil {
		log.file <- fmt.Sprintln("Info", log.tformat(), info)
	}
}

func (log *Logger) Close() {
	for len(log.file) > 0 {
		time.Sleep(1e8)
	}
}

func format() string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		file = "???"
		line = 0
	}
	return time.Now().Format("2006-01-02 15:04:05") + fmt.Sprintf(" %s:%d ", file, line)
}
