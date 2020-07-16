// Copyright 2018 sialot. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ezlog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// 日志级别常量
// LogLevel Constants
const (
	LVL_DEBUG = 1 << iota
	LVL_INFO
	LVL_WARN
	LVL_ERROR
)

// Log
//
// Filename      文件路径
// Pattern       日期表达式（可选，默认无）
// Suffix        日志文件后缀（可选，默认"log"）
// LogLevel      日志级别（可选，默认"LVL_DEBUG"）
// BufferSize    缓存容量
// autoFlush     自动flush标志
// curLogFile    当前日志文件
// buf           用于输出的日志数据缓存
type Log struct {
	Filename          string
	Pattern           string
	Suffix            string
	LogLevel          int
	BufferSize        int
	autoFlush         bool
	autoFlushDuration int
	mu                sync.Mutex
	curLogFile        *os.File
	buf               []byte
	isInited          bool
	isInitFailed      bool
	isFlushTiming     bool
}

// 初始化
// init
func (l *Log) init() error {

	if l.isInitFailed {
		return errors.New("init failed, can't output log")
	}

	if !l.isInited {

		l.mu.Lock()
		defer l.mu.Unlock()

		if !l.isInited {

			if l.Filename == "" {
				l.isInitFailed = true
				fmt.Printf("Filename can't be \"\"!\n")
				return errors.New("Filename can't be \"\"!\n")
			}

			// prepare the parent folder of the log file
			_dir := filepath.Dir(l.Filename)
			exist, err := isPathExist(_dir)
			if err != nil {
				l.isInitFailed = true
				fmt.Printf("get dir error![%v]\n", err)
				return err
			}

			if !exist {
				err := os.MkdirAll(_dir, os.ModePerm)
				if err != nil {
					l.isInitFailed = true
					fmt.Printf("make dir failed![%v]\n", err)
					return err
				}
			}

			if l.LogLevel == 0 {
				l.LogLevel = LVL_DEBUG
			}

			if l.Suffix == "" {
				l.Suffix = "log"
			}

			if l.BufferSize > 0 {
				l.autoFlush = true
				l.autoFlushDuration = 200
			}

			l.isInited = true
		}

	}

	return nil
}

// 判断文件或文件夹是否存在
// isPathExist
func isPathExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// 获取当前日志文件路径
// getLogPath of current log file
func (l *Log) getLogPath(t *time.Time) string {
	var buffer bytes.Buffer
	buffer.WriteString(l.Filename)

	if l.Pattern != "" {
		buffer.WriteString(t.Format(l.Pattern))
	}

	buffer.WriteString(".")
	buffer.WriteString(l.Suffix)
	return buffer.String()
}

// 创建并打开新文件
// createAndOpenFile
func (l *Log) createAndOpenFile(filepath string) error {

	exist, err := isPathExist(filepath)
	if err != nil {
		fmt.Printf("func isPathExist error![%v]\n", err)
		return err
	} else {

		if !exist {

			file, err := os.Create(filepath)
			defer file.Close()
			if err != nil {
				fmt.Printf("create new log file error![%v]\n", err)
				return err
			}
		}
	}

	logFile, err := os.OpenFile(filepath, os.O_RDWR|os.O_APPEND, 0644)

	if err != nil {
		fmt.Printf("open new log file error![%v]\n", err)
		return err
	}

	l.curLogFile = logFile
	return nil
}

// 准备日志文件
// 如果计算出的目标文件与当前打开的文件不符，关闭当前文件，创建并打开新文件。
// prepare log file
func (l *Log) prepareLogFile(filepath string) error {

	if l.curLogFile != nil {

		if strings.Compare(l.curLogFile.Name(), filepath) != 0 {

			if len(l.buf) != 0 {
				_, flushErr := l.curLogFile.Write(l.buf)
				if flushErr != nil {
					fmt.Printf("flush log error![%v]\n", flushErr)
					return flushErr
				}
				l.buf = l.buf[:0]
			}

			l.curLogFile.Close()
			err := l.createAndOpenFile(filepath)
			if err != nil {
				return err
			}

		}
	} else {

		err := l.createAndOpenFile(filepath)
		if err != nil {
			return err
		}
	}
	return nil
}

// 数字补零
// Cheap integer to fixed-width decimal ASCII. Give a negative width to avoid zero-padding.
func itoa(buf *[]byte, i int, wid int) {

	// Assemble decimal in reverse order.
	var b [20]byte
	bp := len(b) - 1
	for i >= 10 || wid > 1 {
		wid--
		q := i / 10
		b[bp] = byte('0' + i - q*10)
		bp--
		i = q
	}
	// i < 10
	b[bp] = byte('0' + i)
	*buf = append(*buf, b[bp:]...)
}

// 添加日志级别信息
// append level info
func appendLevel(buf *[]byte, level int) {

	var prefix string
	switch level {
	case LVL_DEBUG:
		prefix = "[Debug]"
	case LVL_INFO:
		prefix = "[Info]"
	case LVL_WARN:
		prefix = "[Warn]"
	case LVL_ERROR:
		prefix = "[Error]"
	}

	*buf = append(*buf, prefix...)
}

// 输出数据到缓存
// output log data to buf
func (l *Log) output(msg string, level int) error {
	l.mu.Lock()
	t := time.Now()
	err := l.prepareLogFile(l.getLogPath(&t))
	if err != nil {
		l.mu.Unlock()
		flushErr := l.Flush()
		if flushErr != nil {
			return flushErr
		}
		return err
	}

	//format Header
	year, month, day := t.Date()
	itoa(&l.buf, year, 4)
	l.buf = append(l.buf, '/')
	itoa(&l.buf, int(month), 2)
	l.buf = append(l.buf, '/')
	itoa(&l.buf, day, 2)
	l.buf = append(l.buf, ' ')

	hour, min, sec := t.Clock()
	itoa(&l.buf, hour, 2)
	l.buf = append(l.buf, ':')
	itoa(&l.buf, min, 2)
	l.buf = append(l.buf, ':')
	itoa(&l.buf, sec, 2)

	l.buf = append(l.buf, '.')
	itoa(&l.buf, t.Nanosecond()/1e6, 3)
	l.buf = append(l.buf, ' ')

	// log level
	appendLevel(&l.buf, level)

	// log msg
	l.buf = append(l.buf, msg...)
	if len(msg) == 0 || msg[len(msg)-1] != '\n' {
		l.buf = append(l.buf, '\n')
	}

	l.mu.Unlock()
	return nil
}

// 写日志
// write log
func (l *Log) writeLog(msg string, level int) error {

	err := l.init()
	if err != nil {
		return err
	} else {

		if l.LogLevel <= level {

			l.output(msg, level)

			if len(l.buf) > l.BufferSize {
				err := l.Flush()
				if err != nil {
					return err
				}
			} else {
				if (!l.isFlushTiming) && l.autoFlush {
					l.isFlushTiming = true

					go func() {
						if l.autoFlush {
							time.Sleep(time.Duration(l.autoFlushDuration) * time.Millisecond)
							l.Flush()
							l.isFlushTiming = false
						}
					}()
				}
			}
		}
		return nil
	}
}

// 清空缓冲区，写入日志文件
// Flush
func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.buf) != 0 {
		_, err := l.curLogFile.Write(l.buf)
		if err != nil {
			fmt.Printf("flush log error![%v]\n", err)
			return err
		}
		l.buf = l.buf[:0]
	}

	return nil
}

// 关闭自动flush
// disable auto flush
func (l *Log) DisableAutoFlush() error {

	err := l.init()
	if err != nil {
		return err
	} else {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.autoFlush = false
		return nil
	}
}

// 设置自动Flush间隔
// SetFlushDuration
func (l *Log) SetFlushDuration(duration int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.autoFlushDuration = duration
}

// 业务日志，不受日志级别影响
// Arguments are handled in the manner of fmt.Printf.
func (l *Log) Printf(msg string, v ...interface{}) {
	l.writeLog(fmt.Sprintf(msg, v...), 999)
}

// 业务日志，不受日志级别影响
func (l *Log) Print(msg string) {
	l.writeLog(msg, 999)
}

// debug级别
func (l *Log) Debug(msg string) {
	l.writeLog(msg, LVL_DEBUG)
}

// info级别
func (l *Log) Info(msg string) {
	l.writeLog(msg, LVL_INFO)
}

// warn级别
func (l *Log) Warn(msg string) {
	l.writeLog(msg, LVL_WARN)
}

// error级别
func (l *Log) Error(msg string) {
	l.writeLog(msg, LVL_ERROR)
}
