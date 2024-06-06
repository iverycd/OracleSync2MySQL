package logrus

import (
	"OracleSync2MySQL/pkg/logger"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func New() (*logger.ZapLogger, string) {
	c := logger.New()
	logDir, _ := filepath.Abs(CreateDateDir(""))
	c.SetDivision("time")                        // 设置归档方式，"time"时间归档 "size" 文件大小归档，文件大小等可以在配置文件配置
	c.SetTimeUnit(logger.Day)                    // 时间归档 可以设置切割单位
	c.SetEncoding("console")                     // 输出格式 "json" 或者 "console"
	c.Stacktrace = true                          // 添加 Stacktrace, 默认false
	c.SetInfoFile(logDir + "/" + "run.log")      // 设置info级别日志
	c.SetErrorFile(logDir + "/" + "run_err.log") // 设置warn级别日志
	c.SetEncodeTime("2006-01-02 15:04:05")       // 设置时间格式
	return c.InitLogger(), logDir                // 初始化
}

// CreateDateDir 根据当前日期来创建文件夹
func CreateDateDir(basePath string) string {
	folderName := "log/" + time.Now().Format("2006_01_02_15_04_05")
	folderPath := filepath.Join(basePath, folderName)
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		// 必须分成两步
		// 先创建文件夹
		err := os.MkdirAll(folderPath, 0777) //级联创建目录
		if err != nil {
			fmt.Println("create directory log failed ", err)
		}
		// 再修改权限
		err = os.Chmod(folderPath, 0777)
		if err != nil {
			fmt.Println("chmod directory log failed ", err)
		}
	}
	return folderPath
}
