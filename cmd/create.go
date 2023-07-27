package cmd

import (
	"fmt"
	"github.com/spf13/viper"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var tableOnly bool

func init() {
	rootCmd.AddCommand(createTableCmd)
	//rootCmd.AddCommand(seqOnlyCmd)
	//rootCmd.AddCommand(idxOnlyCmd)
	//rootCmd.AddCommand(viewOnlyCmd)
	//rootCmd.AddCommand(onlyDataCmd)
	createTableCmd.Flags().BoolVarP(&tableOnly, "tableOnly", "t", false, "only create table true")
}

var createTableCmd = &cobra.Command{
	Use:   "createTable",
	Short: "Create meta table and no table data rows",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		// 获取配置文件中的数据库连接字符串
		connStr := getConn()
		// 每页的分页记录数,仅全库迁移时有效
		pageSize := viper.GetInt("pageSize")
		// 从配置文件中获取需要排除的表
		excludeTab := viper.GetStringSlice("exclude")
		PrepareSrc(connStr)
		PrepareDest(connStr)
		var tableMap map[string][]string
		// 以下是迁移数据前的准备工作，获取要迁移的表名以及该表查询源库的sql语句(如果有主键生成该表的分页查询切片集合，没有主键的统一是全表查询sql)
		if selFromYml { // 如果用了-s选项，从配置文件中获取表名以及sql语句
			tableMap = viper.GetStringMapStringSlice("tables")
		} else { // 不指定-s选项，查询源库所有表名
			tableMap = fetchTableMap(pageSize, excludeTab)
		}
		// 创建运行日志目录
		logDir, _ := filepath.Abs(CreateDateDir(""))
		f, err := os.OpenFile(logDir+"/"+"run.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				log.Fatal(err) // 或设置到函数返回值中
			}
		}()
		// log信息重定向到平面文件
		multiWriter := io.MultiWriter(os.Stdout, f)
		log.SetOutput(multiWriter)
		// 实例初始化，调用接口中创建目标表的方法
		var db Database
		start := time.Now()
		db = new(Table)
		// 用于控制协程goroutine运行时候的并发数,例如3个一批，3个一批的goroutine并发运行
		ch := make(chan struct{}, viper.GetInt("maxParallel"))
		//遍历tableMap
		for tableName := range tableMap { //获取单个表名
			if selFromYml { //-s自定义迁移表的时候，统一把yml文件的表名转为大写(否则查询语句的表名都是小写)，原因是map键值对(key:value)，key的值始终为小写的值
				tableName = strings.ToUpper(tableName)
			}
			ch <- struct{}{}
			wg2.Add(1)
			go db.TableCreate(logDir, tableName, ch)
		}
		// 这里等待上面所有迁移数据的goroutine协程任务完成才会接着运行下面的主程序，如果这里不wait，上面还在迁移行数据的goroutine会被强制中断
		wg2.Wait()
		cost := time.Since(start)
		log.Info("Table structure synced from MySQL to PolarDB ,Source Table Total ", tableCount, " Failed Total ", strconv.Itoa(failedCount))
		fmt.Println("Table Create finish elapsed time ", cost)
	},
}
