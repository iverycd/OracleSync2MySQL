package cmd

import (
	"fmt"
	"github.com/liushuochen/gotable"
	"github.com/spf13/viper"
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
	rootCmd.AddCommand(onlyDataCmd)
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
		log.Info("Table structure synced finish,Source Table Total ", tableCount, " Failed Total ", strconv.Itoa(failedCount))
		fmt.Println("Table Create finish elapsed time ", cost)
	},
}

var onlyDataCmd = &cobra.Command{
	Use:   "onlyData",
	Short: "only transfer table data rows",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		// 获取配置文件中的数据库连接字符串
		connStr := getConn()
		start := time.Now()
		// map结构，表名以及该表用来迁移查询源库的语句
		var tableMap map[string][]string
		// 从配置文件中获取需要排除的表
		excludeTab := viper.GetStringSlice("exclude")
		log.Info("running SourceDB check connect")
		// 生成源库数据库连接
		PrepareSrc(connStr)
		defer srcDb.Close()
		// 每页的分页记录数,仅全库迁移时有效
		pageSize := viper.GetInt("pageSize")
		log.Info("running TargetDB check connect")
		// 生成目标库的数据库连接
		PrepareDest(connStr)
		defer destDb.Close()
		// 以下是迁移数据前的准备工作，获取要迁移的表名以及该表查询源库的sql语句(如果有主键生成该表的分页查询切片集合，没有主键的统一是全表查询sql)
		if selFromYml { // 如果用了-s选项，从配置文件中获取表名以及sql语句
			tableMap = viper.GetStringMapStringSlice("tables")
		} else { // 不指定-s选项，查询源库所有表名
			tableMap = fetchTableMap(pageSize, excludeTab)
		}
		// 从yml配置文件中获取迁移数据时最大运行协程数
		maxParallel := viper.GetInt("maxParallel")
		// 用于控制协程goroutine运行时候的并发数,例如3个一批，3个一批的goroutine并发运行
		ch := make(chan struct{}, maxParallel)
		// 同时执行goroutine的数量，这里是每个表查询语句切片集合的长度
		var goroutineSize int
		//遍历每个表需要执行的切片查询SQL，累计起来获得总的goroutine并发大小，即所有goroutine协程的数量
		for _, sqlList := range tableMap {
			goroutineSize += len(sqlList)
		}
		//遍历tableMap，先遍历表，再遍历该表的sql切片集合
		migDataStart := time.Now()
		for tableName, sqlFullSplit := range tableMap { //获取单个表名
			colName, colType, tableNotExist := preMigData(tableName, sqlFullSplit) //获取单表的列名，列字段类型
			if !tableNotExist {                                                    //目标表存在就执行数据迁移
				// 遍历该表的sql切片(多个分页查询或者全表查询sql)
				for index, sqlSplitSql := range sqlFullSplit {
					log.Info("Table ", tableName, " total task ", len(sqlFullSplit))
					ch <- struct{}{} //在没有被接收的情况下，至多发送n个消息到通道则被阻塞，若缓存区满，则阻塞，这里相当于占位置排队
					wg.Add(1)        // 每运行一个goroutine等待组加1
					go runMigration(logDir, index, tableName, sqlSplitSql, ch, colName, colType)
				}
			} else { //目标表不存在就往通道写1
				log.Info("table not exists ", tableName)
			}
		}
		// 这里等待上面所有迁移数据的goroutine协程任务完成才会接着运行下面的主程序，如果这里不wait，上面还在迁移行数据的goroutine会被强制中断
		wg.Wait()
		// 单独计算迁移表行数据的耗时
		migDataEnd := time.Now()
		migCost := migDataEnd.Sub(migDataStart)
		tableDataRet := []string{"TableData", migDataStart.Format("2006-01-02 15:04:05.000000"), migDataEnd.Format("2006-01-02 15:04:05.000000"), " - ", migCost.String()}
		// 数据库对象的迁移结果
		var rowsAll = [][]string{{}}
		// 表结构创建以及数据迁移结果追加到切片,进行整合
		rowsAll = append(rowsAll, tableDataRet)
		// 输出配置文件信息
		fmt.Println("------------------------------------------------------------------------------------------------------------------------------")
		Info()
		tblConfig, err := gotable.Create("SourceDb", "DestDb", "MaxParallel", "PageSize", "ExcludeCount")
		if err != nil {
			fmt.Println("Create tblConfig failed: ", err.Error())
			return
		}
		ymlConfig := []string{connStr.SrcHost + "-" + connStr.SrcUserName, connStr.DestHost + "-" + connStr.DestDatabase, strconv.Itoa(maxParallel), strconv.Itoa(pageSize), strconv.Itoa(len(excludeTab))}
		tblConfig.AddRow(ymlConfig)
		fmt.Println(tblConfig)
		// 输出迁移摘要
		table, err := gotable.Create("Object", "BeginTime", "EndTime", "FailedTotal", "ElapsedTime")
		if err != nil {
			fmt.Println("Create table failed: ", err.Error())
			return
		}
		for _, r := range rowsAll {
			_ = table.AddRow(r)
		}
		table.Align("Object", 1)
		table.Align("FailedTotal", 1)
		table.Align("ElapsedTime", 1)
		fmt.Println(table)
		// 总耗时
		cost := time.Since(start)
		log.Info(fmt.Sprintf("All complete totalTime %s The Report Dir %s", cost, logDir))
	},
}
