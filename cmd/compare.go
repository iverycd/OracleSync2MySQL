package cmd

import (
	"fmt"
	"github.com/liushuochen/gotable"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strconv"
	"time"
)

var dbRowsSlice [][]string

//var dbRowsSlice2 []string

func init() {
	rootCmd.AddCommand(compareDbCmd)
}

var compareDbCmd = &cobra.Command{
	Use:   "compareDb",
	Short: "Compare entire source and target database table rows",
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
		// 获取源库的所有表
		if selFromYml { // 如果用了-s选项，从配置文件中获取表名以及sql语句
			tableMap = viper.GetStringMapStringSlice("tables")
		} else { // 不指定-s选项，查询源库所有表名
			tableMap = fetchTableMap(pageSize, excludeTab)
		}
		newLogger() //初始化logrus日志和定义日志文件切割
		// 创建运行日志目录
		//logDir, _ := filepath.Abs(CreateDateDir(""))
		//f, err := os.OpenFile(logDir+"/"+"run.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
		//if err != nil {
		//	log.Fatal(err)
		//}
		//defer func() {
		//	if err := f.Close(); err != nil {
		//		log.Fatal(err) // 或设置到函数返回值中
		//	}
		//}()
		//// log信息重定向到平面文件
		//multiWriter := io.MultiWriter(os.Stdout, f)
		//log.SetOutput(multiWriter)
		// 以下开始调用比对表行数的方法
		start := time.Now()
		// 用于控制协程goroutine运行时候的并发数,例如3个一批，3个一批的goroutine并发运行
		ch := make(chan struct{}, viper.GetInt("maxParallel"))
		//遍历tableMap
		for tableName := range tableMap { //获取单个表名
			ch <- struct{}{}
			wg2.Add(1)
			go compareTable(tableName, ch)
		}
		// 这里等待上面所有迁移数据的goroutine协程任务完成才会接着运行下面的主程序，如果这里不wait，上面还在迁移行数据的goroutine会被强制中断
		wg2.Wait()
		cost := time.Since(start)
		// 输出全库数量的表
		tableTotal, err := gotable.Create("Table", "SourceRows", "DestRows", "DestIsExist", "isOk")
		// 输出比对信息失败的表
		tableFailed, err := gotable.Create("Table", "SourceRows", "DestRows", "DestIsExist", "isOk")
		if err != nil {
			fmt.Println("Create table failed: ", err.Error())
			return
		}
		for _, r := range dbRowsSlice {
			if r[4] == "NO" {
				_ = tableFailed.AddRow(r)
			}
			_ = tableTotal.AddRow(r)
		}
		fmt.Println("Table Compare Total Result")
		tableTotal.Align("Table", 1)
		tableTotal.Align("SourceRows", 1)
		tableTotal.Align("DestRows", 1)
		tableTotal.Align("isOk", 1)
		tableTotal.Align("DestIsExist", 1)
		fmt.Println(tableTotal)
		tableFailed.Align("Table", 1)
		tableFailed.Align("SourceRows", 1)
		tableFailed.Align("DestRows", 1)
		tableFailed.Align("isOk", 1)
		tableFailed.Align("DestIsExist", 1)
		fmt.Println("Table Compare Result (Only Not Ok Displayed)")
		fmt.Println(tableFailed)
		fmt.Println("Table Compare finish elapsed time ", cost)
	},
}

func compareTable(tableName string, ch chan struct{}) {
	var (
		srcRows  int      // 源表行数
		destRows int      // 目标行数
		ret      []string // 比对结果切片
	)
	isOk := "YES"        // 行数是否相同
	destIsExist := "YES" // 目标表是否存在
	defer wg2.Done()
	// 查询源库表行数
	srcSql := fmt.Sprintf("select count(*) from \"%s\"", tableName)
	err := srcDb.QueryRow(srcSql).Scan(&srcRows)
	if err != nil {
		log.Error(err)
	}
	// 查询目标表行数
	destSql := fmt.Sprintf("select count(*) from `%s`", tableName)
	err = destDb.QueryRow(destSql).Scan(&destRows)
	if err != nil {
		log.Error(err)
		isOk, destIsExist = "NO", "NO" // 查询失败就是目标表不存在
	}
	if srcRows != destRows {
		isOk = "NO"
	}
	// 单行比对结果的切片
	ret = append(ret, tableName, strconv.Itoa(srcRows), strconv.Itoa(destRows), destIsExist, isOk)
	// 把每个单行切片追加到用于表格输出的切片里面
	dbRowsSlice = append(dbRowsSlice, ret)
	<-ch
}
