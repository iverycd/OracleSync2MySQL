package cmd

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/mitchellh/go-homedir"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"OracleSync2MySQL/connect"
	"github.com/liushuochen/gotable"
	"github.com/spf13/viper"
)

var log = logrus.New()
var cfgFile string
var selFromYml bool

var wg sync.WaitGroup
var wg2 sync.WaitGroup

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "OracleSync2MySQL",
	Short: "",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		// 获取配置文件中的数据库连接字符串
		connStr := getConn()
		startDataTransfer(connStr)
	},
}

func startDataTransfer(connStr *connect.DbConnStr) {
	// 创建运行日志目录
	logDir, _ := filepath.Abs(CreateDateDir(""))
	// 输出调用文件以及方法位置
	log.SetReportCaller(true)
	f, err := os.OpenFile(logDir+"/"+"run.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	// log信息重定向到平面文件
	multiWriter := io.MultiWriter(os.Stdout, f)
	log.SetOutput(multiWriter)
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
	// 实例初始化，调用接口中创建目标表的方法
	var db Database
	db = new(Table)
	// 从yml配置文件中获取迁移数据时最大运行协程数
	maxParallel := viper.GetInt("maxParallel")
	// 用于控制协程goroutine运行时候的并发数,例如3个一批，3个一批的goroutine并发运行
	ch := make(chan struct{}, maxParallel)
	startTbl := time.Now()
	for tableName := range tableMap { //获取单个表名
		ch <- struct{}{}
		wg2.Add(1)
		go db.TableCreate(logDir, tableName, ch)
	}
	wg2.Wait()
	endTbl := time.Now()
	tableCost := time.Since(startTbl)
	// 创建表完毕
	log.Info("Table structure synced from MySQL to PostgreSQL ,Source Table Total ", tableCount, " Failed Total ", strconv.Itoa(failedCount))
	tabRet = append(tabRet, "Table", startTbl.Format("2006-01-02 15:04:05.000000"), endTbl.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), tableCost.String())
	fmt.Println("Table Create finish elapsed time ", tableCost)
	// 创建表之后，开始准备迁移表行数据
	// 同时执行goroutine的数量，这里是每个表查询语句切片集合的长度
	var goroutineSize int
	//遍历每个表需要执行的切片查询SQL，累计起来获得总的goroutine并发大小，即所有goroutine协程的数量
	for _, sqlList := range tableMap {
		goroutineSize += len(sqlList)
	}
	// 每个goroutine运行开始以及结束之后使用的通道，主要用于控制内层的goroutine任务与外层main线程的同步，即主线程需要等待子任务完成
	// ch := make(chan int, goroutineSize)  //v0.1.4及之前的版本通道使用的通道，配合下面for循环遍历行数据迁移失败的计数
	// 在协程里运行函数response，主要是从下面调用协程go runMigration的时候获取到里面迁移行数据失败的数量
	//go response()
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
	// v0.1.4版本之前通过循环获取ch通道里写的int数据判断是否有迁移行数据失败的表，如果通道里发送的数据是2说明copy失败了
	//migDataFailed := 0
	// 这里是等待上面所有goroutine任务完成，才会执行for循环下面的动作
	//for i := 0; i < goroutineSize; i++ {
	//	migDataRet := <-ch
	//	log.Info("goroutine[", i, "]", " finish ", time.Now().Format("2006-01-02 15:04:05.000000"))
	//	if migDataRet == 2 {
	//		migDataFailed += 1
	//	}
	//}
	tableDataRet := []string{"TableData", migDataStart.Format("2006-01-02 15:04:05.000000"), migDataEnd.Format("2006-01-02 15:04:05.000000"), " - ", migCost.String()}
	// 数据库对象的迁移结果
	var rowsAll = [][]string{{}}
	// 表结构创建以及数据迁移结果追加到切片,进行整合
	rowsAll = append(rowsAll, tabRet, tableDataRet)
	// 如果指定-s模式不创建下面对象
	if selFromYml != true {
		//	// 创建序列
		//	seqRet := db.SeqCreate(logDir)
		// 创建索引、约束
		ch := make(chan struct{}, maxParallel)
		id := 0
		failedCount = 0
		var idxRet []string
		startTime := time.Now()
		for tableName := range tableMap { //获取单个表名
			id += 1
			ch <- struct{}{}
			wg2.Add(1)
			go db.IdxCreate(logDir, tableName, ch, id)
		}
		wg2.Wait()
		endTime := time.Now()
		cost := time.Since(startTime)
		idxRet = append(idxRet, "Index", startTime.Format("2006-01-02 15:04:05.000000"), endTime.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), cost.String())
		//	// 创建外键
		//	fkRet := db.FKCreate(logDir)
		//	// 创建视图
		//	viewRet := db.ViewCreate(logDir)
		//	// 创建触发器
		//	triRet := db.TriggerCreate(logDir)
		// 以上对象迁移结果追加到切片,进行整合
		rowsAll = append(rowsAll, idxRet)
		//rowsAll = append(rowsAll, seqRet, idxRet, fkRet, viewRet, triRet)
	}
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
}

// 自动对表分析，然后生成每个表用来迁移查询源库SQL的集合(全表查询或者分页查询)
// 自动分析是否有排除的表名
// 最后返回map结构即 表:[查询SQL]
func fetchTableMap(pageSize int, excludeTable []string) (tableMap map[string][]string) {
	var tableNumber int // 表总数
	var sqlStr string   // 查询源库获取要迁移的表名
	// 声明一个等待组
	var wg sync.WaitGroup
	// 使用互斥锁 sync.Mutex才能使用并发的goroutine
	mutex := &sync.Mutex{}
	log.Info("exclude table ", excludeTable)
	// 如果配置文件中exclude存在表名，使用not in排除掉这些表，否则获取到所有表名
	if excludeTable != nil {
		sqlStr = "select table_name from user_tables where table_name not in ("
		buffer := bytes.NewBufferString("")
		for index, tabName := range excludeTable {
			if index < len(excludeTable)-1 {
				buffer.WriteString("'" + tabName + "'" + ",")
			} else {
				buffer.WriteString("'" + tabName + "'" + ")")
			}
		}
		sqlStr += buffer.String()
	} else {
		sqlStr = "select table_name from user_tables" // 获取库里全表名称
	}
	// 查询下源库总共的表，获取到表名
	rows, err := srcDb.Query(sqlStr)
	defer rows.Close()
	if err != nil {
		log.Error(fmt.Sprintf("Query "+sqlStr+" failed,\nerr:%v\n", err))
		return
	}
	var tableName string
	//初始化外层的map，键值对，即 表名:[sql语句...]
	tableMap = make(map[string][]string)
	for rows.Next() {
		tableNumber++
		// 每一个任务开始时, 将等待组增加1
		wg.Add(1)
		var sqlFullList []string
		err = rows.Scan(&tableName)
		if err != nil {
			log.Error(err)
		}
		// 使用多个并发的goroutine调用函数获取该表用来执行的sql语句
		log.Info(time.Now().Format("2006-01-02 15:04:05.000000"), "ID[", tableNumber, "] ", "prepare ", tableName, " TableMap")
		go func(tableName string, sqlFullList []string) {
			// 使用defer, 表示函数完成时将等待组值减1
			defer wg.Done()
			// !tableOnly即没有指定-t选项，生成全库的分页查询语句，否则就是指定了-t选项,sqlFullList仅追加空字符串
			if !tableOnly {
				sqlFullList = prepareSqlStr(tableName, pageSize)
				if len(sqlFullList) == 0 { // 如果表没有数据，手动append一条1=0的sql语句,否则该表不会被创建，compareDb运行也会不准确
					sqlFullList = append(sqlFullList, fmt.Sprintf("select * from \"%s\" where 1=0", tableName))
				}
			} else {
				sqlFullList = append(sqlFullList, "")
			}
			// 追加到内层的切片，sql全表扫描语句或者分页查询语句，例如tableMap[test1]="select * from test1"
			for i := 0; i < len(sqlFullList); i++ {
				mutex.Lock()
				tableMap[tableName] = append(tableMap[tableName], sqlFullList[i])
				mutex.Unlock()
			}
		}(tableName, sqlFullList)
	}
	// 等待所有的任务完成
	wg.Wait()
	return tableMap
}

// 迁移数据前先清空目标表数据，并获取每个表查询语句的列名以及列字段类型,表如果不存在返回布尔值true
func preMigData(tableName string, sqlFullSplit []string) (dbCol []string, dbColType []string, tableNotExist bool) {
	var sqlCol string
	// 在写数据前，先清空下目标表数据
	truncateSql := "truncate table " + fmt.Sprintf("`") + tableName + fmt.Sprintf("`")
	if _, err := destDb.Exec(truncateSql); err != nil {
		log.Error("truncate ", tableName, " failed   ", err)
		tableNotExist = true
		return // 表不存在return布尔值
	}
	// 获取表的字段名以及类型
	// 如果指定了参数-s，就读取yml文件中配置的sql获取"自定义查询sql生成的列名"，否则按照select * 查全表获取
	if selFromYml {
		sqlCol = "select * from (" + sqlFullSplit[0] + " )aa where 1=0" // 在自定义sql外层套一个select * from (自定义sql) where 1=0
	} else {
		sqlCol = "select * from " + "\"" + tableName + "\"" + " where 1=0"
	}
	rows, err := srcDb.Query(sqlCol) //源库 SQL查询语句
	defer rows.Close()
	if err != nil {
		log.Error(fmt.Sprintf("Query "+sqlCol+" failed,\nerr:%v\n", err))
		return
	}
	//获取列名，这是字符串切片
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal(err.Error())
	}
	//获取字段类型，看下是varchar等还是blob
	colType, err := rows.ColumnTypes()
	if err != nil {
		log.Fatal(err.Error())
	}
	// 循环遍历列名,把列名全部转为小写
	for i, value := range columns {
		dbCol = append(dbCol, strings.ToLower(value)) //由于CopyIn方法每个列都会使用双引号包围，这里把列名全部转为小写(pg库默认都是小写的列名)，这样即便加上双引号也能正确查询到列
		dbColType = append(dbColType, strings.ToUpper(colType[i].DatabaseTypeName()))
	}
	return dbCol, dbColType, tableNotExist
}

// 根据表是否有主键，自动生成每个表查询sql，有主键就生成分页查询组成的切片，没主键就拼成全表查询sql，最后返回sql切片
func prepareSqlStr(tableName string, pageSize int) (sqlList []string) {
	var colNameFull string
	var totalPageNum int // 每个表的分页查询记录总数，即总共有多少页记录
	var sqlStr string    // 分页查询或者全表扫描sql
	// 获取每个表各个列的名称
	sql1 := fmt.Sprintf("select trim(',' from (xmlagg(xmlparse(content '\"'||column_name||'\"'||',') order by COLUMN_ID).getclobval()))  from user_tab_columns where table_name='%s'", tableName)
	err := srcDb.QueryRow(sql1).Scan(&colNameFull)
	if err != nil {
		log.Fatal(sql1, " exec failed ", err)
		return
	}
	// 根据当前表总数以及每页的页记录大小pageSize，自动计算需要多少页记录数，即总共循环多少次，如果表没有数据，后面判断下切片长度再做处理
	sql2 := "/* goapp */" + "select ceil(count(*)/" + strconv.Itoa(pageSize) + ") as total_page_num from " + "\"" + tableName + "\""
	//以下是直接使用QueryRow
	err = srcDb.QueryRow(sql2).Scan(&totalPageNum)
	if err != nil {
		log.Fatal(sql2, " exec failed ", err)
		return
	}
	// 以下生成分页查询语句
	for i := 0; i < totalPageNum; i++ { // 使用小于而不是小于等于，否则会多生成一条分页查询边界外的sql，即此sql查询源表没有数据，也会导致后面迁移数据有多个无用的goroutine
		curStartPage := i + 1
		//以下计算分页查询起始的页数
		startNum := curStartPage * pageSize
		if curStartPage > 0 {
			startNum = ((curStartPage - 1) * pageSize) + 1
		}
		endNum := startNum + pageSize - 1
		sqlStr = fmt.Sprintf("SELECT %s FROM (SELECT A.*, ROWNUM RN FROM (SELECT * FROM \"%s\") A WHERE ROWNUM <= %s) WHERE RN >=%s", colNameFull, tableName, strconv.Itoa(endNum), strconv.Itoa(startNum))
		sqlList = append(sqlList, sqlStr)
	}
	return sqlList
}

// 使用占位符,目前测下来，在大数据量下相比较不使用占位符的方式，效率较高，遇到blob类型可直接使用go的byte类型数据
func runMigration(logDir string, startPage int, tableName string, sqlStr string, ch chan struct{}, columns []string, colType []string) {
	defer wg.Done()
	log.Info(fmt.Sprintf("%v Taskid[%d] Processing TableData %v ", time.Now().Format("2006-01-02 15:04:05.000000"), startPage, tableName))
	start := time.Now()
	// 直接查询,即查询全表或者分页查询(SELECT t.* FROM (SELECT id FROM test  ORDER BY id LIMIT ?, ?) temp LEFT JOIN test t ON temp.id = t.id;)
	sqlStr = "/* goapp */" + sqlStr
	// 查询源库的sql
	rows, err := srcDb.Query(sqlStr) //传入参数之后执行
	defer rows.Close()
	if err != nil {
		log.Error(fmt.Sprintf("[exec  %v failed ] ", sqlStr), err)
		return
	}
	//values := make([]sql.RawBytes, len(columns)) // 列的值切片,包含多个列,即单行数据的值
	//scanArgs := make([]interface{}, len(values)) // 用来做scan的参数，将上面的列值value保存到scan
	//for i := range values {                      // 这里也是取决于有几列，就循环多少次
	//	scanArgs[i] = &values[i] // 这里scanArgs是指向列值的指针,scanArgs里每个元素存放的都是地址
	//}
	// 生成单行数据的占位符，如(?,?),表有几列就有几个问号
	singleRowCol := fmt.Sprintf("(%s)", strings.Join(strings.Split(strings.Repeat("?", len(columns)), ""), ","))
	// 批量插入时values后面总的占位符，批量有多少行数据，就有多少个(?,?)，例如(?,?),(?,?)
	var totalInsertCol string
	// 表总行数
	var totalRow int
	// 用于给批量插入存储的切片数据，如果batchRowSize是100行，这个切片长度就是100
	var totalPrepareValues []interface{}
	// 单个字段的列值
	var value interface{}
	// 批量插入的sql带有多个占位符，用于给Prepare方法传入参数
	var insertSql string
	// 批量插入批次大小行数,MySQL限制insert语句最多使用65535个占位符，下面计算挑选出最小值(自动计算的结果与yml配置文件设定值)，防止自己设定的批量batchRowSize超过限制
	batchRowSize := int(math.Min(float64(65535/len(columns)-10), float64(viper.GetInt("batchRowSize"))))
	//fmt.Println("batchRowSize:", batchRowSize)
	txn, err := destDb.Begin() //开始一个事务
	if err != nil {
		log.Error(err)
	}
	for rows.Next() { // 从查询结果获取一行行数据
		values := make([]sql.RawBytes, len(columns)) // 列的值切片,包含多个列,即单行数据的值
		scanArgs := make([]interface{}, len(values)) // 用来做scan的参数，将上面的列值value保存到scan
		for i := range values {                      // 这里也是取决于有几列，就循环多少次
			scanArgs[i] = &values[i] // 这里scanArgs是指向列值的指针,scanArgs里每个元素存放的都是地址
		}
		totalRow++                   // 源表行数+1
		err = rows.Scan(scanArgs...) //scanArgs切片里的元素是指向values的指针，通过rows.Scan方法将获取游标结果集的各个列值复制到变量scanArgs各个切片元素(指针)指向的对象即values切片里，这里是一行完整的值
		if err != nil {
			log.Error("ScanArgs Failed ", err.Error())
		}
		// 以下for将单行的byte数据循环转换成string类型(大字段就是用byte类型，剩余非大字段类型获取的值再使用string函数转为字符串)
		for i, colValue := range values { //values是完整的一行所有列值，这里从values遍历，获取每一列的值并赋值到col变量，col是单列的列值
			if colValue == nil {
				value = nil //空值判断
			} else {
				if colType[i] == "BLOB" { //大字段类型就无需使用string函数转为字符串类型，即使用sql.RawBytes类型
					value = colValue
				} else if colType[i] == "DATE" {
					timeValue, err := time.Parse(time.RFC3339, string(colValue)) // RFC3339= "2006-01-02T15:04:05Z07:00",先把列值转为标准的时间格式
					if err != nil {
						fmt.Println("convert date type error :", err)
						return
					}
					timeLayout := "2006-01-02 15:04:05" //date类型转化所需模板
					//timeLayout := "2006-01-02T15:04:05Z"                                  //转化所需模板(不使用time.Parse的时候)
					loc, _ := time.LoadLocation("Local") //重要：获取时区
					//theTime, _ := time.ParseInLocation(timeLayout, string(colValue), loc) //使用模板在对应时区转化为time.time类型
					theTime, _ := time.ParseInLocation(timeLayout, timeValue.Format("2006-01-02 15:04:05"), loc) //使用模板在对应时区转化为time.time类型
					value = theTime.Format("2006-01-02 15:04:05")                                                //格式化，否则时差相差8小时
				} else if colType[i] == "TIMESTAMPDTY" {
					timeValue, err := time.Parse(time.RFC3339, string(colValue)) // RFC3339= "2006-01-02T15:04:05Z07:00",先把列值转为标准的时间格式
					if err != nil {
						fmt.Println("convert timestamp error:", err)
						return
					}
					timeLayout := "2006-01-02 15:04:05.000000 +0000 UTC"                                                          //timestamp类型转化所需模板
					loc, _ := time.LoadLocation("Local")                                                                          //重要：获取时区
					theTime, _ := time.ParseInLocation(timeLayout, timeValue.Format("2006-01-02 15:04:05.000000 +0000 UTC"), loc) //使用模板在对应时区转化为time.time类型
					value = theTime.Format("2006-01-02 15:04:05.000000")                                                          //格式化，否则时差相差8小时
				} else {
					value = string(colValue) //非大字段类型,显式使用string函数强制转换为字符串文本，否则都是字节类型文本(即sql.RawBytes)
				}
			}
			// 把一行一行数据追加到这个totalPrepareValues，便于后面给Prepare方法一次性提供批量的实参,比如该表2个字段，数据即1 tom 2 jim 3 kim
			totalPrepareValues = append(totalPrepareValues, value)
		}
		// 多行数据value拼接的占位符用逗号隔开，例如(?,?),(?,?),(?,?)， -注意这里结尾有逗号
		totalInsertCol += singleRowCol + ","
		// 每隔一定行数，批量插入一次
		if totalRow%batchRowSize == 0 {
			totalInsertCol = strings.TrimRight(totalInsertCol, ",") // 去掉占位符最后括号的逗号
			insertSql = fmt.Sprintf("insert into `%s` values %s", tableName, totalInsertCol)
			if len(totalPrepareValues) != 0 { // 排除掉多线程遇到空切片的数据
				stmt, err := txn.Prepare(insertSql) //prepare里的方法CopyIn只是把copy语句拼接好并返回，并非直接执行copy
				if err != nil {
					log.Error("txn.Prepare(insertSql) failed table[", tableName, "] ", err)
					LogError(logDir, "errorTableData ", tableName, err)
					//responseChannel <- fmt.Sprintf("data error %s", tableName)
					<-ch // 通道向外发送数据
					return
				}
				// 下面是把实参的值传到插入语句的占位符，真正开始批量写入数据
				_, err = stmt.Exec(totalPrepareValues...) //这里Exec只传入实参，即上面prepare的CopyIn所需的参数，这里理解为把stmt所有数据先存放到buffer里面
				if err != nil {
					log.Error(tableName, " stmt.Exec(totalPrepareValues...) Failed: ", err) //注意这里不能使用Fatal，否则会直接退出程序，也就没法遇到错误继续了
					LogError(logDir, "errorTableData ", tableName, err)
					err := txn.Rollback()
					if err != nil {
						log.Error(tableName, " Rollback failed table ", err)
					}
					<-ch // 通道向外发送数据
					return
				}
				err = stmt.Close() //关闭stmt
				if err != nil {
					log.Error(err)
				}
				log.Info(time.Now(), " ID[", startPage, "] insert ", tableName, " ", totalRow, " rows")
				totalPrepareValues = nil
				totalInsertCol = ""
			}
		}
	}
	err = txn.Commit() // 提交事务
	if err != nil {
		err := txn.Rollback()
		if err != nil {
			return
		}
		log.Error("Commit failed ", err)
	}
	// rows.Next方法最后一部分数据的插入
	insertSql = fmt.Sprintf("insert into `%s` values %s", tableName, totalInsertCol)
	insertSql = strings.TrimRight(insertSql, ",")
	txn, err = destDb.Begin() //开始一个事务
	if err != nil {
		log.Error(err)
	}
	if len(totalPrepareValues) != 0 {
		stmt, err := txn.Prepare(insertSql) //prepare里的方法CopyIn只是把copy语句拼接好并返回，并非直接执行copy
		if err != nil {
			log.Error("txn.Prepare(insertSql) failed table[", tableName, "] ", err)
			LogError(logDir, "errorTableData ", tableName, err)
			//responseChannel <- fmt.Sprintf("data error %s", tableName)
			<-ch // 通道向外发送数据
			return
		}
		// 下面是把实参的值传到插入语句的占位符，真正开始批量写入数据
		_, err = stmt.Exec(totalPrepareValues...) //这里Exec只传入实参，即上面prepare的CopyIn所需的参数，这里理解为把stmt所有数据先存放到buffer里面
		if err != nil {
			log.Error(tableName, " last part stmt.Exec(totalPrepareValues...) Failed: ", err) //注意这里不能使用Fatal，否则会直接退出程序，也就没法遇到错误继续了
			LogError(logDir, "errorTableData ", tableName, err)
			err := txn.Rollback()
			if err != nil {
				log.Error(tableName, " Rollback last part failed table ", err)
			}
			<-ch // 通道向外发送数据
			return
		}
		err = stmt.Close() //关闭stmt
		if err != nil {
			log.Error(err)
		}
		//err = txn.Commit() // 提交事务，这里注意Commit在上面Close之后
		//if err != nil {
		//	err := txn.Rollback()
		//	if err != nil {
		//		log.Error("rollback failed ", err)
		//	}
		//	log.Error("Commit failed ", err)
		//}
		log.Info(time.Now().Format("2006-01-02 15:04:05.000"), " ID[", startPage, "] insert ", tableName, " ", totalRow, " rows")
	}
	cost := time.Since(start) //计算时间差
	log.Info(fmt.Sprintf("%v Taskid[%d] table %v complete,processed %d rows,execTime %s", time.Now().Format("2006-01-02 15:04:05.000000"), startPage, tableName, totalRow, cost))
	// 剩下显式的提交一下，不然目标库会有很多sleep的线程导致超出最大连接数
	err = txn.Commit() // 提交事务，这里注意Commit在上面Close之后
	if err != nil {
		log.Error("Commit failed ", err)
	}
	<-ch // 通道向外发送数据
}

func Execute() { // init 函数初始化之后再运行此Execute函数
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// 程序中第一个调用的函数,先初始化config
func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.example.yaml)")
	rootCmd.PersistentFlags().BoolVarP(&selFromYml, "selFromYml", "s", false, "select from yml true")
	//rootCmd.PersistentFlags().BoolVarP(&tableOnly, "tableOnly", "t", false, "only create table true")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name "yml" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".example")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// 通过viper读取配置文件进行加载
	if err := viper.ReadInConfig(); err == nil {
		log.Info("Using config file:", viper.ConfigFileUsed())
	} else {
		log.Fatal(viper.ConfigFileUsed(), " has some error please check your yml file ! ", "Detail-> ", err)
	}
	log.Info("Using selFromYml:", selFromYml)
}
