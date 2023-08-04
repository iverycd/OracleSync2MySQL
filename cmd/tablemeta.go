package cmd

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var tabRet []string
var tableCount int
var failedCount int

type Database interface {
	// TableCreate (logDir string, tableMap map[string][]string) (result []string) 单线程
	TableCreate(logDir string, tblName string, ch chan struct{})
	IdxCreate(logDir string, tableName string, ch chan struct{}, id int)
}

type Table struct {
	columnName             string
	dataType               string
	characterMaximumLength string
	isNullable             string
	columnDefault          string
	numericPrecision       int
	numericScale           int
	datetimePrecision      string
	columnKey              string
	columnComment          string
	ordinalPosition        int
	avgColLen              int
	destType               string
	destNullable           string
	destDefault            string
	autoIncrement          int
	destSeqSql             string
	destDefaultSeq         string
	dropSeqSql             string
	destIdxSql             string
	viewSql                string
}

func (tb *Table) TableCreate(logDir string, tblName string, ch chan struct{}) {
	defer wg2.Done()
	var newTable Table
	var colDefaultValue sql.NullString
	tableCount += 1
	// 使用goroutine并发的创建多个表
	var colTotal int
	createTblSql := "create table " + fmt.Sprintf("`") + tblName + fmt.Sprintf("`") + "("
	// 查询当前表总共有多少个列字段
	colTotalSql := fmt.Sprintf("select count(*) from user_tab_columns where  table_name='%s'", tblName)
	err := srcDb.QueryRow(colTotalSql).Scan(&colTotal)
	if err != nil {
		log.Error(err)
	}
	// 查询源库表结构
	sqlStr := fmt.Sprintf("SELECT A.COLUMN_NAME,A.DATA_TYPE,A.CHAR_LENGTH,case when A.NULLABLE ='Y' THEN 'YES' ELSE 'NO' END as isnull,A.DATA_DEFAULT,case when A.DATA_PRECISION is null then -1 else  A.DATA_PRECISION end DATA_PRECISION,case when A.DATA_SCALE is null then -1 when A.DATA_SCALE >30 then least(A.DATA_PRECISION,30)-1 else  A.DATA_SCALE end DATA_SCALE, nvl(B.COMMENTS,'null') COMMENTS,case when a.AVG_COL_LEN is null then -1 else a.AVG_COL_LEN end AVG_COL_LEN,COLUMN_ID FROM USER_TAB_COLUMNS A LEFT JOIN USER_COL_COMMENTS B ON A.TABLE_NAME=B.TABLE_NAME AND A.COLUMN_NAME=B.COLUMN_NAME WHERE A.TABLE_NAME='%s' ORDER BY COLUMN_ID", tblName)
	//fmt.Println(sqlStr)
	rows, err := srcDb.Query(sqlStr)
	if err != nil {
		log.Error(err)
	}
	// 遍历MySQL表字段,一行就是一个字段的基本信息
	for rows.Next() {
		if err := rows.Scan(&newTable.columnName, &newTable.dataType, &newTable.characterMaximumLength, &newTable.isNullable, &colDefaultValue, &newTable.numericPrecision, &newTable.numericScale, &newTable.columnComment, &newTable.avgColLen, &newTable.ordinalPosition); err != nil {
			log.Error(err)
		}
		// 判断下默认值是否是null，go语言中不能直接把null值转成字符串
		if !colDefaultValue.Valid {
			newTable.columnDefault = colDefaultValue.String
		} else {
			newTable.columnDefault = "null"
		}
		//fmt.Println(columnName,dataType,characterMaximumLength,isNullable,columnDefault,numericPrecision,numericScale,datetimePrecision,columnKey,columnComment,ordinalPosition)
		// 列字段是否允许null
		switch newTable.isNullable {
		case "NO":
			newTable.destNullable = "not null"
		default:
			newTable.destNullable = "" // 原先是"null"
		}
		// 列字段default默认值的处理
		switch {
		case newTable.columnDefault != "null": // 默认值不为空
			if newTable.dataType == "VARCHAR2" || newTable.dataType == "NVARCHAR2" || newTable.dataType == "CHAR" {
				if strings.ToUpper(newTable.columnDefault) == "SYS_GUID()" || strings.ToUpper(newTable.columnDefault) == "USER" { // mysql默认值不支持函数，统一设为null
					newTable.destDefault = "default null"
				} else {
					newTable.destDefault = "default " + strings.ReplaceAll(strings.ReplaceAll(newTable.columnDefault, "(", ""), ")", "") //去掉默认值中的左右括号，如( 'user' )
				}
			} else if newTable.dataType == "NUMBER" {
				// 创建正则表达式，number类型默认值包含括号的情况，如default (1.7),仅提取括号内数字部分
				re := regexp.MustCompile(`[\d.]+`)
				// 提取匹配的字符串
				matches := re.FindAllString(newTable.columnDefault, -1)
				if len(matches) > 0 { //如果能匹配提取数字部分，否则就给null
					newTable.destDefault = "default " + matches[0]
				} else {
					newTable.destDefault = "default null"
				}
			} else if strings.ToUpper(newTable.columnDefault) == "SYSDATE" || strings.ToUpper(newTable.columnDefault) == "CURRENT_TIMESTAMP" {
				re := regexp.MustCompile(`\w+`)
				matches := re.FindAllString(newTable.dataType, -1)
				if matches[0] == "TIMESTAMP" { //timestamp类型，才需要加精度值
					newTable.destDefault = "default current_timestamp(" + strconv.Itoa(newTable.numericScale) + ")"
				}
			} else { // 其余默认值类型无需使用单引号包围
				newTable.destDefault = fmt.Sprintf("default %s", newTable.columnDefault)
			}
		default:
			newTable.destDefault = "" // 如果没有默认值，默认值就是空字符串
		}
		// 列字段类型的处理
		switch newTable.dataType {
		case "NUMBER":
			if newTable.columnDefault == "NULL" {
				newTable.destDefault = "null"
			}
			if newTable.numericPrecision > 0 && newTable.numericScale > 0 { //场景1 Oracle number(m,n) -> MySQL decimal(m,n)
				newTable.destType = "decimal(" + strconv.Itoa(newTable.numericPrecision) + "," + strconv.Itoa(newTable.numericScale) + ")"
			} else if newTable.avgColLen >= 6 { // 场景2 avg_col_len >= 6 ,Oracle number(m,0) -> MySQL bigint
				newTable.destType = "bigint"
			} else if newTable.avgColLen < 6 {
				newTable.destType = "int"
			}
		case "VARCHAR2", "NVARCHAR2", "UROWID":
			newTable.destType = "varchar(" + newTable.characterMaximumLength + ")"
		case "CHAR", "NCHAR":
			newTable.destType = "char(" + newTable.characterMaximumLength + ")"
		case "DATE":
			newTable.destType = "datetime"
		case "CLOB", "NCLOB", "LONG":
			newTable.destType = "longtext"
		case "BLOB", "RAW", "LONG RAW":
			newTable.destType = "longblob"
		// 其余类型，源库使用什么类型，目标库就使用什么类型
		default:
			newTable.destType = newTable.dataType
		}
		// 在目标库创建的语句
		createTblSql += fmt.Sprintf("`%s` %s %s %s,", newTable.columnName, newTable.destType, newTable.destNullable, newTable.destDefault)
		if newTable.ordinalPosition == colTotal {
			createTblSql = createTblSql[:len(createTblSql)-1] + ")" // 最后一个列字段结尾去掉逗号,并且加上语句的右括号
		}
	}
	//fmt.Println(createTblSql) // 打印创建表语句
	// 创建前先删除目标表
	dropDestTbl := "drop table if exists " + fmt.Sprintf("`") + tblName + fmt.Sprintf("`") + " cascade"
	if _, err = destDb.Exec(dropDestTbl); err != nil {
		log.Error("drop table ", tblName, " failed ", err)
	}
	// 创建表结构
	log.Info(fmt.Sprintf("%v Table total %s create table %s", time.Now().Format("2006-01-02 15:04:05.000000"), strconv.Itoa(tableCount), tblName))
	if _, err = destDb.Exec(createTblSql); err != nil {
		log.Error("table ", tblName, " create failed  ", err)
		LogError(logDir, "tableCreateFailed", createTblSql, err)
		failedCount += 1
	}
	<-ch
}

func (tb *Table) IdxCreate(logDir string, tableName string, ch chan struct{}, id int) {
	defer wg2.Done()
	destIdxSql := ""
	// 查询索引、主键、唯一约束等信息，批量生成创建语句
	sqlStr := fmt.Sprintf("SELECT (CASE WHEN C.CONSTRAINT_TYPE = 'P' OR C.CONSTRAINT_TYPE = 'R' THEN 'ALTER TABLE `' || T.TABLE_NAME || '` ADD CONSTRAINT ' ||'`'||T.INDEX_NAME||'`' || (CASE WHEN C.CONSTRAINT_TYPE = 'P' THEN ' PRIMARY KEY (' ELSE ' FOREIGN KEY (' END) || listagg(T.COLUMN_NAME,',') within group(order by T.COLUMN_position) || ');' ELSE 'CREATE ' || (CASE WHEN I.UNIQUENESS = 'UNIQUE' THEN I.UNIQUENESS || ' ' ELSE CASE WHEN I.INDEX_TYPE = 'NORMAL' THEN '' ELSE  I.INDEX_TYPE || ' '  END END) || 'INDEX ' || '`'||T.INDEX_NAME||'`' || ' ON ' || T.TABLE_NAME || '(' ||listagg(T.COLUMN_NAME,',') within group(order by T.COLUMN_position) || ');' END) SQL_CMD FROM USER_IND_COLUMNS T, USER_INDEXES I, USER_CONSTRAINTS C WHERE T.INDEX_NAME = I.INDEX_NAME  AND T.INDEX_NAME = C.CONSTRAINT_NAME(+)  and i.index_type != 'FUNCTION-BASED NORMAL' and i.table_name='%s' GROUP BY T.TABLE_NAME, T.INDEX_NAME, I.UNIQUENESS, I.INDEX_TYPE,C.CONSTRAINT_TYPE", tableName)
	//fmt.Println(sql)
	rows, err := srcDb.Query(sqlStr)
	if err != nil {
		log.Error(err)
	}
	defer rows.Close()
	// 从sql结果集遍历，获取到创建语句
	for rows.Next() {
		if err := rows.Scan(&destIdxSql); err != nil {
			log.Error(err)
		}
		// 创建目标索引，主键、其余约束
		if _, err = destDb.Exec(destIdxSql); err != nil {
			log.Error("index ", destIdxSql, " create index failed ", err)
			LogError(logDir, "idxCreateFailed", destIdxSql, err)
			failedCount += 1
		}
	}
	if destIdxSql != "" {
		log.Info("[", id, "] Table ", tableName, " create index finish ")
	}
	<-ch
}

// 单线程，不使用goroutine创建索引
//func (tb *Table) IdxCreate(logDir string) (result []string) {
//	startTime := time.Now()
//	failedCount := 0
//	id := 0
//	// 查询索引、主键、唯一约束等信息，批量生成创建语句
//	sqlStr := fmt.Sprintf("SELECT (CASE WHEN C.CONSTRAINT_TYPE = 'P' OR C.CONSTRAINT_TYPE = 'R' THEN 'ALTER TABLE `' || T.TABLE_NAME || '` ADD CONSTRAINT ' ||'`'||T.INDEX_NAME||'`' || (CASE WHEN C.CONSTRAINT_TYPE = 'P' THEN ' PRIMARY KEY (' ELSE ' FOREIGN KEY (' END) || listagg(T.COLUMN_NAME,',') within group(order by T.COLUMN_position) || ');' ELSE 'CREATE ' || (CASE WHEN I.UNIQUENESS = 'UNIQUE' THEN I.UNIQUENESS || ' ' ELSE CASE WHEN I.INDEX_TYPE = 'NORMAL' THEN '' ELSE  I.INDEX_TYPE || ' '  END END) || 'INDEX ' || '`'||T.INDEX_NAME||'`' || ' ON ' || T.TABLE_NAME || '(' ||listagg(T.COLUMN_NAME,',') within group(order by T.COLUMN_position) || ');' END) SQL_CMD FROM USER_IND_COLUMNS T, USER_INDEXES I, USER_CONSTRAINTS C WHERE T.INDEX_NAME = I.INDEX_NAME  AND T.INDEX_NAME = C.CONSTRAINT_NAME(+)  and i.index_type != 'FUNCTION-BASED NORMAL'  GROUP BY T.TABLE_NAME, T.INDEX_NAME, I.UNIQUENESS, I.INDEX_TYPE,C.CONSTRAINT_TYPE",tableName)
//	//fmt.Println(sql)
//	rows, err := srcDb.Query(sqlStr)
//	if err != nil {
//		log.Error(err)
//	}
//	defer rows.Close()
//	// 从sql结果集遍历，获取到创建语句
//	for rows.Next() {
//		id += 1
//		if err := rows.Scan(&tb.destIdxSql); err != nil {
//			log.Error(err)
//		}
//		// 创建目标索引，主键、其余约束
//		log.Info(fmt.Sprintf("%v ProcessingID %s %s", time.Now().Format("2006-01-02 15:04:05.000000"), strconv.Itoa(id), tb.destIdxSql))
//		if _, err = destDb.Exec(tb.destIdxSql); err != nil {
//			log.Error("index ", tb.destIdxSql, " create index failed ", err)
//			LogError(logDir, "idxCreateFailed", tb.destIdxSql, err)
//			failedCount += 1
//		}
//	}
//	endTime := time.Now()
//	cost := time.Since(startTime)
//	log.Info("index  count ", id)
//	result = append(result, "Index", startTime.Format("2006-01-02 15:04:05.000000"), endTime.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), cost.String())
//	return result
//}
