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
	sql := fmt.Sprintf("SELECT A.COLUMN_NAME,A.DATA_TYPE,A.CHAR_LENGTH,case when A.NULLABLE ='Y' THEN 'YES' ELSE 'NO' END as isnull,A.DATA_DEFAULT,case when A.DATA_PRECISION is null then -1 else  A.DATA_PRECISION end DATA_PRECISION,case when A.DATA_SCALE is null then -1 when A.DATA_SCALE >30 then least(A.DATA_PRECISION,30)-1 else  A.DATA_SCALE end DATA_SCALE, nvl(B.COMMENTS,'null') COMMENTS,case when a.AVG_COL_LEN is null then -1 else a.AVG_COL_LEN end AVG_COL_LEN,COLUMN_ID FROM USER_TAB_COLUMNS A LEFT JOIN USER_COL_COMMENTS B ON A.TABLE_NAME=B.TABLE_NAME AND A.COLUMN_NAME=B.COLUMN_NAME WHERE A.TABLE_NAME='%s' ORDER BY COLUMN_ID", tblName)
	//fmt.Println(sql)
	rows, err := srcDb.Query(sql)
	if err != nil {
		log.Error(err)
	}
	// 遍历MySQL表字段,一行就是一个字段的基本信息
	for rows.Next() {
		if err := rows.Scan(&newTable.columnName, &newTable.dataType, &newTable.characterMaximumLength, &newTable.isNullable, &colDefaultValue, &newTable.numericPrecision, &newTable.numericScale, &newTable.columnComment, &newTable.avgColLen, &newTable.ordinalPosition); err != nil {
			log.Error(err)
		}
		// 判断下默认值是否是null，go语言中不能直接把null值转成字符串
		if colDefaultValue.Valid {
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
	// 创建PostgreSQL表结构
	log.Info(fmt.Sprintf("%v Table total %s create table %s", time.Now().Format("2006-01-02 15:04:05.000000"), strconv.Itoa(tableCount), tblName))
	if _, err = destDb.Exec(createTblSql); err != nil {
		log.Error("table ", tblName, " create failed  ", err)
		LogError(logDir, "tableCreateFailed", createTblSql, err)
		failedCount += 1
	}
	<-ch
}
