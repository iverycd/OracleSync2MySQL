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
	SeqCreate(logDir string) (ret []string)
	FkCreate(logDir string) (ret []string)
	NormalIdx(logDir string) (ret []string)
	CommentCreate(logDir string) (ret []string)
	ViewCreate(logDir string) (ret []string)
	PrintDbFunc(logDir string)
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
	if selFromYml { //-s自定义迁移表的时候，统一把yml文件的表名转为大写(否则查询语句的表名都是小写)，原因是map键值对(key:value)，viper这个库始终把key的值转为小写的值
		tblName = strings.ToUpper(tblName)
	}
	createTblSql := "create table " + fmt.Sprintf("`") + tblName + fmt.Sprintf("`") + "("
	// 查询当前表总共有多少个列字段
	colTotalSql := fmt.Sprintf("select count(*) from user_tab_columns where  table_name='%s'", tblName)
	err := srcDb.QueryRow(colTotalSql).Scan(&colTotal)
	if err != nil {
		log.Error(err)
	}
	if colTotal == 0 {
		log.Error("Table ", tblName, " not exist and not create table")
		return
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
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			log.Fatal(err)
		}
	}(rows)
	// 从sql结果集遍历，获取到创建语句
	for rows.Next() {
		if err := rows.Scan(&destIdxSql); err != nil {
			log.Error(err)
		}
		destIdxSql = "/* goapp */" + destIdxSql
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

func (tb *Table) SeqCreate(logDir string) (ret []string) {
	startTime := time.Now()
	failedCount = 0
	var dbRet, tableName string
	rows, err := srcDb.Query("select table_name,trigger_body from user_triggers where upper(trigger_type) ='BEFORE EACH ROW'")
	if err != nil {
		log.Error(err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			log.Fatal(err)
		}
	}(rows)
	idx := 0
	for rows.Next() {
		idx += 1
		err := rows.Scan(&tableName, &dbRet)
		if err != nil {
			log.Error(err)
		}
		dbRet = strings.ToUpper(dbRet)
		dbRet = strings.ReplaceAll(dbRet, "INTO:", "INTO :")
		dbRet = strings.ReplaceAll(dbRet, "SYS.DUAL ", "DUAL")
		dbRet = strings.ReplaceAll(dbRet, "SYS.DUAL", "DUAL")
		dbRet = strings.ReplaceAll(dbRet, "\n", "")
		pattern := `SELECT\s+(.*?)\.NEXTVAL\s+INTO\s+:NEW\.`
		re := regexp.MustCompile(pattern)
		match := re.FindStringSubmatch(dbRet)
		if len(match) > 0 { // 第一层，先正则匹配SELECT .NEXTVAL INTO :NEW包含的字符窜,主要是要匹配到自增列性质的触发器
			//如果符合第一层正则的条件，再匹配第二层，第二层主要是获取:NEW.后面的名称，即自增列名称
			re := regexp.MustCompile(`:NEW\.(\w+)`) // 正则表达式，匹配以 ":NEW." 开头的字符串，并提取后面的单词字符（包括字母、数字和下划线）
			match := re.FindStringSubmatch(dbRet)   // 查找匹配项
			if len(match) == 2 {
				autoColName := match[1]
				// 创建目标数据库该表表的自增列索引
				sqlAutoColIdx := "/* goapp */" + "create index ids_" + tableName + "_" + autoColName + "_" + strconv.Itoa(idx) + " on " + tableName + "(" + autoColName + ")"
				log.Info("[", idx, "] create auto_increment for table ", tableName)
				if _, err = destDb.Exec(sqlAutoColIdx); err != nil {
					log.Error(sqlAutoColIdx, " create index autoCol failed ", err)
					LogError(logDir, "AutoIdxCreateFailed", sqlAutoColIdx, err)
					failedCount += 1
				}
				// 更改目标数据库该表的列属性为自增列
				sqlModifyAuto := "/* goapp */" + "alter table " + tableName + " modify " + autoColName + " bigint auto_increment"
				if _, err = destDb.Exec(sqlModifyAuto); err != nil {
					log.Error(sqlModifyAuto, " failed ", err)
					LogError(logDir, "alterTableFailed", sqlModifyAuto, err)
					failedCount += 1
				}
			}
		}
	}
	endTime := time.Now()
	cost := time.Since(startTime)
	ret = append(ret, "AutoIncrement", startTime.Format("2006-01-02 15:04:05.000000"), endTime.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), cost.String())
	return ret
}

func (tb *Table) FkCreate(logDir string) (ret []string) {
	startTime := time.Now()
	failedCount = 0
	var tableName, sqlStr string
	rows, err := srcDb.Query("SELECT B.TABLE_NAME,'ALTER TABLE ' || B.TABLE_NAME || ' ADD CONSTRAINT ' ||\n       B.CONSTRAINT_NAME || ' FOREIGN KEY (' ||\n       (SELECT listagg(A.COLUMN_NAME,',') within group(order by a.position)\n        FROM USER_CONS_COLUMNS A\n        WHERE A.CONSTRAINT_NAME = B.CONSTRAINT_NAME) || ') REFERENCES ' ||\n       (SELECT B1.table_name FROM USER_CONSTRAINTS B1\n        WHERE B1.CONSTRAINT_NAME = B.R_CONSTRAINT_NAME) || '(' ||\n       (SELECT listagg(A.COLUMN_NAME,',') within group(order by a.position)\n        FROM USER_CONS_COLUMNS A\n        WHERE A.CONSTRAINT_NAME = B.R_CONSTRAINT_NAME) || ');'\nFROM USER_CONSTRAINTS B\nWHERE B.CONSTRAINT_TYPE = 'R' ")
	if err != nil {
		log.Error(err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			log.Fatal(err)
		}
	}(rows)
	idx := 0
	for rows.Next() {
		idx += 1
		err := rows.Scan(&tableName, &sqlStr)
		if err != nil {
			log.Error(err)
		}
		log.Info("[", idx, "] create foreign key for table ", tableName)
		sqlStr = "/* goapp */" + sqlStr
		if _, err = destDb.Exec(sqlStr); err != nil {
			log.Error(sqlStr, " create foreign key failed ", err)
			LogError(logDir, "FKCreateFailed", sqlStr, err)
			failedCount += 1
		}
	}
	endTime := time.Now()
	cost := time.Since(startTime)
	ret = append(ret, "ForeignKey", startTime.Format("2006-01-02 15:04:05.000000"), endTime.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), cost.String())
	return ret
}

func (tb *Table) NormalIdx(logDir string) (ret []string) {
	startTime := time.Now()
	failedCount = 0
	var idxName, tableName, sqlStr, userName, createSql string
	err := srcDb.QueryRow("select user from dual").Scan(&userName)
	if err != nil {
		log.Error(err)
	}
	rows, err := srcDb.Query("Select index_name,table_name from user_indexes where index_type='FUNCTION-BASED NORMAL'")
	if err != nil {
		log.Error(err)
	}
	defer rows.Close()
	idx := 0
	for rows.Next() {
		idx += 1
		err := rows.Scan(&idxName, &tableName) // 先获取normal-index的索引名称和表名
		if err != nil {
			log.Error(err)
		}
		if len(idxName) > 0 { // 如果有normal-index，就通过dbms_metadata获取该normal-index的DDL语句
			sqlStr = fmt.Sprintf("select trim(replace(regexp_replace(regexp_replace(SUBSTR(upper(to_char(dbms_metadata.get_ddl('INDEX','%s','%s'))), 1, INSTR(upper(to_char(dbms_metadata.get_ddl('INDEX','%s','%s'))), ' PCTFREE')-1),'\"','',1,0,'i'),'%s'||'.','',1,0,'i'),chr(10),'')) from dual", idxName, userName, idxName, userName, userName)
			err := srcDb.QueryRow(sqlStr).Scan(&createSql) // 获取到创建normal-index的sql语句
			if err != nil {
				log.Error(err)
			}
			log.Info("[", idx, "] create normal index for table ", tableName)
			createSql = "/* goapp */" + createSql
			if _, err = destDb.Exec(createSql); err != nil {
				log.Error(createSql, " create normal index failed ", err)
				LogError(logDir, "NormalIdxCreateFailed", createSql, err)
				failedCount += 1
			}
		}

	}
	endTime := time.Now()
	cost := time.Since(startTime)
	ret = append(ret, "NormalIndex", startTime.Format("2006-01-02 15:04:05.000000"), endTime.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), cost.String())
	return ret
}

func (tb *Table) CommentCreate(logDir string) (ret []string) {
	startTime := time.Now()
	failedCount = 0
	var tableName, createSql string
	rows, err := srcDb.Query("select TABLE_NAME,'alter table '||TABLE_NAME||' comment '||''''||COMMENTS||'''' as create_comment  from USER_TAB_COMMENTS where COMMENTS is not null")
	if err != nil {
		log.Error(err)
	}
	defer rows.Close()
	idx := 0
	for rows.Next() {
		idx += 1
		err := rows.Scan(&tableName, &createSql) // 先获取normal-index的索引名称和表名
		if err != nil {
			log.Error(err)
		}
		if len(createSql) > 0 { // 如果有normal-index，就通过dbms_metadata获取该normal-index的DDL语句
			log.Info("[", idx, "] create comment for table ", tableName)
			if _, err = destDb.Exec(createSql); err != nil {
				log.Error(createSql, " create comment failed ", err)
				LogError(logDir, "CommentCreateFailed", createSql, err)
				failedCount += 1
			}
		}

	}
	endTime := time.Now()
	cost := time.Since(startTime)
	ret = append(ret, "Comment", startTime.Format("2006-01-02 15:04:05.000000"), endTime.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), cost.String())
	return ret
}

func (tb *Table) ViewCreate(logDir string) (ret []string) {
	startTime := time.Now()
	failedCount = 0
	var dbRet, viewName string
	rows, err := srcDb.Query("select view_name,text from user_views")
	if err != nil {
		log.Error(err)
	}
	defer rows.Close()
	idx := 0
	for rows.Next() {
		idx += 1
		err := rows.Scan(&viewName, &dbRet)
		if err != nil {
			log.Error(err)
		}
		if _, err = srcDb.Exec("alter view " + viewName + " compile"); err != nil { // 先编译下源数据库的视图
			log.Error(" alter view ", viewName, " compile failed", err)
		}
		dbRet = strings.ToUpper(dbRet)
		dbRet = strings.ReplaceAll(dbRet, "--", "-- -- ")
		dbRet = strings.ReplaceAll(dbRet, "\"", "`")
		dbRet = strings.ReplaceAll(dbRet, "NVL(", "IFNULL(")
		dbRet = strings.ReplaceAll(dbRet, "unistr('\0030')", "0")
		dbRet = strings.ReplaceAll(dbRet, "unistr('\0031')", "1")
		dbRet = strings.ReplaceAll(dbRet, "unistr('\0033')", "3")
		if len(viewName) > 0 {
			sqlStr := "create or replace view " + viewName + " as " + dbRet
			log.Info("[", idx, "] create view ", viewName)
			if _, err = destDb.Exec(sqlStr); err != nil {
				//log.Error(sqlStr, " create view failed ", err)
				LogError(logDir, "ViewCreateFailed", sqlStr, err)
				failedCount += 1
			}
		}
	}
	endTime := time.Now()
	cost := time.Since(startTime)
	ret = append(ret, "View", startTime.Format("2006-01-02 15:04:05.000000"), endTime.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), cost.String())
	return ret
}

func (tb *Table) PrintDbFunc(logDir string) { //转储源数据库的函数、存储过程、包等对象到平面文件
	var dbRet string
	rows, err := srcDb.Query("SELECT DBMS_METADATA.GET_DDL(U.OBJECT_TYPE, u.object_name) ddl_sql FROM USER_OBJECTS u where U.OBJECT_TYPE IN ('FUNCTION','PROCEDURE','PACKAGE') order by OBJECT_TYPE")
	if err != nil {
		log.Error(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&dbRet)
		if err != nil {
			log.Error(err)
		}
		LogError(logDir, "FuncObject", dbRet, err)
	}
}
