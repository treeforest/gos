package bydb

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"log"

	_ "github.com/go-sql-driver/mysql"
)

type TableDesc struct {
	Type         string
	Default      string
	Extra        string
	PRIMARY_KEY  bool
	AutoIncrease bool
	UNIQUE       bool
	CanNull      bool
}

type MSqlTableDesc map[string]*TableDesc

type MSqlDB struct {
	db        *sql.DB
	stmts     map[string]*sql.Stmt
	stmtLock  sync.Mutex
	tableList map[string]bool
}

// 打开数据库
func (p *MSqlDB) open(param string) (err error) {
	p.db, err = sql.Open("mysql", param)
	if err != nil {
		return err
	}

	return nil
}

// 关闭数据库
func (p *MSqlDB) Close() {
	if p.db != nil {
		p.db.Close()
		p.db = nil
	}
}

// Query 执行sql语句 主要用于查询
func (p *MSqlDB) Query(keys []string, table string, where []string) (rs sqlResult, err error) {
	sql := "SELECT "
	for i, k := range keys {
		if i == 1 {
			sql += k
		} else {
			sql += "," + k
		}
	}
	sql += " FROM " + table
	if where != nil {
		sql += " WHERE "

		for i, k := range where {
			if i == 1 {
				sql += k
			} else {
				sql += " and " + k
			}
		}
	}

	rows, err := p.db.Query(sql)
	defer rows.Close()
	if err != nil {
		return nil, err
	}

	return p.doRows(rows)
}

// 执行sql语句，不告知结果，只告诉成功与否，主要是创建表什么的调用
func (p *MSqlDB) Exec(sql string) error {
	_, err := p.db.Exec(sql)
	/*fmt.Println(rs, err)
	if err == nil {
		fmt.Println(rs.LastInsertId())
		fmt.Println(rs.RowsAffected())
	}*/
	return err
}

// 执行sql语句，不告知结果，只告诉成功与否，主要是创建表什么的调用
func (p *MSqlDB) QuerySQL(sql string) (rs sqlResult, err error) {
	rows, err := p.db.Query(sql)
	if err != nil {
		return rs, err
	}

	defer rows.Close()
	return p.doRows(rows)
}

// Prepare 生成预操作
func (p *MSqlDB) Prepare(key, sql string) error {
	defer p.stmtLock.Unlock()
	p.stmtLock.Lock()
	if p.stmts[key] == nil {
		stmt, err := p.db.Prepare(sql)
		if err == nil {
			p.stmts[key] = stmt
		} else {
			return err
		}
	} else {
		return errors.New(key + " Prepare is exist")
	}

	return nil
}

type sqlRow map[string]interface{}
type sqlResult map[int]sqlRow

// 处理结果
func (p *MSqlDB) doRows(rows *sql.Rows) (rs sqlResult, err error) {
	// 读取数据到map中来
	columns, _ := rows.Columns()
	rs = make(sqlResult, 10)
	length := len(columns)
	var i int = 0
	for rows.Next() {
		value := make([]interface{}, length)
		columnPointers := make([]interface{}, length)
		for i := 0; i < length; i++ {
			columnPointers[i] = &value[i]
		}
		rows.Scan(columnPointers...)
		data := make(sqlRow)
		for i := 0; i < length; i++ {
			columnName := columns[i]
			columnValue := columnPointers[i].(*interface{})
			data[columnName] = *columnValue
		}
		log.Println("doRows", data)
		rs[i] = data
		i++
	}

	return rs, err
}

// 处理结果
func (p *MSqlDB) doRow(rows *sql.Rows) (sqlRow, error) {
	rs, err := p.doRows(rows)
	if err != nil {
		return nil, err
	}

	if rs[0] == nil {
		return nil, errors.New("not data")
	}

	if rs[1] != nil {
		return rs[0], errors.New("have more then one data")
	}

	return rs[0], err
}

// QueryPrepare 执行预操作
func (p *MSqlDB) QueryPrepare(key string, args ...interface{}) (rs sqlResult, err error) {
	stmt, ok := p.stmts[key]
	if !ok {
		return nil, errors.New(fmt.Sprintf("not this prepare %s", key))
	}

	rows, err1 := stmt.Query(args...)
	defer rows.Close()
	if err1 != nil {
		return nil, err1
	}

	return p.doRows(rows)
}

// QueryPrepare 执行预操作
func (p *MSqlDB) QueryRowPrepare(key string, args ...interface{}) (row sqlRow, err error) {
	stmt, ok := p.stmts[key]
	if !ok {
		return nil, errors.New(fmt.Sprintf("not this prepare %s", key))
	}

	rows, err1 := stmt.Query(args...)
	defer rows.Close()
	if err1 != nil {
		return nil, err1
	}

	return p.doRow(rows)
}

// ExecPrepare 执行预操作
func (p *MSqlDB) ExecPrepare(key string, args ...interface{}) (int64, error) {
	stmt, ok := p.stmts[key]
	if !ok {
		return 0, errors.New(fmt.Sprintf("not this prepare %s", key))
	}

	res, err := stmt.Exec(args...)
	if err != nil {
		return 0, err
	}
	id, err1 := res.LastInsertId()
	return id, err1
}

// 创建一个mysql对象
func NewMysqlDB(param string) *MSqlDB {
	db := &MSqlDB{
		stmts:     make(map[string]*sql.Stmt, 10),
		tableList: make(map[string]bool, 10),
	}

	db.open(param)
	db.GetDBTableList()

	return db
}

// 获取表字段描述
func (p *MSqlDB) GetDBTableList() {
	tSqlResult, _ := p.QuerySQL("show tables")
	for _, v := range tSqlResult {
		for _, name := range v {
			p.tableList[string(name.([]uint8))] = true
		}
	}

	fmt.Println(p.tableList)
}

// 获取表字段描述
func (p *MSqlDB) CreatTableDesc(name string, desc MSqlTableDesc, AUTO_INCREMENT int) {
	if p.tableList[name] {
		fmt.Println("开始判断表是否一样")
		return
	}

	szSql := fmt.Sprintf("create table if not exists `%s` (", name)
	bFirstField := true

	for fieldName, v := range desc {
		szField := fmt.Sprintf("%s %s", fieldName, v.Type)
		if v.PRIMARY_KEY {
			szField += " PRIMARY KEY"
		}

		if v.UNIQUE {
			szField += " UNIQUE"
		}

		if !v.CanNull {
			szField += " not null"
		}

		if v.Default != "" {
			szField += " default " + v.Default
		}

		if v.AutoIncrease {
			szField += " auto_increment"
		}

		if !bFirstField {
			szSql += ", " + szField
		} else {
			szSql += szField
			bFirstField = false
		}
	}

	if AUTO_INCREMENT != 0 {
		szSql += fmt.Sprintf(") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=%d;", AUTO_INCREMENT)
	} else {
		szSql += ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
	}
	log.Println("BuildCreateTBSql", szSql)
	p.Exec(szSql)
	// return szSql
}

// 获取表字段描述
func (p *MSqlDB) GetTableDesc(table string) MSqlTableDesc {
	desc := make(MSqlTableDesc, 10)
	mp, _ := p.QuerySQL("desc " + table)
	for k, v := range mp {
		v1 := string(v["Field"].([]uint8))
		v2 := string(v["Extra"].([]uint8))
		v3 := string(v["Type"].([]uint8))
		v4 := string(v["Null"].([]uint8))
		v5 := string(v["Key"].([]uint8))
		var def string
		if v["Default"] != nil {
			def = string(v["Default"].([]uint8))
		}
		log.Println(k, "Extra:", v1, "Field:", v2, "Type:", v3, "Null:", v4, "Key:", v5, "Default", def)
		desc[v1] = &TableDesc{v3, def, v2, strings.Index(v5, "PRI") > -1, strings.Index(v5, "auto_increment") > -1, strings.Index(v5, "UNI") > -1, v4 == "YES"}
	}

	return desc
}
