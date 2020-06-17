package mongodb

import (
	"log"
	"time"

	"errors"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type MongoOp interface {
	MongoOpClose() // 连接池关闭

	// 通用接口
	GetOne(database, table string, query, fields, ret interface{}) (err error) // 根据条件获取文档

	GetAll(database, table string, query, fields interface{}, sort []string, ret interface{}) (err error) //获取所有文档

	FindN(database, table string, query, fields interface{}, sort []string, skip, limit int, ret interface{}) (err error) // 获取分页文档

	FindAndAll(database, table string, query, fields, ret interface{}) (err error) // 条件and查询     and($and)

	FindOrAll(database, table string, querys, fields, ret interface{}) (err error) // 多条件or查询     or($or)

	Insert(database, table string, docs interface{}) error // 插入文档

	NumRows(database, table string, query interface{}) (int, error) // 根据条件获取文档数量

	Update(database, table string, query, update interface{}, upsert bool) (err error) // 更新集合中的文档

	UpdateSet(database, table string, query, modify interface{}, upsert bool) (err error) // 修改文档字段值

	UpdateInc(database, table string, query, increment interface{}, upsert bool) (err error) // 字段增加值

	UpdatePush(database, table string, query, push interface{}, upsert bool) (err error) // 增加元素

	UpdatePull(database, table string, query, pull interface{}, upsert bool) (err error) // 删除元素

	Remove(database, table string, query interface{}) error // 移除集合中符合条件的一个文档

	RemoveAll(database, table string, query interface{}) error // 移除集合中符合条件的所有文档

	Exists(database, table string, query interface{}) (bool, error) // 是否存在文档

	AddIndex(database, table string, key interface{}) error // 添加索引, desc 若为true, 表示逆序

	AddUniqueIndexs(database, table string, keys []string, unique bool) error // 添加唯一索引, unique 若为true 表示该字段只能存在一份不同值

	AddUniqueAndSparseIndexs(database, table string, keys []string, unique bool, sparse bool) error // 添加唯一索引, unique 若为true 表示该字段只能存在一份不同值

	Distinct(database, table, field string, query, ret interface{}) (err error)

	// 单条件查询
	FindEqualeAll(database, table, key string, value, ret interface{}) (err error) // =($eq)

	FindNotEqualeAll(database, table, key string, value, ret interface{}) (err error) // !=($ne)

	FindGreatAll(database, table, key string, value, ret interface{}) (err error) // >($gt)

	FindLessAll(database, table, key string, value, ret interface{}) (err error) // <($lt)

	FindGreatEqualAll(database, table, key string, value, ret interface{}) (err error) // >=($gte)

	FindLessEqualAll(database, table, key string, value, ret interface{}) (err error) // <=($lte)

	FindInAll(database, table, key string, values, ret interface{}) (err error) // in($in)

	FindAndModify(database, table string, query, update interface{}, upsert, returnNew bool, ret interface{}) error //更新并查找

	FindAndInc(database, table string, query, Update, ret interface{}) (err error) // 增加值并返回新结果

	Count(database, table string, query interface{}) (count int, err error) // 计数

	UpdateAll(database, table string, query, update interface{}) (err error) // 批量更新

	UpdateSetAll(database, table string, query, modify interface{}) (err error) // 修改多个文档字段值

	UpdateIncAll(database, table string, query, increment interface{}) (err error) // 修改多个文档的字段增加值

	UpdatePushAll(database, table string, query, push interface{}) (err error) // 增加多个文档的元素

	UpdatePullAll(database, table string, query, pull interface{}) (err error) // 删除多个文档的元素

	UpdateAddToSet(database, table string, selector interface{}, push interface{}, upsert bool) (err error) // 字段元素去重

}

type mongoOp struct {
	Seesio    *mgo.Session
	Addrs     []string // {"192.168.202.124:5000", }
	PoolLimit int      // 连接池限制
	User      string   // 用户名
	Passwd    string   // 密码
}

// mongo连接池
func NewMongoCenter(addrs []string, user, passwd string, poolLimit int) MongoOp {
	db := &mongoOp{
		Addrs:     addrs,
		User:      user,
		Passwd:    passwd,
		PoolLimit: poolLimit,
	}
	db.newMongoSession()
	return db
}

func (self *mongoOp) newMongoSession() {
	dialInfo := &mgo.DialInfo{
		Addrs:     self.Addrs,
		Direct:    false,
		Timeout:   time.Second * 5,
		PoolLimit: self.PoolLimit,
		Username:  self.User,
		Password:  self.Passwd,
	}
	session, err := mgo.DialWithInfo(dialInfo)

	if err != nil {
		log.Println("DialWithInfo err: ", err.Error(), self.Addrs, self.User, self.Passwd)
	}
	session.SetMode(mgo.Monotonic, true)
	self.Seesio = session
}

func (self *mongoOp) MongoOpClose() {
	self.Seesio.Close()
}

/*
Copy works just like New, but preserves the exact authentication
information from the original session.
*/
func (self *mongoOp) newSessionstore() *mgo.Session {
	return self.Seesio.Copy()
}

// 根据表名获取操作的seesion句柄和操作对象，使用完后需要关闭session
func (self *mongoOp) tableCollection(database, table string) (*mgo.Session, *mgo.Collection) {
	sess := self.newSessionstore()
	conn := sess.DB(database).C(table)
	return sess, conn
}

//查询
func (self *mongoOp) GetOne(database, table string, query, fields, ret interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	if fields == nil {
		err = conn.Find(query).One(ret)
	} else {
		err = conn.Find(query).Select(fields).One(ret)
	}
	return
}

//查询
func (self *mongoOp) GetAll(database, table string, query, fields interface{}, sort []string, ret interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	if len(sort) == 0 {
		sort = []string{"_id"}
	}
	if fields == nil {
		err = conn.Find(query).Sort(sort...).All(ret)
	} else {
		err = conn.Find(query).Select(fields).Sort(sort...).All(ret)
	}
	return
}

//查询
func (self *mongoOp) FindN(database, table string, query, fields interface{}, sort []string, skip, limit int, ret interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	if len(sort) == 0 {
		sort = []string{"_id"}
	}
	if fields == nil {
		err = conn.Find(query).Sort(sort...).Skip(skip).Limit(limit).All(ret)
	} else {
		err = conn.Find(query).Select(fields).Sort(sort...).Skip(skip).Limit(limit).All(ret)
	}
	return
}

func (self *mongoOp) FindAndAll(database, table string, query, fields, ret interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	if fields == nil {
		err = conn.Find(query).All(ret)
	} else {
		err = conn.Find(query).Select(fields).All(ret)
	}
	return
}

//查询
func (self *mongoOp) FindOrAll(database, table string, querys, fields, ret interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	if fields == nil {
		err = conn.Find(bson.M{"$or": querys}).All(ret)
	} else {
		err = conn.Find(bson.M{"$or": querys}).Select(fields).All(ret)
	}
	return
}

//插入
func (self *mongoOp) Insert(database, table string, docs interface{}) error {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	err := conn.Insert(docs)
	return err
}

//获取数量
func (self *mongoOp) NumRows(database, table string, query interface{}) (int, error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	ret, err := conn.Find(query).Count()
	return ret, err
}

//更新文档
func (self *mongoOp) Update(database, table string, query, update interface{}, upsert bool) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	if upsert {
		_, err = conn.Upsert(query, update)
	} else {
		err = conn.Update(query, update)
	}
	return err
}

//修改
func (self *mongoOp) UpdateSet(database, table string, query, modify interface{}, upsert bool) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	if upsert {
		_, err = conn.Upsert(query, bson.M{"$set": modify})
	} else {
		err = conn.Update(query, bson.M{"$set": modify})
	}
	return err
}

func (self *mongoOp) UpdateInc(database, table string, query, increment interface{}, upsert bool) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	if upsert {
		_, err = conn.Upsert(query, bson.M{"$inc": increment})
	} else {
		err = conn.Update(query, bson.M{"$inc": increment})
	}
	return err
}

func (self *mongoOp) UpdatePush(database, table string, query, push interface{}, upsert bool) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	if upsert {
		_, err = conn.Upsert(query, bson.M{"$push": push})
	} else {
		err = conn.Update(query, bson.M{"$push": push})
	}
	return err
}

func (self *mongoOp) UpdatePull(database, table string, query, pull interface{}, upsert bool) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	if upsert {
		_, err = conn.Upsert(query, bson.M{"$pull": pull})
	} else {
		err = conn.Update(query, bson.M{"$pull": pull})
	}
	return err
}

// 删除
func (self *mongoOp) Remove(database, table string, query interface{}) error {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	err := conn.Remove(query)
	return err
}

func (self *mongoOp) RemoveAll(database, table string, query interface{}) error {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	_, err := conn.RemoveAll(query)
	return err
}

func (self *mongoOp) Exists(database, table string, query interface{}) (bool, error) {
	ret, err := self.NumRows(database, table, query)
	return ret > 0, err

}

//添加索引
func (self *mongoOp) AddIndex(database, table string, key interface{}) error {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()

	var indexKey []string
	keyMap, ok := key.(bson.M)
	if !ok {
		return errors.New("type(key) is not bson.M")
	}

	for k, v := range keyMap {
		if v.(int) == 0 { //降序
			k = "-" + k
		}
		indexKey = append(indexKey, k)
	}
	index := mgo.Index{
		Key:        indexKey, // 索引字段， 默认升序,若需降序在字段前加-
		Unique:     false,    // 唯一索引 同mysql唯一索引
		DropDups:   true,     // 索引重复替换旧文档,Unique为true时失效
		Background: true,     // 后台创建索引
	}

	err := conn.EnsureIndex(index)
	return err
}

// 批量添加唯一索引
func (self *mongoOp) AddUniqueIndexs(database, table string, keys []string, unique bool) error {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()

	index := mgo.Index{
		Key:        keys,   // 索引字段， 默认升序,若需降序在字段前加-
		Unique:     unique, // 唯一索引 同mysql唯一索引
		DropDups:   true,   // 索引重复替换旧文档,Unique为true时失效
		Background: true,   // 后台创建索引
	}

	err := conn.EnsureIndex(index)
	return err
}

// 添加唯一和稀疏索引
func (self *mongoOp) AddUniqueAndSparseIndexs(database, table string, keys []string, unique bool, sparse bool) error {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()

	index := mgo.Index{
		Key:    keys,   // 索引字段， 默认升序,若需降序在字段前加-
		Unique: unique, // 唯一索引 同mysql唯一索引
		//DropDups:   true,   // 索引重复替换旧文档,Unique为true时失效
		Background: false, // 后台创建索引
		Sparse:     sparse,
	}

	err := conn.EnsureIndex(index)
	return err
}

// 查询去重结果
func (self *mongoOp) Distinct(database, table, field string, query, ret interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	err = conn.Find(query).Distinct(field, ret)
	return
}

func (self *mongoOp) FindEqualeAll(database, table, key string, value, ret interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	err = conn.Find(bson.M{key: value}).All(ret)
	return
}

func (self *mongoOp) FindNotEqualeAll(database, table, key string, value, ret interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	err = conn.Find(bson.M{key: bson.M{"$ne": value}}).All(ret)
	return
}

func (self *mongoOp) FindGreatAll(database, table, key string, value, ret interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	err = conn.Find(bson.M{key: bson.M{"$gt": value}}).All(ret)
	return
}

func (self *mongoOp) FindLessAll(database, table, key string, value, ret interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	err = conn.Find(bson.M{key: bson.M{"$lt": value}}).All(ret)
	return
}

func (self *mongoOp) FindGreatEqualAll(database, table, key string, value, ret interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	err = conn.Find(bson.M{key: bson.M{"$gte": value}}).All(ret)
	return
}

func (self *mongoOp) FindLessEqualAll(database, table, key string, value, ret interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	err = conn.Find(bson.M{key: bson.M{"$lte": value}}).All(ret)
	return
}

func (self *mongoOp) FindInAll(database, table, key string, values, ret interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	err = conn.Find(bson.M{key: bson.M{"$in": values}}).All(ret)
	return
}

func (self *mongoOp) FindAndModify(database, table string, query, update interface{}, upsert, returnNew bool, ret interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	change := mgo.Change{
		Update:    update,
		Upsert:    upsert,
		Remove:    false,
		ReturnNew: returnNew,
	}
	_, err = conn.Find(query).Apply(change, ret)
	return err
}

// 修改并查询结果
func (self *mongoOp) FindAndInc(database, table string, query, Update, ret interface{}) (err error) {
	change := mgo.Change{
		Update:    bson.M{"$inc": Update},
		Upsert:    true,
		Remove:    false,
		ReturnNew: true,
	}

	sess, conn := self.tableCollection(database, table)
	defer sess.Close()

	_, err = conn.Find(query).Apply(change, ret)
	return
}

func (self *mongoOp) Count(database, table string, query interface{}) (count int, err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	count, err = conn.Find(query).Count()
	return count, err
}

func (self *mongoOp) UpdateAll(database, table string, query, update interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	_, err = conn.UpdateAll(query, update)
	return err
}

//修改
func (self *mongoOp) UpdateSetAll(database, table string, query, modify interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	_, err = conn.UpdateAll(query, bson.M{"$set": modify})

	return err
}

func (self *mongoOp) UpdateIncAll(database, table string, query, increment interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	_, err = conn.UpdateAll(query, bson.M{"$inc": increment})

	return err
}

func (self *mongoOp) UpdatePushAll(database, table string, query, push interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	_, err = conn.UpdateAll(query, bson.M{"$push": push})

	return err
}

func (self *mongoOp) UpdatePullAll(database, table string, query, pull interface{}) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	_, err = conn.UpdateAll(query, bson.M{"$pull": pull})

	return err
}

func (self *mongoOp) UpdateAddToSet(database, table string, selector interface{}, push interface{}, upsert bool) (err error) {
	sess, conn := self.tableCollection(database, table)
	defer sess.Close()
	if upsert {
		_, err = conn.Upsert(selector, bson.M{"$addToSet": push})
	} else {
		err = conn.Update(selector, bson.M{"$addToSet": push})

	}
	return err
}

/*
type Dummy struct {
	ID   string `bson:"_id"`
	Mid  int64  `bson:"mid"`
	Fid  int64  `bson:"fid"`
	Time int64  `bson:"time"`
}

func NewDummy() *Dummy {
	return &Dummy{}
}

func (self *Dummy) Insert(operate MongoOp, database, table string, docs *Dummy) error {
	err := operate.Insert(database, table, docs)
	return err
}

func (self *Dummy) ConvertDummy(src map[string]interface{}) (*Dummy, error) {
	obj := &Dummy{}
	OK := false
	if obj.ID, OK = src["_id"].(string); !OK {
		return nil, fmt.Errorf("Dummy,ID convert fail!")
	}
	if obj.Mid, OK = src["mid"].(int64); !OK {
		return nil, fmt.Errorf("Dummy.Mid convert fail!")
	}
	if obj.Fid, OK = src["fid"].(int64); !OK {
		return nil, fmt.Errorf("Dummy.Mid convert fail!")
	}
	if obj.Time, OK = src["time"].(int64); !OK {
		return nil, fmt.Errorf("Dummy.Time convert fail!")
	}

	return obj, nil
}

func (self *Dummy) FindOne(operate MongoOp, database, table, key string) (docs *Dummy, err error) {
	ret, err := operate.GetOneByKey(database, table, "_id", key)
	if err != nil {
		return nil, err
	}
	docs, err = self.ConvertDummy(ret)
	return
}

func (self *Dummy) FindN(operate MongoOp, database, table, key string, value interface{}, limit int) (docs []*Dummy, err error) {
	ret, err := operate.FindN(database, table, key, value, limit)
	if err != nil {
		return nil, err
	}
	for _, v := range ret {
		vv, err := self.ConvertDummy(v)
		if err != nil {
			return nil, err
		}
		docs = append(docs, vv)
	}
	return
}

func (self *Dummy) Remove(operate MongoOp, database, table, key string) error {
	err := operate.RemoveByKey(database, table, "_id", key)
	return err
}

func (self *Dummy) Exists(operate MongoOp, database, table, key string) (bool, error) {
	ret, err := operate.NumRowsByKey(database, table, "_id", key)
	return ret > 0, err
}

func (self *Dummy) Count(operate MongoOp, database, table string, mid int64) (int, error) {
	return operate.NumRowsByKey(database, table, "mid", mid)
}
*/
