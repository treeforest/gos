package mgor

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/treeforest/gos/utils/dao/cache/redis"
	"github.com/treeforest/gos/utils/dao/db/mongodb"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"reflect"
	"strings"
	"time"
)

var (
	//ErrNotFound 错误码，没有找到数据
	ErrNotFound = mgo.ErrNotFound
)

//DB 数据库连接实例
type DB struct {
	M mongodb.MongoOp
	R redis.RedisOp

	cacheableDBs map[string]*CacheaDB
}

type (
	//CacheaEntry 缓存实体接口
	CacheaEntry interface {
		DataBaseTableName() string
	}
	//CacheaDB 缓存实体
	CacheaDB struct {
		*DB
		databaseName      string
		tableName         string
		cachePriKeyPrefix string
		cacheExpiration   time.Duration
		typeName          string
		module            *redis.Module
	}
)

var (
	//ErrCacheNil 缓存值为空
	ErrCacheNil = errors.New("DB.Cache (redis) is nil")
	//ErrName 缓存名称错误
	ErrName = errors.New("Name format should DataBaseName:TableName")
)

//RegCacheableDB 注册缓存实体，CacheaEntry 缓存对象实体, time.Duration 过期时间
func (d *DB) RegCacheableDB(ormStructPtr CacheaEntry, cacheExpiration time.Duration) (*CacheaDB, error) {
	dtName := ormStructPtr.DataBaseTableName()
	s := strings.Split(dtName, ":")
	if len(s) != 2 {
		return nil, ErrName
	}
	databaseName := s[0]
	tableName := s[1]

	if _, ok := d.cacheableDBs[dtName]; ok {
		return nil, fmt.Errorf("Reg repeated cacheable table: %s", dtName)
	}
	if d.R == nil {
		return nil, ErrCacheNil
	}

	module := redis.NewModule(dtName)
	t := reflect.TypeOf(ormStructPtr)
	c := &CacheaDB{
		DB:           d,
		tableName:    tableName,
		databaseName: databaseName,
		// 采用固定前缀的二级缓存key
		cachePriKeyPrefix: module.Key("_id"),
		cacheExpiration:   cacheExpiration,
		typeName:          t.String(),
		module:            module,
	}
	d.cacheableDBs[dtName] = c
	return c, nil
}

//GetCacheableDB 获得一个缓存的实体, dtName 为数据库表名
func (d *DB) GetCacheableDB(dtName string) (*CacheaDB, error) {
	c, ok := d.cacheableDBs[dtName]
	if !ok {
		return nil, fmt.Errorf("has not called *DB.RegCacheableDB() to register: %s", dtName)
	}
	return c, nil
}

//CacheKey 缓存key
type CacheKey struct {
	Key         string
	FieldValues []interface{}
	isPriKey    bool
}

var emptyCacheKey = CacheKey{}

//CreateCacheKeyByFields 通过字段生成缓存键值,fields 字段名数组， values 值数据
func (c *CacheaDB) CreateCacheKeyByFields(fields []string, values []interface{}) (string, error) {
	if len(fields) != len(values) {
		return "", errors.New("CreateCacheKeyByFields(): len(fields) != len(values)")
	}
	bs, err := json.Marshal(values)
	if err != nil {
		return "", errors.New("CreateCacheKeyByFields(): " + err.Error())
	}
	log.Printf("CreateCacheKeyByFields json OriKey(%s) \n", string(bs))

	return c.module.Key(strings.Join(fields, "&") + string(bs)), nil
}

func (c *CacheaDB) createPrikey(structPtr CacheaEntry) (string, error) {
	var v = reflect.ValueOf(structPtr).Elem()
	// Parse objectIdHex's hex
	//解析objectIdHex的十六进制
	objectIdHex := bson.ObjectId(fmt.Sprintf("%v", v.FieldByName("ID").Interface()))
	values := []interface{}{
		objectIdHex.Hex(),
	}
	bs, err := json.Marshal(values)
	if err != nil {
		return "", errors.New("*CacheaDB.createPrikey(): " + err.Error())
	}

	//log.Printf("createPrikey (priKey :%s)\n", string(bs))

	return c.cachePriKeyPrefix + string(bs), nil
}

func (c *CacheaDB) createPrikeyById(id string) (string, error) {
	//解析objectIdHex的十六进制
	objectIdHex := bson.ObjectId(id)
	values := []interface{}{
		objectIdHex.Hex(),
	}

	bs, err := json.Marshal(values)
	//bs, err := json.Marshal([]byte(id))
	if err != nil {
		return "", errors.New("*CacheaDB.createPrikeyById(): " + err.Error())
	}
	//log.Printf("createPrikeyById (priKey :%s)\n", string(bs))
	return c.cachePriKeyPrefix + string(bs), nil
}

//CreateCacheKey 创建一个缓存键值， 通过缓存实体 和Mongo 表实体
func (c *CacheaDB) CreateCacheKey(structPtr CacheaEntry, m bson.M) (CacheKey, error) {
	var t = reflect.TypeOf(structPtr)
	var typeName = t.String()
	if c.typeName != typeName {
		return emptyCacheKey, fmt.Errorf("CreateCacheKey(): unmatch Cacheable: want %s, have %s", c.typeName, typeName)
	}
	//var v = reflect.ValueOf(structPtr).Elem()
	var fields = make([]string, 0, 2)
	var values = make([]interface{}, 0, 2)
	var cacheKey string
	var isPri bool
	if len(m) == 0 {
		// TODO 这里必须要传fields
		return emptyCacheKey, errors.New("CreateCacheKey(): must transfer fields")
	} else if v, ok := m["_id"]; len(m) == 1 && ok {
		var err error
		cacheKey, err = c.createPrikeyById(fmt.Sprintf("%v", v))
		//log.Printf("createPrikeyById id(%d), idKey（%s）\n",v,cacheKey)

		if err != nil {
			return emptyCacheKey, err
		}
		isPri = true

	} else {
		for f, v := range m {
			switch v.(type) {
			case bson.M:
				_v := v.(bson.M)
				for ff, vv := range _v {
					values = append(values, vv)
					fields = append(fields, ff)
				}

			case []bson.M:
				_v := v.([]bson.M)
				for _, vv := range _v {
					for fff, vvv := range vv {
						values = append(values, vvv)
						fields = append(fields, fff)
					}
				}
			default:

				values = append(values, v)
				fields = append(fields, f)
			}

			//values = append(values, v)
			//fields = append(fields,f)
		}

		var err error
		cacheKey, err = c.CreateCacheKeyByFields(fields, values)
		if err != nil {
			return emptyCacheKey, err
		}
	}
	return CacheKey{
		Key:         cacheKey,
		FieldValues: values,
		isPriKey:    isPri,
	}, nil
}

//CacheGet 从缓存中读取数据实体，destStructPtr
func (c *CacheaDB) CacheGet(destStructPtr CacheaEntry, m bson.M) error {
	var cacheKey, err = c.CreateCacheKey(destStructPtr, m)
	if err != nil {
		return err
	}

	var (
		key                 = cacheKey.Key
		gettedFirstCacheKey = cacheKey.isPriKey
	)

	//二级缓存读一级缓存
	if !gettedFirstCacheKey {
		var s string
		s, err = c.R.GetKey(key)
		if err == nil {
			//log.Printf("CacheGet(): 读到一级缓存, second（%s）, first(%s)\n", key, s)
			key = s
			gettedFirstCacheKey = true
		} else if !redis.IsRedisNil(err) {
			log.Printf("CacheGet(): IsRedisNil: %s \n", err.Error())

			return err
		}
	}

	var exist bool
	var checkSrc string
	// 一级缓存
	if gettedFirstCacheKey {
		//log.Printf("找到一级缓存 key(%s)\n", key)
		exist, checkSrc, err = c.getFirstCache(key, destStructPtr)
		if err != nil {
			return err
		}
		if exist {
			//fmt.Println("获取成功")

			// check
			if !cacheKey.isPriKey && !c.checkSecondCache(checkSrc, m, cacheKey.FieldValues) {
				err = c.R.DelKey(cacheKey.Key)
				if err != nil {
					fmt.Println("删除缓存key失败")
				}

				log.Printf("校验失败, 二级缓存key(%s) \n", cacheKey.Key)
			} else {
				//log.Println("校验成功,命中")

				return nil
			}
		}
	}

	//锁定或获取第一个缓存
	c.R.LockCallback("MgoRlock_"+key, func(l string) {
		defer c.R.DelKey(l)
		var s string
		if !exist {
		FIRST:

			if gettedFirstCacheKey {
				// 别的地方获取到了

				exist, _, err = c.getFirstCache(key, destStructPtr)
				if exist {
					err = nil
					return
				}

				if err != nil {
					return
				}

			} else {

				s, err = c.R.GetKey(key)
				if err == nil {
					key = s
					gettedFirstCacheKey = true
					goto FIRST
				} else if !redis.IsRedisNil(err) {
					return
				}
			}
		}

		err = c.DB.M.GetOne(c.databaseName, c.tableName, m, nil, destStructPtr)
		if err != nil {
			log.Println("getone err")
			return
		}

		key, err = c.createPrikey(destStructPtr)
		if err != nil {
			println("CacheGet(): createPrikey: %s", err.Error())
			err = nil
			return
		}

		// write cache
		data, err := json.Marshal(destStructPtr)
		if err != nil {
			log.Fatalf("CacheGet(): json Marshal : %s", err.Error())

		}

		//println("CacheGet json Marshal key ,data :",key,string(data))
		err = c.R.SetExpireKey(key, string(data), int(c.cacheExpiration))
		if err == nil && !cacheKey.isPriKey {
			log.Printf("CacheGet():SetExpireKey cacheKey:(%s), prikey(%s)\n", cacheKey.Key, key)

			err = c.R.SetExpireKey(cacheKey.Key, key, int(c.cacheExpiration))
		}
		if err != nil {
			fmt.Printf("CacheGet():SetExpireKey err: %s", err.Error())
			err = nil
		}
	}, 5)

	return err
}

//CreateGetQueryByField
func (c *CacheaDB) CreateGetQueryByField(values []interface{}, whereFields ...string) bson.M {
	m := bson.M{}
	if len(whereFields) == 0 {
		return m
	}
	for index, col := range whereFields {
		println("CreateGetQuery query :", col, values[index])

		m[col] = values[index]
	}
	return m
}

func (c *CacheaDB) checkSecondCache(checkSrc string, m bson.M, values []interface{}) bool {
	//第一版
	//v := reflect.ValueOf(destStructPtr).Elem()
	//for i, field := range fields {
	//	vv := v.FieldByName(CamelString(field))
	//	if vv.Kind() == reflect.Ptr {
	//		vv = vv.Elem()
	//	}
	//	if values[i] != vv.Interface() {
	//		return false
	//	}
	//}
	//第二版
	//b := bson.M{}
	//bd,_:= bson.Marshal(destStructPtr)
	//bson.Unmarshal(bd,&b)
	//
	//for k,_:=range b {
	//	println(k)
	//}

	//for k,v := range m {
	//
	//	if vv,ok := b[k] ;!ok {
	//		println("checkSecondCache !ok ")
	//
	//		return false
	//	} else {
	//		println("v,vv  :",v,vv)
	//		if vv != v {
	//			return false
	//		}
	//	}
	//}

	for k, v := range m {
		switch v.(type) {
		case bson.M:
			_v := v.(bson.M)
			b := c.checkSecondCache(checkSrc, _v, values)
			if b == false {
				return false
			}
		case []bson.M:
			vv := v.([]bson.M)
			for _, _v := range vv {
				b := c.checkSecondCache(checkSrc, _v, values)
				if b == false {
					return false
				}
			}
		default:
			if ok := strings.Contains(checkSrc, fmt.Sprintf("%s", v)); !ok {
				log.Printf("失败： key(%s), v(%s),\n check(%s)\n", k, fmt.Sprintf("%s", v), checkSrc)
				return false
			}
		}

	}

	return true
}

func (c *CacheaDB) getFirstCache(key string, destStructPtr CacheaEntry) (bool, string, error) {
	data, err := c.R.GetKey(key)
	if err == nil {
		err = json.Unmarshal([]byte(data), destStructPtr)
		if err == nil {
			//log.Printf("getFirstCache json Unmarshal,stringData(%s) Data(%+v)\n",string(data),destStructPtr)
			return true, data, nil
		}
		//fmt.Printf("getFirstCache:json.Unmarshal (err :%s)", err.Error())
	} else if !redis.IsRedisNil(err) {
		//log.Println("getFirstCache :", ""+key)
		return false, "", err
	}

	return false, "", nil
}

//DeleteCache 删除缓存
func (c *CacheaDB) DeleteCache(structPrt CacheaEntry, m bson.M) error {

	cacheKey, err := c.CreateCacheKey(structPrt, m)
	if err != nil {
		return err
	}

	var keys = []string{cacheKey.Key}

	if !cacheKey.isPriKey {
		firstKey, err := c.R.GetKey(cacheKey.Key)
		if err == nil {
			keys = append(keys, firstKey)
		}
	}

	return c.R.DelMKey(keys)

}

//PutCache 缓存对象
func (c *CacheaDB) PutCache(structPrt CacheaEntry, m bson.M) error {

	cacheKey, err := c.CreateCacheKey(structPrt, m)
	if err != nil {
		return err
	}

	data, err := json.Marshal(structPrt)
	if err != nil {
		return err
	}

	key := cacheKey.Key

	if cacheKey.isPriKey {
		return c.R.SetExpireKey(key, string(data), int(c.cacheExpiration))
	}

	// secondary cache
	key, err = c.createPrikey(structPrt)
	if err != nil {
		return err
	}
	err = c.R.SetExpireKey(key, string(data), int(c.cacheExpiration))
	if err != nil {
		return err
	}
	return c.R.SetExpireKey(cacheKey.Key, key, int(c.cacheExpiration))

}
