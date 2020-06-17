package mgor

import (
	"fmt"
	"github.com/treeforest/gos/utils/dao/cache/redis"
	"github.com/treeforest/gos/utils/dao/db/mongodb"
	"time"
)

//MgoR 数据读取类
type MgoR struct {
	*DB
	preFuncs map[string]func() error
	inited   bool
}

//NewMgoR 创建一个数据读取器对象
func NewMgoR() *MgoR {
	return &MgoR{
		DB: &DB{
			cacheableDBs: make(map[string]*CacheaDB),
		},
		preFuncs: make(map[string]func() error),
	}
}

//Init 初始化一个数据读取器对象，入参为Mongo,redisOp
func (p *MgoR) Init(m mongodb.MongoOp, r redis.RedisOp) error {
	if m == nil || r == nil {
		//print("Init err, m or r is nil")
		return fmt.Errorf("Init err, m or r is nil")
	}

	p.DB.M = m
	p.DB.R = r

	for _, preFunc := range p.preFuncs {
		if err := preFunc(); err != nil {
			return err
		}
	}

	p.inited = true
	return nil
}

//RegCacheableDB 注册缓存对象和过期时间
func (p *MgoR) RegCacheableDB(ormStructPtr CacheaEntry, cacheExpiration time.Duration) (*CacheaDB, error) {
	if p.inited {
		return p.DB.RegCacheableDB(ormStructPtr, cacheExpiration)
	}

	dtName := ormStructPtr.DataBaseTableName()
	if _, ok := p.preFuncs[ormStructPtr.DataBaseTableName()]; ok {
		return nil, fmt.Errorf("RegCacheableDB repeated cacheable datebase:table: %s", dtName)
	}
	var cacheableDB = new(CacheaDB)

	var preFunc = func() error {
		_cacheableDB, err := p.DB.RegCacheableDB(ormStructPtr, cacheExpiration)
		if err == nil {
			*cacheableDB = *_cacheableDB
			p.DB.cacheableDBs[dtName] = cacheableDB
		}
		return err
	}

	p.preFuncs[dtName] = preFunc
	return cacheableDB, nil
}
