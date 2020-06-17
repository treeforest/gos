package redis

import (
	"fmt"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

type RedisOp interface {
	RedisOpClose() // 关闭池子
	// key
	GetKeyByByte(sKey string, value BasePB) error          // 获取满足pb协议的对象值
	GetKey(sKey string) (string, error)                    // 返回 key 所关联的字符串值
	Incrbykey(sKey string, increment int64) (int64, error) // 将 key 所储存的值加上增量 increment, 如果key不存在,那么 key 的值会先被初始化为 0 ，然后再执行 INCRBY 命令
	Mget(keys []string) ([]string, error)                  // 返回所有(一个或多个)给定 key 的值。
	ExpireKey(sKey string, lExpireTime int) error          // 为给定 key 设置生存时间，当 key 过期时(生存时间为 0 )，它会被自动删除
	DelKey(sKey string) error                              // 删除给定的key
	DelMKey(keys []string) error                           // 删除多个key
	KeyExists(sKey string) (bool, error)                   // 检查给定 key 是否存在, 若 key 存在，返回true
	GetKeyByInt64(sKey string) (int64, error)              // 返回 key 所关联的int64值
	KeyTTL(sKey string) (int64, error)
	// string
	SetKeyByByte(sKey string, value BasePB) error                        // 设定满足pb协议的对象值
	SetKey(sKey, sValue string) error                                    // 将字符串值 value 关联到 key
	SetExpireNXkey(sKey, sValue string, lExpireTime int) (string, error) // 设置键的过期时间为 second 秒, 只在键不存在时，才对键进行设置操作
	SetExpireKey(sKey, sValue string, lExpireTime int) error             // 设置键的过期时间为 second 秒

	// hash
	HMSet(hashkey string, args interface{}) error                     // args maybe struct or map
	HMListset(hashkey string, fields, values []string) error          // 同时将多个 field-value (域-值)对设置到哈希表 key 中
	HSet(hashkey, field, value string) error                          // 将 field-value (域-值)对设置到哈希表 key 中
	HGet(hashkey, field string) (string, error)                       // 返回哈希表 key 中给定域 field 的值
	HMget(hashkey string, fields []string) (map[string]string, error) // 返回哈希表 key 中给定域列表 field list 的值
	HMgetSlice(hashkey string, fields []string) ([]string, error)
	HDel(hashkey, filed string) error                              // 删除哈希表 key 中的一个指定域，不存在的域将被忽略
	HLen(hashkey string) (int, error)                              // 返回哈希表 key 中域的数量
	HKeys(hashkey string) ([]string, error)                        // 返回哈希表 key 中的所有域
	HIncrby(hashkey, filed string, increment int32) (int64, error) // 为哈希表 key 中的域 field 的值加上增量 increment, 增量也可以为负数
	HGetAllIntInt(hashkey string) (map[int64]int64, error)         // 获取field、value 为int型的哈希表map
	HGetAllIntString(hashkey string) (map[int64]string, error)     // 获取field为int型、value为sting型的哈希表map
	HGetAllStringString(hashkey string) (map[string]string, error) // 获取field、value为string型的哈希表map
	HExistsInt(hashkey string, filed int64) (bool, error)          // 查看哈希表 key 中，给定域int型 field 是否存在, 如果哈希表含有给定域，返回true
	HExistsString(hashkey string, filed string) (bool, error)      // 查看哈希表 key 中，给定域string型 field 是否存在, 如果哈希表含有给定域，返回true

	//list
	LPush(key, value string) error               // 将一个值 value 插入到列表 key 的表头
	LPushMore(key string, values []string) error // 将一个或多个值 value 插入到列表 key 的表头
	RPush(key, value string) error               // 将一个值 value 插入到列表 key 的表尾
	RPushMore(key string, values []interface{}) error
	RPushMoreByByte(key string, value []BasePB) error
	LRange(key string, begin, end int) ([]string, error)               // 返回列表 key 中指定区间内的元素，区间以偏移量 start 和 stop 指定
	LPop(key string) (string, error)                                   // 移除并返回列表 key 的头元素
	RPop(key string) (string, error)                                   // 移除并返回列表 key 的尾元素
	LLEN(key string) (int, error)                                      // 返回列表 key 的长度， 如果 key 不存在，则 key 被解释为一个空列表，返回 0
	LIndex(key string, index int) (interface{}, error)                 // 通过索引获取列表中的元素
	LInsert(key, pivot string, value ...interface{}) error             // 将值 value 插入到列表 key 当中，位于值 pivot 之前。
	LRem(key interface{}, count int, value interface{}) (int64, error) // 根据参数 count 的值，移除列表中与参数 value 相等的元素
	LTrim(key string, begin, end int) error                            // 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除

	// set
	SAdd(key, member interface{}) (int, error)            // 将一个 member 元素加入到集合 key 当中，已经存在于集合的 member 元素将被忽略
	SCard(key string) (int, error)                        // 取集合 key 中的成员数量
	SMAdd(key string, members []interface{}) (int, error) // 将一个或多个 member 元素加入到集合 key 当中，已经存在于集合的 member 元素将被忽略
	SMembers(key string) ([]string, error)                // 返回集合 key 中的所有成员
	SRem(key, member interface{}) (int, error)            // 移除集合 key 中的一个 member 元素，不存在的 member 元素会被忽略
	SMRem(key string, members []string) (int, error)      // 移除集合 key 中的多个 member 元素，不存在的 member 元素会被忽略
	SIsmembers(key, member string) (bool, error)          // 判断 member 元素是否集合 key 的成员
	SMsUint64(key string) ([]int64, error)
	//zset
	ZAdd(key string, score int, member string) error // 将一个 member 元素及其 score 值加入到有序集 key 当中
	ZMAdd(key string, members []interface{}) error   // 将一个 member 元素及其 score 值加入到有序集 key 当中

	ZCount(key string, min, max int) (int, error)
	ZRange(key string, start, stop int) ([]string, error)
	ZRangeWithScores(key string, start, stop int) (map[string]string, error)
	ZRangeByScores(key string, start, stop int) ([]string, error)
	ZRangeByScoresWithLimit(key string, min, max string, offset, count int) ([]string, error) //
	ZIncrby(key string, increment int, member string) (int64, error)                          // 为有序集 key 的成员 member 的 score 值加上增量 increment
	ZSocre(key, member string) (int64, error)                                                 // 返回有序集 key 中，成员 member 的 score 值
	ZRem(key string, members []string) error                                                  // 移除有序集 key 中的一个或多个成员，不存在的成员将被忽略
	ZRevRangeWithScores(key string, begin, end int) (map[string]string, error)                // 返回有序集 key 中，指定区间内的成员， 其中成员的位置按 score 值递减(从大到小)来排列
	ZRevRange(key string, begin, end int) ([]string, error)                                   // 返回有序集 key 中，指定区间内的成员
	ZRevRank(key string, member string) (int64, error)
	ZRevRangeWithScores2(key string, begin, end int) ([]string, error) // 返回有序的
	ZCard(key string) (int64, error)
	ZREMRANGEBYSCORE(key string, begin, end int) (int64, error)
	ZPop(key string, num int) (result map[string]int, err error)
	ZRem2(key string, members []string) (int64, error) // 移除有序集 key 中的一个或多个成员，不存在的成员将被忽略

	// redis cmd
	RedisDocmdArgs(cmd string, args ...interface{}) (repley interface{}, err error) // 自定义命令操作

	RedisPing() (bool, error) // ping

	LockCallback(lockKey string, callback func(lockKey string), maxLock ...time.Duration) error
}

type redisOp struct {
	*redis.Pool
	ServerAdd string // "192.168.202.128:4600"
	Password  string // 密码
	Maxidle   int    // Maximum number of idle connections in the pool.
}

// INIT OBJ
func NewRedisObj(serverAdd, password string, maxidle int) RedisOp {
	db := &redisOp{
		ServerAdd: serverAdd,
		Password:  password,
		Maxidle:   maxidle,
	}
	db.NewRedisCenter()
	return db
}

// INIT CONNECT POOL
//func (self *redisOp) NewRedisCenter() {
//	self.Pool = &redis.Pool{
//		MaxIdle:     self.Maxidle,
//		IdleTimeout: 0,
//		Dial: func() (redis.Conn, error) {
//			c, err := redis.Dial("tcp", self.ServerAdd)
//			if err != nil {
//				return nil, err
//			}
//			if len(self.Password) > 0 {
//				if _, err := c.Do("AUTH", self.Password); err != nil {
//					c.Close()
//					return nil, err
//				}
//			}
//			return c, err
//		},
//		TestOnBorrow: func(c redis.Conn, t time.Time) error {
//			_, err := c.Do("PING")
//			return err
//		},
//	}
//}

func (self *redisOp) NewRedisCenter() {
	readTimeout := redis.DialReadTimeout(time.Second * time.Duration(2))
	writeTimeout := redis.DialWriteTimeout(time.Second * time.Duration(2))
	conTimeout := redis.DialConnectTimeout(time.Second * time.Duration(5))

	self.Pool = &redis.Pool{
		MaxIdle:     self.Maxidle,
		MaxActive:   1024,
		IdleTimeout: 0,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", self.ServerAdd, readTimeout, writeTimeout, conTimeout)
			if err != nil {
				return nil, err
			}
			if len(self.Password) > 0 {
				if _, err := c.Do("AUTH", self.Password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func (self *redisOp) RedisOpClose() {
	self.Pool.Close()
}

// Get gets a connection. The application must close the returned connection.
func (self *redisOp) NewRedisConnect() redis.Conn {
	return self.Pool.Get() // 从池里获取连接
}

/********************* 兼容pb协议 **************************/
type BasePB interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

func (self *redisOp) GetKeyByByte(sKey string, value BasePB) error {
	conn := self.NewRedisConnect()
	defer conn.Close() // 用完后将连接放回连接池
	v, err := redis.Bytes(conn.Do("get", sKey))
	if err != nil {
		return err
	}

	err = value.Unmarshal(v)
	return err
}

func (self *redisOp) SetKeyByByte(sKey string, value BasePB) error {
	conn := self.NewRedisConnect()
	v, err := value.Marshal()
	if err != nil {
		return err
	}
	defer conn.Close() // 用完后将连接放回连接池
	_, err = conn.Do("set", sKey, v)
	return err
}

func (self *redisOp) GetKey(sKey string) (string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close() // 用完后将连接放回连接池
	r, err := conn.Do("get", sKey)
	sValue, err := redis.String(r, err)
	if err != nil {
		return "", err
	}
	return sValue, nil
}

func (self *redisOp) GetKeyByInt64(sKey string) (int64, error) {
	conn := self.NewRedisConnect()
	defer conn.Close() // 用完后将连接放回连接池
	r, err := conn.Do("get", sKey)
	nValue, err := redis.Int64(r, err)
	if err != nil {
		return 0, err
	}
	return nValue, nil
}

func (self *redisOp) SetKey(sKey, sValue string) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("set", sKey, sValue)
	if err != nil {
		println("conn.Do set:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) SetExpireKey(sKey, sValue string, lExpireTime int) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("SET", sKey, sValue, "EX", lExpireTime)
	if err != nil {
		println("conn.Do SetExpireCache:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) ExpireKey(sKey string, lExpireTime int) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("expire", sKey, lExpireTime)
	if err != nil {
		println("conn.Do ExpireKey:  ", err.Error())
		return err
	}
	return nil
}

func (self *redisOp) SetExpireNXkey(sKey, sValue string, lExpireTime int) (string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("SET", sKey, sValue, "EX", lExpireTime, "NX")
	v, err := redis.String(r, err)
	if err != nil {
		println("conn.Do SetExpireNXkey:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) Incrbykey(sKey string, increment int64) (int64, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("INCRBY", sKey, increment)
	v, err := redis.Int64(r, err)
	if err != nil {
		println("conn.Do Incrbykey:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) Mget(keys []string) ([]string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("MGET", redis.Args{}.AddFlat(keys)...)
	v, err := redis.Strings(r, err)
	if err != nil {
		return v, err
	}

	return v, nil
}

func (self *redisOp) DelKey(sKey string) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("del", sKey)
	if err != nil {
		println("conn.Do del:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) DelMKey(keys []string) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("del", redis.Args{}.AddFlat(keys)...)
	if err != nil {
		println("conn.Do del:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) KeyExists(sKey string) (bool, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("exists", sKey)
	ret, err := redis.Bool(r, err)
	if err != nil {
		println("CacheExists:  ", err.Error())
		return false, err
	}

	return ret, nil
}

func (self *redisOp) KeyTTL(sKey string) (int64, error) {
	conn := self.NewRedisConnect()
	defer conn.Close() // 用完后将连接放回连接池
	r, err := conn.Do("TTL", sKey)
	nValue, err := redis.Int64(r, err)
	if err != nil {
		return 0, err
	}
	return nValue, nil
}

func (self *redisOp) SAdd(key, member interface{}) (int, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	v, err := redis.Int(conn.Do("SADD", key, member))
	if err != nil {
		println("conn.Do SetAdd:  ", err.Error())
		return 0, err
	}

	return v, nil
}

func (self *redisOp) SCard(key string) (int, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("SCARD", key)
	v, err := redis.Int(r, err)
	if err != nil {
		println("conn.Do SCARD:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) SMAdd(key string, members []interface{}) (int, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	v, err := redis.Int(conn.Do("SADD", redis.Args{}.Add(key).AddFlat(members)...))
	if err != nil {
		println("conn.Do SetAdd:  ", err.Error())
		return 0, err
	}

	return v, nil
}

func (self *redisOp) SMembers(key string) ([]string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("SMEMBERS", key)
	v, err := redis.Strings(r, err)
	if err != nil {
		println("conn.Do SetMembers:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) SMsUint64(key string) ([]int64, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("SMEMBERS", key)
	v, err := redis.Int64s(r, err)
	if err != nil {
		println("conn.Do SetMembers:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) SRem(key, member interface{}) (int, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	v, err := redis.Int(conn.Do("SREM", key, member))
	if err != nil {
		println("conn.Do SREM:  ", err.Error())
		return 0, err
	}

	return v, nil
}

func (self *redisOp) SMRem(key string, members []string) (int, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	v, err := redis.Int(conn.Do("SREM", redis.Args{}.Add(key).AddFlat(members)...))
	if err != nil {
		println("conn.Do SREM:  ", err.Error())
		return 0, err
	}

	return v, nil
}

func (self *redisOp) SIsmembers(key, member string) (bool, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("SISMEMBER", key, member)
	v, err := redis.Bool(r, err)
	if err != nil {
		println("conn.Do SetSismembers:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) ZAdd(key string, score int, member string) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("ZADD", key, score, member)
	if err != nil {
		println("conn.Do ZADD:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) ZMAdd(key string, members []interface{}) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("ZADD", redis.Args{}.Add(key).AddFlat(members)...)
	//_, err := conn.Do("ZADD", key, score, member)
	if err != nil {
		println("conn.Do ZADD More:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) ZCount(key string, min, max int) (int, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("ZCOUNT", key, min, max)
	v, err := redis.Int(r, err)
	if err != nil {
		println("conn.Do ZCOUNT:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) ZRange(key string, start, stop int) ([]string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("ZRANGE", key, start, stop)
	v, err := redis.Strings(r, err)
	if err != nil {
		println("conn.Do ZRANGE:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) ZRangeWithScores(key string, start, stop int) (map[string]string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("ZRANGE", key, start, stop, "WITHSCORES")
	v, err := redis.StringMap(r, err)
	if err != nil {
		println("conn.Do ZRANGE Withscores:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) ZPop(key string, num int) (result map[string]int, err error) {
	conn := self.NewRedisConnect()
	defer conn.Close()

	defer func() {
		// Return connection to normal state on error.
		if err != nil {
			_, err = conn.Do("DISCARD")
			if err != nil {
				println("conn.Do ZPop DISCARD:  ", err.Error())

			}
		}
	}()

	// Loop until transaction is successful.
	for {
		if _, err := conn.Do("WATCH", key); err != nil {
			println("conn.Do ZPop WATCH:  ", err.Error())

			return nil, err
		}

		members, err := redis.IntMap(conn.Do("ZRANGE", key, 0, num, "WITHSCORES"))
		if err != nil {
			println("conn.Do ZPop StringMap:  ", err.Error())

			return nil, err
		}
		//if len(members) != 1 {
		//	println("conn.Do ZPop StringMap:  ", err.Error())
		//	return nil, redis.ErrNil
		//}

		err = conn.Send("MULTI")
		if err != nil {
			println("conn.Do ZPop Send MULTI:  ", err.Error())

		}
		//dels := []string{}
		//for k,_ := range members {
		//	dels = append(dels,k)
		//	println(k)
		//
		//}

		all := []interface{}{}
		all = append(all, key)
		//all = append(all,dels...)
		for k, _ := range members {
			all = append(all, k)
			println(k)

		}

		err = conn.Send("ZREM", all...)
		if err != nil {
			println("conn.Do ZPop Send ZREM:  ", err.Error())

		}

		queued, err := conn.Do("EXEC")
		if err != nil {
			println("conn.Do ZPop EXEC:  ", err.Error())
			return nil, err
		}

		if queued != nil {
			println("conn.Do ZPop queued:  ")

			result = members
			break
		}
	}

	return result, nil
}

func (self *redisOp) ZRangeByScores(key string, start, stop int) ([]string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("ZRANGEBYSCORE", key, start, stop, "WITHSCORES")
	v, err := redis.Strings(r, err)
	if err != nil {
		println("conn.Do ZRANGEBYSCORE:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) ZRangeByScoresWithLimit(key string, min, max string, offset, count int) ([]string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()

	r, err := conn.Do("ZRANGEBYSCORE", key, min, max, "LIMIT", offset, count)
	v, err := redis.Strings(r, err)
	if err != nil {
		println("conn.Do ZRANGEBYSCORE:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) ZIncrby(key string, increment int, member string) (int64, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("ZINCRBY", key, increment, member)
	v, err := redis.Int64(r, err)
	if err != nil {
		println("conn.Do ZINCRBY:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) ZSocre(key, member string) (int64, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("ZSCORE", key, member)
	v, err := redis.Int64(r, err)
	if err != nil {
		println("conn.Do ZSCORE:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) ZRem(key string, members []string) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("ZREM", redis.Args{}.Add(key).AddFlat(members)...)
	if err != nil {
		println("conn.Do ZREM:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) ZRem2(key string, members []string) (int64, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("ZREM2", redis.Args{}.Add(key).AddFlat(members)...)
	v, err := redis.Int64(r, err)
	if err != nil {
		println("conn.Do ZREM2:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) ZRevRangeWithScores(key string, begin, end int) (map[string]string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("ZREVRANGE", key, begin, end, "WITHSCORES")
	v, err := redis.StringMap(r, err)
	if err != nil {
		println("conn.Do ZREVRANGE Withscores:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) ZRevRangeWithScores2(key string, begin, end int) ([]string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("ZREVRANGE", key, begin, end, "WITHSCORES")
	v, err := redis.Strings(r, err)
	if err != nil {
		println("conn.Do ZREVRANGE Withscores:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) ZCard(key string) (int64, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("ZCARD", key)
	v, err := redis.Int64(r, err)
	if err != nil {
		println("conn.Do ZCARD:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) ZRevRange(key string, begin, end int) ([]string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("ZREVRANGE", key, begin, end)
	v, err := redis.Strings(r, err)
	if err != nil {
		println("conn.Do ZREVRANGE:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) ZRevRank(key string, member string) (int64, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("ZREVRANK", key, member)
	v, err := redis.Int64(r, err)
	if err != nil {
		println("conn.Do ZREVRANK:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) ZREMRANGEBYSCORE(key string, begin, end int) (int64, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("ZREMRANGEBYSCORE", key, begin, end)
	v, err := redis.Int64(r, err)
	if err != nil {
		println("conn.Do ZREMRANGEBYSCORE:  ", err.Error())
		return v, err
	}

	return v, nil
}

// list lpush
func (self *redisOp) LPush(key, value string) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("lpush", key, value)
	if err != nil {
		println("conn.Do lpush:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) LPushMore(key string, values []string) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("lpush", redis.Args{}.Add(key).AddFlat(values)...)
	if err != nil {
		println("conn.Do lpush:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) RPushMore(key string, values []interface{}) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("rpush", redis.Args{}.Add(key).AddFlat(values)...)
	if err != nil {
		println("conn.Do rpushmore:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) RPush(key, value string) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("rpush", key, value)
	if err != nil {
		println("conn.Do rpush:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) RPushMoreByByte(key string, values []BasePB) error {
	conn := self.NewRedisConnect()
	defer conn.Close()

	bys := make([][]byte, len(values))

	for i, pb := range values {
		v, err := pb.Marshal()
		if err != nil {
			return err
		}
		bys[i] = v
	}

	_, err := conn.Do("rpush", redis.Args{}.Add(key).AddFlat(bys)...)
	if err != nil {
		println("conn.Do rpush:  ", err.Error())
		return err
	}

	return nil

}

//  list range
func (self *redisOp) LRange(key string, begin, end int) ([]string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("LRANGE", key, begin, end)
	v, err := redis.Strings(r, err)
	if err != nil {
		println("conn.Do LRANGE:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) LPop(key string) (string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("LPOP", key)
	v, err := redis.String(r, err)
	if err != nil {
		println("conn.Do LPOP:  ", err.Error())
		return v, err
	}

	return v, nil
}

// list rpop
func (self *redisOp) RPop(key string) (string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("RPOP", key)
	v, err := redis.String(r, err)
	if err != nil {
		println("conn.Do RPOP:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) LLEN(key string) (int, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("LLEN", key)
	len, err := redis.Int(r, err)
	if err != nil {
		println("conn.Do LLEN:  ", err.Error())
		return 0, err
	}

	return len, nil
}

func (self *redisOp) LIndex(key string, index int) (interface{}, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("LINDEX", key, index)
	v, err := redis.Values(r, err)
	if err != nil {
		println("conn.Do LINDEX:  ", err.Error())
		return 0, err
	}

	return v[0], nil
}

func (self *redisOp) LInsert(key, pivot string, value ...interface{}) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("LINSERT", key, "BEFORE", pivot, value)
	if err != nil {
		println("conn.Do LINSERT:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) LTrim(key string, begin, end int) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("LTRIM", key, begin, end)
	if err != nil {
		println("conn.Do LTRIM:  ", err.Error())
		return err
	}

	return nil
}

// list rem
func (self *redisOp) LRem(key interface{}, count int, value interface{}) (int64, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("LREM", key, count, value)
	v, err := redis.Int64(r, err)
	if err != nil {
		println("conn.Do LREM:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) HMSet(hashkey string, args interface{}) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("HMSET", redis.Args{}.Add(hashkey).AddFlat(args)...)
	if err != nil {
		println("conn.Do HMSET:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) HMListset(hashkey string, fields, values []string) error {
	if len(fields) == 0 || len(values) == 0 || len(values) != len(fields) {
		return fmt.Errorf("size of keys not equle size of values")
	}

	args := make(map[string]string)
	for i := 0; i < len(fields); i++ {
		args[fields[i]] = values[i]
	}

	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("HMSET", redis.Args{}.Add(hashkey).AddFlat(args)...)
	if err != nil {
		println("conn.Do HashMListset:  ", err.Error())
		return err
	}

	return nil
}
func (self *redisOp) HSet(hashkey, field, value string) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("HSET", hashkey, field, value)
	if err != nil {
		println("conn.Do HSET:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) HGet(hashkey, field string) (string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("HGET", hashkey, field)
	v, err := redis.String(r, err)
	if err != nil {
		println("conn.Do HGET:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) HMget(hashkey string, fields []string) (map[string]string, error) {
	if len(fields) == 0 {
		return map[string]string{}, fmt.Errorf("size of fields is zero")
	}
	conn := self.NewRedisConnect()
	defer conn.Close()

	values := make(map[string]string)

	r, err := conn.Do("HMGET", redis.Args{}.Add(hashkey).AddFlat(fields)...)
	v, err := redis.Strings(r, err)
	if err != nil {
		println("conn.Do HashMget:  ", err.Error())
		return values, err
	}

	for i := 0; i < len(fields); i++ {
		values[fields[i]] = v[i]
	}

	return values, nil
}

func (self *redisOp) HMgetSlice(hashkey string, fields []string) ([]string, error) {
	if len(fields) == 0 {
		return nil, fmt.Errorf("size of fields is zero")
	}
	conn := self.NewRedisConnect()
	defer conn.Close()

	r, err := conn.Do("HMGET", redis.Args{}.Add(hashkey).AddFlat(fields)...)
	v, err := redis.Strings(r, err)
	if err != nil {
		println("conn.Do HMGET:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) HDel(hashkey, filed string) error {
	conn := self.NewRedisConnect()
	defer conn.Close()
	_, err := conn.Do("HDEL", hashkey, filed)
	if err != nil {
		println("conn.Do HDEL:  ", err.Error())
		return err
	}

	return nil
}

func (self *redisOp) HLen(hashkey string) (int, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("HLEN", hashkey)
	v, err := redis.Int(r, err)
	if err != nil {
		println("conn.Do HLEN:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) HKeys(hashkey string) ([]string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("HKEYS", hashkey)
	v, err := redis.Strings(r, err)
	if err != nil {
		println("conn.Do HKEYS:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) HIncrby(hashkey, filed string, increment int32) (int64, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("HINCRBY", hashkey, filed, increment)
	v, err := redis.Int64(r, err)
	if err != nil {
		println("conn.Do HINCRBY:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) HGetAllIntInt(hashkey string) (map[int64]int64, error) {
	conn := self.NewRedisConnect()
	r, err := conn.Do("HGETALL", hashkey)
	value, err := redis.StringMap(r, err)
	if err != nil {
		println("conn.Do HGETALL:  ", err.Error())
		conn.Close()
		return map[int64]int64{}, err
	}
	ret := make(map[int64]int64)
	for k, v := range value {
		lk, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			conn.Close()
			return map[int64]int64{}, err
		}
		lv, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			conn.Close()
			return map[int64]int64{}, err
		}
		ret[lk] = lv
	}
	conn.Close()
	return ret, nil
}

func (self *redisOp) HGetAllIntString(hashkey string) (map[int64]string, error) {
	conn := self.NewRedisConnect()
	r, err := conn.Do("HGETALL", hashkey)
	value, err := redis.StringMap(r, err)
	if err != nil {
		println("conn.Do HGETALL:  ", err.Error())
		conn.Close()
		return map[int64]string{}, err
	}
	ret := make(map[int64]string)
	for k, v := range value {
		lk, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			conn.Close()
			return map[int64]string{}, err
		}
		ret[lk] = v
	}
	conn.Close()
	return ret, nil
}

func (self *redisOp) HGetAllStringString(hashkey string) (map[string]string, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("HGETALL", hashkey)
	value, err := redis.StringMap(r, err)
	if err != nil {
		println("conn.Do HGETALL:  ", err.Error())
		return map[string]string{}, err
	}
	return value, nil
}

func (self *redisOp) HExistsInt(hashkey string, filed int64) (bool, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("HEXISTS", hashkey, filed)
	v, err := redis.Bool(r, err)
	if err != nil {
		println("conn.Do HEXISTS:  ", err.Error())
		return v, err
	}
	return v, nil
}

func (self *redisOp) HExistsString(hashkey string, filed string) (bool, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("HEXISTS", hashkey, filed)
	v, err := redis.Bool(r, err)
	if err != nil {
		println("conn.Do HEXISTS:  ", err.Error())
		return v, err
	}

	return v, nil
}

func (self *redisOp) RedisDocmdArgs(cmd string, args ...interface{}) (repley interface{}, err error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	repley, err = conn.Do(cmd, args...)
	return
}

func (self *redisOp) RedisPing() (bool, error) {
	conn := self.NewRedisConnect()
	defer conn.Close()
	r, err := conn.Do("PING")
	v, err := redis.String(r, err)
	if err != nil {
		println("conn.Do PING:  ", err.Error())
		return false, err
	}

	return v == "PONG", nil
}

var Nil = redis.ErrNil

func IsRedisNil(err error) bool {
	return redis.ErrNil == err
}

func (self *redisOp) LockCallback(lockKey string, callback func(lockKey string), maxLock ...time.Duration) error {
	conn := self.NewRedisConnect()

	var d = 1 * time.Second
	if len(maxLock) > 0 {
		d = maxLock[0]
	}
	// lock
	var tempDelay time.Duration
	for lockOk, err := redis.String(conn.Do("SET", lockKey, "", "EX", int(d), "NX")); lockOk != "OK"; lockOk, err = redis.String(conn.Do("SET", lockKey, "", "EX", int(d), "NX")) {
		if err != nil && !IsRedisNil(err) {
			println("LockCallback err", err.Error())
			conn.Close()
			return err
		}
		if tempDelay == 0 {
			tempDelay = 10 * time.Millisecond
		} else {
			tempDelay *= 2

		}

		if max := 1 * time.Second; tempDelay > max {
			conn.Close()
			return err
		}

		time.Sleep(tempDelay)
	}
	conn.Close()

	// do
	callback(lockKey)

	// unlock
	//conn = self.NewRedisConnect()
	//conn.Do("del", lockKey)
	return nil
}
