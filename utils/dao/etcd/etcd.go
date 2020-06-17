package byetcd

import (
	"context"
	"errors"
	"reflect"
	"time"

	"log"

	"sync"

	"go.etcd.io/etcd/clientv3"
	"gopkg.in/mgo.v2/bson"
)

func DateToBson(obj interface{}) []byte {
	data, err := bson.Marshal(obj)
	if err != nil {
		log.Fatalf("to bson error %v", err)
	}

	return data
}

func BsonToDate(data []byte, obj interface{}) {
	err := bson.Unmarshal(data, obj)
	if err != nil {
		log.Fatalf("to bson error %v", err)
	}
}

type lesseData struct {
	leaseID  clientv3.LeaseID
	revision int64
}

type Etcd struct {
	*clientv3.Client
	LeaseIDs  map[string]*lesseData
	lock      sync.RWMutex
	lockWatch sync.Mutex
}

// 根据key 获取结构体，必须支持bson obj是一个*[]type对象
func (p *Etcd) GetStructData(key string, isPrefix bool, obj interface{}) error {
	var resp *clientv3.GetResponse
	var err error
	if isPrefix {
		resp, err = p.Get(context.Background(), key, clientv3.WithPrefix())
	} else {
		resp, err = p.Get(context.Background(), key)
	}

	if err != nil {
		log.Fatalf("GetStructData data fail", err)
		return err
	}

	rv := reflect.ValueOf(obj)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		log.Fatalf("GetStructData data fail type error %v", reflect.TypeOf(obj))
		return err
	}

	if len(resp.Kvs) == 0 {
		return nil
	}

	// log.Println("GetStructData Type", reflect.TypeOf(obj), rv.Elem().Type())
	objs := reflect.MakeSlice(rv.Elem().Type(), len(resp.Kvs), len(resp.Kvs))
	// log.Println("GetStructData ", key, len(resp.Kvs), "Kind=", objs, reflect.ValueOf(objs).Kind())
	for k, ev := range resp.Kvs {
		// 把0号位拷贝走，然后0号位 解析
		data := objs.Index(k)
		data = reflect.New(data.Type())
		BsonToDate([]byte(ev.Value), data.Interface())
		objs.Index(k).Set(data.Elem())
		// log.Println("GetStructData === ", k, string(ev.Key), data.Type(), objs.Index(k), " value ===", data)
	}

	// log.Println("GetStructData Type", objs.Type(), rv.Type())
	rv.Elem().Set(objs)
	return err
}

// put一个支持bson的struct对象
func (p *Etcd) PutData(key string, obj interface{}) bool {
	value := string(DateToBson(obj))
	_, err := p.Put(context.Background(), key, value)
	if err != nil {
		log.Errorf("PutData data fail", err)
		return false
	}

	// log.Println("PutData", key, resp)

	return true
}

func (p *Etcd) PutString(key, value string) bool {
	_, err := p.Put(context.Background(), key, value)

	if err != nil {
		log.Fatalf("PutData data fail", err)
		return false
	}

	// log.Println("PutID", key, resp)

	return true
}

func (p *Etcd) GetString(key string) string {
	resp, err := p.Get(context.Background(), key)
	if err != nil {
		log.Fatalf("PutData data fail %v", err)
		return ""
	}

	for _, ev := range resp.Kvs {
		ret := string(ev.Value)
		// log.Printf("GetData === %s : %s\n", ev.Key, ret)
		return ret
	}

	return ""
}

func (p *Etcd) GetStrings(key string) [][]string {
	resp, err := p.Get(context.Background(), key, clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("PutData data fail %v", err)
		return nil
	}

	ret := make([][]string, len(resp.Kvs))
	var i int = 0
	for _, ev := range resp.Kvs {
		ret[i] = []string{string(ev.Key), string(ev.Value)}
		// log.Printf("GetData === %s : %s\n", ev.Key, ret[i])
		i++
	}
	return ret
}

func (p *Etcd) DelKey(key string) bool {
	_, err := p.Delete(context.Background(), key)
	if err != nil {
		log.Fatalf("DelKey data fail, %v", err)
		return false
	}

	delete(p.LeaseIDs, key)
	// log.Println("DelKey", delp)
	return true
}

type Etcd_EventType int32

const (
	PUT    Etcd_EventType = 0
	DELETE Etcd_EventType = 1
)

var Event_EventType_name = map[int32]string{
	0: "PUT",
	1: "DELETE",
}

func (x Etcd_EventType) String() string {
	return Event_EventType_name[int32(x)]
}

type WatchFun func(Type Etcd_EventType, key string, value []byte)
type WatchDataFun func(Type Etcd_EventType, key string, obj interface{})

func (p *Etcd) WatchKey(key string, fun WatchFun) {
	rch := p.Watch(context.Background(), key, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			// log.Printf("WatchData %s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			fun(Etcd_EventType(ev.Type), string(ev.Kv.Key), ev.Kv.Value)
		}
	}
}

// 该函数返回的对象会反复利用，不要直接保存，最好拷贝一下
func (p *Etcd) WatchKeyByData(key string, obj interface{}, fun WatchDataFun) {
	rch := p.Watch(context.Background(), key, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			// log.Printf("WatchData %s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			Type := Etcd_EventType(ev.Type)
			p.lockWatch.Lock() // 一次只允许一个进来操作
			if PUT == Type {
				err := bson.Unmarshal(ev.Kv.Value, obj)
				if err != nil {
					log.Fatalf("WatchData error %v\n", err)
				}
			}
			fun(Type, string(ev.Kv.Key), obj)
			p.lockWatch.Unlock()
		}
	}
}

func (p *Etcd) onKeepAliveUpdata(data *lesseData, key string, ch <-chan *clientv3.LeaseKeepAliveResponse) {
	for {
		ka, ok := <-ch
		if !ok {
			log.Println("keep alive channel closed")
			p.lock.Lock()
			delete(p.LeaseIDs, key)
			p.lock.Unlock()
			return
		} else {
			// log.Println("recv server reply", ka.TTL, data.leaseID, ka.Revision, ka.GetRaftTerm())
			data.revision = ka.Revision
		}

	}
}

//插入一个短期key,并且一直续约
func (p *Etcd) PushKeepData(key string, obj interface{}, ktime int) error {
	value := string(DateToBson(obj))
	data := p.getLesseID(key, true, ktime)
	return p.upKeepKey(data, key, value)
}

//插入一个短期key,并且一直续约
func (p *Etcd) PushKeepKey(key string, value string, ktime int) error {
	data := p.getLesseID(key, true, ktime)
	return p.upKeepKey(data, key, value)
}

//插入一个短期key,并且一直续约
func (p *Etcd) UpKeepData(key string, obj interface{}, ktime int) error {
	value := string(DateToBson(obj))
	data := p.getLesseID(key, false, ktime)
	if data == nil {
		return errors.New("key is not self, cant to updata")
	}

	return p.upKeepKey(data, key, value)
}

// 获取keepid
func (p *Etcd) getLesseID(key string, canNew bool, ktime int) (data *lesseData) {
	p.lock.Lock()
	if p.LeaseIDs[key] == nil {
		if canNew {
			//分配一个租约 30秒
			lease, err := p.Grant(context.TODO(), int64(ktime))
			if err != nil {
				log.Fatalf("分配续租id错误, %v", err)
				p.lock.Unlock()
				return nil
			}

			data = &lesseData{
				leaseID:  lease.ID,
				revision: 0,
			}
			p.LeaseIDs[key] = data
		}
	} else {
		data = p.LeaseIDs[key]
	}

	p.lock.Unlock()

	return data
}

//插入一个短期key,并且一直续约
func (p *Etcd) upKeepKey(data *lesseData, key string, value string) error {
	if data == nil {
		return errors.New("fail to add key, not lesse ID")
	}

	//事物操作: 先对比create版本是否=0
	put := clientv3.OpPut(key, value, clientv3.WithLease(data.leaseID))
	var resp *clientv3.TxnResponse
	var err error
	if data.revision == 0 {
		cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", data.revision)
		resp, err = p.Txn(context.TODO()).If(cmp).Then(put).Commit()
		//get := clientv3.OpGet(key) /*.Else(get)*/
	} else {
		resp, err = p.Txn(context.TODO()).Then(put).Commit()
	}

	if err != nil {
		log.Println("txt exec error:", err)
		return err
	}

	if resp.Succeeded {
		log.Println("put key success !", key)
	} else {
		for _, v := range resp.Responses {
			for _, ev := range v.GetResponseRange().Kvs {
				log.Println("put key error , previous value:", string(ev.Value))
			}
		}
		return errors.New("fail to add key")
	}

	ch, err := p.KeepAlive(context.TODO(), data.leaseID)
	if err != nil {
		log.Println("ka error:", err)
		return err
	}

	go p.onKeepAliveUpdata(data, key, ch)

	return nil
}

func NewEtcd(endpoints []string, username, password string) *Etcd {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second * 3,
		Username:    username,
		Password:    password,
	}

	etcdClient, err := clientv3.New(cfg)
	if err != nil {
		log.Fatalf("Error: cannot connec to etcd: %v", err)
	}

	e := &Etcd{Client: etcdClient, LeaseIDs: make(map[string]*lesseData, 10)}

	return e
}
