package kvraft

import (
	"crypto/rand"
	"lab1/src/labrpc"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId    int64 // client的唯一标识符
	knownLeader int   // 已知的leader，请求到非leader的server后的reply中获取
	commandNum  int   // 标志client为每个command分配的序列号到哪了（采用自增，从1开始）
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.knownLeader = -1
	ck.commandNum = 1

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		CmdNum:   ck.commandNum,
	}

	i := 0 // 从server 0开始发送请求
	serversNum := len(ck.servers)
	for ; ; i = (i + 1) % serversNum { // 循环依次向每一个server请求直至找到leader

		if ck.knownLeader != -1 { // 如果找到了leader
			i = ck.knownLeader // 下次直接向leader发起请求
		} else {
			time.Sleep(time.Millisecond * 5) // 为了不短时间内再次请求非leader的server
		}

		reply := GetReply{}

		DPrintf("Client[%d] request [%d] for Get(key:%v)......\n", ck.clientId, i, args.Key)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

		if !ok {
			DPrintf("Client[%d] Get request failed!\n", ck.clientId)
			ck.knownLeader = -1 // 没收到回复则下次向其他kvserver发送
			continue
		} else { // 成功收到rpc回复
			switch reply.Err {
			case OK:
				ck.commandNum++
				ck.knownLeader = i
				DPrintf("Client[%d] request(Seq:%d) for Get(key:%v) [successfully]!\n", ck.clientId, args.CmdNum, args.Key)
				return reply.Value
			case ErrWrongLeader:
				ck.knownLeader = -1
				DPrintf("Client[%d] request(Seq:%d) for Get(key:%v) but [WrongLeader]!\n", ck.clientId, args.CmdNum, args.Key)
				continue
			case ErrNoKey:
				ck.commandNum++
				ck.knownLeader = i
				DPrintf("Client[%d] request(Seq:%d) for Get(key:%v) but [NoKey]!\n", ck.clientId, args.CmdNum, args.Key)
				return ""
			case ErrTimeout:
				ck.knownLeader = -1
				DPrintf("Client[%d] request(Seq:%d) for Get(key:%v) but [Timeout]!\n", ck.clientId, args.CmdNum, args.Key)
				continue
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// Put和Append操作都会调它，相当于集成（因为KVServer的这两个操作是用一个函数 实现）
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		CmdNum:   ck.commandNum,
	}

	i := 0
	serversNum := len(ck.servers)
	for ; ; i = (i + 1) % serversNum {

		if ck.knownLeader != -1 {
			i = ck.knownLeader
		} else {
			time.Sleep(time.Millisecond * 5) // 为了不短时间内再次请求非leader的server
		}

		reply := PutAppendReply{}

		DPrintf("Client[%d] request [%d] for PutAppend(key:%v, value:%v)......\n", ck.clientId, i, args.Key, args.Value)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)

		if !ok {
			DPrintf("Client[%d] PutAppend request failed!\n", ck.clientId)
			ck.knownLeader = -1 // 没收到回复则下次向其他kvserver发送
			continue
		} else {
			switch reply.Err {
			case OK:
				ck.commandNum++
				ck.knownLeader = i
				DPrintf("Client[%d] request(Seq:%d) for PutAppend [successfully]!\n", ck.clientId, args.CmdNum)
				return
			case ErrWrongLeader:
				ck.knownLeader = -1
				DPrintf("Client[%d] request(Seq:%d) for PutAppend but [WrongLeader]!\n", ck.clientId, args.CmdNum)
				continue
			case ErrNoKey:
				ck.commandNum++
				ck.knownLeader = i
				DPrintf("Client[%d] request(Seq:%d) for Append but [NoKey]!\n", ck.clientId, args.CmdNum)
				return
			case ErrTimeout:
				ck.knownLeader = -1
				DPrintf("Client[%d] request(Seq:%d) for PutAppend but [Timeout]!\n", ck.clientId, args.CmdNum)
				continue
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
