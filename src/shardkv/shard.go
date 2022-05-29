package shardkv

import (
	"time"

	"6.824/shardctrler"
)

const (
	PutShard  = "PutShard"
	PushShard = "PushShard"
	GCShard   = "GCShard"
)

const (
	invalid = iota
	valid
	migrating
	waitMigrate
)

type Shardstate int

func (s Shardstate) String() string {
	switch s {
	case invalid:
		return "invalid"
	case valid:
		return "valid"
	case migrating:
		return "migrating"
	case waitMigrate:
		return "waitMigrate"
	}
	return "unknown"
}

type ShardComponent struct {
	ShardIndex  int
	KVDBofShard map[string]string
	ClientSeq   map[int64]int64
	State       Shardstate
}

type ShardOp struct {
	Optype    string
	Shard     map[int]ShardComponent
	ShardList []int
	Group     int
	Confignum int
}

func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {
	kv.mu.Lock()
	_, isleader := kv.rf.GetState()
	reply.ConfigNum = kv.config.Num
	if !isleader {
		DPrintf("[%d,%d,%d]: PutShard Wrong Leader: %d", kv.gid, kv.me, kv.config.Num)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	sop := ShardOp{
		Optype:    PutShard,
		Shard:     args.Shard,
		Confignum: args.ConfigNum,
	}
	kv.mu.Unlock()
	logindex, _, _ := kv.rf.Start(sop)

	kv.mu.Lock()
	DPrintf("[%d,%d,%d]: PutShard Start: raftindex:%d, %v", kv.gid, kv.me, kv.config.Num, logindex, sop.Shard)
	chForIndex, exist := kv.WaitMap[logindex]
	if !exist {
		DPrintf("[%d,%d,%d]: PutShard WaitMap: %d", kv.gid, kv.me, kv.config.Num, logindex)
		kv.WaitMap[logindex] = make(chan WaitMsg, 1)
		chForIndex = kv.WaitMap[logindex]
	}
	defer func() {
		kv.mu.Lock()
		delete(kv.WaitMap, logindex)
		kv.mu.Unlock()
	}()
	kv.mu.Unlock()

	select {
	case msg := <-chForIndex:
		reply.Err = msg.Err
		return
	case <-time.After(time.Duration(3000) * time.Millisecond):
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) pushShard(shards []int, group int, configNum int) {
	kv.mu.Lock()
	if _, isleader := kv.rf.GetState(); !isleader {
		kv.mu.Unlock()
		return
	}

	if configNum != kv.config.Num {
		DPrintf("[%d,%d,%d]: pushShard out of date config: %v", kv.gid, kv.me, kv.config.Num, shards)
		kv.mu.Unlock()
		return
	}
	if group == kv.gid {
		DPrintf("[%d,%d,%d]: pushShard same group: %v", kv.gid, kv.me, kv.config.Num, shards)
		kv.mu.Unlock()
		return
	}
	sendshards := make(map[int]ShardComponent)

	for _, shard := range shards {
		if kv.kvDB[shard].State != migrating {
			DPrintf("[%d,%d,%d]: pushShard no migrating: %d", kv.gid, kv.me, kv.config.Num, shard)
			kv.mu.Unlock()
			return
		}
		sendshards[shard] = kv.kvDB[shard]
	}

	DPrintf("[%d,%d,%d]: pushShard: %v->%d", kv.gid, kv.me, kv.config.Num, shards, group)
	args := PushShardArgs{
		Shard:     sendshards,
		ConfigNum: kv.config.Num,
	}
	if servers, ok := kv.config.Groups[group]; ok {
		for i := 0; i < len(servers); i++ {
			srv := kv.make_end(servers[i])
			reply := PushShardReply{}
			kv.mu.Unlock()
			ok := srv.Call("ShardKV.PushShard", &args, &reply)
			kv.mu.Lock()
			if ok && reply.Err == OK {
				DPrintf("[%d,%d,%d]: pushShard done: %d->%v", kv.gid, kv.me, kv.config.Num, group, shards)
				sop := ShardOp{
					Optype:    GCShard,
					ShardList: shards,
					Confignum: kv.config.Num,
				}
				kv.mu.Unlock()
				kv.rf.Start(sop)
				return
			}
			if ok && reply.Err == ErrConfigOutOfDate {
				DPrintf("[%d,%d,%d]: pushShard config out of date: %d->%v", kv.gid, kv.me, kv.config.Num, group, shards)
				kv.mu.Unlock()
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				DPrintf("[%d,%d,%d]: pushShard wrong group: %d->%v", kv.gid, kv.me, kv.config.Num, group, shards)
				kv.mu.Unlock()
				return
			}
			// wrong leader or timeout
			DPrintf("[%d,%d,%d]: pushShard wrong leader: %d->%v", kv.gid, kv.me, kv.config.Num, group, shards)
		}
	}
	kv.mu.Unlock()
	DPrintf("[%d,%d,%d]: pushShard failed: %d->%v", kv.gid, kv.me, kv.config.Num, group, shards)
}

func (kv *ShardKV) ApplyShardOp(op ShardOp, raftindex int) {
	var err Err
	defer func() {
		kv.mu.Lock()
		ch, channelexist := kv.WaitMap[raftindex]
		kv.mu.Unlock()
		if channelexist {
			ch <- WaitMsg{Err: err}
		}
	}()

	switch op.Optype {
	case PutShard:
		kv.mu.Lock()
		DPrintf("[%d,%d,%d]: ApplyShardOp: raftIndex:%d,%v", kv.gid, kv.me, kv.config.Num, raftindex, op.Shard)
		for index, value := range op.Shard {
			if kv.kvDB[index].State != waitMigrate {
				if kv.config.Num > op.Confignum {
					DPrintf("[%d,%d,%d]: ApplyShardOp Out of Date Config: %d", kv.gid, kv.me, kv.config.Num, op.Confignum)
					err = OK
					kv.mu.Unlock()
					return
				}
				DPrintf("[%d,%d,%d]: ApplyShardOp Shard not wait migrate: %d,%s", kv.gid, kv.me, kv.config.Num, index, kv.kvDB[index].State)
				err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			kv.kvDB[index] = value
			if kv.config.Shards[index] != kv.gid {
				kv.kvDB[index].State = migrating
			} else {
				kv.kvDB[index].State = valid
			}
		}
		DPrintf("[%d,%d,%d]: ApplyShardOp done: raftIndex:%d,Shard:%v", kv.gid, kv.me, kv.config.Num, raftindex, op.Shard)
		err = OK
		kv.mu.Unlock()
	case GCShard:
		kv.gcShard(op)
	case PushShard:
		kv.pushShard(op.ShardList, op.Group, op.Confignum)
	}
}

func (kv *ShardKV) gcShard(op ShardOp) {
	kv.mu.Lock()
	for _, shard := range op.ShardList {
		if kv.kvDB[shard].State != migrating {
			DPrintf("[%d,%d,%d]: gcShard no wait migrate: %d", kv.gid, kv.me, kv.config.Num, shard)
			continue
		}
		kv.kvDB[shard] = ShardComponent{
			ShardIndex:  shard,
			KVDBofShard: nil,
			ClientSeq:   nil,
			State:       invalid,
		}
	}
	DPrintf("[%d,%d,%d]: shardGCOp done: %v", kv.gid, kv.me, kv.config.Num, op.ShardList)
	kv.mu.Unlock()
}

func (kv *ShardKV) checkShardMigrate(oldcfg shardctrler.Config) {
	for shard := range kv.config.Shards {
		if kv.config.Shards[shard] == oldcfg.Shards[shard] {
			continue
		}
		if kv.config.Shards[shard] == kv.gid {
			if oldcfg.Shards[shard] == 0 && kv.config.Num == 1 {
				kv.kvDB[shard].State = valid
				continue
			}
			switch kv.kvDB[shard].State {
			case invalid:
				DPrintf("[%d,%d,%d]: checkShardwaitMigrate: %d,%s", kv.gid, kv.me, kv.config.Num, shard, kv.kvDB[shard].State)
				kv.kvDB[shard].State = waitMigrate
			}
			continue
		}
		if oldcfg.Shards[shard] == kv.gid {
			switch kv.kvDB[shard].State {
			case valid:
				DPrintf("[%d,%d,%d]: checkShardneedMigrate: %d,%s", kv.gid, kv.me, kv.config.Num, shard, kv.kvDB[shard].State)
				kv.kvDB[shard].State = migrating
			}
		}
	}
}

func (kv *ShardKV) checkShardNeedPush() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sendmap := make(map[int][]int)
	for index, compoment := range kv.kvDB {
		if compoment.State != migrating {
			continue
		}
		DPrintf("[%d,%d,%d]: checkShardNeedPush: %d->%d", kv.gid, kv.me, kv.config.Num, index, kv.config.Shards[index])
		if _, ok := sendmap[kv.config.Shards[index]]; !ok {
			sendmap[kv.config.Shards[index]] = make([]int, 0)
		}
		sendmap[kv.config.Shards[index]] = append(sendmap[kv.config.Shards[index]], index)
	}
	for group, shardlist := range sendmap {
		go kv.pushShard(shardlist, group, kv.config.Num)
	}
}
