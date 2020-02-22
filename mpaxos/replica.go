package mpaxos

import (
	"encoding/json"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/lib"
	"github.com/ailidani/paxi/log"
	"math/rand"
	"strings"
	"time"
)

type Config []paxi.ID
type ConfigState uint8
type ConfigVer int32

//const TOLERATE_F = 1

const (
	CONFIG_NORMAL ConfigState = iota // can accept new config
	CONFIG_NEW_UNCOMMITTED
	CONFIG_NEW_COMMITTED // in transfer process
)

type ReconfigCmd struct {
	Newconfig Config
	Ver       ConfigVer
}

type Replica struct {
	paxi.Node
	log          map[paxi.ID]map[int]*instance
	slot         map[paxi.ID]int // current instance number, start with 1
	committed    map[paxi.ID]int
	executed     map[paxi.ID]int
	conflicts    map[paxi.ID]map[paxi.Key]int // <=-2的值表示reconfig命令，其他的是普通命令
	maxSeqPerKey map[paxi.Key]int

	// added for migration
	configList         map[paxi.Key][]Config // current config for new cmd
	curConfigVer       map[paxi.Key]ConfigVer
	committedConfigVer map[paxi.Key]ConfigVer
	newConfigVer       map[paxi.Key]ConfigVer
	newConfigState     map[paxi.Key]ConfigState     // state of new config
	waitCmdsQueue      map[paxi.Key][]*paxi.Request // these cmds must be commit after reconfig

	graph *lib.Graph

	fast int
	slow int
}

// NewReplica initialize replica and register all message types
func NewReplica(id paxi.ID) *Replica {
	r := &Replica{
		Node:         paxi.NewNode(id),
		log:          make(map[paxi.ID]map[int]*instance),
		slot:         make(map[paxi.ID]int),
		committed:    make(map[paxi.ID]int),
		executed:     make(map[paxi.ID]int),
		conflicts:    make(map[paxi.ID]map[paxi.Key]int),
		maxSeqPerKey: make(map[paxi.Key]int),
		graph:        lib.NewGraph(),
		// for migration
		configList:         make(map[paxi.Key][]Config),
		curConfigVer:       make(map[paxi.Key]ConfigVer),
		committedConfigVer: make(map[paxi.Key]ConfigVer),
		newConfigVer:       make(map[paxi.Key]ConfigVer),
		newConfigState:     make(map[paxi.Key]ConfigState),     // state of new config
		waitCmdsQueue:      make(map[paxi.Key][]*paxi.Request), // these cmds must be commit after reconfig

	}
	for id := range paxi.GetConfig().Addrs {
		r.log[id] = make(map[int]*instance, paxi.GetConfig().BufferSize)
		r.slot[id] = -1
		r.committed[id] = -1
		r.executed[id] = -1
		r.conflicts[id] = make(map[paxi.Key]int, paxi.GetConfig().BufferSize)
	}

	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(PreAccept{}, r.handlePreAccept)
	r.Register(PreAcceptReply{}, r.handlePreAcceptReply)
	r.Register(Accept{}, r.handleAccept)
	r.Register(AcceptReply{}, r.handleAcceptReply)
	r.Register(Commit{}, r.handleCommit)
	r.Register(TransferMsg{}, r.handleTransfer)
	r.Register(TransferReply{}, r.handleTransferReply)

	rand.Seed(time.Now().UnixNano())

	return r
}

// attibutes generates the sequence and dependency attributes for command
func (r Replica) attributes(cmd paxi.Command) (seq int, dep map[paxi.ID]int) {
	seq = 0
	dep = make(map[paxi.ID]int)
	for id := range r.conflicts {
		if d, exists := r.conflicts[id][cmd.Key]; exists {
			if getRealInstance(d) > getRealInstance(dep[id]) {
				dep[id] = d
				if seq <= r.log[id][getRealInstance(d)].seq {
					seq = r.log[id][getRealInstance(d)].seq + 1
				}
			}
		}
	}
	if s, exists := r.maxSeqPerKey[cmd.Key]; exists {
		if seq <= s {
			seq = s + 1
		}
	}
	return seq, dep
}

// 将config在deps中存储为<=-2的值
func getDepOfConfig(instance int, operation paxi.Operation) int {
	if operation == paxi.RECONFIG {
		if(instance < 0) {
			log.Errorf("getDepOfConfig meet invalid instance %v", instance)
			return instance
		}
		return -instance - 2
	} else {
		return instance
	}
}

func isConfigDep(instance int) bool {
	return instance <= -2
}

func getRealInstance(instance int) int {
	if instance <= -2 {
		return -(instance + 2)
	} else {
		return instance
	}
}

// updates local record for conflicts
func (r *Replica) update(cmd paxi.Command, id paxi.ID, slot, seq int) {
	k := cmd.Key
	d, exists := r.conflicts[id][k]
	if exists {
		if getRealInstance(d) < slot {
			r.conflicts[id][k] = getDepOfConfig(slot, cmd.Op)
		}
	} else {
		r.conflicts[id][k] = getDepOfConfig(slot, cmd.Op)
	}
	s, exists := r.maxSeqPerKey[k]
	if exists {
		if s < seq {
			r.maxSeqPerKey[k] = seq
		}
	} else {
		r.maxSeqPerKey[k] = seq
	}
}

func (r *Replica) updateCommit(id paxi.ID) {
	s := r.committed[id]
	for r.log[id][s+1] != nil && (r.log[id][s+1].status == COMMITTED || r.log[id][s+1].status == EXECUTED) {
		r.committed[id] = r.committed[id] + 1
		s = r.committed[id]
	}
	r.execute()
}

func (r *Replica) initClusterForKey(k paxi.Key) {
	peers := paxi.GetConfig().Addrs
	var firstConfig Config
	curConfigList := make([]Config, 1)
	for k := range peers {
		firstConfig = append(firstConfig, k)
	}
	curConfigList[0] = firstConfig
	r.configList[k] = curConfigList
	r.curConfigVer[k] = ConfigVer(0)

	//r.newConfigState[k] = CONFIG_NORMAL
}

func (r *Replica) generateZoneConfig() Config {
	peers := paxi.GetConfig().Addrs
	var zoneConfig Config
	fields := strings.Split(string(r.ID()), ".")
	pref := fields[0] + "."
	log.Infof("prefix of %v is %v", r.ID(), pref)
	for k := range peers {
		if len(zoneConfig) >= paxi.GetConfig().TolerateF*2+1 {
			break
		}
		if strings.HasPrefix(string(k), pref) {
			zoneConfig = append(zoneConfig, k)
		}
	}
	log.Infof("zone of %v is %v", r.ID(), zoneConfig)
	return zoneConfig
}

func (r *Replica) initConfig(m paxi.Request) paxi.Request {
	curKey := m.Command.Key
	log.Infof("before init config of %v", curKey)
	peers := paxi.GetConfig().Addrs
	//nPeers := len(peers)
	var firstConfig Config
	for k := range peers {
		firstConfig = append(firstConfig, k)
	}
	r.initClusterForKey(curKey)
	// 将当前命令推入等待队列，等待reconfig完成后提交
	log.Infof("push cmd(%v) of key %v in wait list(%v)",
		m.Command.Value, curKey, len(r.waitCmdsQueue[curKey]))
	r.waitCmdsQueue[curKey] = append(r.waitCmdsQueue[curKey], &m)
	// 生成一个小的config，并生成RECONFIG命令尝试进行提交
	var nextConfig Config
	nextConfig = r.generateZoneConfig()
	//selfpos := 0
	//for firstConfig[selfpos] != r.ID() {
	//	selfpos++
	//}
	//for i := 0; i < paxi.GetConfig().TolerateF*2+1; i++ {
	//	nextConfig = append(nextConfig, firstConfig[(selfpos+i)%nPeers])
	//}
	jsonstr, _ := json.Marshal(ReconfigCmd{nextConfig, ConfigVer(0)})
	reconfigValue := paxi.Value(jsonstr)
	log.Infof("first time propose cmd:%v, generate small config:%v, configValue:%s\n",
		curKey, nextConfig, reconfigValue)

	ch := make(chan paxi.Reply, 1)
	go func() {
		log.Infof("receive reply %v of reconfig for config:%v",
			<-ch, nextConfig)
	}()

	reconfigCmd := paxi.Request{
		paxi.Command{curKey, reconfigValue, r.ID(), 0, paxi.RECONFIG},
		make(map[string]string),
		m.Timestamp,
		r.ID(),
		ch,
	}
	return reconfigCmd
}

func contains(s Config, e paxi.ID) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func updateConfigVersion(value paxi.Value, ver ConfigVer) paxi.Value {
	var cmd ReconfigCmd
	json.Unmarshal(value, &cmd)
	log.Info("update config Ver ", cmd, " to ", ver)
	cmd.Ver = ver
	newjson, _ := json.Marshal(cmd)
	return newjson
}

//func (r *Replica) encodeForReconfig(dep map[paxi.ID]int) map[paxi.ID]int {
//	for id, inst := range(dep) {
//		if r.log[id][inst].cmd.Op == paxi.RECONFIG {
//			dep[id] = getDepOfConfig(inst, paxi.RECONFIG)
//		}
//	}
//	return dep
//}

//func (r *Replica) decodeForReconfig(dep map[paxi.ID]int) map[paxi.ID]int {
//	ret := make(map[paxi.ID]int)
//	for id, inst := range(dep) {
//		ret[id] = getRealInstance(inst)
//	}
//	return ret
//}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Infof("Replica %s receives request %+v", r.ID(), m)
	if _, ok := r.curConfigVer[m.Command.Key]; !ok {
		//未初始化working cluster，尝试新建一个并提交reconfig，并将当前m推入待处理队列
		reconfigCmd := r.initConfig(m)
		r.handleRequest(reconfigCmd)
		return
	} else {
		needProxy := false
		curKey := m.Command.Key
		var curCluster Config

		if r.newConfigState[curKey] != CONFIG_NORMAL && m.Command.Op == paxi.RECONFIG {
			// 上次reconfig完成前不允许新reconfig
			log.Warningf("already in config, do not receive client request")
			reply := paxi.Reply{}
			reply.Command = m.Command
			reply.Value = paxi.Value("already in reconfig")
			m.Reply(reply)
			return
		}
		// 查看普通命令是否需要代理，或者暂存等到reconfig结束在提交
		if m.Command.Op != paxi.TRANSFERFINISH {
			if r.newConfigState[curKey] == CONFIG_NEW_UNCOMMITTED {
				log.Infof("push %v cmd in wait list(%v)", m.Command, len(r.waitCmdsQueue[curKey]))
				r.waitCmdsQueue[curKey] = append(r.waitCmdsQueue[curKey], &m)
				return;
			} else if r.newConfigState[curKey] == CONFIG_NEW_COMMITTED {
				if !contains(r.configList[curKey][r.newConfigVer[curKey]], r.ID()) {
					//当前机器不在 新 config中，进行代理
					curCluster = r.configList[curKey][r.newConfigVer[curKey]]
					needProxy = true
				} else {
					log.Infof("push %v cmd in wait list(%v)", m.Command, len(r.waitCmdsQueue[curKey]))
					r.waitCmdsQueue[curKey] = append(r.waitCmdsQueue[curKey], &m)
					return;
				}
			} else { // CONFIG_NORMAL
				if !contains(r.configList[curKey][r.curConfigVer[curKey]], r.ID()) {
					//当前机器不在 当前 config中，进行代理
					curCluster = r.configList[curKey][r.curConfigVer[curKey]]
					needProxy = true
				}
			}
		}
		// 当前节点不是working cluster成员，发给成员节点
		if needProxy {
			next := curCluster[rand.Intn(len(curCluster))]
			log.Info("propose as proxy:", curKey, r.ID(), curCluster, next)
			go r.Forward(next, m)
			return
		}

		// 如果收到reconfig命令，设置对应key的config状态
		if m.Command.Op == paxi.RECONFIG {
			newVersion := r.curConfigVer[curKey] + 1
			m.Command.Value = updateConfigVersion(m.Command.Value, newVersion); // 更新config版本号
			log.Infof("encounter client reconfig, newconfigstate of key=%v %v -> CONFIG_NEW_UNCOMMITTED",
				curKey, r.newConfigState[curKey])
			r.newConfigState[curKey] = CONFIG_NEW_UNCOMMITTED
			// r.newConfig[cmds[0].K] = something
		}

		id := r.ID()
		ballot := paxi.NewBallot(0, id)
		r.slot[id]++
		s := r.slot[id]
		seq, dep := r.attributes(m.Command)

		r.log[id][s] = &instance{
			cmd:     m.Command,
			ballot:  ballot,
			status:  PREACCEPTED,
			seq:     seq,
			dep:     dep,
			changed: false,
			request: &m,
			quorum:  paxi.NewQuorum(),
		}

		// self ack
		r.log[id][s].quorum.ACK(id)

		r.update(m.Command, id, s, seq)

		r.Broadcast(PreAccept{
			Ballot:  ballot,
			Replica: id,
			Slot:    s,
			Command: m.Command,
			Seq:     seq,
			Dep:     r.log[id][s].copyDep(),
		})
	}
}

func (r *Replica) handlePreAccept(m PreAccept) {
	log.Debugf("Replica %s receives PreAccept %+v", r.ID(), m)
	id := m.Replica
	s := m.Slot
	i := r.log[id][s]

	if i == nil {
		r.log[id][s] = &instance{}
		i = r.log[id][s]
	}

	if i.status == COMMITTED || i.status == ACCEPTED {
		if i.cmd.Empty() { // 先收到committed 或 accepted 消息，后收到preaccept，只需存储log内容
			i.cmd = m.Command
			r.update(m.Command, id, s, m.Seq)
		}
		return
	}

	// 如果是reconfig命令，自己要切换到对应状态
	key := m.Command.Key
	if m.Command.Op == paxi.RECONFIG {
		if _, ok := r.configList[key]; !ok {
			// 第一次收到key的命令，初始化cluster
			r.initClusterForKey(key)
		}

		var reconfig ReconfigCmd
		json.Unmarshal(m.Command.Value, &reconfig)
		log.Infof("receive config of key %v from %v, config:%v, curVer:%v, curNewVer:%v",
			key, m.Replica, reconfig, r.curConfigVer[key], r.newConfigVer[key])

		if reconfig.Ver > r.curConfigVer[key] { // 版本更高，覆盖当前状态
			r.newConfigVer[key] = reconfig.Ver
			log.Infof("encounter newer config, newconfigstate of key=%v %v -> CONFIG_NEW_UNCOMMITTED",
				key, r.newConfigState[key])
			r.newConfigState[key] = CONFIG_NEW_UNCOMMITTED
		}
	}

	if s > r.slot[id] {
		r.slot[id] = s
	}

	seq, dep := r.attributes(m.Command)

	if m.Ballot >= i.ballot {
		i.ballot = m.Ballot
		i.cmd = m.Command
		i.status = PREACCEPTED
		i.seq = seq
		i.dep = dep
	}

	r.update(m.Command, id, s, seq)

	c := make(map[paxi.ID]int)
	for id, d := range r.committed {
		c[id] = d
	}
	r.Send(m.Replica, PreAcceptReply{
		Replica:   r.ID(),
		Slot:      s,
		Ballot:    i.ballot,
		Seq:       seq,
		Dep:       i.copyDep(),
		Committed: c,
		Ok:		   true,
	})
}

func (r *Replica) handlePreAcceptReply(m PreAcceptReply) {
	log.Debugf("Replica %s receives PreAcceptReply %+v", r.ID(), m)
	i := r.log[r.ID()][m.Slot]
	curKey := i.cmd.Key
	if i.status != PREACCEPTED {
		return
	}

	if m.Ballot > i.ballot {
		// TODO merge or not
		return
	}



	// TODO: 如果收到version不一致的反馈，则去获取更新version的config

	if m.Ok == false {
		if i.cmd.Op == paxi.TRANSFERFINISH {
			log.Infof("receive a refuse of TRANSFERFINISH key=%v", curKey)
			// 暂时不能提交，将此处命令替换为NOP
			i.cmd.Value = nil
		} else {
			// TODO: there is probably another active leader
			i.quorum.NACK(m.Replica)
			return
		}
	}

	hasUnknownConfig := false

	i.quorum.ACK(m.Replica)
	i.merge(m.Seq, m.Dep)

	committed := true
	for id, d := range m.Committed {
		if d > r.committed[id] {
			r.committed[id] = d
		}
		if r.committed[id] >= 0 && r.committed[id] < i.dep[id] {
			committed = false
		}
	}

	// 设置当前使用的config，如果是普通命令则使用新的已提交config来进行提交
	curConfig := r.configList[curKey][r.curConfigVer[curKey]]
	//if i.cmd.Op != paxi.RECONFIG && r.newConfigState[curKey] == CONFIG_NEW_COMMITTED {
	//	curConfig = r.configList[curKey][r.newConfigVer[curKey]]
	//}
	if i.quorum.FastWorkingQuorum(curConfig) {
		// TODO: 采集到不知道的dep之后的处理
		// 满足quorum则dep更新到最新状态，再检查是否依赖于不知道的reconfig
		//for id, inst := range(i.dep) {
		//	if isConfigDep(inst) && r.log[id][getRealInstance(inst)] != nil && // 依赖于未知reconfig
		//		i.cmd.Op != paxi.RECONFIG && i.cmd.Op != paxi.TRANSFERFINISH && // 普通cmd才检查
		//		i.cmd.Value != nil /* nil命令都可以提交 */ {
		//
		//		hasUnknownConfig = true
		//		log.Infof("encounter unknown config(id:%v, inst:%v, exec[%v]:%v), newconfigstate of key=%v %v -> CONFIG_NEW_UNCOMMITTED",
		//			id, inst, id, r.executed[id], curKey, r.newConfigState[curKey])
		//		r.newConfigState[curKey] = CONFIG_NEW_UNCOMMITTED
		//		// 这条命令在reconfig之后，现在不能提交，所以修改为nop命令
		//		i.cmd.Value = nil
		//	}
		//
		//}

		// fast path or slow path
		if !i.changed && committed && !hasUnknownConfig{
			// fast path
			r.fast++
			log.Debugf("Replica %s number of fast instance: %d", r.ID(), r.fast)
			i.status = COMMITTED
			r.updateCommit(r.ID())

			if i.cmd.Op == paxi.RECONFIG { // 新config提交成功，输出信息
				var newconfig ReconfigCmd
				json.Unmarshal(i.cmd.Value, &newconfig)
				log.Infof("handlePreAcceptReply:reconfig of key:%v committed in fast path, config:%v, curConfig:%v, curConfigVer:%v",
					i.cmd.Key, newconfig, r.configList[i.cmd.Key][r.curConfigVer[i.cmd.Key]], r.curConfigVer[i.cmd.Key])
			}
			r.Broadcast(Commit{
				Ballot:  i.ballot,
				Replica: r.ID(),
				Slot:    m.Slot,
				Command: i.cmd,
				Seq:     i.seq,
				Dep:     i.copyDep(),
			})
			//
			//if *replyWhenCommit {
			//	i.request.Reply(paxi.Reply{
			//		Command: i.cmd,
			//	})
			//}
		} else {
			// slow path
			r.slow++
			log.Debugf("Replica %s number of slow instance: %d", r.ID(), r.slow)
			i.status = ACCEPTED
			// reset quorum for accept message
			i.quorum.Reset()
			// self ack
			i.quorum.ACK(r.ID())
			r.Broadcast(Accept{
				Ballot:  i.ballot,
				Replica: r.ID(),
				Slot:    m.Slot,
				Seq:     i.seq,
				Dep:     i.copyDep(),
			})
		}
	}
}

func (r *Replica) handleAccept(m Accept) {
	log.Debugf("Replica %s receives Accept %+v", r.ID(), m)
	id := m.Replica
	s := m.Slot
	i := r.log[id][s]

	if i == nil {
		r.log[id][s] = &instance{}
		i = r.log[id][s]
	}

	if i.status == COMMITTED || i.status == EXECUTED {
		return
	}

	if s > r.slot[id] {
		r.slot[id] = s
	}

	if m.Ballot >= i.ballot {
		i.status = ACCEPTED
		i.ballot = m.Ballot
		i.seq = m.Seq
		i.dep = m.Dep
	}

	r.Send(id, AcceptReply{
		Ballot:  i.ballot,
		Replica: r.ID(),
		Slot:    s,
	})
}

func (r *Replica) handleAcceptReply(m AcceptReply) {
	log.Debugf("Replica %s receives AcceptReply %+v", r.ID(), m)
	i := r.log[r.ID()][m.Slot]

	if i.status != ACCEPTED {
		return
	}

	if i.ballot < m.Ballot {
		i.ballot = m.Ballot
		return
	}

	i.quorum.ACK(m.Replica)
	if i.quorum.WorkingMajority(r.configList[i.cmd.Key][r.curConfigVer[i.cmd.Key]]) {
		i.status = COMMITTED
		r.updateCommit(r.ID())
		//if *replyWhenCommit {
		//	i.request.Reply(paxi.Reply{
		//		Command: i.cmd,
		//	})
		//}
		r.Broadcast(Commit{
			Ballot:  i.ballot,
			Replica: r.ID(),
			Slot:    m.Slot,
			Command: i.cmd,
			Seq:     i.seq,
			Dep:     i.copyDep(),
		})
	}
}

func (r *Replica) handleCommit(m Commit) {
	log.Debugf("Replica %s receives Commit %+v", r.ID(), m)
	i := r.log[m.Replica][m.Slot]

	if m.Slot > r.slot[m.Replica] {
		r.slot[m.Replica] = m.Slot
	}

	if i == nil {
		r.log[m.Replica][m.Slot] = &instance{}
		i = r.log[m.Replica][m.Slot]
	}

	if m.Ballot >= i.ballot {
		i.ballot = m.Ballot
		i.cmd = m.Command
		i.status = COMMITTED
		i.seq = m.Seq
		i.dep = m.Dep
		r.update(m.Command, m.Replica, m.Slot, m.Seq)
	}

	if i.request != nil {
		// someone committed NOOP retry current request
		r.Retry(*i.request)
		i.request = nil
	}
	r.updateCommit(m.Replica)
}

func (r *Replica) handleTransfer(m TransferMsg) {
	log.Debugf("Replica %s receives TransferMsg %+v", r.ID(), m)
	i := r.log[m.Replica][m.Slot]

	if m.Slot > r.slot[m.Replica] {
		r.slot[m.Replica] = m.Slot
	}

	if i == nil {
		r.log[m.Replica][m.Slot] = &instance{}
		i = r.log[m.Replica][m.Slot]
	}

	if m.Ballot >= i.ballot {
		i.ballot = m.Ballot
		i.cmd = m.Command
		i.status = COMMITTED
		i.seq = m.Seq
		i.dep = m.Dep
		r.update(m.Command, m.Replica, m.Slot, m.Seq)
	}

	//if i.request != nil {
	//	// someone committed NOOP retry current request
	//	r.Retry(*i.request)
	//	i.request = nil
	//}
	r.updateCommit(m.Replica) // 执行到这条命令
	r.Send(m.Replica, TransferReply{
		Ballot:  i.ballot,
		Replica: r.ID(),
		Slot:    m.Slot,
		Ok:		 true,
	})
}

func (r *Replica) handleTransferReply(m TransferReply) {
	log.Debugf("Replica %s receives TransferReply %+v", r.ID(), m)
	i := r.log[r.ID()][m.Slot]

	if i.ballot < m.Ballot {
		i.ballot = m.Ballot
		return
	}

	if i.transferOk {
		return
	}

	i.quorum.ACK(m.Replica)
	if i.quorum.WorkingMajority(r.configList[i.cmd.Key][r.newConfigVer[i.cmd.Key]]) {
		i.transferOk = true
		// 多数新quorum中节点ack后，提交transferfinish命令通知transfer结束
		log.Infof("majority of new quorum acked, send transferfinish")
		ch := make(chan paxi.Reply, 1)
		go func(c chan paxi.Reply, k paxi.Key) {
			log.Infof("receive reply %v of transferfinish for config:%v",
				<-c, k)
		}(ch, i.cmd.Key)

		transferReq := paxi.Request{
			paxi.Command{
				i.cmd.Key,
				i.cmd.Value,
				r.ID(),
				0,
				paxi.TRANSFERFINISH},
			make(map[string]string),
			time.Now().UnixNano(),
			r.ID(),
			ch,
		}
		r.Retry(transferReq)
	}
}

func (r *Replica) handleConfigExecuted(id paxi.ID, slot int, cmd paxi.Command) {
	key := cmd.Key
	op := cmd.Op
	i := r.log[id][slot]
	var newconfig ReconfigCmd
	json.Unmarshal(cmd.Value, &newconfig)
	log.Infof("handleConfigExecuted: key=%v, config=%v", key, newconfig)

	if op == paxi.RECONFIG{
		// 如果是最新的config，修改机器状态
		if newconfig.Ver > r.curConfigVer[key] {
		//if r.newConfigState[key] == CONFIG_NEW_UNCOMMITTED {
			log.Infof("reconfig of key:%v executed, config:%v, curConfig:%v, curConfigVer:%v",
				key, newconfig, r.configList[key][r.curConfigVer[key]], r.curConfigVer[key])

			if(cap(r.configList[key]) <= int(newconfig.Ver)) { // 按需扩容底层数组
				newlist := make([]Config, len(r.configList[key]), 2 * int(newconfig.Ver))
				copy(newlist, r.configList[key])
				r.configList[key] = newlist
			}
			if(len(r.configList[key]) <= int(newconfig.Ver)) { // 按需扩大切片
				r.configList[key] = r.configList[key][:int(newconfig.Ver) + 1]
			}
			r.configList[key][newconfig.Ver] = newconfig.Newconfig
			r.newConfigVer[key] = newconfig.Ver

			log.Infof("newconfigstate(%v,%v) of key=%v %v -> CONFIG_NEW_COMMITTED",
				id, slot, key, r.newConfigState[key])
			r.newConfigState[key] = CONFIG_NEW_COMMITTED

			//(如果自己是command leader)通知新的机器transfer
			//要先通过inst找到所属的replica和entry
			//for i:=int(r.CommittedUpTo[r.Id]); i >= 0; i-- {
			//	if r.InstanceSpace[r.Id][i] == info.inst {
			//		log.Printf("in handleConfigExecuted, found that instance, replica:%v, instance:%v, len(cmds)=%v, inst.cmds.k=%v, info.key=%v", r.Id, i, len(info.inst.Cmds), info.inst.Cmds[0].K, info.key)
			//		r.bcastTransfer(r.Id, int32(i), info.inst.Cmds, info.inst.Seq, info.inst.Deps)
			//		break
			//	}
			//}
			if id == r.ID() {
				log.Infof("in handleConfigExecuted, found that instance, replica:%v, instance:%v, key:%v",
					id, slot, key)
				// 广播Transfer开始消息给新quorum
				transMsg := TransferMsg{
					Ballot:  i.ballot,
					Replica: r.ID(),
					Slot:    slot,
					Command: cmd,
					Seq:     i.seq,
					Dep:     i.copyDep(),
				}
				for _, dest := range newconfig.Newconfig {
					r.Send(dest, transMsg)
				}
				//ch := make(chan paxi.Reply, 1)
				//go func() {
				//	log.Infof("receive reply %v of transferfinish for config:%v",
				//		<-ch, newconfig)
				//}()
				//transfercmd := paxi.Request{
				//	paxi.Command{key, cmd.Value, r.ID(), 0, paxi.TRANSFERFINISH},
				//	make(map[string]string),
				//	nil,
				//	r.ID(),
				//	ch,
				//}
				//r.handleRequest(transfercmd)
			}
		} else {
			log.Infof("receive redundant reconfig cmd %+v", i)
		}
	} else if op == paxi.TRANSFERFINISH {
		// 如果是最新的config传输完成，修改机器状态
		if newconfig.Ver >= r.newConfigVer[key]{
			log.Infof("transfer finish of key:%v executed, config:%v, curConfig:%v, curConfigVer:%v",
				key, newconfig, r.configList[key][r.curConfigVer[key]], r.curConfigVer[key])

			r.curConfigVer[key] = r.newConfigVer[key]

			r.newConfigVer[key] = 0

			log.Infof("encounter transferfinish(%v, %v), newconfigstate of key=%v %v -> CONFIG_NORMAL",
				id, slot, key, r.newConfigState[key])
			r.newConfigState[key] = CONFIG_NORMAL

			// TODO: 将等待队列中的proposal都放到propose channel中
			log.Infof("push %v proposal to proposeChan", len(r.waitCmdsQueue[key]))
			for _, cmd := range r.waitCmdsQueue[key] {
				r.handleRequest(*cmd)
			}
			r.waitCmdsQueue[key] = r.waitCmdsQueue[key][:0]
		}
	}
}

func (r *Replica) execute() {
	for id, logs := range r.log {
		for s := r.executed[id] + 1; s <= r.slot[id]; s++ {
			i := logs[s]
			if i == nil {
				continue
			}
			if i.status == EXECUTED {
				if s == r.executed[id]+1 {
					r.executed[id] = s
				}
				continue
			}
			if i.status != COMMITTED {
				break
			}

			var v paxi.Value
			if i.cmd.Op != paxi.RECONFIG && i.cmd.Op != paxi.TRANSFERFINISH {
				v = r.Execute(i.cmd)
			} else {
				if i.cmd.Op == paxi.RECONFIG && i.cmd.Value != nil {
					var newconfig ReconfigCmd
					json.Unmarshal(i.cmd.Value, &newconfig)
					log.Infof("touch a reconfig command in execution, key:%v, config%v", i.cmd.Key, newconfig);

				}
				if i.cmd.Op == paxi.TRANSFERFINISH && i.cmd.Value != nil{
					log.Infof("touch a transfer finish of key[%v] in execution", i.cmd.Key);
					var newconfig ReconfigCmd
					json.Unmarshal(i.cmd.Value, &newconfig)
				}
				r.handleConfigExecuted(id, s, i.cmd)
			}

			// reply to client
			if i.request != nil {
				i.request.Reply(paxi.Reply{
					Command: i.cmd,
					Value:   v,
				})
			}
			if s == r.executed[id]+1 {
				r.executed[id] = s
			}
		}
	}
}

func (r *Replica) execute2() {

}
