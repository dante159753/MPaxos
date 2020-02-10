package mpaxos

import (
	"encoding/json"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/lib"
	"github.com/ailidani/paxi/log"
)

type Config []paxi.ID
type ConfigState uint8
type ConfigVer int32

const TOLERATE_F = 1

const (
	CONFIG_INIT ConfigState = iota
	CONFIG_NEW_UNCOMMITTED
	CONFIG_NEW_COMMITTED // in transfer process
	CONFIG_NORMAL        // can accept new config
)

type ReconfigCmd struct {
	newconfig Config
	ver       ConfigVer
}

type Replica struct {
	paxi.Node
	log          map[paxi.ID]map[int]*instance
	slot         map[paxi.ID]int // current instance number, start with 1
	committed    map[paxi.ID]int
	executed     map[paxi.ID]int
	conflicts    map[paxi.ID]map[paxi.Key]int
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

	return r
}

// attibutes generates the sequence and dependency attributes for command
func (r Replica) attributes(cmd paxi.Command) (seq int, dep map[paxi.ID]int) {
	seq = 0
	dep = make(map[paxi.ID]int)
	for id := range r.conflicts {
		if d, exists := r.conflicts[id][cmd.Key]; exists {
			if d > dep[id] {
				dep[id] = d
				if seq <= r.log[id][d].seq {
					seq = r.log[id][d].seq + 1
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

// updates local record for conflicts
func (r *Replica) update(cmd paxi.Command, id paxi.ID, slot, seq int) {
	k := cmd.Key
	d, exists := r.conflicts[id][k]
	if exists {
		if d < slot {
			r.conflicts[id][k] = slot
		}
	} else {
		r.conflicts[id][k] = slot
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

func (r *Replica) initConfig(m paxi.Request) paxi.Request {
	curKey := m.Command.Key
	log.Infof("before init config of %v", curKey)
	peers := paxi.GetConfig().Addrs
	nPeers := len(peers)
	var firstConfig Config
	curConfigList := make([]Config, 1)
	for k := range peers {
		firstConfig = append(firstConfig, k)
	}
	curConfigList[0] = firstConfig
	r.configList[curKey] = curConfigList
	r.curConfigVer[curKey] = ConfigVer(0)
	// 将当前命令推入等待队列，等待reconfig完成后提交
	log.Infof("push cmd(%v) of key %v in wait list(%v)",
		m.Command.Value, curKey, len(r.waitCmdsQueue[curKey]))
	r.waitCmdsQueue[curKey] = append(r.waitCmdsQueue[curKey], &m)
	// 生成一个小的config，并生成RECONFIG命令尝试进行提交
	var nextConfig Config
	i := 0
	for firstConfig[i] != r.ID() {
		i++
	}
	for ; i < TOLERATE_F*2+1; i++ {
		nextConfig = append(nextConfig, firstConfig[i%nPeers])
	}
	jsonstr, _ := json.Marshal(ReconfigCmd{nextConfig, ConfigVer(0)})
	reconfigValue := paxi.Value(jsonstr)
	log.Infof("first time propose cmd:%v, generate small config:%v, configValue:%v\n",
		curKey, nextConfig, reconfigValue)
	reconfigCmd := paxi.Request{
		paxi.Command{curKey, reconfigValue, r.ID(), 0, paxi.RECONFIG},
		make(map[string]string),
		m.Timestamp,
		r.ID(),
		nil,
	}
	return reconfigCmd
}

func (r *Replica) handleRequest(m paxi.Request) {
	if _, ok := r.curConfigVer[m.Command.Key]; !ok {
		//未初始化working cluster，尝试新建一个并提交reconfig，并将当前m推入待处理队列
		reconfigCmd := r.initConfig(m)
		r.handleRequest(reconfigCmd)
	} else {
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
		if i.cmd.Empty() {
			i.cmd = m.Command
			r.update(m.Command, id, s, m.Seq)
		}
		return
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
	})
}

func (r *Replica) handlePreAcceptReply(m PreAcceptReply) {
	log.Debugf("Replica %s receives PreAcceptReply %+v", r.ID(), m)
	i := r.log[r.ID()][m.Slot]
	if i.status != PREACCEPTED {
		return
	}

	if m.Ballot > i.ballot {
		// TODO merge or not
		return
	}

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

	if i.quorum.FastQuorum() {
		// fast path or slow path
		if !i.changed && committed {
			// fast path
			r.fast++
			log.Debugf("Replica %s number of fast instance: %d", r.ID(), r.fast)
			i.status = COMMITTED
			r.updateCommit(r.ID())
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
		// TODO
		return
	}

	i.quorum.ACK(m.Replica)
	if i.quorum.Majority() {
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

func (r *Replica) execute() {
	for id, log := range r.log {
		for s := r.executed[id] + 1; s <= r.slot[id]; s++ {
			i := log[s]
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
			v := r.Execute(i.cmd)
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
