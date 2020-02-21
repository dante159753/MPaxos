package mpaxos

import (
	"github.com/ailidani/paxi"
	"log"
	"sort"
	"time"
)

const (
	WHITE int8 = iota
	GRAY
	BLACK
)

type Exec struct {
	r *Replica
}

type SCComponent struct {
	nodes []*instance
	color int8
}

func (e *Exec) executeCommand(replica paxi.ID, instance int) bool {
	if e.r.log[replica][instance] == nil {
		return false
	}
	inst := e.r.log[replica][instance]
	if inst.status == EXECUTED {
		return true
	}
	if inst.status != COMMITTED {
		return false
	}

	if !e.findSCC(inst) {
		return false
	}

	return true
}

var stack []*instance = make([]*instance, 0, 100)

func (e *Exec) findSCC(root *instance) bool {
	index := 1
	//find SCCs using Tarjan's algorithm
	stack = stack[0:0]
	return e.strongconnect(root, &index)
}

func (e *Exec) strongconnect(v *instance, index *int) bool {
	v.Index = *index
	v.Lowlink = *index
	*index = *index + 1

	l := len(stack)
	if l == cap(stack) {
		newSlice := make([]*instance, l, 2*l)
		copy(newSlice, stack)
		stack = newSlice
	}
	stack = stack[0 : l+1]
	stack[l] = v

	for q := range e.r.log {
		inst := getRealInstance(v.dep[q])
		for i := e.r.executed[q] + 1; i <= inst; i++ {
			for e.r.log[q][i] == nil || e.r.log[q][i].cmd.Value == nil {
				time.Sleep(1000 * 1000)
			}
			/*        if !state.Conflict(v.Command, e.r.InstanceSpace[q][i].Command) {
			          continue
			          }
			*/
			if e.r.log[q][i].status == EXECUTED {
				continue
			}
			for e.r.log[q][i].status != COMMITTED {
				time.Sleep(1000 * 1000)
			}
			w := e.r.log[q][i]

			if w.Index == 0 {
				//e.strongconnect(w, index)
				if !e.strongconnect(w, index) {
					for j := l; j < len(stack); j++ {
						stack[j].Index = 0
					}
					stack = stack[0:l]
					return false
				}
				if w.Lowlink < v.Lowlink {
					v.Lowlink = w.Lowlink
				}
			} else { //if e.inStack(w)  //<- probably unnecessary condition, saves a linear search
				if w.Index < v.Lowlink {
					v.Lowlink = w.Index
				}
			}
		}
	}

	if v.Lowlink == v.Index {
		//found SCC
		list := stack[l:len(stack)]

		//execute commands in the increasing order of the Seq field
		sort.Sort(nodeArray(list))
		for _, w := range list {
			for w.cmd.Value == nil {
				time.Sleep(1000 * 1000)
			}
			val := e.r.Execute(w.cmd)
			// 如果是reconfig，将config发送给主线程进行应用
			if w.cmd.Op == paxi.RECONFIG {
				log.Printf("touch a reconfig command in execution, key:%v, ver=%v, config%v");
			}
			if w.cmd.Op == paxi.TRANSFERFINISH && w.cmd.Value != nil{
				log.Printf("touch a transfer finish command in execution");
			}
			w.status = EXECUTED
			// reply to client
			if w.request != nil {
				w.request.Reply(paxi.Reply{
					Command: w.cmd,
					Value:   val,
				})
			}
		}
		stack = stack[0:l]
	}
	return true
}

func (e *Exec) inStack(w *instance) bool {
	for _, u := range stack {
		if w == u {
			return true
		}
	}
	return false
}

type nodeArray []*instance

func (na nodeArray) Len() int {
	return len(na)
}

func (na nodeArray) Less(i, j int) bool {
	return na[i].seq < na[j].seq
}

func (na nodeArray) Swap(i, j int) {
	na[i], na[j] = na[j], na[i]
}

