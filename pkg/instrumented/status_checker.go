package instrumented

import (
	"fmt"
	"strings"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/utilitywarehouse/go-operational/op"
	"github.com/uw-labs/substrate"
)

const (
	failingAction = "Troubleshoot proximo backend."
	failingImpact = "Proximo not working."
)

// BackendStatusChecker is an interface that check status of substrate connections.
type BackendStatusChecker interface {
	CheckStatus(resp *op.CheckResponse)

	RemoveStatuser(id string)
	RegisterStatuser(id string, conn substrate.Statuser, cancel func())
}

type statusConnection struct {
	conn        substrate.Statuser
	cancel      func()
	failedCount int
}

type statusChecker struct {
	mutex          sync.Mutex
	conns          map[string]*statusConnection
	sg             singleflight.Group
	maxFailedCount int
}

// NewBackendStatusChecker returns a new instance of BackendStatusChecker.
func NewBackendStatusChecker(maxFailedCount int) BackendStatusChecker {
	return &statusChecker{
		conns:          make(map[string]*statusConnection),
		maxFailedCount: maxFailedCount,
	}
}

func (cs *statusChecker) RegisterStatuser(id string, conn substrate.Statuser, cancel func()) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.conns[id] = &statusConnection{
		conn:   conn,
		cancel: cancel,
	}
}

func (cs *statusChecker) RemoveStatuser(id string) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	delete(cs.conns, id)
}

func (cs *statusChecker) CheckStatus(resp *op.CheckResponse) {
	result := cs.checkStatus()
	if result.totalConns == 0 {
		resp.Healthy("no active connections")
		return
	}

	connsStr := fmt.Sprintf("%v/%v backend connections working;", result.workingConns, result.totalConns)
	problemsStr := strings.Join(result.problems, ", ")

	if result.workingConns*2 < len(cs.conns) {
		// More than half of active connections are unhealthy
		resp.Unhealthy(connsStr+problemsStr, failingAction, failingImpact)
		return
	}
	if result.workingConns != result.totalConns || len(result.problems) > 0 {
		resp.Degraded(connsStr+problemsStr, failingAction)
		return
	}
	resp.Healthy(connsStr)
}

type checkResult struct {
	workingConns int
	totalConns   int
	problems     []string
}

func (cs *statusChecker) checkStatus() checkResult {
	// use singleflight to deduplicate concurrent check requests
	result, _, _ := cs.sg.Do("check", func() (interface{}, error) {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()

		if len(cs.conns) == 0 {
			return checkResult{}, nil
		}

		var (
			working  int
			problems []string
		)

		for _, c := range cs.conns {
			s, err := c.conn.Status()
			if err != nil {
				problems = append(problems, err.Error())
				cs.incFailedForConn(c)
				continue
			}
			if len(s.Problems) > 0 {
				problems = append(problems, s.Problems...)
			}
			if !s.Working {
				cs.incFailedForConn(c)
				continue
			}
			c.failedCount = 0
			working++
		}

		return checkResult{
			workingConns: working,
			totalConns:   len(cs.conns),
			problems:     problems,
		}, nil
	})

	return result.(checkResult)
}

func (cs *statusChecker) incFailedForConn(c *statusConnection) {
	c.failedCount++
	if c.failedCount >= cs.maxFailedCount {
		c.cancel() // close unhealthy connection
	}
}
