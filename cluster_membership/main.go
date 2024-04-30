package cluster_membership

import "sync"

type MembershipManager struct {
	lock sync.Mutex
}
