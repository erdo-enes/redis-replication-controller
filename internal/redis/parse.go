package redis

import (
	"fmt"
	"strconv"
	"strings"
)

// Redis reports replica nodes using the historical term "slave" on the wire.
const (
	RoleMaster  = "master"
	RoleReplica = "slave"
)

// RoleInfo is the parsed result of the ROLE command.
type RoleInfo struct {
	Role       string // "master", "slave", or other
	ReplOffset int64
	// Populated only when Role == "slave".
	MasterHost string
	MasterPort int
	ReplState  string // connect | connecting | sync | connected
}

// ReplicationInfo is the parsed result of INFO replication.
type ReplicationInfo struct {
	Role             string
	ConnectedSlaves  int
	MasterHost       string
	MasterPort       int
	MasterLinkStatus string // "up" | "down" (replicas only)
	MasterReplOffset int64
	SlaveReplOffset  int64
}

// IsMaster reports whether the node currently acts as a master.
func (r *ReplicationInfo) IsMaster() bool { return r != nil && r.Role == RoleMaster }

// IsReplica reports whether the node currently acts as a replica.
func (r *ReplicationInfo) IsReplica() bool { return r != nil && r.Role == RoleReplica }

// Offset returns the most relevant replication offset for the node's role.
// It is used to pick the most up-to-date replica during failover.
func (r *ReplicationInfo) Offset() int64 {
	if r == nil {
		return 0
	}
	if r.Role == RoleMaster {
		return r.MasterReplOffset
	}
	return r.SlaveReplOffset
}

// parseRoleReply decodes the nested array returned by the ROLE command.
//
//	master: ["master", <offset>, [[ip, port, offset], ...]]
//	replica: ["slave", <master-ip>, <master-port>, <state>, <offset>]
func parseRoleReply(reply interface{}) (*RoleInfo, error) {
	arr, ok := reply.([]interface{})
	if !ok || len(arr) == 0 {
		return nil, fmt.Errorf("redis: unexpected ROLE reply %T", reply)
	}
	role, ok := arr[0].(string)
	if !ok {
		return nil, fmt.Errorf("redis: ROLE role field is %T, want string", arr[0])
	}
	info := &RoleInfo{Role: role}
	switch role {
	case RoleMaster:
		if len(arr) >= 2 {
			info.ReplOffset = toInt64(arr[1])
		}
		return info, nil
	case RoleReplica:
		if len(arr) >= 5 {
			info.MasterHost, _ = arr[1].(string)
			info.MasterPort = int(toInt64(arr[2]))
			info.ReplState, _ = arr[3].(string)
			info.ReplOffset = toInt64(arr[4])
		}
		return info, nil
	default:
		return info, fmt.Errorf("redis: unexpected ROLE value %q", role)
	}
}

// parseReplicationInfo parses the bulk text returned by INFO replication.
// Malformed input never panics; an empty Role indicates the role could not be
// determined and the caller must treat the node as unverified.
func parseReplicationInfo(s string) *ReplicationInfo {
	info := &ReplicationInfo{}
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch key {
		case "role":
			info.Role = val
		case "connected_slaves":
			info.ConnectedSlaves = int(toInt64(val))
		case "master_host":
			info.MasterHost = val
		case "master_port":
			info.MasterPort = int(toInt64(val))
		case "master_link_status":
			info.MasterLinkStatus = val
		case "master_repl_offset":
			info.MasterReplOffset = toInt64(val)
		case "slave_repl_offset":
			info.SlaveReplOffset = toInt64(val)
		}
	}
	return info
}

// toInt64 coerces a RESP value (int64 or string) into an int64, returning 0 on
// failure.
func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		return n
	default:
		return 0
	}
}
