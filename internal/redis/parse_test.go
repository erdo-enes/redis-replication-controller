package redis

import "testing"

func TestParseRoleReply(t *testing.T) {
	t.Run("master", func(t *testing.T) {
		reply := []interface{}{"master", int64(3129659), []interface{}{}}
		info, err := parseRoleReply(reply)
		if err != nil {
			t.Fatal(err)
		}
		if info.Role != RoleMaster || info.ReplOffset != 3129659 {
			t.Fatalf("got %+v", info)
		}
	})

	t.Run("slave", func(t *testing.T) {
		reply := []interface{}{"slave", "10.0.0.5", int64(6379), "connected", int64(42)}
		info, err := parseRoleReply(reply)
		if err != nil {
			t.Fatal(err)
		}
		if info.Role != RoleReplica || info.MasterHost != "10.0.0.5" ||
			info.MasterPort != 6379 || info.ReplState != "connected" || info.ReplOffset != 42 {
			t.Fatalf("got %+v", info)
		}
	})

	t.Run("unexpected role value", func(t *testing.T) {
		_, err := parseRoleReply([]interface{}{"sentinel"})
		if err == nil {
			t.Fatal("expected error for unexpected role value")
		}
	})

	t.Run("not an array", func(t *testing.T) {
		if _, err := parseRoleReply("master"); err == nil {
			t.Fatal("expected error for non-array reply")
		}
	})
}

func TestParseReplicationInfo(t *testing.T) {
	t.Run("master", func(t *testing.T) {
		const s = "# Replication\r\nrole:master\r\nconnected_slaves:2\r\nmaster_repl_offset:1500\r\n"
		info := parseReplicationInfo(s)
		if !info.IsMaster() {
			t.Fatalf("IsMaster() = false: %+v", info)
		}
		if info.ConnectedSlaves != 2 || info.Offset() != 1500 {
			t.Fatalf("got %+v", info)
		}
	})

	t.Run("slave", func(t *testing.T) {
		const s = "# Replication\r\nrole:slave\r\nmaster_host:10.0.0.7\r\nmaster_port:6379\r\n" +
			"master_link_status:up\r\nslave_repl_offset:1490\r\n"
		info := parseReplicationInfo(s)
		if !info.IsReplica() {
			t.Fatalf("IsReplica() = false: %+v", info)
		}
		if info.MasterHost != "10.0.0.7" || info.MasterPort != 6379 ||
			info.MasterLinkStatus != "up" || info.Offset() != 1490 {
			t.Fatalf("got %+v", info)
		}
	})

	t.Run("malformed", func(t *testing.T) {
		info := parseReplicationInfo("this is not\nvalid redis info\n\r\n###")
		if info == nil {
			t.Fatal("returned nil, want non-nil with empty role")
		}
		if info.Role != "" {
			t.Fatalf("Role = %q, want empty", info.Role)
		}
		if info.IsMaster() {
			t.Fatal("malformed info must not report master")
		}
	})
}
