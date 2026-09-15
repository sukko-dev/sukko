package platform

// UseValkeySentinel reports whether a Valkey client should connect in Sentinel failover
// mode rather than directly to a single instance. Sentinel mode requires multiple addresses
// (the sentinels) AND a master name to select the primary. A single address is always a
// direct connection, even though VALKEY_MASTER_NAME defaults to "mymaster" — this matches the
// documented VALKEY_ADDRS contract ("single instance = one address; Sentinel = all sentinel
// addresses") and prevents a single-instance deployment from wrongly speaking the Sentinel
// protocol, which FATALs against a standalone Valkey.
//
// Shared by every ws-server Valkey client builder: the broadcast bus, the history client,
// and the connections registry / admin pub/sub clients.
func UseValkeySentinel(addrs []string, masterName string) bool {
	return len(addrs) > 1 && masterName != ""
}
