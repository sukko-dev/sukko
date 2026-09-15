package broadcast

import "errors"

// ErrUnknownBroadcastType is returned by NewBus when cfg.Type is not a recognized value.
var ErrUnknownBroadcastType = errors.New("unknown broadcast type")

// ErrSubscriberNotFound is returned by Unsubscribe / UnsubscribeAll when the
// provided channel was not registered with the bus.
var ErrSubscriberNotFound = errors.New("subscriber not found")

// ErrEmptyTenantID is returned by Publish and Subscribe when the tenant ID is empty.
var ErrEmptyTenantID = errors.New("empty tenant ID")

// ErrInvalidTenantID is returned by Publish and Subscribe when the tenant ID contains
// the Valkey channel separator (":"), which would produce an ambiguous channel name.
var ErrInvalidTenantID = errors.New("tenant ID contains reserved separator character")
