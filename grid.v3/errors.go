package grid

import "errors"

var (
	ErrInvalidName      = errors.New("invalid name")
	ErrInvalidNamespace = errors.New("invalid namespace")
)

var (
	ErrReceiverBusy     = errors.New("receiver busy")
	ErrUnknownMailbox   = errors.New("unknown mailbox")
	ErrContextFinished  = errors.New("context finished")
	ErrInvalidActorType = errors.New("invalid actor type")
	ErrInvalidActorName = errors.New("invalid actor name")
)

var (
	ErrNilResponse               = errors.New("nil response")
	ErrInvalidEtcd               = errors.New("invalid etcd")
	ErrInvalidContext            = errors.New("invalid context")
	ErrServerNotRunning          = errors.New("server not running")
	ErrAlreadyRegistered         = errors.New("already registered")
	ErrInvalidMailboxName        = errors.New("invalid mailbox name")
	ErrGridReturnedNilActor      = errors.New("grid returned nil actor")
	ErrWatchClosedUnexpectedly   = errors.New("watch closed unexpectedly")
	ErrConnectionIsUnavailable   = errors.New("connection is unavailable")
	ErrActorCreationNotSupported = errors.New("actor creation not supported")
)
