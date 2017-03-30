package grid

import "errors"

var (
	ErrInvalidName      = errors.New("grid: invalid name")
	ErrInvalidNamespace = errors.New("grid: invalid namespace")
)

var (
	ErrReceiverBusy        = errors.New("grid: receiver busy")
	ErrUnknownMailbox      = errors.New("grid: unknown mailbox")
	ErrUnregisteredMailbox = errors.New("grid: unregistered mailbox")
	ErrContextFinished     = errors.New("grid: context finished")
	ErrInvalidActorType    = errors.New("grid: invalid actor type")
	ErrInvalidActorName    = errors.New("grid: invalid actor name")
)

var (
	ErrNilResponse               = errors.New("grid: nil response")
	ErrInvalidEtcd               = errors.New("grid: invalid etcd")
	ErrInvalidContext            = errors.New("grid: invalid context")
	ErrDefNotRegistered          = errors.New("grid: def not registered")
	ErrServerNotRunning          = errors.New("grid: server not running")
	ErrAlreadyRegistered         = errors.New("grid: already registered")
	ErrInvalidMailboxName        = errors.New("grid: invalid mailbox name")
	ErrNilActorDefinition        = errors.New("grid: nil actor definition")
	ErrWatchClosedUnexpectedly   = errors.New("grid: watch closed unexpectedly")
	ErrActorCreationNotSupported = errors.New("grid: actor creation not supported")
	ErrUnRegisteredMsgType       = errors.New("grid: unregistered message type")
)
