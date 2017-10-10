package ardb

import (
	"errors"

	"github.com/garyburd/redigo/redis"
)

// StorageAction defines the interface of an
// action which can be aplied to an ARDB connection.
type StorageAction interface {
	// Do applies this StorageAction to a given ARDB connection.
	Do(conn Conn) (reply interface{}, err error)
}

// StorageBufferAction defines the interface of an
// action which can be buffered together with other commands,
// such that they can all be applied together to a given ARDB connection.
type StorageBufferAction interface {
	// Send buffers the StorageAction to be aplied to a given ARDB connection.
	Send(conn Conn) (err error)
}

// Command creates a single command on the fly,
// ready to be used as a(n) (ARDB) StorageAction.
func Command(name string, args ...interface{}) *StorageCommand {
	return &StorageCommand{
		Name:      name,
		Arguments: args,
	}
}

// StorageCommand defines a structure which allows you
// to encapsulate a commandName and arguments,
// such that it can be (re)used as a StorageAction.
type StorageCommand struct {
	Name      string
	Arguments []interface{}
}

// Do implements StorageAction.Do
func (cmd *StorageCommand) Do(conn Conn) (reply interface{}, err error) {
	if cmd == nil {
		return nil, errNoCommandDefined
	}

	return conn.Do(cmd.Name, cmd.Arguments...)
}

// Send implements StorageBufferAction.Send
func (cmd *StorageCommand) Send(conn Conn) error {
	if cmd == nil {
		return errNoCommandDefined
	}
	return conn.Send(cmd.Name, cmd.Arguments...)
}

// Script creates a single script on the fly,
// ready to be used as a(n) (ARDB) StorageAction.
func Script(keyCount int, src string, keysAndArgs ...interface{}) *StorageScript {
	return &StorageScript{
		Script:           redis.NewScript(keyCount, src),
		KeysAndArguments: keysAndArgs,
	}
}

// StorageScript defines a structure which allows you
// to encapsulate a lua script and arguments,
// such that it can be (re)used as a StorageAction.
type StorageScript struct {
	Script           *redis.Script
	KeysAndArguments []interface{}
}

// Do implements StorageAction.Do
func (cmd *StorageScript) Do(conn Conn) (reply interface{}, err error) {
	if cmd == nil || cmd.Script == nil {
		return nil, errNoCommandDefined
	}

	return cmd.Script.Do(conn, cmd.KeysAndArguments...)
}

// Send implements StorageBufferAction.Send
func (cmd *StorageScript) Send(conn Conn) error {
	if cmd == nil || cmd.Script == nil {
		return errNoCommandDefined
	}
	return cmd.Script.Send(conn, cmd.KeysAndArguments...)
}

// Commands creates a slice of commands on the fly,
// ready to be used as a(n) (ARDB) StorageAction.
func Commands(cmds ...StorageBufferAction) *StorageCommands {
	return &StorageCommands{commands: cmds}
}

// StorageCommands defines a structure which allows you
// to encapsulate a slice of commands (see: StorageCommand),
// such that it can be (re)used as a StorageAction.
type StorageCommands struct {
	commands []StorageBufferAction
}

// Do implements StorageAction.Do
func (cmds *StorageCommands) Do(conn Conn) (replies interface{}, err error) {
	if cmds == nil || cmds.commands == nil {
		return nil, errNoCommandsDefined
	}

	// 1. send all commands
	for _, cmd := range cmds.commands {
		err = cmd.Send(conn)
		if err != nil {
			return nil, err
		}
	}

	// 2. flush all commands
	err = conn.Flush()
	if err != nil {
		return nil, err
	}

	allReplies := make([]interface{}, len(cmds.commands))

	// 3. receive the resulting replies for all commands
	for index := range cmds.commands {
		allReplies[index], err = conn.Receive()
		if err != nil {
			return nil, err
		}
	}

	// success, return all replies
	return allReplies, nil
}

// Various action-related errors returned by this file.
var (
	errNoCommandDefined  = errors.New("no ARDB command defined")
	errNoCommandsDefined = errors.New("no ARDB commands defined")
)
