package ardb

import (
	"errors"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/nbd/ardb/command"
)

// StorageAction defines the interface of an
// action which can be aplied to an ARDB connection.
type StorageAction interface {
	// Do applies this StorageAction to a given ARDB connection.
	Do(conn Conn) (reply interface{}, err error)

	// Send buffers the StorageAction to be aplied to a given ARDB connection.
	Send(conn Conn) (err error)

	// Write returns true in case the action writes to the ARDB connection.
	Write() bool

	// Keys returns the keys that are written to the ARDB connection.
	Keys() []string
}

// Command creates a single command on the fly,
// ready to be used as a(n) (ARDB) StorageAction.
func Command(ct command.Type, args ...interface{}) *StorageCommand {
	return &StorageCommand{
		Type:      ct,
		Arguments: args,
	}
}

// StorageCommand defines a structure which allows you
// to encapsulate a commandName and arguments,
// such that it can be (re)used as a StorageAction.
type StorageCommand struct {
	Type      command.Type
	Arguments []interface{}
}

// Do implements StorageAction.Do
func (cmd *StorageCommand) Do(conn Conn) (reply interface{}, err error) {
	if cmd == nil {
		return nil, errNoCommandDefined
	}

	return conn.Do(cmd.Type.Name, cmd.Arguments...)
}

// Send implements StorageAction.Send
func (cmd *StorageCommand) Send(conn Conn) error {
	if cmd == nil {
		return errNoCommandDefined
	}
	return conn.Send(cmd.Type.Name, cmd.Arguments...)
}

// Write implements StorageAction.Write
func (cmd *StorageCommand) Write() bool {
	if cmd == nil {
		return false
	}
	return cmd.Type.Write
}

// Keys implements StorageAction.Keys
func (cmd *StorageCommand) Keys() []string {
	if cmd == nil || !cmd.Type.Write || len(cmd.Arguments) == 0 {
		return nil
	}
	return []string{cmd.Arguments[0].(string)}
}

// Script creates a single script on the fly,
// ready to be used as a(n) (ARDB) StorageAction.
func Script(keyCount int, src string, valueKeys []string, keysAndArgs ...interface{}) *StorageScript {
	return &StorageScript{
		Script:           redis.NewScript(keyCount, src),
		KeysAndArguments: keysAndArgs,
		ValueKeys:        valueKeys,
	}
}

// StorageScript defines a structure which allows you
// to encapsulate a lua script and arguments,
// such that it can be (re)used as a StorageAction.
type StorageScript struct {
	Script           *redis.Script
	KeysAndArguments []interface{}
	// ValueKeys is a list of keys which is written to the ARDB connection.
	ValueKeys []string // TODO: define this automatically
}

// Do implements StorageAction.Do
func (cmd *StorageScript) Do(conn Conn) (reply interface{}, err error) {
	if cmd == nil || cmd.Script == nil {
		return nil, errNoCommandDefined
	}

	return cmd.Script.Do(conn, cmd.KeysAndArguments...)
}

// Send implements StorageAction.Send
func (cmd *StorageScript) Send(conn Conn) error {
	if cmd == nil || cmd.Script == nil {
		return errNoCommandDefined
	}
	return cmd.Script.Send(conn, cmd.KeysAndArguments...)
}

// Write implements StorageAction.Write
func (cmd *StorageScript) Write() bool {
	if cmd == nil {
		return false
	}

	// TODO: define automatically if a script actually writes
	return true
}

// Keys implements StorageAction.Keys
func (cmd *StorageScript) Keys() []string {
	if cmd == nil {
		return nil
	}
	return cmd.ValueKeys
}

// Commands creates a slice of commands on the fly,
// ready to be used as a(n) (ARDB) StorageAction.
func Commands(cmds ...StorageAction) *StorageCommands {
	return &StorageCommands{commands: cmds}
}

// StorageCommands defines a structure which allows you
// to encapsulate a slice of commands (see: StorageCommand),
// such that it can be (re)used as a StorageAction.
type StorageCommands struct {
	commands []StorageAction
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

// Send implements StorageAction.Send
func (cmds *StorageCommands) Send(conn Conn) (err error) {
	if cmds == nil {
		return nil
	}

	for _, cmd := range cmds.commands {
		err = cmd.Send(conn)
		if err != nil {
			return
		}
	}

	return nil
}

// Write implements StorageAction.Write
func (cmds *StorageCommands) Write() (w bool) {
	if cmds == nil {
		return
	}

	for _, cmd := range cmds.commands {
		w = cmd.Write()
		if w {
			return
		}
	}

	return
}

// Keys implements StorageAction.Keys
func (cmds *StorageCommands) Keys() (keys []string) {
	if cmds == nil {
		return
	}

	for _, cmd := range cmds.commands {
		keys = append(keys, cmd.Keys()...)
	}
	return
}

var (
	_ StorageAction = (*StorageCommand)(nil)
	_ StorageAction = (*StorageCommands)(nil)
	_ StorageAction = (*StorageScript)(nil)
)

// Various action-related errors returned by this file.
var (
	errNoCommandDefined  = errors.New("no ARDB command defined")
	errNoCommandsDefined = errors.New("no ARDB commands defined")
)
