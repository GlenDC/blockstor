package ardb

import "errors"

// StorageAction defines the interface of an
// action which can be aplied to an ARDB connection.
type StorageAction interface {
	// Do applies this StorageAction to a given ARDB connection.
	Do(conn Conn) (reply interface{}, err error)
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

// send is a private function which allows you to send
// the command to a connection,
// such that even an undefined StorageCommand
// does not result in a panic.
// See `(*StorageCommands).Do` to see it in action.
func (cmd *StorageCommand) send(conn Conn) error {
	if cmd == nil {
		return errNoCommandDefined
	}
	return conn.Send(cmd.Name, cmd.Arguments...)
}

// Commands creates a slice of commands on the fly,
// ready to be used as a(n) (ARDB) StorageAction.
func Commands(cmds ...*StorageCommand) *StorageCommands {
	return &StorageCommands{commands: cmds}
}

// StorageCommands defines a structure which allows you
// to encapsulate a slice of commands (see: StorageCommand),
// such that it can be (re)used as a StorageAction.
type StorageCommands struct {
	commands []*StorageCommand
}

// Do implements StorageAction.Do
func (cmds *StorageCommands) Do(conn Conn) (replies interface{}, err error) {
	if cmds == nil || cmds.commands == nil {
		return nil, errNoCommandsDefined
	}

	// 1. send all commands
	for _, cmd := range cmds.commands {
		err = cmd.send(conn)
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
