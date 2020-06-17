package redis

import (
	"fmt"
)

type Module struct {
	keyFormat string
	module    string
	keyPrefix string
}

func NewModule(module string) *Module {
	return &Module{
		keyFormat: "%s:%s", // module:key
		module:    module,
		keyPrefix: fmt.Sprintf("%s:", module),
	}
}

func (m *Module) Key(shortKey string) string {
	return fmt.Sprintf(m.keyFormat, m.module, shortKey)
}

func (m *Module) Prefix() string {
	return m.keyPrefix
}

func (m *Module) String() string {
	return fmt.Sprintf("module: %s", m.module)
}
