package kiln

// Engine represents the top-level kiln database engine.
type Engine struct {
	path string
}

// Open initializes a new kiln database engine at the given path.
func Open(path string) (*Engine, error) {
	return &Engine{path: path}, nil
}

// Close closes the engine and flushes to disk.
func (e *Engine) Close() error {
	return nil
}
