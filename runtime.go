package main

// Runtime information
var runtime *Runtime

type Runtime struct {
	Id string
}

func newRuntime() *Runtime {
	return &Runtime{
		Id: uuidToString(randomUuid()),
	}
}
