package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// Runtime information
var runtime *Runtime

type Runtime struct {
	Id string
}

// Save
func (this *Runtime) Save(runtimeFilePath string) (bool, error) {
	jb, je := json.Marshal(this)
	if je != nil {
		return false, je
	}
	ioe := ioutil.WriteFile(runtimeFilePath, jb, conf.UnixFilePermissions)
	if ioe != nil {
		return false, ioe
	}
	return true, nil
}

// New runtime
func newRuntime() *Runtime {
	var runtimeFilePath string = fmt.Sprintf("%s/runtime.json", conf.MetaBasePath)

	// Read from disk?
	data, err := ioutil.ReadFile(runtimeFilePath)
	if err == nil && len(data) > 0 {
		var r *Runtime
		je := json.Unmarshal(data, &r)
		if je != nil {
			log.Errorf("Failed to parse JSON runtime: %s", je)
		} else if r != nil {
			return r
		}
	} else {
		log.Errorf("Failed to read runtime in %s: %s", runtimeFilePath, err)
	}

	// New
	r := &Runtime{
		Id: uuidToString(randomUuid()),
	}

	// Save
	_, e := r.Save(runtimeFilePath)
	if e != nil {
		log.Errorf("Failed to write runtime file to %s: %s", runtimeFilePath, e)
	}

	return r
}
