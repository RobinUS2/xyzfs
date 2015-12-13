package main

// Data store

var datastore *Datastore

type Datastore struct {
}

// Prepare data store for interactions
func (this *Datastore) prepare() {
	// Load volumes
	for _, volume := range conf.Datastore.Volumes {
		volume.prepare()
	}
}

func newDatastore() *Datastore {
	d := &Datastore{}
	d.prepare()
	return d
}
