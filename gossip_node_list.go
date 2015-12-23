package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// Receive list of nodes
func (this *Gossip) _receiveNodeList(cmeta *TransportConnectionMeta, msg *GossipMessage) {
	log.Debugf("Receiving nodes list %s", string(msg.Data))
	this._startGossipFromJson(msg.Data)
}

// Start gossip from json array with nodes
func (this *Gossip) _startGossipFromJson(b []byte) {
	// Parse JSON
	var list []string
	jsonE := json.Unmarshal(b, &list)
	if jsonE != nil {
		log.Warnf("Failed to read nodes list: %s", jsonE)
		return
	}

	// New nodes?
	var newList []string = make([]string, 0)
	this.nodesMux.RLock()
	for _, elm := range list {
		if this.nodes[elm] == nil {
			newList = append(newList, elm)
		}
	}
	this.nodesMux.RUnlock()

	// Connect to new nodes
	if len(newList) > 0 {
		log.Infof("Discovered new nodes: %v", newList)
		for _, newElm := range newList {
			go this._sendHello(newElm)
		}
	}
}

// Send list of nodes
func (this *Gossip) _sendNodeList(node string) {
	log.Infof("Sending node list to %s", node)
	msg := newGossipMessage(NodeListGossipMessageType, this.NodesToJSON())
	this._send(node, msg)
}

// Persist list of nodes to disk for future
func (this *Gossip) NodesToJSON() []byte {
	// To JSON
	this.nodesMux.RLock()
	list := make([]string, 0)
	for node, _ := range this.nodes {
		list = append(list, node)
	}
	jsonBytes, jsonE := json.Marshal(list)
	this.nodesMux.RUnlock()
	panicErr(jsonE)
	return jsonBytes
}

// Persist list of nodes to disk for future
func (this *Gossip) PersistNodesToDisk() {
	// To JSON
	jsonBytes := this.NodesToJSON()

	// Write to disk
	err := ioutil.WriteFile(fmt.Sprintf("%s/gossip_nodes.json", conf.MetaBasePath), jsonBytes, 0644)
	if err != nil {
		log.Errorf("Failed to persist gossip nodes list to disk: %s", err)
	}
}

// Recover gossip
func (this *Gossip) recoverGossipFromDisk() {
	// Read JSON
	jsonBytes, err := ioutil.ReadFile(fmt.Sprintf("%s/gossip_nodes.json", conf.MetaBasePath))
	if err != nil {
		log.Errorf("Failed to read gossip nodes list from disk: %s", err)
	}

	// Start gossip
	this._startGossipFromJson(jsonBytes)
}
