package main

// All that has to be written to the physical devices has to implement this interface for (de)serialization

type BinaryWritable interface {
	Bytes() []byte
	FromBytes([]byte)
}
