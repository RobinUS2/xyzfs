package main

var conf *Conf

type Conf struct {
	HttpPort int
}

func newConf() *Conf {
	return &Conf{
		HttpPort: 8080,
	}
}
