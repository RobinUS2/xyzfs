package main

import (
	"github.com/Sirupsen/logrus"
)

var log *logrus.Logger

func init() {
	logrus.SetLevel(logrus.DebugLevel)
	log = logrus.New()
}
