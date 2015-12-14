package main

func panicErr(e error) {
	if e != nil {
		panic(e)
	}
}
