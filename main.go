package main

import (
	"flag"
	"fmt"
	"time"

	_ "net/http/pprof"

	"github.com/Avash027/read-1b-lines/methods"
)

var (
	generateFile = flag.Bool("generate", false, "Generate measurements")
	numOfRecords = flag.Int64("num", 100, "Number of records")
	methodName   = flag.Int("method", 1, "Method number")
)

func main() {
	flag.Parse()

	if *generateFile {
		CreateMeasurements(*numOfRecords)
		return
	}

	// if *methodName == -1 {
	// analysis()
	// }

	// var data string
	var timeTaken time.Duration
	var err error

	if *methodName == 1 {

		_, timeTaken, err = methods.ProcessFirst("/home/avash/Projects/read-1b-lines/measurements_large.txt")
	}

	if *methodName == 2 {
		_, timeTaken, err = methods.ProcessSecond("/home/avash/Projects/read-1b-lines/measurements.txt")
	}
	// data := ""
	if *methodName == 3 {
		_, timeTaken, err = methods.ProcessThird("/home/avash/Projects/read-1b-lines/measurements.txt")
		// fmt.Printf(data)
	}
	if *methodName == 4 {
		_, timeTaken, err = methods.ProcessFourth("/home/avash/Projects/read-1b-lines/measurements_large.txt")
	}

	fmt.Printf("Time taken: %v\nError if any: %v\n", timeTaken, err)
}
