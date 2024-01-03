package methods

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MAX_GO_ROUTINES = 1_000_000

func ProcessThird(fileName string) (string, time.Duration, error) {
	startTimer := time.Now()

	store := make(map[string]float64)
	cnt := make(map[string]int)
	minTemp := make(map[string]float64)
	maxTemp := make(map[string]float64)

	f, err := os.Open(fileName)

	if err != nil {
		return "", 0, err
	}

	fmt.Println("File opened")

	scanner := bufio.NewScanner(f)

	fmt.Println("File scanned")

	scanningTimeStart := time.Now()

	var cityData string
	limiter := make(chan (int), MAX_GO_ROUTINES)

	var wg sync.WaitGroup
	var mutex sync.Mutex
	num := 0
	for scanner.Scan() {
		wg.Add(1)
		limiter <- 1
		cityData = string(scanner.Bytes())
		num++
		go func(cityData string, wg *sync.WaitGroup, mutex *sync.Mutex, limier chan<- int) {

			defer func() {
				wg.Done()
				<-limiter
			}()
			if cityData == "" {
				return
			}
			data := strings.Split(cityData, ";")
			temp, _ := strconv.ParseFloat(data[1], 32)

			if err != nil {
				panic(err)
			}

			mutex.Lock()
			store[data[0]] += temp
			cnt[data[0]]++
			mutex.Unlock()

			mutex.Lock()
			if _, ok := minTemp[data[0]]; ok {
				minTemp[data[0]] = min(minTemp[data[0]], temp)
			} else {
				minTemp[data[0]] = temp
			}

			if _, ok := maxTemp[data[0]]; ok {
				maxTemp[data[0]] = max(maxTemp[data[0]], temp)
			} else {
				maxTemp[data[0]] = temp
			}
			mutex.Unlock()
		}(cityData, &wg, &mutex, limiter)

		if num%1_000_000 == 0 {
			runtime.GC()
		}
	}

	wg.Wait()

	fmt.Printf("Scanning time : %v\n", time.Since(scanningTimeStart))
	fmt.Printf("Number of records : %v\n", len(store))
	fmt.Printf("Scanned records: %v\n", num)
	finalData := []CityData{}

	appendTimeStart := time.Now()

	for k, v := range cnt {
		finalData = append(finalData, CityData{
			CityName: k,
			AvgTemp:  store[k] / float64(v),
			MaxTemp:  maxTemp[k],
			MinTemp:  minTemp[k],
		})
	}

	fmt.Printf("Time to append %v\n", time.Since(appendTimeStart))

	sortTimeStart := time.Now()
	sort.Slice(finalData, func(i, j int) bool {
		return strings.Compare(finalData[i].CityName, finalData[j].CityName) == -1
	})
	fmt.Printf("Sort time %v\n", time.Since(sortTimeStart))

	return formatCityData(finalData), time.Since(startTimer), nil

}
