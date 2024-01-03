package methods

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func ProcessSecond(fileName string) (string, time.Duration, error) {
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

	num := 0
	var cityData string
	var data []string
	var temp float64

	for scanner.Scan() {
		num++
		cityData = scanner.Text()
		if cityData == "" {
			continue
		}
		data = strings.Split(cityData, ";")
		temp, _ = strconv.ParseFloat(data[1], 32)

		if err != nil {
			panic(err)
		}

		store[data[0]] += temp
		cnt[data[0]]++

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

	}

	fmt.Printf("Scanning time : %d\n", time.Since(scanningTimeStart))
	fmt.Printf("Number of records : %v\n", len(store))
	fmt.Printf("Total records : %v\n", num)
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

	fmt.Printf("Time to append %d\n", time.Since(appendTimeStart))

	sortTimeStart := time.Now()
	sort.Slice(finalData, func(i, j int) bool {
		return strings.Compare(finalData[i].CityName, finalData[j].CityName) == -1
	})
	fmt.Printf("Sort time %d\n", time.Since(sortTimeStart))

	return formatCityData(finalData), time.Since(startTimer), nil

}
