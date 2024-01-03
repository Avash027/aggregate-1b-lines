package methods

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type CityData struct {
	CityName string
	AvgTemp  float64
	MaxTemp  float64
	MinTemp  float64
}

func min(x, y float64) float64 {
	if x < y {
		return x
	}
	return y
}

func max(x, y float64) float64 {
	if x > y {
		return x
	}
	return y
}

func ProcessFirst(fileName string) (string, time.Duration, error) {
	startTimer := time.Now()
	content_, err := os.ReadFile(fileName)

	if err != nil {
		return "", time.Since(startTimer), nil
	}

	content := string(content_)
	store := make(map[string]float64)
	cnt := make(map[string]int)
	minTemp := make(map[string]float64)
	maxTemp := make(map[string]float64)
	eachCityTemp := strings.Split(content, "\n")

	for _, cityData := range eachCityTemp {
		if cityData == "" {
			continue
		}
		data := strings.Split(cityData, ";")
		temp, _ := strconv.ParseFloat(data[1], 32)

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

	finalData := []CityData{}

	for k, v := range cnt {
		finalData = append(finalData, CityData{
			CityName: k,
			AvgTemp:  store[k] / float64(v),
			MaxTemp:  maxTemp[k],
			MinTemp:  minTemp[k],
		})
	}

	sort.Slice(finalData, func(i, j int) bool {
		return strings.Compare(finalData[i].CityName, finalData[j].CityName) == -1
	})

	return formatCityData(finalData), time.Since(startTimer), nil

}

func formatCityData(cityData []CityData) string {
	finalData := ""
	for _, data := range cityData {
		finalData += fmt.Sprintf("%s=%.1f/%.1f/%.1f\n", data.CityName, data.AvgTemp, data.MaxTemp, data.MinTemp)
	}

	return finalData
}
