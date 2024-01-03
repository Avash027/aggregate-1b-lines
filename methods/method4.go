package methods

import (
	"bufio"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const HASH_MAP_SIZE = 100000

var sumHashMap [HASH_MAP_SIZE]float64
var minHashMap [HASH_MAP_SIZE]float64
var maxHashMap [HASH_MAP_SIZE]float64
var countHashMap [HASH_MAP_SIZE]int32
var isMapFilled [HASH_MAP_SIZE]int8
var name sync.Map

func ProcessFourth(fileName string) (string, time.Duration, error) {
	startTime := time.Now()
	f, _ := os.Open(fileName)

	scanner := bufio.NewScanner(f)

	const SEMI_COLON_BYTE = 59

	for scanner.Scan() {
		contentBytes := scanner.Bytes()

		i := int8(0)

		for i = 0; int(i) < len(contentBytes) && contentBytes[i] != SEMI_COLON_BYTE; i++ {
		}

		h := hash(contentBytes, 0, i)

		i++

		var num float64 = 0
		isNeg := false
		for ; int(i) < len(contentBytes); i++ {
			if contentBytes[i] == '-' {
				isNeg = true
				continue
			}
			if contentBytes[i] == '.' {
				num += float64(contentBytes[i+1]-'0') / 10.0
				break
			}

			num *= 10
			num += float64(contentBytes[i] - '0')
		}
		if isNeg {
			num = -num
		}
		sumHashMap[h] += num
		countHashMap[h]++
		if isMapFilled[h]&1 == 1 {
			minHashMap[h] = min(minHashMap[h], num)
		} else {
			minHashMap[h] = num
			isMapFilled[h] |= 1
		}

		if isMapFilled[h]&2 == 2 {
			maxHashMap[h] = max(maxHashMap[h], num)
		} else {
			maxHashMap[h] = num
			isMapFilled[h] |= 2
		}

		if isMapFilled[h]&4 == 0 {
			name.Store(h, contentBytes)
			isMapFilled[h] |= 4
		}

	}

	var cityData []CityData

	for i := int32(0); i < HASH_MAP_SIZE; i++ {
		if isMapFilled[i] == 0 {
			continue
		}

		val, _ := name.Load(i)

		cityData = append(cityData, CityData{
			CityName: getNameFromBytes(val.([]byte)),
			MaxTemp:  maxHashMap[i],
			MinTemp:  minHashMap[i],
			AvgTemp:  sumHashMap[i] / float64(countHashMap[i]),
		})

	}

	sort.Slice(cityData, func(i, j int) bool {
		return strings.Compare(cityData[i].CityName, cityData[j].CityName) == -1
	})

	return formatCityData(cityData), time.Since(startTime), nil
}

func getNameFromBytes(content []byte) string {
	for i := 0; i < len(content); i++ {
		if content[i] == ';' {
			return string(content[:i])
		}
	}

	return ""
}

func hash(content []byte, loInd int8, hiInd int8) int32 {
	const p = 19
	hash := 0
	pPow := 1

	for i := loInd; i < hiInd; i++ {
		hash = (hash + (int(content[i])-int('a')+1)*pPow) % HASH_MAP_SIZE
		pPow = (pPow * p) % HASH_MAP_SIZE
	}
	if hash < 0 {
		hash += HASH_MAP_SIZE
	}
	return int32(hash)
}
