package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
)

const (
	tokenGroupStart = "["
	tokenGroupEnd   = "]"
)

func must(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

type Data struct {
	ZipCode    uint64
	City       string
	State      string
	County     string
	ZHIs       []float64
	GrowthRate float64
	Years      float64
	Dataset    string
}

func (d *Data) String() string {
	return fmt.Sprintf(`
Dataset    : %v
Zip Code   : %v
City       : %v
State      : %v
County     : %v
Growth Rate: %v
Years      : %v
Price      : $%v
Google Map : https://www.google.com/maps/place/%v
`, d.Dataset, d.ZipCode, d.City, d.State, d.County, d.GrowthRate, d.Years, humanize.Comma(int64(d.ZHIs[len(d.ZHIs)-1])), d.ZipCode)
}

type SortableData []Data

func (d SortableData) Len() int           { return len(d) }
func (d SortableData) Less(i, j int) bool { return d[i].GrowthRate < d[j].GrowthRate }
func (d SortableData) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }

func calculateGrowthRate(vs []float64) (float64, float64) {
	start := 0
	for i, v := range vs {
		if v != 0.0 {
			start = i
			break
		}
	}

	months := len(vs) - start
	rem := months % 12
	start += rem

	future := vs[len(vs)-1]
	present := vs[start]
	years := float64(len(vs)-start) / 12

	return (math.Pow(future/present, 1/years) - 1) * 100, years
}

type FilterFn func(*Data) bool

func filterByZipCode(zipCode uint64) FilterFn {
	return FilterFn(func(d *Data) bool {
		return d.ZipCode == zipCode
	})
}

func filterByState(state string) FilterFn {
	state = strings.ToLower(state)
	return FilterFn(func(d *Data) bool {
		return strings.ToLower(d.State) == state
	})
}

func filterByCounty(county string) FilterFn {
	county = strings.ToLower(county)
	return FilterFn(func(d *Data) bool {
		return strings.ToLower(d.County) == county
	})
}

func filterByCity(city string) FilterFn {
	city = strings.ToLower(city)
	return FilterFn(func(d *Data) bool {
		return strings.ToLower(d.City) == city
	})
}

func filterByPrice(price float64) FilterFn {
	return FilterFn(func(d *Data) bool {
		return d.ZHIs[len(d.ZHIs)-1] <= price
	})
}

func filterByGrowthRate(rate float64) FilterFn {
	return FilterFn(func(d *Data) bool {
		return d.GrowthRate >= rate
	})
}

func chainByAnd(filters ...FilterFn) FilterFn {
	return FilterFn(func(d *Data) bool {
		for _, f := range filters {
			if !f(d) {
				return false
			}
		}

		return true
	})
}

func chainByOr(filters ...FilterFn) FilterFn {
	return FilterFn(func(d *Data) bool {
		for _, f := range filters {
			if f(d) {
				return true
			}
		}

		return false
	})
}

func parseFilters(tokens []string) (FilterFn, int, error) {
	var filters []FilterFn
	var operators []string
	i := 0

	if len(tokens) == 0 {
		return nil, -1, fmt.Errorf("No token given")
	}

	if tokens[0] != tokenGroupStart {
		return nil, -1, fmt.Errorf("Tokens need to always start with a %s", tokenGroupStart)
	}

	parseOperator := func(token string) (string, bool) {
		if token == "and" || token == "or" {
			return token, true
		}

		return "", false
	}

	parseFilter := func(token string) (FilterFn, error) {
		splitted := strings.Split(token, ":")
		kind, arg := splitted[0], splitted[1]

		stringFilters := map[string]func(string) FilterFn{
			"State":  filterByState,
			"County": filterByCounty,
			"City":   filterByCity,
		}
		floatFilters := map[string]func(float64) FilterFn{
			"GrowthRate": filterByGrowthRate,
			"Price":      filterByPrice,
		}
		uintFilters := map[string]func(uint64) FilterFn{
			"ZipCode": filterByZipCode,
		}

		if f, ok := stringFilters[kind]; ok {
			return f(arg), nil
		} else if f, ok := floatFilters[kind]; ok {
			arg, err := strconv.ParseFloat(arg, 64)
			if err != nil {
				return nil, err
			}
			return f(arg), nil
		} else if f, ok := uintFilters[kind]; ok {
			arg, err := strconv.ParseUint(arg, 10, 64)
			if err != nil {
				return nil, err
			}
			return f(arg), nil
		}

		return nil, fmt.Errorf("Couldn't find filter")
	}

	tokens = tokens[1:]

	for i < len(tokens) {
		token := tokens[i]

		if token == tokenGroupEnd {
			f := filters[0]
			for i, op := range operators {
				nextFilter := filters[i+1]
				if op == "and" {
					f = chainByAnd(f, nextFilter)
				} else if op == "or" {
					f = chainByOr(f, nextFilter)
				} else {
					return nil, -1, fmt.Errorf("Invalid operator")
				}
			}

			return f, i + 1, nil
		}

		if token == tokenGroupStart {
			f, length, err := parseFilters(tokens[i:])
			if err != nil {
				return nil, -1, err
			}

			i += length
			filters = append(filters, f)
		} else if op, ok := parseOperator(token); ok {
			operators = append(operators, op)
		} else {
			f, err := parseFilter(token)
			if err != nil {
				return nil, -1, err
			}

			filters = append(filters, f)
		}

		i++
	}

	return nil, -1, fmt.Errorf("Unfinished tokens")
}

func help() {
	fmt.Printf(`
Usage: ./zhiquery <dataset_dir> [ <kind_1>:<arg_1> or/and <kind_2>:<arg_2> or/and [ <kind_n>:<arg_n> ... ]]

Kinds and Arguments:
  * State:
    * arg_1: exact match state (string)
  * County
    * arg_1: exact match county (string)
  * City
    * arg_1: exact match city (string)
  * GrowthRate
    * arg_1: lower bound growth rate (float)
  * Price
    * arg_1: upper bound price (float)
  * ZipCode
    * arg_1: exact match zip code (unsigned integer)
`)
}

func main() {
	if len(os.Args) < 2 {
		help()
		return
	}

	repository := os.Args[1]
	datasets, err := ioutil.ReadDir(repository)
	must(err)

	filter, _, err := parseFilters(os.Args[2:])
	must(err)

	var datas []Data
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(len(datasets))
	for _, dataset := range datasets {
		dataset := dataset
		go func() {
			var datasetDatas []Data
			f, err := os.Open(path.Join(repository, dataset.Name()))
			must(err)
			defer f.Close()

			scanner := bufio.NewScanner(f)
			// ignore header
			scanner.Scan()

			for scanner.Scan() {
				var data Data

				line := scanner.Text()

				// RegionID,SizeRank,RegionName,RegionType,StateName,State,City,Metro,CountyName,...
				fields := strings.Split(line, ",")

				data.Dataset = dataset.Name()
				data.City = fields[6]
				data.State = fields[5]
				data.County = fields[8]
				zipCode, err := strconv.ParseUint(fields[2], 10, 64)
				must(err)
				data.ZipCode = zipCode

				zhis := fields[9:]
				for _, zhi := range zhis {
					v, _ := strconv.ParseFloat(zhi, 64)
					data.ZHIs = append(data.ZHIs, v)
				}
				data.GrowthRate, data.Years = calculateGrowthRate(data.ZHIs)

				if filter(&data) {
					datasetDatas = append(datasetDatas, data)
				}
			}

			mu.Lock()
			datas = append(datas, datasetDatas...)
			mu.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()
	sort.Sort(SortableData(datas))
	for _, data := range datas {
		fmt.Println(&data)
	}

	fmt.Println("Total zip codes:", len(datas))
}
