package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// ReducerRoutine procesa los diccionarios locales generados por los workers para un shard
func ReducerRoutine(ch <-chan map[string]*AggregationData) map[string]*AggregationData {
	shardAgg := make(map[string]*AggregationData)

	for localAgg := range ch {
		for key, lData := range localAgg {
			gData, exists := shardAgg[key]
			if !exists {
				gData = &AggregationData{
					PassengerCount: 0,
					Services:       make(map[string]struct{}),
					CardTypes:      make(map[string]int),
				}
				shardAgg[key] = gData
			}

			gData.PassengerCount += lData.PassengerCount
			for svc := range lData.Services {
				gData.Services[svc] = struct{}{}
			}
			for ct, count := range lData.CardTypes {
				gData.CardTypes[ct] += count
			}
		}
	}
	return shardAgg
}

// FormatResults aplana y ordena los diccionarios finales (sharded)
func FormatResults(globalAggs []map[string]*AggregationData) []ResultRow {
	var outRows []ResultRow
	
	for _, shardAgg := range globalAggs {
		for key, agg := range shardAgg {
			parts := strings.Split(key, "|")
			stn := parts[0]

			var domCard string
			var maxC int
			for ct, count := range agg.CardTypes {
				if count > maxC || (count == maxC && ct > domCard) {
					maxC = count
					domCard = ct
				}
			}

			var ts int64
			fmt.Sscanf(parts[1], "%d", &ts)
			binTime := time.Unix(ts, 0).UTC()

			hour := binTime.Hour()
			weekday := int(binTime.Weekday())
			if weekday == 0 {
				weekday = 7
			}

			outRows = append(outRows, ResultRow{
				BoardingStopStn:  stn,
				TimeBin15Min:     binTime,
				PassengerCount:   agg.PassengerCount,
				UniqueServices:   len(agg.Services),
				DominantCardType: domCard,
				Hour:             hour,
				DayOfWeek:        weekday,
				Date:             binTime.Format("2006-01-02"),
			})
		}
	}

	sort.Slice(outRows, func(i, j int) bool {
		if outRows[i].BoardingStopStn == outRows[j].BoardingStopStn {
			return outRows[i].TimeBin15Min.Before(outRows[j].TimeBin15Min)
		}
		return outRows[i].BoardingStopStn < outRows[j].BoardingStopStn
	})

	return outRows
}
