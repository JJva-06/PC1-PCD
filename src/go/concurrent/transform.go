package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
)

// ColumnIndices contiene los indices extraidos
type ColumnIndices struct {
	CardType         int
	BusSvc           int
	BoardingStn      int
	RideDatetime     int
}

// ExtractColumns localiza todas las columnas necesarias en el schema
func ExtractColumns(schema *parquet.Schema) ColumnIndices {
	return ColumnIndices{
		CardType:     findColumnIndex(schema, "Card_Type"),
		BusSvc:       findColumnIndex(schema, "Bus_Service_Number"),
		BoardingStn:  findColumnIndex(schema, "Boarding_stop_stn"),
		RideDatetime: findColumnIndex(schema, "ride_start_datetime"),
	}
}

// findColumnIndex busca el índice de una columna por nombre en el schema del Parquet
func findColumnIndex(schema *parquet.Schema, name string) int {
	for i, field := range schema.Fields() {
		if field.Name() == name {
			return i
		}
	}
	log.Fatalf("Columna '%s' no encontrada en el schema del Parquet", name)
	return -1
}

// worker transforma chunks de InputRecord y aplica agregacion local sharded
func worker(jobs <-chan Chunk, reducerChannels []chan map[string]*AggregationData, wg *sync.WaitGroup) {
	defer wg.Done()
	numReducers := uint32(len(reducerChannels))

	// Arreglo de N mapas locales por worker (Acumulación de vida completa)
	localAggs := make([]map[string]*AggregationData, numReducers)
	for i := uint32(0); i < numReducers; i++ {
		localAggs[i] = make(map[string]*AggregationData)
	}

	for chunk := range jobs {
		for _, record := range chunk {
			stn := record.BoardingStopStn
			svc := record.BusServiceNumber
			cType := record.CardType

			tsUs := record.RideStartDatetimeUs
			dt := time.Unix(tsUs/1_000_000, (tsUs%1_000_000)*1000).UTC()

			bin := dt.Truncate(15 * time.Minute)
			key := fmt.Sprintf("%s|%d", stn, bin.Unix())

			h := HashString(key) % numReducers
			shardMap := localAggs[h]

			agg, exists := shardMap[key]
			if !exists {
				agg = &AggregationData{
					PassengerCount: 0,
					Services:       make(map[string]struct{}),
					CardTypes:      make(map[string]int),
				}
				shardMap[key] = agg
			}

			agg.PassengerCount++
			agg.Services[svc] = struct{}{}
			agg.CardTypes[cType]++
		}
		
		chunk = chunk[:0]
		chunkPool.Put(&chunk)
	}
	
	// Enviar cada mapa a su respectivo reducer solo al morir el worker
	for i, m := range localAggs {
		reducerChannels[i] <- m
	}
}
