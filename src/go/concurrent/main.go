package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
)

func main() {
	numWorkers := flag.Int("workers", runtime.NumCPU(), "Número de workers")
	numReducers := flag.Int("reducers", 4, "Número de reducers (shards)")
	chunkSize := flag.Int("chunksize", 5000, "Tamaño del chunk de registros")
	inputFile := flag.String("input", "../../../data/silver/bus_data_oct2017_clean.parquet", "Input file")
	outputFile := flag.String("output", "../../../data/gold/dataset_go_conc.parquet", "Output file")
	flag.Parse()

	start := time.Now()

	chunkPool.New = func() interface{} {
		c := make(Chunk, 0, *chunkSize)
		return &c
	}

	f, err := os.Open(*inputFile)
	if err != nil {
		log.Fatalf("Error abriendo archivo input: %v", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		log.Fatalf("Error obteniendo stat del archivo: %v", err)
	}

	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		log.Fatalf("Error abriendo Parquet: %v", err)
	}

	cols := ExtractColumns(pf.Schema())

	jobs := make(chan Chunk, *numWorkers*2)
	
	reducerChannels := make([]chan map[string]*AggregationData, *numReducers)
	for i := 0; i < *numReducers; i++ {
		reducerChannels[i] = make(chan map[string]*AggregationData, *numWorkers)
	}

	var wg sync.WaitGroup

	for w := 1; w <= *numWorkers; w++ {
		wg.Add(1)
		go worker(jobs, reducerChannels, &wg)
	}

	go ReaderRoutine(pf, *chunkSize, cols, jobs)

	go func() {
		wg.Wait()
		for i := 0; i < *numReducers; i++ {
			close(reducerChannels[i])
		}
	}()

	var reducerWg sync.WaitGroup
	globalAggs := make([]map[string]*AggregationData, *numReducers)

	for i := 0; i < *numReducers; i++ {
		reducerWg.Add(1)
		go func(shardID int, ch <-chan map[string]*AggregationData) {
			defer reducerWg.Done()
			globalAggs[shardID] = ReducerRoutine(ch)
		}(i, reducerChannels[i])
	}

	reducerWg.Wait()
	
	totalBins := 0
	for _, shardAgg := range globalAggs {
		totalBins += len(shardAgg)
	}

	fmt.Printf("[Concurrente] Procesamiento finalizado en %v (Bins globales: %d)\n", time.Since(start), totalBins)

	outRows := FormatResults(globalAggs)

	var parquetOut []OutputRow
	for _, r := range outRows {
		parquetOut = append(parquetOut, OutputRow{
			BoardingStopStn:  r.BoardingStopStn,
			TimeBin15Min:     r.TimeBin15Min.Format("2006-01-02 15:04:05.000000"),
			PassengerCount:   int64(r.PassengerCount),
			UniqueServices:   int64(r.UniqueServices),
			DominantCardType: r.DominantCardType,
			Hour:             int64(r.Hour),
			DayOfWeek:        int64(r.DayOfWeek),
			Date:             r.Date,
		})
	}

	if err := parquet.WriteFile(*outputFile, parquetOut); err != nil {
		log.Fatalf("Error escribiendo archivo Parquet de salida: %v", err)
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	elapsedMs := time.Since(start).Milliseconds()
	fmt.Printf("STATS|%d|%d\n", elapsedMs, m.Sys/1024/1024)
}
