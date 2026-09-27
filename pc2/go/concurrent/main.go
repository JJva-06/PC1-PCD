package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// Indices de las columnas en el CSV de Silver
const (
	IdxCardType         = 1
	IdxBusServiceNumber = 3
	IdxBoardingStopStn  = 7
	IdxRideStartDatetime = 13
)

type AggregationData struct {
	PassengerCount int
	Services       map[string]struct{}
	CardTypes      map[string]int
}

type ResultRow struct {
	BoardingStopStn  string
	TimeBin15Min     time.Time
	PassengerCount   int
	UniqueServices   int
	DominantCardType string
	Hour             int
	DayOfWeek        int
	Date             string
}

// Representa un lote de filas leidas del CSV para amortizar el costo de enviarlas por channel
type Chunk [][]string

func worker(jobs <-chan Chunk, results chan<- map[string]*AggregationData, wg *sync.WaitGroup) {
	defer wg.Done()

	// Memoria local para este worker. NO se requiere Mutex.
	localAgg := make(map[string]*AggregationData)
	layout := "2006-01-02T15:04:05.000000"

	for chunk := range jobs {
		for _, record := range chunk {
			stn := record[IdxBoardingStopStn]
			dtStr := record[IdxRideStartDatetime]
			svc := record[IdxBusServiceNumber]
			cType := record[IdxCardType]

			dt, err := time.Parse(layout, dtStr)
			if err != nil {
				dt, err = time.Parse("2006-01-02T15:04:05", dtStr) // Fallback 1
				if err != nil {
					dt, _ = time.Parse("2006-01-02 15:04:05", dtStr) // Fallback 2
				}
			}

			bin := dt.Truncate(15 * time.Minute)
			key := fmt.Sprintf("%s|%d", stn, bin.Unix())

			agg, exists := localAgg[key]
			if !exists {
				agg = &AggregationData{
					PassengerCount: 0,
					Services:       make(map[string]struct{}),
					CardTypes:      make(map[string]int),
				}
				localAgg[key] = agg
			}

			agg.PassengerCount++
			agg.Services[svc] = struct{}{}
			agg.CardTypes[cType]++
		}
	}
	results <- localAgg
}

func main() {
	numWorkers := flag.Int("workers", runtime.NumCPU(), "Número de workers")
	chunkSize := flag.Int("chunksize", 5000, "Tamaño del chunk de registros")
	flag.Parse()

	start := time.Now()

	inputFile := "data/silver/bus_data_oct2017_clean.csv"
	outputFile := "data/gold/dataset_go_conc.csv"

	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("Error abriendo archivo input: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	if _, err := reader.Read(); err != nil {
		log.Fatalf("Error leyendo header: %v", err)
	}

	jobs := make(chan Chunk, *numWorkers*2)
	results := make(chan map[string]*AggregationData, *numWorkers)

	var wg sync.WaitGroup

	// Iniciar pool de workers
	for w := 1; w <= *numWorkers; w++ {
		wg.Add(1)
		go worker(jobs, results, &wg)
	}

	// Goroutine Productor: Lee CSV y encola Chunks
	go func() {
		var currentChunk [][]string
		for {
			record, err := reader.Read()
			if err == io.EOF {
				if len(currentChunk) > 0 {
					jobs <- currentChunk
				}
				break
			}
			if err != nil {
				continue
			}

			currentChunk = append(currentChunk, record)
			if len(currentChunk) == *chunkSize {
				jobs <- currentChunk
				currentChunk = nil // Reset
			}
		}
		close(jobs) // Equivale a EOF_SIGNAL en el modelo Promela
	}()

	// Goroutine Monitor: Cierra el canal de resultados cuando todos los workers terminen
	go func() {
		wg.Wait()
		close(results)
	}()

	// Hilo principal (Reducer): Unifica los resultados parciales en un mapa global
	globalAgg := make(map[string]*AggregationData)

	for localAgg := range results {
		for key, lData := range localAgg {
			gData, exists := globalAgg[key]
			if !exists {
				// Crear e inicializar para evitar referencias cruzadas
				gData = &AggregationData{
					PassengerCount: 0,
					Services:       make(map[string]struct{}),
					CardTypes:      make(map[string]int),
				}
				globalAgg[key] = gData
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

	fmt.Printf("[Concurrente] Procesamiento finalizado en %v (Bins globales: %d)\n", time.Since(start), len(globalAgg))

	// Procesar a formato de salida para determinismo
	var outRows []ResultRow
	for key, agg := range globalAgg {
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

	// Escribir CSV
	outFile, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Error creando archivo de salida: %v", err)
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	defer writer.Flush()

	writer.Write([]string{
		"Boarding_stop_stn", "time_bin_15min", "passenger_count",
		"unique_services", "dominant_card_type", "hour", "day_of_week", "date",
	})

	sort.Slice(outRows, func(i, j int) bool {
		if outRows[i].BoardingStopStn == outRows[j].BoardingStopStn {
			return outRows[i].TimeBin15Min.Before(outRows[j].TimeBin15Min)
		}
		return outRows[i].BoardingStopStn < outRows[j].BoardingStopStn
	})

	for _, r := range outRows {
		writer.Write([]string{
			r.BoardingStopStn,
			r.TimeBin15Min.Format("2006-01-02 15:04:05.000000"),
			fmt.Sprintf("%d", r.PassengerCount),
			fmt.Sprintf("%d", r.UniqueServices),
			r.DominantCardType,
			fmt.Sprintf("%d", r.Hour),
			fmt.Sprintf("%d", r.DayOfWeek),
			r.Date,
		})
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	elapsedMs := time.Since(start).Milliseconds()
	fmt.Printf("STATS|%d|%d\n", elapsedMs, m.Sys/1024/1024)
}
