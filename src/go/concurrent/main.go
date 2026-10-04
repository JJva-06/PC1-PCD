package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
)

// InputRecord representa una fila del Parquet de Silver (solo los campos necesarios)
type InputRecord struct {
	CardType            string
	BusServiceNumber    string
	BoardingStopStn     string
	RideStartDatetimeUs int64
}

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

// OutputRow es la estructura para escribir el Parquet de salida (Gold)
type OutputRow struct {
	BoardingStopStn  string `parquet:"Boarding_stop_stn"`
	TimeBin15Min     string `parquet:"time_bin_15min"`
	PassengerCount   int64  `parquet:"passenger_count"`
	UniqueServices   int64  `parquet:"unique_services"`
	DominantCardType string `parquet:"dominant_card_type"`
	Hour             int64  `parquet:"hour"`
	DayOfWeek        int64  `parquet:"day_of_week"`
	Date             string `parquet:"date"`
}

// Representa un lote de filas leidas del Parquet para amortizar el costo de enviarlas por channel
type Chunk []InputRecord

var chunkPool sync.Pool

func worker(jobs <-chan Chunk, results chan<- map[string]*AggregationData, wg *sync.WaitGroup) {
	defer wg.Done()

	// Memoria local para este worker. NO se requiere Mutex.
	localAgg := make(map[string]*AggregationData)

	for chunk := range jobs {
		for _, record := range chunk {
			stn := record.BoardingStopStn
			svc := record.BusServiceNumber
			cType := record.CardType

			// Timestamp en microsegundos desde epoch (timestamp[us] en Parquet)
			tsUs := record.RideStartDatetimeUs
			dt := time.Unix(tsUs/1_000_000, (tsUs%1_000_000)*1000).UTC()

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
		
		// Reciclar el chunk usando sync.Pool (GAP-05 resuelto)
		chunk = chunk[:0]
		chunkPool.Put(&chunk)
	}
	results <- localAgg
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

func main() {
	numWorkers := flag.Int("workers", runtime.NumCPU(), "Número de workers")
	chunkSize := flag.Int("chunksize", 5000, "Tamaño del chunk de registros")
	inputFile := flag.String("input", "../../../data/silver/bus_data_oct2017_clean.parquet", "Input file")
	outputFile := flag.String("output", "../../../data/gold/dataset_go_conc.parquet", "Output file")
	flag.Parse()

	start := time.Now()

	chunkPool.New = func() interface{} {
		c := make(Chunk, 0, *chunkSize)
		return &c
	}

	// Abrir archivo Parquet de entrada
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

	// Descubrir índices de columnas por nombre
	schema := pf.Schema()
	idxCardType := findColumnIndex(schema, "Card_Type")
	idxBusSvc := findColumnIndex(schema, "Bus_Service_Number")
	idxBoardingStn := findColumnIndex(schema, "Boarding_stop_stn")
	idxRideDatetime := findColumnIndex(schema, "ride_start_datetime")

	jobs := make(chan Chunk, *numWorkers*2)
	resultsCh := make(chan map[string]*AggregationData, *numWorkers)

	var wg sync.WaitGroup

	// Iniciar pool de workers
	for w := 1; w <= *numWorkers; w++ {
		wg.Add(1)
		go worker(jobs, resultsCh, &wg)
	}

	// Goroutine Productor: Lee Parquet row groups y encola Chunks
	go func() {
		currentChunkPtr := chunkPool.Get().(*Chunk)

		for _, rg := range pf.RowGroups() {
			rows := make([]parquet.Row, *chunkSize)
			reader := rg.Rows()

			for {
				n, err := reader.ReadRows(rows)
				for i := 0; i < n; i++ {
					row := rows[i]
					rec := InputRecord{
						CardType:            row[idxCardType].String(),
						BusServiceNumber:    row[idxBusSvc].String(),
						BoardingStopStn:     row[idxBoardingStn].String(),
						RideStartDatetimeUs: row[idxRideDatetime].Int64(),
					}
					*currentChunkPtr = append(*currentChunkPtr, rec)
					if len(*currentChunkPtr) == *chunkSize {
						jobs <- *currentChunkPtr
						currentChunkPtr = chunkPool.Get().(*Chunk)
					}
				}
				if err != nil {
					break
				}
			}
			reader.Close()
		}

		if len(*currentChunkPtr) > 0 {
			jobs <- *currentChunkPtr
		}
		close(jobs)
	}()

	// Goroutine Monitor: Cierra el canal de resultados cuando todos los workers terminen
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Hilo principal (Reducer): Unifica los resultados parciales en un mapa global
	globalAgg := make(map[string]*AggregationData)

	for localAgg := range resultsCh {
		for key, lData := range localAgg {
			gData, exists := globalAgg[key]
			if !exists {
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

	sort.Slice(outRows, func(i, j int) bool {
		if outRows[i].BoardingStopStn == outRows[j].BoardingStopStn {
			return outRows[i].TimeBin15Min.Before(outRows[j].TimeBin15Min)
		}
		return outRows[i].BoardingStopStn < outRows[j].BoardingStopStn
	})

	// Escribir Parquet de salida
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
