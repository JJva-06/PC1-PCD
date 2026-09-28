package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parquet-go/parquet-go"
)

// InputRecord representa una fila del Parquet de Silver.
type InputRecord struct {
	CardType            string
	BusServiceNumber    string
	BoardingStopStn     string
	RideStartDatetimeUs int64
}

// AggregationData almacena la información agregada por estación y bloque de tiempo.
type AggregationData struct {
	PassengerCount int
	Services       map[string]struct{}
	CardTypes      map[string]int
}

// ResultRow representa una fila procesada antes de escribir el resultado.
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

// OutputRow representa una fila del Parquet Gold.
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

// Chunk representa un lote de registros enviado a los workers.
type Chunk []InputRecord

// worker procesa chunks de manera independiente.
// Cada worker utiliza un mapa local, evitando escrituras concurrentes
// sobre una misma estructura compartida.
func worker(
	jobs <-chan Chunk,
	results chan<- map[string]*AggregationData,
	wg *sync.WaitGroup,
	processedRecords *int64,
) {
	defer wg.Done()

	localAgg := make(map[string]*AggregationData)

	for chunk := range jobs {
		for _, record := range chunk {
			stn := record.BoardingStopStn
			svc := record.BusServiceNumber
			cType := record.CardType

			// Timestamp en microsegundos desde epoch.
			tsUs := record.RideStartDatetimeUs
			dt := time.Unix(
				tsUs/1_000_000,
				(tsUs%1_000_000)*1000,
			).UTC()

			// Agrupar en intervalos de 15 minutos.
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

			// Contador seguro para múltiples goroutines.
			atomic.AddInt64(processedRecords, 1)
		}
	}

	results <- localAgg
}

// findColumnIndex busca el índice de una columna por nombre
// dentro del schema del archivo Parquet.
func findColumnIndex(schema *parquet.Schema, name string) int {
	for i, field := range schema.Fields() {
		if field.Name() == name {
			return i
		}
	}

	log.Fatalf(
		"Columna '%s' no encontrada en el schema del Parquet",
		name,
	)

	return -1
}

func main() {

	// =========================================================
	// CONFIGURACIÓN
	// =========================================================

	numWorkers := flag.Int(
		"workers",
		runtime.NumCPU(),
		"Número de workers concurrentes",
	)

	chunkSize := flag.Int(
		"chunksize",
		5000,
		"Tamaño de cada chunk de registros",
	)

	inputFile := flag.String(
		"input",
		"../data/silver/bus_data_oct2017_clean.parquet",
		"Ruta del archivo Parquet de entrada",
	)

	outputFile := flag.String(
		"output",
		"../data/gold/dataset_go_conc.parquet",
		"Ruta del archivo Parquet de salida",
	)

	flag.Parse()

	// =========================================================
	// VALIDACIÓN DE PARÁMETROS
	// =========================================================

	if *numWorkers <= 0 {
		log.Fatalf(
			"El número de workers debe ser mayor que 0",
		)
	}

	if *chunkSize <= 0 {
		log.Fatalf(
			"El tamaño del chunk debe ser mayor que 0",
		)
	}

	if strings.TrimSpace(*inputFile) == "" {
		log.Fatalf(
			"La ruta del archivo de entrada no puede estar vacía",
		)
	}

	if strings.TrimSpace(*outputFile) == "" {
		log.Fatalf(
			"La ruta del archivo de salida no puede estar vacía",
		)
	}

	// Crear automáticamente el directorio de salida.
	outputDir := filepath.Dir(*outputFile)

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf(
			"Error creando directorio de salida: %v",
			err,
		)
	}

	// =========================================================
	// INFORMACIÓN DE EJECUCIÓN
	// =========================================================

	fmt.Println("========================================")
	fmt.Println("     PROCESAMIENTO CONCURRENTE")
	fmt.Println("========================================")
	fmt.Printf("Workers       : %d\n", *numWorkers)
	fmt.Printf("Chunk size    : %d\n", *chunkSize)
	fmt.Printf("Archivo input : %s\n", *inputFile)
	fmt.Printf("Archivo output: %s\n", *outputFile)
	fmt.Println("========================================")

	start := time.Now()

	// =========================================================
	// APERTURA DEL PARQUET
	// =========================================================

	f, err := os.Open(*inputFile)

	if err != nil {
		log.Fatalf(
			"Error abriendo archivo input: %v",
			err,
		)
	}

	defer f.Close()

	stat, err := f.Stat()

	if err != nil {
		log.Fatalf(
			"Error obteniendo stat del archivo: %v",
			err,
		)
	}

	pf, err := parquet.OpenFile(
		f,
		stat.Size(),
	)

	if err != nil {
		log.Fatalf(
			"Error abriendo Parquet: %v",
			err,
		)
	}

	// =========================================================
	// COLUMNAS
	// =========================================================

	schema := pf.Schema()

	idxCardType := findColumnIndex(
		schema,
		"Card_Type",
	)

	idxBusSvc := findColumnIndex(
		schema,
		"Bus_Service_Number",
	)

	idxBoardingStn := findColumnIndex(
		schema,
		"Boarding_stop_stn",
	)

	idxRideDatetime := findColumnIndex(
		schema,
		"ride_start_datetime",
	)

	// =========================================================
	// WORKER POOL
	// =========================================================

	jobs := make(
		chan Chunk,
		*numWorkers*2,
	)

	resultsCh := make(
		chan map[string]*AggregationData,
		*numWorkers,
	)

	var wg sync.WaitGroup

	var processedRecords int64

	for w := 1; w <= *numWorkers; w++ {
		wg.Add(1)

		go worker(
			jobs,
			resultsCh,
			&wg,
			&processedRecords,
		)
	}

	// =========================================================
	// PRODUCTOR
	// =========================================================

	go func() {

		var currentChunk []InputRecord

		for _, rg := range pf.RowGroups() {

			rows := make(
				[]parquet.Row,
				*chunkSize,
			)

			reader := rg.Rows()

			for {

				n, readErr := reader.ReadRows(rows)

				for i := 0; i < n; i++ {

					row := rows[i]

					rec := InputRecord{
						CardType: row[
							idxCardType
						].String(),

						BusServiceNumber: row[
							idxBusSvc
						].String(),

						BoardingStopStn: row[
							idxBoardingStn
						].String(),

						RideStartDatetimeUs: row[
							idxRideDatetime
						].Int64(),
					}

					currentChunk = append(
						currentChunk,
						rec,
					)

					if len(currentChunk) == *chunkSize {

						jobs <- currentChunk

						currentChunk = nil
					}
				}

				if readErr != nil {
					break
				}
			}

			reader.Close()
		}

		// Enviar último chunk incompleto.
		if len(currentChunk) > 0 {
			jobs <- currentChunk
		}

		close(jobs)
	}()

	// =========================================================
	// MONITOR DE WORKERS
	// =========================================================

	go func() {

		wg.Wait()

		close(resultsCh)

	}()

	// =========================================================
	// REDUCER
	// =========================================================

	globalAgg := make(
		map[string]*AggregationData,
	)

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

			gData.PassengerCount +=
				lData.PassengerCount

			for svc := range lData.Services {

				gData.Services[svc] =
					struct{}{}
			}

			for ct, count := range lData.CardTypes {

				gData.CardTypes[ct] +=
					count
			}
		}
	}

	// =========================================================
	// CONSTRUIR RESULTADO
	// =========================================================

	var outRows []ResultRow

	for key, agg := range globalAgg {

		parts := strings.Split(
			key,
			"|",
		)

		stn := parts[0]

		var domCard string
		var maxC int

		for ct, count := range agg.CardTypes {

			if count > maxC ||
				(count == maxC && ct > domCard) {

				maxC = count
				domCard = ct
			}
		}

		var ts int64

		fmt.Sscanf(
			parts[1],
			"%d",
			&ts,
		)

		binTime := time.Unix(
			ts,
			0,
		).UTC()

		hour := binTime.Hour()

		weekday := int(
			binTime.Weekday(),
		)

		if weekday == 0 {
			weekday = 7
		}

		outRows = append(
			outRows,
			ResultRow{
				BoardingStopStn:  stn,
				TimeBin15Min:     binTime,
				PassengerCount:   agg.PassengerCount,
				UniqueServices:   len(agg.Services),
				DominantCardType: domCard,
				Hour:             hour,
				DayOfWeek:        weekday,
				Date: binTime.Format(
					"2006-01-02",
				),
			},
		)
	}

	// =========================================================
	// ORDENAMIENTO DETERMINISTA
	// =========================================================

	sort.Slice(
		outRows,
		func(i, j int) bool {

			if outRows[i].BoardingStopStn ==
				outRows[j].BoardingStopStn {

				return outRows[i].
					TimeBin15Min.
					Before(
						outRows[j].
							TimeBin15Min,
					)
			}

			return outRows[i].
				BoardingStopStn <
				outRows[j].
					BoardingStopStn
		},
	)

	// =========================================================
	// PREPARAR PARQUET GOLD
	// =========================================================

	var parquetOut []OutputRow

	for _, r := range outRows {

		parquetOut = append(
			parquetOut,
			OutputRow{
				BoardingStopStn:
					r.BoardingStopStn,

				TimeBin15Min:
					r.TimeBin15Min.Format(
						"2006-01-02 15:04:05.000000",
					),

				PassengerCount:
					int64(r.PassengerCount),

				UniqueServices:
					int64(r.UniqueServices),

				DominantCardType:
					r.DominantCardType,

				Hour:
					int64(r.Hour),

				DayOfWeek:
					int64(r.DayOfWeek),

				Date:
					r.Date,
			},
		)
	}

	// =========================================================
	// ESCRITURA
	// =========================================================

	if err := parquet.WriteFile(
		*outputFile,
		parquetOut,
	); err != nil {

		log.Fatalf(
			"Error escribiendo archivo Parquet de salida: %v",
			err,
		)
	}

	// =========================================================
	// MÉTRICAS
	// =========================================================

	elapsed := time.Since(start)

	var m runtime.MemStats

	runtime.ReadMemStats(&m)

	memoryMB := m.Sys / 1024 / 1024

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("       RESUMEN DE EJECUCIÓN")
	fmt.Println("========================================")

	fmt.Printf(
		"Registros procesados : %d\n",
		atomic.LoadInt64(&processedRecords),
	)

	fmt.Printf(
		"Bins generados       : %d\n",
		len(globalAgg),
	)

	fmt.Printf(
		"Workers utilizados   : %d\n",
		*numWorkers,
	)

	fmt.Printf(
		"Chunk size           : %d\n",
		*chunkSize,
	)

	fmt.Printf(
		"Tiempo total         : %v\n",
		elapsed,
	)

	fmt.Printf(
		"Memoria Sys          : %d MB\n",
		memoryMB,
	)

	fmt.Println("========================================")

	// Mantener esta salida para que el benchmark pueda leer
	// automáticamente tiempo y memoria.
	fmt.Printf(
		"STATS|%d|%d\n",
		elapsed.Milliseconds(),
		memoryMB,
	)
}