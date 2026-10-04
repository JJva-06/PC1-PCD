package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

// InputRecord representa una fila del Parquet de Silver (solo los campos necesarios)
type InputRecord struct {
	CardType          string
	BusServiceNumber  string
	BoardingStopStn   string
	RideStartDatetime string
}

// AggregationData almacena los calculos parciales de un bin
type AggregationData struct {
	PassengerCount int
	Services       map[string]struct{}
	CardTypes      map[string]int
}

// ResultRow representa una fila de salida (Gold)
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
	start := time.Now()

	flag.Parse()
	inputFile := flag.String("input", "../../../data/silver/bus_data_oct2017_clean.parquet", "Ruta al archivo de entrada")
	outputFile := flag.String("output", "../../../data/gold/dataset_go_seq.parquet", "Ruta al archivo de salida")
	flag.Parse()

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

	// El mapa global usa como llave: BoardingStopStn|TimeBin
	globalAgg := make(map[string]*AggregationData)
	rowCount := 0

	// Bucle principal de procesamiento: iterar sobre row groups
	buf := make([]parquet.Row, 4096)
	for _, rg := range pf.RowGroups() {
		reader := rg.Rows()

		for {
			n, err := reader.ReadRows(buf)
			for i := 0; i < n; i++ {
				row := buf[i]
				rowCount++

				// Extraccion
				stn := row[idxBoardingStn].String()
				svc := row[idxBusSvc].String()
				cType := row[idxCardType].String()

				// Timestamp en microsegundos desde epoch (timestamp[us] en Parquet)
				tsUs := row[idxRideDatetime].Int64()
				dt := time.Unix(tsUs/1_000_000, (tsUs%1_000_000)*1000).UTC()

				// Binning 15 min (floor)
				bin := dt.Truncate(15 * time.Minute)

				// Llave de agregacion
				key := fmt.Sprintf("%s|%d", stn, bin.Unix())

				// Agregacion
				agg, exists := globalAgg[key]
				if !exists {
					agg = &AggregationData{
						PassengerCount: 0,
						Services:       make(map[string]struct{}),
						CardTypes:      make(map[string]int),
					}
					globalAgg[key] = agg
				}

				agg.PassengerCount++
				agg.Services[svc] = struct{}{}
				agg.CardTypes[cType]++
			}
			if err != nil {
				if err != io.EOF {
					log.Fatalf("Error fatal al leer el dataset Parquet: %v", err)
				}
				break
			}
		}
		reader.Close()
	}

	fmt.Printf("[Secuencial] Procesados %d registros en %v. Agrupados en %d bins.\n", rowCount, time.Since(start), len(globalAgg))

	// Procesar a formato de salida y ordenar para ser determinista
	var results []ResultRow
	for key, agg := range globalAgg {
		parts := strings.Split(key, "|")
		stn := parts[0]

		// Encontrar dominante
		var domCard string
		var maxC int
		for ct, count := range agg.CardTypes {
			// Empate se rompe lexicamente para determinismo estricto
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
			weekday = 7 // Go Sunday es 0, Python/Polars usó isoweekday? En polars es 1=Lunes, 7=Domingo
		}
		dateStr := binTime.Format("2006-01-02")

		results = append(results, ResultRow{
			BoardingStopStn:  stn,
			TimeBin15Min:     binTime,
			PassengerCount:   agg.PassengerCount,
			UniqueServices:   len(agg.Services),
			DominantCardType: domCard,
			Hour:             hour,
			DayOfWeek:        weekday,
			Date:             dateStr,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].BoardingStopStn == results[j].BoardingStopStn {
			return results[i].TimeBin15Min.Before(results[j].TimeBin15Min)
		}
		return results[i].BoardingStopStn < results[j].BoardingStopStn
	})

	// Escribir Parquet de salida
	var parquetOut []OutputRow
	for _, r := range results {
		dtOut := r.TimeBin15Min.Format("2006-01-02 15:04:05.000000")
		parquetOut = append(parquetOut, OutputRow{
			BoardingStopStn:  r.BoardingStopStn,
			TimeBin15Min:     dtOut,
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
