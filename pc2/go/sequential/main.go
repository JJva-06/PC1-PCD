package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"
)

// Indices de las columnas en el CSV de Silver
const (
	IdxCardType          = 1
	IdxBusServiceNumber  = 3
	IdxBoardingStopStn   = 7
	IdxRideStartDatetime = 13
)

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

func main() {
	start := time.Now()

	inputFile := "../../../data/silver/bus_data_oct2017_clean.csv"
	outputFile := "../../../data/gold/dataset_go_seq.csv"

	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("Error abriendo archivo input: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Saltar header
	if _, err := reader.Read(); err != nil {
		log.Fatalf("Error leyendo header: %v", err)
	}

	// El mapa global usa como llave: BoardingStopStn|TimeBin
	globalAgg := make(map[string]*AggregationData)
	rowCount := 0

	// Layout para la fecha (ISO 8601 truncado)
	layout := "2006-01-02T15:04:05.000000"

	// Bucle principal de procesamiento
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Aviso: omitiendo fila defectuosa %d: %v", rowCount, err)
			continue
		}
		rowCount++

		// Extraccion
		stn := record[IdxBoardingStopStn]
		dtStr := record[IdxRideStartDatetime]
		svc := record[IdxBusServiceNumber]
		cType := record[IdxCardType]

		// Parseo de timestamp
		dt, err := time.Parse(layout, dtStr)
		if err != nil {
			// Fallback
			dt, err = time.Parse("2006-01-02T15:04:05.000000", dtStr)
			if err != nil {
				dt, err = time.Parse("2006-01-02 15:04:05", dtStr)
				if err != nil {
					log.Printf("Error parseando fecha '%s': %v", dtStr, err)
					continue
				}
			}
		}

		// Binning 15 min (floor)
		bin := dt.Truncate(15 * time.Minute)

		// Llave de agregacion
		key := fmt.Sprintf("%s|%d", stn, bin.Unix())

		// Agregacion local/global
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

		// Recuperar el bin time parseando Unix, pero aqui simplemente iteramos la llave.
		// Mejor aun, iteramos globalAgg, pero no guardamos el bin Time ahi. Guardamos y lo reconstruimos:
		// wait, el struct puede retenerlo. Lo reconstruiremos del string (Unix timestamp):
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

	// Escribir CSV
	outFile, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Error creando archivo de salida: %v", err)
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	defer writer.Flush()

	// Header
	writer.Write([]string{
		"Boarding_stop_stn", "time_bin_15min", "passenger_count",
		"unique_services", "dominant_card_type", "hour", "day_of_week", "date",
	})

	sort.Slice(results, func(i, j int) bool {
		if results[i].BoardingStopStn == results[j].BoardingStopStn {
			return results[i].TimeBin15Min.Before(results[j].TimeBin15Min)
		}
		return results[i].BoardingStopStn < results[j].BoardingStopStn
	})

	for _, r := range results {
		// Output datetime en UTC con formato similar a polars (ej. 2017-10-01 05:00:00.000000)
		dtOut := r.TimeBin15Min.Format("2006-01-02 15:04:05.000000")
		writer.Write([]string{
			r.BoardingStopStn,
			dtOut,
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
