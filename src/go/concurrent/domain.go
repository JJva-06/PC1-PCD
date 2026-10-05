package main

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// InputRecord representa una fila del Parquet de Silver (solo los campos necesarios)
type InputRecord struct {
	CardType            string
	BusServiceNumber    string
	BoardingStopStn     string
	RideStartDatetimeUs int64
}

// AggregationData almacena los calculos parciales de un bin
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

// HashString implementa FNV-1a para distribuir claves a los shards
func HashString(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// ResolveInputFiles resuelve si la ruta es un archivo unico, lista por comas, directorio o patron glob,
// buscando candidatos relativos si el directorio de ejecucion varia.
func ResolveInputFiles(input string) ([]string, error) {
	candidates := []string{
		input,
		"../../" + strings.TrimPrefix(input, "../../../"),
		strings.TrimPrefix(input, "../../../"),
	}

	for _, cand := range candidates {
		files, err := resolveCandidate(cand)
		if err == nil && len(files) > 0 {
			return files, nil
		}
	}

	return resolveCandidate(input)
}

func resolveCandidate(input string) ([]string, error) {
	if strings.Contains(input, ",") {
		parts := strings.Split(input, ",")
		var files []string
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				files = append(files, trimmed)
			}
		}
		return files, nil
	}

	fi, err := os.Stat(input)
	if err == nil && fi.IsDir() {
		matches, err := filepath.Glob(filepath.Join(input, "bus_data_*_clean.parquet"))
		if err != nil || len(matches) == 0 {
			matches, _ = filepath.Glob(filepath.Join(input, "*.parquet"))
		}
		sort.Strings(matches)
		return matches, nil
	}

	if strings.ContainsAny(input, "*?[") {
		matches, err := filepath.Glob(input)
		if err != nil {
			return nil, err
		}
		sort.Strings(matches)
		return matches, nil
	}

	if _, err := os.Stat(input); err == nil {
		return []string{input}, nil
	}

	return nil, os.ErrNotExist
}

// ResolveOutputPath ajusta la ruta de salida segun el directorio de trabajo actual
func ResolveOutputPath(out string) string {
	candidates := []string{
		out,
		"../../" + strings.TrimPrefix(out, "../../../"),
		strings.TrimPrefix(out, "../../../"),
	}
	for _, c := range candidates {
		d := filepath.Dir(c)
		if fi, err := os.Stat(d); err == nil && fi.IsDir() {
			return c
		}
	}
	return out
}

