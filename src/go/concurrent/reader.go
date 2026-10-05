package main

import (
	"io"
	"log"
	"os"

	"github.com/parquet-go/parquet-go"
)

// ReaderRoutine lee los archivos Parquet en secuencia y encola chunks al canal de jobs
func ReaderRoutine(filePaths []string, chunkSize int, jobs chan<- Chunk) {
	currentChunkPtr := chunkPool.Get().(*Chunk)

	for _, path := range filePaths {
		f, err := os.Open(path)
		if err != nil {
			log.Fatalf("Error abriendo archivo input '%s': %v", path, err)
		}

		stat, err := f.Stat()
		if err != nil {
			f.Close()
			log.Fatalf("Error obteniendo stat del archivo '%s': %v", path, err)
		}

		pf, err := parquet.OpenFile(f, stat.Size())
		if err != nil {
			f.Close()
			log.Fatalf("Error abriendo Parquet '%s': %v", path, err)
		}

		cols := ExtractColumns(pf.Schema())

		for _, rg := range pf.RowGroups() {
			rows := make([]parquet.Row, chunkSize)
			reader := rg.Rows()

			for {
				n, err := reader.ReadRows(rows)
				for i := 0; i < n; i++ {
					row := rows[i]
					rec := InputRecord{
						CardType:            row[cols.CardType].String(),
						BusServiceNumber:    row[cols.BusSvc].String(),
						BoardingStopStn:     row[cols.BoardingStn].String(),
						RideStartDatetimeUs: row[cols.RideDatetime].Int64(),
					}
					*currentChunkPtr = append(*currentChunkPtr, rec)
					if len(*currentChunkPtr) == chunkSize {
						jobs <- *currentChunkPtr
						currentChunkPtr = chunkPool.Get().(*Chunk)
					}
				}
				if err != nil {
					if err != io.EOF {
						log.Fatalf("Error fatal al leer el dataset Parquet '%s': %v", path, err)
					}
					break
				}
			}
			reader.Close()
		}
		f.Close()
	}

	if len(*currentChunkPtr) > 0 {
		jobs <- *currentChunkPtr
	}
	close(jobs)
}
