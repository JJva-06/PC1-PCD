package main

import (
	"io"
	"log"

	"github.com/parquet-go/parquet-go"
)

// ReaderRoutine lee el archivo Parquet y encola chunks al canal de jobs
func ReaderRoutine(pf *parquet.File, chunkSize int, cols ColumnIndices, jobs chan<- Chunk) {
	currentChunkPtr := chunkPool.Get().(*Chunk)

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
					log.Fatalf("Error fatal al leer el dataset Parquet: %v", err)
				}
				break
			}
		}
		reader.Close()
	}

	if len(*currentChunkPtr) > 0 {
		jobs <- *currentChunkPtr
	}
	close(jobs)
}
