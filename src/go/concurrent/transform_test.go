package main

import (
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

func TestFindColumnIndex(t *testing.T) {
	type DummySchema struct {
		FieldA string `parquet:"FieldA"`
		FieldB string `parquet:"FieldB"`
	}
	
	schema := parquet.SchemaOf(new(DummySchema))

	idxA := findColumnIndex(schema, "FieldA")
	if idxA == -1 {
		t.Errorf("Expected to find FieldA, got %d", idxA)
	}

	idxB := findColumnIndex(schema, "FieldB")
	if idxB == -1 {
		t.Errorf("Expected to find FieldB, got %d", idxB)
	}
}

func TestWorkerAggregation(t *testing.T) {
	// Preparar datos de prueba
	jobs := make(chan Chunk, 1)
	results := make(chan map[string]*AggregationData, 1)
	var wg sync.WaitGroup

	// Usaremos una pool local para no ensuciar la global en tests
	chunkPool.New = func() interface{} {
		c := make(Chunk, 0, 10)
		return &c
	}

	tsUs := time.Date(2023, 10, 1, 8, 14, 0, 0, time.UTC).UnixMicro()

	rec1 := InputRecord{
		CardType:            "ADULT",
		BusServiceNumber:    "10",
		BoardingStopStn:     "STN_A",
		RideStartDatetimeUs: tsUs,
	}
	rec2 := InputRecord{
		CardType:            "CHILD",
		BusServiceNumber:    "10",
		BoardingStopStn:     "STN_A",
		RideStartDatetimeUs: tsUs + 1000000, // +1 sec, is in same 15-min bin
	}
	
	chunk := Chunk{rec1, rec2}
	jobs <- chunk
	close(jobs)

	wg.Add(1)
	go worker(jobs, results, &wg)
	wg.Wait()
	close(results)

	res := <-results
	
	// Verificar la agregación
	if len(res) != 1 {
		t.Fatalf("Se esperaba 1 bin generado, se obtuvieron %d", len(res))
	}

	var data *AggregationData
	for _, d := range res {
		data = d
	}

	if data.PassengerCount != 2 {
		t.Errorf("Expected passenger count 2, got %d", data.PassengerCount)
	}

	if len(data.Services) != 1 {
		t.Errorf("Expected 1 unique service, got %d", len(data.Services))
	}

	if data.CardTypes["ADULT"] != 1 || data.CardTypes["CHILD"] != 1 {
		t.Errorf("Expected CardTypes ADULT:1 CHILD:1, got %v", data.CardTypes)
	}
}
