package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

const Runs = 15
const TrimPercentage = 0.1 // 10% media recortada

type Result struct {
	TimeMs int64
	MemMB  int64
}

// execute corre el binario y extrae STATS
func execute(cmdPath string, args ...string) Result {
	cmd := exec.Command(cmdPath, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("Error ejecutando %s %v: %v\nSalida: %s", cmdPath, args, err, out)
	}

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "STATS|") {
			parts := strings.Split(strings.TrimSpace(line), "|")
			t, _ := strconv.ParseInt(parts[1], 10, 64)
			m, _ := strconv.ParseInt(parts[2], 10, 64)
			return Result{TimeMs: t, MemMB: m}
		}
	}
	log.Fatalf("No se encontraron STATS en %s", out)
	return Result{}
}

// computeTrimmedMean descarta extremos (outliers)
func computeTrimmedMean(runs []Result) (float64, float64) {
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].TimeMs < runs[j].TimeMs
	})

	trimCount := int(float64(len(runs)) * TrimPercentage)
	if trimCount == 0 && len(runs) > 2 {
		trimCount = 1
	}

	trimmed := runs[trimCount : len(runs)-trimCount]

	var sumT, sumM float64
	for _, r := range trimmed {
		sumT += float64(r.TimeMs)
		sumM += float64(r.MemMB)
	}

	n := float64(len(trimmed))
	return sumT / n, sumM / n
}

func main() {
	// 1. Compilar binarios
	fmt.Println("Compilando binarios (optimizados)...")
	exec.Command("go", "build", "-o", "seq.exe", "../sequential/main.go").Run()
	exec.Command("go", "build", "-o", "conc.exe", "../concurrent/main.go").Run()
	defer os.Remove("seq.exe")
	defer os.Remove("conc.exe")

	fmt.Printf("CPU Cores: %d\n", runtime.NumCPU())
	fmt.Printf("Realizando %d corridas por configuración...\n", Runs)
	fmt.Println("--------------------------------------------------")

	// 2. Correr Secuencial
	var seqRuns []Result
	for i := 0; i < Runs; i++ {
		seqRuns = append(seqRuns, execute(".\\seq.exe"))
		fmt.Printf(".")
	}
	fmt.Println()
	seqTime, seqMem := computeTrimmedMean(seqRuns)
	fmt.Printf("[Secuencial] Tiempo Medio Recortado: %.2f ms | Memoria: %.2f MB\n", seqTime, seqMem)

	// 3. Correr Concurrente variando Workers
	configs := []int{1, 2, 4, 8, 16, runtime.NumCPU()}
	// Deduplicar configuraciones
	uniqueConfigs := make(map[int]bool)
	var finalConfigs []int
	for _, w := range configs {
		if !uniqueConfigs[w] {
			uniqueConfigs[w] = true
			finalConfigs = append(finalConfigs, w)
		}
	}
	sort.Ints(finalConfigs)

	fmt.Println("\n| Workers | T. Recortado (ms) | Memoria (MB) | Speedup | Eficiencia |")
	fmt.Println("|---------|-------------------|--------------|---------|------------|")
	
	fmt.Printf("| %d (Seq) | %.2f | %.2f | 1.00x | - |\n", 1, seqTime, seqMem)

	for _, w := range finalConfigs {
		var concRuns []Result
		fmt.Printf("Corriendo con %d workers ", w)
		for i := 0; i < Runs; i++ {
			concRuns = append(concRuns, execute(".\\conc.exe", "-workers", strconv.Itoa(w)))
			fmt.Printf(".")
		}
		
		cTime, cMem := computeTrimmedMean(concRuns)
		speedup := seqTime / cTime
		eficiencia := speedup / float64(w)

		fmt.Printf("\r| %d       | %.2f          | %.2f       | %.2fx   | %.2f%%     |\n", w, cTime, cMem, speedup, eficiencia*100)
	}

	fmt.Println("\n--- Benchmark Finalizado ---")
}
