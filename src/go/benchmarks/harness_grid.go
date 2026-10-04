package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
)

const Runs = 3

type Result struct {
	TimeMs int64
	MemMB  int64
}

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
	return Result{}
}

func computeTrimmedMean(runs []Result) (float64, float64) {
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].TimeMs < runs[j].TimeMs
	})
	var totalT, totalM float64
	for _, r := range runs {
		totalT += float64(r.TimeMs)
		totalM += float64(r.MemMB)
	}
	return totalT / float64(len(runs)), totalM / float64(len(runs))
}

func main() {
	fmt.Println("Compilando binarios (optimizados)...")
	exec.Command("go", "build", "-o", "seq.exe", "../sequential/main.go").Run()
	exec.Command("go", "build", "-o", "conc.exe", "../concurrent/main.go").Run()
	defer os.Remove("seq.exe")
	defer os.Remove("conc.exe")

	var seqRuns []Result
	for i := 0; i < Runs; i++ {
		seqRuns = append(seqRuns, execute(".\\seq.exe"))
	}
	seqTime, seqMem := computeTrimmedMean(seqRuns)
	fmt.Printf("[Secuencial] Tiempo Medio: %.2f ms | Memoria: %.2f MB\n", seqTime, seqMem)

	workers := []int{2, 4, 8}
	reducers := []int{2, 4, 8}

	f, _ := os.Create("results_grid.csv")
	defer f.Close()
	f.WriteString("W,N,TimeMs,MemMB,Speedup\n")

	for _, w := range workers {
		for _, n := range reducers {
			var runs []Result
			for i := 0; i < Runs; i++ {
				runs = append(runs, execute(".\\conc.exe", fmt.Sprintf("-workers=%d", w), fmt.Sprintf("-reducers=%d", n)))
			}
			t, m := computeTrimmedMean(runs)
			speedup := seqTime / t
			fmt.Printf("W=%d N=%d | Tiempo: %.2f ms | Memoria: %.2f MB | Speedup: %.2fx\n", w, n, t, m, speedup)
			f.WriteString(fmt.Sprintf("%d,%d,%.2f,%.2f,%.2f\n", w, n, t, m, speedup))
		}
	}
}
