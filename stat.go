package paxi

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"time"
)

// Stat stores the statistics data for benchmarking results
type Stat struct {
	Data   []float64
	Size   int
	Mean   float64
	Min    float64
	Max    float64
	P10		float64
	P20		float64
	P30		float64
	P40		float64
	Median float64
	P60		float64
	P70		float64
	P80		float64
	P90		float64
	P95    float64
	P99    float64
	P999   float64
}

// WriteFile writes stat to new file in path
func (s Stat) WriteFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range s.Data {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

func (s Stat) String() string {
	return fmt.Sprintf("size = %d\nmean = %f\nmin = %f\nmax = %f\n" + "" +
		"p10 = %f\np20 = %f\np30 = %f\np40 = %f\nmedian = %f\n" +
		"p60 = %f\np70 = %f\np80 = %f\np90 = %f\np95 = %f\np99 = %f\np999 = %f\n",
		s.Size, s.Mean, s.Min, s.Max,
		s.P10, s.P20, s.P30, s.P40, s.Median,
		s.P60, s.P70, s.P80, s.P90, s.P95, s.P99, s.P999)
}

// Statistic function creates Stat object from raw latency data
func Statistic(latency []time.Duration) Stat {
	ms := make([]float64, 0)
	for _, l := range latency {
		ms = append(ms, float64(l.Nanoseconds())/1000000.0)
	}
	sort.Float64s(ms)
	sum := 0.0
	for _, m := range ms {
		sum += m
	}
	size := len(ms)
	return Stat{
		Data:   ms,
		Size:   size,
		Mean:   sum / float64(size),
		Min:    ms[0],
		Max:    ms[size-1],
		P10:    ms[int(0.1*float64(size))],
		P20:    ms[int(0.2*float64(size))],
		P30:    ms[int(0.3*float64(size))],
		P40:    ms[int(0.4*float64(size))],
		Median: ms[int(0.5*float64(size))],
		P60:    ms[int(0.6*float64(size))],
		P70:    ms[int(0.7*float64(size))],
		P80:    ms[int(0.8*float64(size))],
		P90:    ms[int(0.9*float64(size))],
		P95:    ms[int(0.95*float64(size))],
		P99:    ms[int(0.99*float64(size))],
		P999:   ms[int(0.999*float64(size))],
	}
}
