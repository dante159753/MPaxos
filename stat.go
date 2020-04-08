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

func (s Stat) WriteCDFFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for i := 1; i<=100; i++ {
		pos := float64(i) / 100
		fmt.Fprintln(w, s.Data[int(pos*float64(len(s.Data)))])
	}
	return w.Flush()
}

func (s Stat) String() string {
	return fmt.Sprintf("size = %d\nmean = %f\nmin = %f\nmax = %f\n" +
		"median = %f\n" +
		"p90 = %f\np95 = %f\np99 = %f\np999 = %f\n",
		s.Size, s.Mean, s.Min, s.Max,
		s.Median,
		s.P90, s.P95, s.P99, s.P999)
	//+ s.getCDF()

}

func (s Stat) getCDF() string {
	ret := ""
	for i := 1; i<=100; i++ {
		pos := float64(i) / 100
		ret += fmt.Sprintf("%.1f\n", s.Data[int(pos*float64(len(s.Data)))])
	}
	return ret
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
		Median: ms[int(0.5*float64(size))],
		P90:    ms[int(0.9*float64(size))],
		P95:    ms[int(0.95*float64(size))],
		P99:    ms[int(0.99*float64(size))],
		P999:   ms[int(0.999*float64(size))],
	}
}
