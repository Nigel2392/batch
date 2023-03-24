package batch_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Nigel2392/batch"
	"github.com/Nigel2392/batch/funcs"
)

var (
	batchSizes = [][]int{
		{1, 1},
		{1, 10},
		{10, 1},
		{10, 10},
		{10, 100},
		{100, 10},
		{100, 100},
		// {100, 1000},
		// {1000, 100},
		// {1000, 1000},
	}

	funcsAmount = []int{
		10, 100, 1000, 10000, 100000, 1000000,
		// 10000000, 100000000, //, 1000000000, 10000000000,
	}
)

func defaultFunc(i int) int {
	return i
}

var integer = 100
var DefaultIntFunc = funcs.New(defaultFunc, integer)

//
//	func sum(i ...int) int {
//		var sum int
//		for _, v := range i {
//			sum += v
//		}
//		return sum
//	}

func TestBatch(t *testing.T) {

	for _, batchSize := range batchSizes {
		for _, amountOfFuncs := range funcsAmount {
			t.Logf("Testing btch with %d workers, %d buffer-size and %d funcs", batchSize[0], batchSize[1], amountOfFuncs)
			var (
				workers = batchSize[0]
				buffer  = batchSize[1]
			)
			var funcsSlice = make([]funcs.Func[int, int], 0, amountOfFuncs)
			for i := 0; i < amountOfFuncs; i++ {
				funcsSlice = append(funcsSlice, DefaultIntFunc)
			}

			var start = time.Now()
			var b = batch.New[int, int](amountOfFuncs)

			b.AddFuncs(funcsSlice...)

			var result, _ = b.Run(batch.Size(workers), batch.Size(buffer))

			var sum int

			for i := 0; i < amountOfFuncs; i++ {
				sum += <-result.Read()
			}

			if sum != amountOfFuncs*integer {
				t.Errorf("sum is not correct: %d, expected: %d", sum, amountOfFuncs*integer)
			}

			t.Logf("Finished in %v with sum: %d", time.Since(start), sum)
		}
	}
}

func TestDone(t *testing.T) {
	var b = batch.New[int, int](10)
	for i := 0; i < 10; i++ {
		b.Add(defaultFunc, 1)
	}

	var result, cc = b.Run(1, 5)
	for i := 0; i < 10; i++ {
		if i == 9 {
			select {
			case <-cc.Done():
				t.Log("batch is done!")
				break
			default:
				t.Error("batch is not done")
			}
		}
		fmt.Println("Waiting for result")
		var r = <-result.Read()
		if r != 1 {
			t.Errorf("result is not correct: %d, expected: %d", r, 1)
		}
	}
	select {
	case <-cc.Done():
		t.Log("batch is done!")
	default:
		t.Error("batch is not done")
	}
}

func TestCancelFuture(t *testing.T) {
	var b = batch.New[uint32, uint32](10)
	for i := 0; i < 10; i++ {
		b.Add(ret1, 1)
	}
	var in1Seconds = time.Now().Add(time.Second)
	var result, cc = b.RunAt(1, 5, in1Seconds)
	time.Sleep(time.Second / 2)
	cc.Cancel()
	select {
	case <-cc.Done():
		t.Log("batch is done!")
	default:
		t.Error("batch is not done")
	}
	select {
	case <-result.Read():
		t.Error("result is not done")
	default:
		t.Log("result is done")
	}
	result, cc = b.RunAt(1, 5, in1Seconds)
	var r = sumUint32(result.Wait()...)
	if r != 10 {
		t.Errorf("result is not correct: %d, expected: %d", r, 10)
	}
	select {
	case <-cc.Done():
		t.Log("batch is done!")
	default:
		t.Error("batch is not done")
	}
}

type benchmark struct {
	workers int
	buffer  int
	funcs   int
}

type benchmarksTyp []benchmark

func (b benchmarksTyp) String() string {
	var bld strings.Builder

	bld.WriteString("Benchmarking:\n")

	for _, v := range b {
		bld.WriteString(fmt.Sprintf("%d-%d-%d\n", v.workers, v.buffer, v.funcs))
	}
	return bld.String()
}

var benchmarks = func() benchmarksTyp {
	// -> benchmarkAmount^2 * len(defaults) = 300
	var benchmarkAmount int = 5
	var defaults = make([]benchmark, 0)
	defaults = append(defaults, benchmark{10, 10, 150})
	defaults = append(defaults, benchmark{10, 1, 150})
	defaults = append(defaults, benchmark{1, 10, 150})
	var newDefaults = make([]benchmark, 0)
	for _, v := range defaults {
		for i := 1; i < benchmarkAmount+1; i++ {
			for j := 1; j < benchmarkAmount+1; j++ {
				var bench = benchmark{v.workers * i, v.buffer * j, v.funcs}
				if containsBuffer(newDefaults, bench) && containsWorkers(newDefaults, bench) {
					continue
				}
				newDefaults = append(newDefaults, bench)
			}
		}
	}
	return newDefaults
}()

func containsWorkers(s []benchmark, e benchmark) bool {
	return containsFunc(s, e, func(a, b benchmark) bool { return a.workers == b.workers })
}

func containsBuffer(s []benchmark, e benchmark) bool {
	return containsFunc(s, e, func(a, b benchmark) bool { return a.buffer == b.buffer })
}

func containsFunc[T comparable](s []T, e T, f func(T, T) bool) bool {
	for _, a := range s {
		if f(a, e) {
			return true
		}
	}
	return false
}

func ret1(i uint32) uint32 {
	return 1
}

func sumUint32(i ...uint32) uint32 {
	var sum uint32
	for _, v := range i {
		sum += v
	}
	return sum
}

func BenchmarkBatch(b *testing.B) {

	b.Log(benchmarks.String())

	for _, v := range benchmarks {
		b.Run(fmt.Sprintf("WRKRS:%d-BUF:%d-FUNCS:%d", v.workers, v.buffer, v.funcs), func(b *testing.B) {
			benchmarkBatch(b, v.workers, v.buffer, v.funcs)
		})
	}
}

var defaultFunction funcs.Func[uint16, uint32] = &funcs.Function[uint32, uint32]{
	Callable: ret1,
	Arg:      1,
}

func benchmarkBatch(b *testing.B, workers, buffer, funcCount int) {
	b.StopTimer()
	var funcsSlice = make([]funcs.Func[uint32, uint32], 0, funcCount)
	for i := 0; i < funcCount; i++ {
		funcsSlice = append(funcsSlice, defaultFunction)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		var btch = batch.New[uint32, uint32](funcCount)
		btch.AddFuncs(funcsSlice...)
		var result, _ = btch.Run(batch.Size(workers), batch.Size(buffer))

		var sum uint32
		for i := 0; i < funcCount; i++ {
			sum += <-result.Read()
		}

		if sum != uint32(funcCount) {
			b.Errorf("sum is not correct: %d, expected: %d", sum, funcsAmount)
			return
		}
	}
}
