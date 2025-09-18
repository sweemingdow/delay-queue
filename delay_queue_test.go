package delay_queue

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type MyDelayItem struct {
	expire time.Time
	data   string
	id     string
}

func (mdi MyDelayItem) GetExpire() time.Time {
	return mdi.expire
}

func (mdi MyDelayItem) GetId() string {
	return mdi.id
}

func (mdi MyDelayItem) String() string {
	return fmt.Sprintf("id:%s, time:%v, data:%s", mdi.id, mdi.expire, mdi.data)
}

func TestDelayQueue(t *testing.T) {
	dq := NewDelayQueue[MyDelayItem](100, func(o1, o2 MyDelayItem) bool {
		return o1.id == o2.id
	})
	now := time.Now()
	fmt.Printf("now:%s\n", now)
	dq.Offer(MyDelayItem{data: "wdsg", id: "1", expire: now.Add(5 * time.Millisecond)})

	item, _ := dq.BlockTake()

	fmt.Printf("take over, item:%v, diff:%v\n", item, time.Since(item.expire))

}

func TestTimeoutTake(t *testing.T) {
	dq := NewDelayQueue[MyDelayItem](10, func(o1, o2 MyDelayItem) bool {
		return o1.id == o2.id
	})
	now := time.Now()
	fmt.Printf("now:%v\n", now)

	dq.Offer(MyDelayItem{
		id:     "1",
		expire: time.Now().Add(time.Duration(randInt(4, 5)) * time.Second),
		data:   "item:1",
	})

	dq.Offer(MyDelayItem{
		id:     "2",
		expire: time.Now().Add(time.Duration(randInt(5, 6)) * time.Second),
		data:   "item:2",
	})

	item, err := dq.TimeoutTake(3 * time.Second)
	if err != nil {
		fmt.Printf("poll err:%v\n", err)
		if !errors.Is(err, QueWasClosedErr) {
			waitingItems, err := dq.Close()
			if err != nil {
				fmt.Printf("close err:%v\n", err)
				return
			}
			fmt.Printf("que closed, waitingItems:%v\n", waitingItems)
		}
	}

	fmt.Printf("item:%v\n", item)
}

func TestDelayQueueConcurrentWriteAndRead(t *testing.T) {
	var (
		total    = 8000000
		initCap  = 8000000
		writeCnt = 500
		readCnt  = 200
	)

	dq := NewDelayQueue[MyDelayItem](initCap, func(o1, o2 MyDelayItem) bool {
		return o1.id == o2.id
	})

	var (
		start   = time.Now()
		maxTime = start
		wgg     sync.WaitGroup
		mu      sync.Mutex
	)
	wgg.Add(writeCnt)
	for i := 0; i < writeCnt; i++ {
		i := i
		go func() {
			defer wgg.Done()
			cnt := total / writeCnt
			maxInOne := time.Now()
			for j := 0; j < cnt; j++ {
				ex := time.Now().Add(time.Duration(randInt(100, 30*1000)) * time.Millisecond)
				if ex.After(maxInOne) {
					maxInOne = ex
				}

				dq.Offer(MyDelayItem{
					id:     fmt.Sprintf("%d-%d", i, j),
					expire: ex,
					data:   fmt.Sprintf("item:%d-%d", i, j),
				})

			}

			mu.Lock()
			if maxInOne.After(maxTime) {
				maxTime = maxInOne
			}
			mu.Unlock()
		}()
	}

	var (
		counter     atomic.Uint32
		overCnt     atomic.Uint32
		diffCounter atomic.Uint32
		maxDiff     atomic.Uint32
		threshold   = 1000 * 1000 * 1000 // 100ms
		exitCh      = make(chan struct{})
		lessCounter atomic.Uint32
	)

	go func() {
		wgg.Wait()
		time.Sleep(maxTime.Sub(start) + 1*time.Second)

		close(exitCh)
	}()

	for i := 0; i < readCnt; i++ {
		go func() {
			for {
				select {
				case <-exitCh:
					return
				default:
					item, _ := dq.BlockTake()
					n := time.Now()
					diff := float64(n.UnixMicro() - item.expire.UnixMicro())
					if diff < 0 {
						lessCounter.Add(1)
						diff = math.Abs(diff)
					}

					diffCounter.Add(uint32(diff))
					if int(diff) > threshold {
						overCnt.Add(1)
						fmt.Printf("expire:%v, now:%v overCnt delay thredshold:%d, diff:%.2fus, overCnt:%d, total:%d\n",
							item.expire, n, threshold, diff, overCnt.Load(), total)
					}

					casUpdateUi32Max(&maxDiff, uint32(diff))

					counter.Add(1)
				}
			}
		}()
	}

	<-exitCh

	avgDiff := float64(diffCounter.Load()) / float64(counter.Load())

	fmt.Printf("exit now! writeCnt:%d, readCnt:%d, total:%d, initCap:%d, totalTakeCnt:%d, overCnt:%d, avgDiff:%.2fus, maxDiff:%dus, lessCount:%d\n",
		writeCnt, readCnt,
		total, initCap, counter.Load(), overCnt.Load(), avgDiff,
		maxDiff.Load(),
		lessCounter.Load(),
	)

	time.Sleep(200 * time.Microsecond)
}

func casUpdateUi32Max(max *atomic.Uint32, newVal uint32) {
	for {
		curr := max.Load()
		if newVal <= curr {
			return
		}

		if max.CompareAndSwap(curr, newVal) {
			return
		}
	}
}

func BenchmarkDelayQueue_OfferTake(b *testing.B) {
	dq := NewDelayQueue[MyDelayItem](5000, func(o1, o2 MyDelayItem) bool {
		return o1.id == o2.id
	})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dq.Offer(MyDelayItem{
			id:     fmt.Sprintf("%d", i),
			expire: time.Now(),
			data:   fmt.Sprintf("item:%d", i),
		})
		dq.BlockTake()
	}
}
