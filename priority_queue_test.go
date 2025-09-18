package delay_queue

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

type MyItem struct {
	data   string
	expire time.Time
}

func (mi MyItem) String() string {
	return fmt.Sprintf("data:%s, expire:%v", mi.data, mi.expire)
}

func TestPriorityQueue(t *testing.T) {
	pq := NewPriorityQueue[MyItem](10, func(o1, o2 MyItem) bool {
		return o1.expire.Before(o2.expire)
	})

	for i := 0; i < 10; i++ {
		pq.Enqueue(MyItem{
			data:   fmt.Sprintf("id:%d", i),
			expire: time.Now().Add(time.Duration(i+1) * time.Millisecond),
		})
	}

	var m MyItem
	for {
		mi, ok := pq.Dequeue()
		if !ok {
			break
		}

		m = mi

		//fmt.Printf("item:%+v\n", mi)
	}

	fmt.Printf("last item:%+v\n", m)
}

// [1,3] => 1,2,3
func randInt(start, end int) int {
	if start > end {
		start, end = end, start
	}

	return rand.Intn(end-start+1) + start
}
