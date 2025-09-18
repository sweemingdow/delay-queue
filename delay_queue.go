package delay_queue

import (
	"errors"
	"runtime"
	"sync"
	"time"
)

var (
	QueWasClosedErr = errors.New("queue was closed")
	TakeTimeoutErr  = errors.New("take timeout")
)

type Delay interface {
	GetExpire() time.Time
}

type DelayItem interface {
	Delay
}

// Item comparison function to determine whether two items are "suitable"
type ItemEqFunc[T any] func(o1, o2 T) bool

// A local delay queue implemented based on PriorityQueue
// thread safe
type DelayQueue[T DelayItem] struct {
	q         *PriorityQueue[T]
	mu        sync.Mutex
	available *sync.Cond
	tp        *timerPool
	wakeup    chan struct{}
	ief       ItemEqFunc[T]
	closed    bool
}

func NewDelayQueue[T DelayItem](initCap int, ief ItemEqFunc[T]) *DelayQueue[T] {
	dq := &DelayQueue[T]{
		// min heap
		q: NewPriorityQueue[T](initCap, func(o1, o2 T) bool {
			return o1.GetExpire().Before(o2.GetExpire())
		}),
		tp:     newTimerPool(),
		wakeup: make(chan struct{}, 1),
		ief:    ief,
	}

	dq.available = sync.NewCond(&dq.mu)

	return dq
}

func (dq *DelayQueue[T]) Offer(item T) error {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	if dq.closed {
		return QueWasClosedErr
	}

	dq.q.Enqueue(item)

	if head, ok := dq.q.Peek(); ok && dq.ief(head, item) {
		dq.available.Signal()
		select {
		case dq.wakeup <- struct{}{}:
		default:

		}
	}

	return nil
}

func (dq *DelayQueue[T]) BlockTake() (T, error) {
	dq.mu.Lock()

	for !dq.closed {
		head, ok := dq.q.Peek()
		if !ok {
			// queue is empty, block
			dq.available.Wait()
			continue
		}

		now := time.Now()
		// There is an element and the execution time has expired
		if head.GetExpire().Before(now) || now.Equal(head.GetExpire()) {
			item, _ := dq.q.Dequeue()
			dq.mu.Unlock()
			return item, nil
		}

		waitDur := head.GetExpire().Sub(now)
		if waitDur <= 0 {
			item, _ := dq.q.Dequeue()
			dq.mu.Unlock()
			return item, nil
		}

		// Subtle level
		if waitDur < 500*time.Microsecond {
			dq.mu.Unlock()

			end := now.Add(waitDur)
			for time.Now().Before(end) {
				runtime.Gosched()
			}
		} else {
			dq.mu.Unlock()

			dq.waiting(waitDur)
		}

		// Try to acquire the lock again and cycle again
		dq.mu.Lock()
	}

	var zero T
	return zero, QueWasClosedErr
}

func (dq *DelayQueue[T]) TimeoutTake(timeout time.Duration) (T, error) {
	if timeout <= 0 {
		return dq.BlockTake()
	}

	endTime := time.Now().Add(timeout)
	dq.mu.Lock()

	for !dq.closed {
		head, ok := dq.q.Peek()
		if !ok {
			remaining := endTime.Sub(time.Now())
			if remaining <= 0 {
				dq.mu.Unlock()

				var zero T
				return zero, TakeTimeoutErr
			}

			dq.mu.Unlock()

			dq.waiting(remaining)
		} else {
			now := time.Now()
			// There is an element and the execution time has expired
			if head.GetExpire().Before(now) || now.Equal(head.GetExpire()) {
				item, _ := dq.q.Dequeue()
				dq.mu.Unlock()
				return item, nil
			}

			remaining := endTime.Sub(now)
			if remaining <= 0 {
				dq.mu.Unlock()

				var zero T
				return zero, TakeTimeoutErr
			}

			waitDur := head.GetExpire().Sub(now)

			if waitDur <= 0 {
				item, _ := dq.q.Dequeue()
				dq.mu.Unlock()
				return item, nil
			}

			if waitDur > remaining {
				waitDur = remaining
			}

			if waitDur < 600*time.Microsecond {
				dq.mu.Unlock()

				end := now.Add(waitDur)
				for time.Now().Before(end) {
					runtime.Gosched()
				}
			} else {
				dq.mu.Unlock()

				dq.waiting(waitDur)
			}
		}

		dq.mu.Lock()
	}

	var zero T
	return zero, QueWasClosedErr
}

func (dq *DelayQueue[T]) Clear() error {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	if dq.closed {
		return QueWasClosedErr
	}

	dq.q.Clear()

	return nil
}

func (dq *DelayQueue[T]) Close() ([]T, error) {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	if dq.closed {
		return nil, QueWasClosedErr
	}

	dq.closed = true
	dq.available.Broadcast()

	select {
	case dq.wakeup <- struct{}{}:
	default:
	}

	if dq.q.Len() <= 0 {
		return make([]T, 0), nil
	}

	waitingItems := make([]T, 0, dq.q.Len())
	for dq.q.Len() > 0 {
		if item, ok := dq.q.Dequeue(); ok {
			waitingItems = append(waitingItems, item)
		}
	}

	dq.q.Clear()

	return waitingItems, nil
}

func (dq *DelayQueue[T]) waiting(dur time.Duration) {
	timer := dq.tp.get(dur)
	defer dq.tp.put(timer)

	select {
	case <-timer.C:
	case <-dq.wakeup:
	}
}

type timerPool struct {
	timerPool sync.Pool
}

func (tp *timerPool) get(timeout time.Duration) *time.Timer {
	t := tp.timerPool.Get().(*time.Timer)
	t.Reset(timeout)
	return t
}

func (tp *timerPool) put(t *time.Timer) {
	t.Stop()
	tp.timerPool.Put(t)
}

func newTimerPool() *timerPool {
	return &timerPool{
		timerPool: sync.Pool{
			New: func() interface{} {
				t := time.NewTimer(time.Hour)
				t.Stop()
				return t
			},
		},
	}
}
