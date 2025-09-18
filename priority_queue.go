package delay_queue

import "container/heap"

type PriorityFunc[T any] func(o1, o2 T) bool

type PriorityQueue[T any] struct {
	items   []T
	pf      PriorityFunc[T]
	initCap int
}

func NewPriorityQueue[T any](initCap int, pf PriorityFunc[T]) *PriorityQueue[T] {
	if initCap < 0 {
		initCap = 0
	}

	pq := &PriorityQueue[T]{
		initCap: initCap,
		items:   make([]T, 0, initCap),
		pf:      pf,
	}

	heap.Init(pq)

	return pq
}

func (pq *PriorityQueue[T]) Len() int {
	return len(pq.items)
}

func (pq *PriorityQueue[T]) Less(i, j int) bool {
	return pq.pf(pq.items[i], pq.items[j])
}

func (pq *PriorityQueue[T]) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

func (pq *PriorityQueue[T]) Push(x interface{}) {
	pq.items = append(pq.items, x.(T))
}

func (pq *PriorityQueue[T]) Pop() interface{} {
	n := len(pq.items)
	x := pq.items[n-1]
	pq.items = pq.items[:n-1]
	return x
}

func (pq *PriorityQueue[T]) Enqueue(item T) {
	heap.Push(pq, item)
}

func (pq *PriorityQueue[T]) EnqueueItems(items []T) {
	for _, item := range items {
		heap.Push(pq, item)
	}
}

func (pq *PriorityQueue[T]) Dequeue() (T, bool) {
	if pq.Empty() {
		var zero T
		return zero, false
	}

	item := heap.Pop(pq).(T)
	return item, true
}

func (pq *PriorityQueue[T]) Peek() (T, bool) {
	if pq.Empty() {
		var zero T
		return zero, false
	}

	return pq.items[0], true
}

func (pq *PriorityQueue[T]) Empty() bool {
	return pq.Len() == 0
}

func (pq *PriorityQueue[T]) Size() int {
	return pq.Len()
}

func (pq *PriorityQueue[T]) Clear() {
	pq.items = make([]T, 0, pq.initCap)
}
