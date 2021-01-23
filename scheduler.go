// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"container/list"
	"context"
	"sync"
	"time"
)

func GoEmpty(work func()) {
	go work()
}

func Go(ctx context.Context, work func(ctx context.Context)) {
	go work(ctx)
}

func DoTimerSchedule(ctx context.Context, work func(), delay time.Duration, supplier func() time.Duration) {
	go func(ctx context.Context) {
		timer := time.NewTimer(delay)
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
				work()
				timer.Reset(supplier())
			}
		}
	}(ctx)
}

func DoTickerSchedule(ctx context.Context, work func(), delay time.Duration) {
	go func(ctx context.Context) {
		ticker := time.NewTicker(delay)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
			case <-ticker.C:
				work()
			}
		}

	}(ctx)
}

func DelaySchedule(work func(), delay time.Duration) {
	time.AfterFunc(delay, work)
}

type TimeFunction interface {
	Run()
}

type Options struct {
	Tick        int64
	SlotNum     int32
	Interval    time.Duration
	MaxDealTask int32
}

type Option func(opts *Options)

type HashTimeWheel struct {
	rwLock     sync.RWMutex
	buckets    []*timeBucket
	timeTicker *time.Ticker
	stopSign   chan bool
	opts       *Options
}

func NewTimeWheel(opt ...Option) *HashTimeWheel {
	opts := &Options{}
	for _, op := range opt {
		op(opts)
	}

	htw := &HashTimeWheel{
		rwLock:     sync.RWMutex{},
		opts:       opts,
		timeTicker: time.NewTicker(opts.Interval),
		stopSign:   make(chan bool),
		buckets:    make([]*timeBucket, opts.Interval, opts.SlotNum),
	}

	for i := int32(0); i < opts.SlotNum; i++ {
		htw.buckets[i] = newTimeBucket()
	}
	return htw
}

func (htw *HashTimeWheel) Start() {
	Go(context.Background(), func(ctx context.Context) {
		for {
			select {
			case <-htw.timeTicker.C:
				htw.process()
			case <-htw.stopSign:
				return
			}
		}
	})
}

func (htw *HashTimeWheel) AddTask(f TimeFunction, delay time.Duration) {
	pos, circle := htw.getSlots(delay)
	task := timeTask{
		circle: circle,
		f:      f,
		delay:  delay,
	}
	bucket := htw.buckets[pos]
	bucket.addUserTask(task)
}

func (htw *HashTimeWheel) Stop() {
	htw.stopSign <- true
	htw.clearAllAndProcess()
	for _, b := range htw.buckets {
		close(b.worker)
	}
}

func (htw *HashTimeWheel) process() {
	currentBucket := htw.buckets[htw.opts.Tick]
	htw.scanExpireAndRun(currentBucket)
	htw.opts.Tick = (htw.opts.Tick + 1) % int64(htw.opts.SlotNum)
}

func (htw *HashTimeWheel) clearAllAndProcess() {
	for i := htw.opts.SlotNum; i > 0; i-- {
		htw.process()
	}
}

type timeBucket struct {
	rwLock sync.RWMutex
	queue  *list.List
	worker chan timeTask
}

func (tb *timeBucket) addUserTask(t timeTask) {
	defer tb.rwLock.Unlock()
	tb.rwLock.Lock()
	tb.queue.PushBack(t)
}

func (tb *timeBucket) execUserTask() {
	for task := range tb.worker {
		task.f.Run()
	}
}

func newTimeBucket() *timeBucket {
	tb := &timeBucket{
		rwLock: sync.RWMutex{},
		queue:  list.New(),
		worker: make(chan timeTask, 32),
	}
	GoEmpty(func() {
		tb.execUserTask()
	})
	return tb
}

func (htw *HashTimeWheel) scanExpireAndRun(tb *timeBucket) {
	execCnt := int32(0)
	maxDealTaskCnt := htw.opts.MaxDealTask
	timeout := time.NewTimer(htw.opts.Interval)
	defer tb.rwLock.Unlock()
	tb.rwLock.Lock()
	for item := tb.queue.Front(); item != nil && maxDealTaskCnt >= execCnt; {
		task := item.Value.(timeTask)
		if task.circle < 0 {
			deal := func() {
				defer timeout.Reset(htw.opts.Interval)
				select {
				case tb.worker <- task:
					execCnt++
					next := item.Next()
					tb.queue.Remove(item)
					item = next
				case <-timeout.C:
					item = item.Next()
					return
				}
			}
			deal()
		} else {
			task.circle -= 1
			item = item.Next()
			continue
		}
	}
}

type timeTask struct {
	circle int32
	f      TimeFunction
	delay  time.Duration
}

func (htw *HashTimeWheel) getSlots(d time.Duration) (pos int32, circle int32) {
	delayTime := int64(d.Seconds())
	interval := int64(htw.opts.Interval.Seconds())
	return int32(htw.opts.Tick+delayTime/interval) % htw.opts.SlotNum, int32(delayTime / interval / int64(htw.opts.SlotNum))
}
