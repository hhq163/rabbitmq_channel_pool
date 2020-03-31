package util

import (
	"sync"
)

type WorkPool struct {
	workChan chan func()
	wg       sync.WaitGroup
}

func InitWorkPool(maxGos, maxChanBuf int) *WorkPool {
	if maxGos <= 0 {
		return nil
	}
	if maxChanBuf <= 0 {
		maxChanBuf = 10000
	}
	wl := &WorkPool{
		workChan: make(chan func(), maxChanBuf),
	}

	wl.wg.Add(maxGos)
	for i := 0; i < maxGos; i++ {
		go func() {
			for {
				if f, ok := <-wl.workChan; ok {
					f()
				} else {
					break
				}
			}
			wl.wg.Done()
		}()
	}

	return wl
}

//增加work到协程池
func (w *WorkPool) Push(f func()) {
	w.workChan <- f
}

//关闭协程池
func (w *WorkPool) Close() {
	close(w.workChan)
	w.wg.Wait()
}
