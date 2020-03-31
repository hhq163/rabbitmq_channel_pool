package util

var works *WorkPool

func StartWorks(maxGos, chanLength int) {
	works = InitWorkPool(maxGos, chanLength)
}

func PushJob(f func()) {
	works.Push(f)
}

func StopWorkers() {
	works.Close()
}
