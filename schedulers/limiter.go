package schedulers

// Limiter operation limiter
type Limiter struct {
	c chan bool
}

// NewLimiter create new limiter
func NewLimiter(size int) *Limiter {
	return &Limiter{make(chan bool, size)}
}

// Set set
func (l *Limiter) Set() {
	l.c <- true
}

// Release release
func (l *Limiter) Release() {
	<-l.c
}

// Close close limiter
func (l *Limiter) Close() {
	close(l.c)
}
