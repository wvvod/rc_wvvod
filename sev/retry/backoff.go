package retry

import (
	"math/rand"
	"time"
)

func CalculateNextRetry(retryCount int, baseDelay, maxDelay time.Duration, jitterFraction float64, nextFloat func() float64) time.Duration {
	if retryCount <= 0 {
		retryCount = 1
	}
	if baseDelay <= 0 {
		baseDelay = time.Second
	}
	if maxDelay <= 0 {
		maxDelay = baseDelay
	}
	if nextFloat == nil {
		nextFloat = rand.Float64
	}

	delay := baseDelay
	for i := 1; i < retryCount && delay < maxDelay; i++ {
		if delay > maxDelay/2 {
			delay = maxDelay
			break
		}
		delay *= 2
	}
	if delay > maxDelay {
		delay = maxDelay
	}

	if jitterFraction <= 0 {
		return delay
	}

	variation := (nextFloat()*2 - 1) * float64(delay) * jitterFraction
	result := time.Duration(float64(delay) + variation)
	if result < 0 {
		return 0
	}
	if result > maxDelay {
		return maxDelay
	}
	return result
}
