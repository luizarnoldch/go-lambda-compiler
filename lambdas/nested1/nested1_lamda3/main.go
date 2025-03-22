package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/avast/retry-go"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/sony/gobreaker"
)

var config = struct {
	LambdaName      string
	MaxConcurrency  int
	DataValidation  bool
	ProcessingDelay time.Duration
	RetryAttempts   uint
}{
	LambdaName:      "nested1_lamda3",
	MaxConcurrency:  8,
	DataValidation:  true,
	ProcessingDelay: 100 * time.Millisecond,
	RetryAttempts:   3,
}

type Payload struct {
	ID        string      `json:"id"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data"`
	Signature string      `json:"signature"`
	Attempts  int         `json:"-"`
	IsValid   bool        `json:"-"`
}

type Processor struct {
	circuitBreaker *gobreaker.CircuitBreaker
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	inputChan      chan Payload
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configureLogger()
	log.Info().Str("lambda", config.LambdaName).Msg("🚀 Starting Lambda execution")

	processor := NewProcessor(ctx)
	go handleShutdown(processor, cancel)

	start := time.Now()
	results := processor.Run(generateMockData(50))
	showStatistics(results, time.Since(start))

	log.Info().Str("lambda", config.LambdaName).Msg("✅ Processing completed")
}

func configureLogger() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).
		With().Str("lambda", config.LambdaName).Logger()
}

func NewProcessor(parentCtx context.Context) *Processor {
	ctx, cancel := context.WithCancel(parentCtx)
	return &Processor{
		circuitBreaker: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        "LambdaProcessor",
			MaxRequests: 5,
			Interval:    10 * time.Second,
			Timeout:     15 * time.Second,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				return counts.ConsecutiveFailures > 3
			},
		}),
		ctx:       ctx,
		cancel:    cancel,
		inputChan: make(chan Payload, config.MaxConcurrency*2),
	}
}

func (p *Processor) Run(generator <-chan Payload) <-chan Payload {
	output := make(chan Payload)

	go func() {
		defer close(p.inputChan)
		for payload := range generator {
			select {
			case p.inputChan <- payload:
			case <-p.ctx.Done():
				return
			}
		}
	}()

	for i := 0; i < config.MaxConcurrency; i++ {
		p.wg.Add(1)
		go p.worker(i, output)
	}

	go func() {
		p.wg.Wait()
		close(output)
	}()

	return output
}

func (p *Processor) worker(id int, output chan<- Payload) {
	defer p.wg.Done()
	logger := log.With().Int("worker", id).Logger()

	for {
		select {
		case payload, ok := <-p.inputChan:
			if !ok {
				return
			}
			result, err := p.processWithRetry(payload)
			if err != nil {
				logger.Error().
					Err(err).
					Str("payloadID", payload.ID).
					Msg("Processing failed")
				continue
			}
			output <- result

		case <-p.ctx.Done():
			logger.Warn().Msg("Context cancelled")
			return
		}
	}
}

func (p *Processor) processWithRetry(payload Payload) (Payload, error) {
	var result Payload
	var lastErr error

	retryCtx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer cancel()

	err := retry.Do(
		func() error {
			res, err := p.circuitBreaker.Execute(func() (interface{}, error) {
				return processPayload(payload)
			})
			if err != nil {
				lastErr = err
				return err
			}
			result = res.(Payload)
			return nil
		},
		retry.Attempts(config.RetryAttempts),
		retry.Delay(config.ProcessingDelay),
		retry.Context(retryCtx),
		retry.LastErrorOnly(true),
	)

	if err != nil {
		return Payload{}, fmt.Errorf("%w (last error: %v)", err, lastErr)
	}
	return result, nil
}

func validatePayload(p Payload) error {
	if _, err := uuid.Parse(p.ID); err != nil {
		return fmt.Errorf("invalid UUID format: %w", err)
	}

	if p.Data == nil {
		return errors.New("data field is required")
	}

	return nil
}

func processPayload(p Payload) (Payload, error) {
	if config.DataValidation {
		if err := validatePayload(p); err != nil {
			return p, fmt.Errorf("validation failed: %w", err)
		}
	}

	processingTime := time.Duration(rand.Intn(100)) * time.Millisecond
	time.Sleep(processingTime)

	signatureData := struct {
		ID        string      `json:"id"`
		Timestamp time.Time   `json:"timestamp"`
		Data      interface{} `json:"data"`
	}{
		ID:        p.ID,
		Timestamp: p.Timestamp,
		Data:      p.Data,
	}

	p.Signature = generateSignature(signatureData)
	p.IsValid = true

	return p, nil
}

func generateSignature(data interface{}) string {
	jsonData, _ := json.Marshal(data)
	hash := sha256.Sum256(jsonData)
	return fmt.Sprintf("%x", hash)
}

func generateMockData(count int) <-chan Payload {
	ch := make(chan Payload)
	go func() {
		defer close(ch)
		for i := 0; i < count; i++ {
			payload := Payload{
				ID:        uuid.New().String(),
				Timestamp: time.Now().UTC(),
				Data:      generateComplexData(),
			}
			select {
			case ch <- payload:
			case <-time.After(1 * time.Second):
				return
			}
		}
	}()
	return ch
}

func showStatistics(results <-chan Payload, totalDuration time.Duration) {
	var stats struct {
		Total     int
		Success   int
		Failed    int
		AvgTime   time.Duration
		Checksum  string
		StartTime time.Time
		EndTime   time.Time
	}

	stats.StartTime = time.Now()
	for p := range results {
		stats.Total++
		if p.IsValid {
			stats.Success++
		} else {
			stats.Failed++
		}
	}
	stats.EndTime = time.Now()

	if stats.Total > 0 {
		stats.AvgTime = totalDuration / time.Duration(stats.Total)
	}

	stats.Checksum = generateSignature(stats)

	log.Info().
		Dur("total_duration", totalDuration).
		Dur("avg_processing_time", stats.AvgTime).
		Int("total_processed", stats.Total).
		Int("successful", stats.Success).
		Int("failed", stats.Failed).
		Str("checksum", stats.Checksum).
		Time("start_time", stats.StartTime).
		Time("end_time", stats.EndTime).
		Msg("📊 Processing statistics")
}

func handleShutdown(p *Processor, cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Warn().Msg("🛑 Shutdown signal received")

	cancel()

	select {
	case <-time.After(5 * time.Second):
		log.Error().Msg("Shutdown timeout forced")
	case <-p.ctx.Done():
		log.Info().Msg("Graceful shutdown completed")
	}
	os.Exit(0)
}

func generateComplexData() interface{} {
	types := []string{"string", "number", "object", "array"}
	switch types[rand.Intn(len(types))] {
	case "string":
		return randomString(15)
	case "number":
		return rand.Float64() * 1000
	case "object":
		return map[string]interface{}{
			"id":   uuid.New().String(),
			"data": randomString(20),
		}
	case "array":
		return []int{rand.Intn(100), rand.Intn(100), rand.Intn(100)}
	}
	return nil
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
