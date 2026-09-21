package app

import (
	"errors"
	"math"
	"time"
)

const (
	autoscaleEWMAAlpha       = 0.2
	autoscaleInitialRate     = float64(1 << 20)
	autoscaleScaleInStable   = 5 * time.Minute
	autoscaleMaxScaleInShare = 0.25
	globalPGConnectionBudget = 64
	reservedPGConnections    = 8
)

type ScaleConfig struct {
	MinWorkers, MaxWorkers int
	TargetDrain            time.Duration
	OldestTarget           time.Duration
}

type ScaleSample struct {
	Now                  time.Time
	QueuedBytes          int64
	OldestAge            time.Duration
	CompletedBytes       int64
	Interval             time.Duration
	CurrentWorkers       int
	DependencyErrorRatio float64
	PoolWaitP95          time.Duration
}

type ScaleDecision struct {
	DesiredWorkers int
	ServiceRate    float64
	Frozen         bool
}

type Scaler struct {
	config      ScaleConfig
	serviceRate float64
	breaches    int
	stableSince time.Time
}

func NewScaler(config ScaleConfig) (*Scaler, error) {
	if config.MinWorkers < 0 || config.MaxWorkers < config.MinWorkers || config.MaxWorkers == 0 || config.TargetDrain <= 0 || config.OldestTarget <= 0 {
		return nil, errors.New("invalid autoscale configuration")
	}
	return &Scaler{config: config, serviceRate: autoscaleInitialRate}, nil
}

func (scaler *Scaler) Observe(sample ScaleSample) ScaleDecision {
	if sample.CompletedBytes > 0 && sample.Interval > 0 && sample.CurrentWorkers > 0 {
		observed := float64(sample.CompletedBytes) / sample.Interval.Seconds() / float64(sample.CurrentWorkers)
		scaler.serviceRate = autoscaleEWMAAlpha*observed + (1-autoscaleEWMAAlpha)*scaler.serviceRate
	}
	current := clamp(sample.CurrentWorkers, scaler.config.MinWorkers, scaler.config.MaxWorkers)
	if sample.DependencyErrorRatio > 0.2 || sample.PoolWaitP95 > time.Second {
		scaler.breaches, scaler.stableSince = 0, time.Time{}
		return ScaleDecision{DesiredWorkers: current, ServiceRate: scaler.serviceRate, Frozen: true}
	}
	desired := scaler.config.MinWorkers
	if sample.QueuedBytes > 0 {
		capacity := scaler.serviceRate * scaler.config.TargetDrain.Seconds()
		desired = int(math.Ceil(float64(sample.QueuedBytes) / capacity))
		if desired < scaler.config.MinWorkers {
			desired = scaler.config.MinWorkers
		}
	}
	if sample.OldestAge > scaler.config.OldestTarget && desired <= current {
		desired = current + 1
	}
	desired = clamp(desired, scaler.config.MinWorkers, scaler.config.MaxWorkers)
	if desired > current {
		scaler.breaches++
		scaler.stableSince = time.Time{}
		if scaler.breaches < 2 {
			desired = current
		}
		return ScaleDecision{DesiredWorkers: desired, ServiceRate: scaler.serviceRate}
	}
	scaler.breaches = 0
	if desired == current {
		scaler.stableSince = time.Time{}
		return ScaleDecision{DesiredWorkers: current, ServiceRate: scaler.serviceRate}
	}
	if scaler.stableSince.IsZero() {
		scaler.stableSince = sample.Now
		return ScaleDecision{DesiredWorkers: current, ServiceRate: scaler.serviceRate}
	}
	if sample.Now.Sub(scaler.stableSince) < autoscaleScaleInStable {
		return ScaleDecision{DesiredWorkers: current, ServiceRate: scaler.serviceRate}
	}
	maximumReduction := int(math.Max(1, math.Floor(float64(current)*autoscaleMaxScaleInShare)))
	if floor := current - maximumReduction; desired < floor {
		desired = floor
	}
	scaler.stableSince = sample.Now
	return ScaleDecision{DesiredWorkers: desired, ServiceRate: scaler.serviceRate}
}

func ValidateReplicaConnectionBudget(apiReplicas, workerReplicas, schedulerReplicas int) error {
	if apiReplicas < 0 || workerReplicas < 0 || schedulerReplicas < 0 {
		return errors.New("replica counts cannot be negative")
	}
	used := apiReplicas*8 + workerReplicas*2 + schedulerReplicas*2
	if used > globalPGConnectionBudget-reservedPGConnections {
		return errors.New("replicas exceed the PostgreSQL connection budget")
	}
	return nil
}

func clamp(value, minimum, maximum int) int {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}
