package clientcommon

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination drain_observer_mock.go . DrainSignalObserver

// DrainSignalObserver observes infra drain signals
// When a drain is detected, ShouldStop() returns a closed channel.
type DrainSignalObserver interface {
	ShouldStop() <-chan struct{}
}
