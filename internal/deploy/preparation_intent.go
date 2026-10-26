package deploy

// PreparationIntent distinguishes bundle preparation from worker activation.
type PreparationIntent uint8

const (
	PrepareIntentUnspecified PreparationIntent = iota
	PrepareKeepStopped
	PrepareActivateNext
	PrepareWhileServing
	PrepareKeepHibernated
)

func (i PreparationIntent) message() string {
	switch i {
	case PrepareKeepHibernated:
		return "Bundle prepared; app remains hibernated"
	case PrepareKeepStopped:
		return "Bundle prepared; app remains stopped"
	case PrepareActivateNext:
		return "Bundle prepared; worker activation follows"
	case PrepareWhileServing:
		return "Bundle prepared; current version continues serving"
	default:
		return "Bundle prepared"
	}
}
