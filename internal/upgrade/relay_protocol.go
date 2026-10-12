package upgrade

import "time"

// These V1 DTOs are a frozen wire contract between different binary versions.
// Do not change fields or their JSON names. Add a new protocol version instead;
// the current owner must continue decoding V1 while V1 relay leases are live.
// Explicit Go struct conversions in the owner deliberately fail to compile if
// its internal identity model changes without a compatibility decision.
type RelayPrincipalV1 struct {
	Slug                   string
	EntitlementAppID       int64
	EntitlementFingerprint string
	UserID                 int64
	Role                   string
	SessionEpoch           int64
	JTI                    string
	ActorID                int64
	ActorRole              string
	ActorSessionEpoch      int64
	SupportAppID           int64
	RoutedAppID            int64
	SupportExpiresAt       time.Time
	SessionExpiresAt       time.Time
}

type RelayUsageStartV1 struct {
	ID               string
	Slug             string
	DeploymentID     int64
	UserID           int64
	ViewerKey        string
	PrincipalKind    string
	IdentityMode     string
	PolicyGeneration int64
	InstanceID       string
	StartedAt        time.Time
}
