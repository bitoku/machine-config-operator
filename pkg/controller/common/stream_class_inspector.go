package common

// StreamClassInspector inspects a container image URL and returns its OS stream
// class (e.g. "rhel-9", "rhel-10").
// TODO(OCP 5.3): Remove when runc is removed.
type StreamClassInspector func(imageURL string) (string, error)
