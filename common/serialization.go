package common

type SerializableObject interface {
	ToString() []string
}

type DeserializableObject interface {
	FromString(data []string) error
}
