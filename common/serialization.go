package common

type SerializableObject interface {
	Serialize() []string
}

type DeserializableObject interface {
	Deserialize(data []string) error
}
