package messaging

type (
	Receiver interface {
		Consume() (<-chan *Message, error)
	}
)

