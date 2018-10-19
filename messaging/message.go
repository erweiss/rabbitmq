package messaging

type (
	Message struct {
		Notification string
		Payload []byte
		Tag string
	}
)

func NewMessage(notification string, payload []byte, tag string) *Message {
	return &Message{
		Notification: notification,
		Payload: payload,
		Tag: tag,
	}
}
