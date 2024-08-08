package rabbit

import (
	"encoding/json"
	"inboxsuite/internal/models"
	"sync/atomic"

	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type RMQService struct {
	Conn         *amqp.Connection
	Channel      *amqp.Channel
	ExchangeName string
	RoutingKey   string
	StatsQueue   string
	EventCount   int32
	Logger       *zap.Logger
}

func NewRMQService(
	connStr, exchangeName, routingKey, statsQueue string,
	logger *zap.Logger,
) (*RMQService, error) {
	conn, err := amqp.Dial(connStr)
	if err != nil {
		logger.Error("Failed to connect to RabbitMQ", zap.Error(err))
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		logger.Error("Failed to open a channel", zap.Error(err))
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Error("Failed to declare an exchange", zap.Error(err))
		return nil, err
	}

	_, err = ch.QueueDeclare(
		statsQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Error("Failed to declare stats queue", zap.Error(err))
		return nil, err
	}

	_, err = ch.QueueDeclare(
		"profile_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Error("Failed to declare profile queue", zap.Error(err))
		return nil, err
	}

	logger.Info("RabbitMQ service initialized")

	return &RMQService{
		Conn:         conn,
		Channel:      ch,
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		StatsQueue:   statsQueue,
		Logger:       logger,
	}, nil
}

func (r *RMQService) SendMessage(msg models.ResultMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		r.Logger.Error("Failed to marshal result message", zap.Error(err))
		return err
	}

	err = r.Channel.Publish(
		r.ExchangeName,
		r.RoutingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		r.Logger.Error("Failed to publish message", zap.Error(err))
	} else {
		r.Logger.Info("Message published", zap.Any("msg", msg))
	}
	return err
}

func (r *RMQService) SendStatsMessage(statsMsg models.StatsMessage) error {
	body, err := json.Marshal(statsMsg)
	if err != nil {
		r.Logger.Error("Failed to marshal stats message", zap.Error(err))
		return err
	}

	err = r.Channel.Publish(
		"",
		r.StatsQueue,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		r.Logger.Error("Failed to publish stats message", zap.Error(err))
	} else {
		r.Logger.Info("Stats message published", zap.Any("statsMsg", statsMsg))
	}
	return err
}

func (r *RMQService) IncrementCounter() {
	count := atomic.AddInt32(&r.EventCount, 1)
	r.Logger.Info("Incremented event count", zap.Int32("count", count))
	if count%10 == 0 {
		r.SendStatsMessage(models.StatsMessage{Count: count})
	}
}

func (r *RMQService) Close() {
	if err := r.Channel.Close(); err != nil {
		r.Logger.Error("Failed to close channel", zap.Error(err))
	}
	if err := r.Conn.Close(); err != nil {
		r.Logger.Error("Failed to close connection", zap.Error(err))
	}
}
