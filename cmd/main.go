package main

import (
	"encoding/json"
	"flag"
	"inboxsuite/internal/logger"
	"inboxsuite/internal/models"
	"inboxsuite/internal/repo"
	"inboxsuite/internal/repo/postgre"
	"inboxsuite/internal/repo/rabbit"
	"inboxsuite/internal/service"
	"os"

	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

const (
	queueName      = "profile_queue"
	exchangeName   = "profile_exchange"
	routingKey     = "profile_key"
	statsQueueName = "stats_queue"
)

func main() {
	numWorkers := flag.Int("workers", 5, "Number of workers")
	flag.Parse()

	logger := logger.CreateLogger()
	defer logger.Sync()

	dbpool := postgre.ConnectDB(logger)
	defer dbpool.Close()

	rmqService, err := rabbit.NewRMQService(
		os.Getenv("RABBITMQ_URL"),
		exchangeName,
		routingKey,
		statsQueueName,
		logger,
	)
	if err != nil {
		logger.Fatal("Failed to create RabbitMQ service", zap.Error(err))
	}

	repository := repo.NewRepository(dbpool, rmqService)
	service := service.NewService(repository, *numWorkers, logger)

	if err := service.LoadCache(); err != nil {
		logger.Fatal("Failed to load cache", zap.Error(err))
	}

	service.StartWorkers(*numWorkers)

	conn, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		logger.Fatal("Failed to connect to RabbitMQ", zap.Error(err))
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		logger.Fatal("Failed to open a channel", zap.Error(err))
	}
	defer ch.Close()

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
		logger.Fatal("Failed to declare an exchange", zap.Error(err))
	}

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Fatal("Failed to declare queue", zap.Error(err))
	}

	err = ch.QueueBind(
		queueName,
		routingKey,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		logger.Fatal("Failed to bind queue", zap.Error(err))
	}

	msgs, err := ch.Consume(
		queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Fatal("Failed to register a consumer", zap.Error(err))
	}

	for msg := range msgs {
		var profileMsg models.ProfileMessage
		if err := json.Unmarshal(msg.Body, &profileMsg); err != nil {
			logger.Warn("Failed to unmarshal message", zap.Error(err))
			continue
		}
		service.ProcessMessage(profileMsg)
	}

	service.Stop()
}
