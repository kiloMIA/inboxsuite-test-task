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

	logger.Info("Starting application")

	dbpool := postgre.ConnectDB(logger)
	if dbpool == nil {
		logger.Fatal("Failed to create pgxpool connection")
	}
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
	defer rmqService.Close()

	_, err = rmqService.Channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Fatal("Failed to declare profile queue", zap.Error(err))
	}

	repository := repo.NewRepository(dbpool, rmqService)
	service := service.NewService(repository, *numWorkers, logger)

	if err := service.LoadCache(); err != nil {
		logger.Fatal("Failed to load cache", zap.Error(err))
	}

	service.StartWorkers(*numWorkers)

	service.StartAutomaticMessageGeneration()

	msgs, err := rmqService.Channel.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Fatal("Failed to register a consumer", zap.Error(err))
	}

	logger.Info("Waiting for messages...")

	for msg := range msgs {
		var profileMsg models.ProfileMessage
		if err := json.Unmarshal(msg.Body, &profileMsg); err != nil {
			logger.Warn("Failed to unmarshal message", zap.Error(err))
			msg.Nack(false, true)
			continue
		}
		logger.Info("Received message", zap.Any("profileMsg", profileMsg))

		service.ProcessMessage(profileMsg)

		msg.Ack(false)
	}

	service.Stop()
	logger.Info("Application stopped")
}
