package postgre

import (
	"context"
	"fmt"
	"inboxsuite/internal/models"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

func ConnectDB(logger *zap.Logger) *pgxpool.Pool {
	connURL := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s",
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_PASSWORD"),
		os.Getenv("POSTGRES_HOST"),
		os.Getenv("POSTGRES_PORT"),
		os.Getenv("POSTGRES_DB"),
	)
	pool, err := pgxpool.New(context.Background(), connURL)
	if err != nil {
		logger.Error("Failed to connect to DB", zap.Error(err))
		return nil
	}
	logger.Info("Connected to DB")
	return pool
}

func LoadCache(db *pgxpool.Pool, logger *zap.Logger) (map[uint8]uint8, error) {
	cache := make(map[uint8]uint8)
	rows, err := db.Query(context.Background(), "SELECT class_id, roadmap_id FROM messages")
	if err != nil {
		logger.Error("Failed to query data", zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var classID, roadmapID uint8
		if err := rows.Scan(&classID, &roadmapID); err != nil {
			logger.Error("Failed to scan row", zap.Error(err))
			return nil, err
		}
		cache[classID] = roadmapID
	}
	logger.Info("Cache loaded from DB")
	return cache, nil
}

func SaveMessage(db *pgxpool.Pool, msg models.ResultMessage, logger *zap.Logger) error {
	_, err := db.Exec(context.Background(),
		"INSERT INTO messages (profile_id, class_id, roadmap_id) VALUES ($1, $2, $3)",
		msg.ProfileID, msg.ClassID, msg.RoadmapID)
	if err != nil {
		logger.Error("Failed to insert message into database", zap.Error(err))
		return err
	}
	logger.Info("Message saved to DB", zap.Any("msg", msg))
	return nil
}
