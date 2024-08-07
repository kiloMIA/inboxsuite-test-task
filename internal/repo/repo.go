package repo

import (
	"inboxsuite/internal/models"
	"inboxsuite/internal/repo/postgre"
	"inboxsuite/internal/repo/rabbit"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Repository struct {
	DB  *pgxpool.Pool
	RMQ *rabbit.RMQService
}

func NewRepository(db *pgxpool.Pool, rmq *rabbit.RMQService) *Repository {
	return &Repository{
		DB:  db,
		RMQ: rmq,
	}
}

func (r *Repository) LoadCache(logger *zap.Logger) (map[uint8]uint8, error) {
	return postgre.LoadCache(r.DB, logger)
}

func (r *Repository) SaveMessage(msg models.ResultMessage, logger *zap.Logger) error {
	return postgre.SaveMessage(r.DB, msg, logger)
}
