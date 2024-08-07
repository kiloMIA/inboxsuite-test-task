package service

import (
	"inboxsuite/internal/models"
	"inboxsuite/internal/repo"
	"inboxsuite/internal/repo/postgre"
	"sync"

	"go.uber.org/zap"
)

type Service struct {
	Repo      *repo.Repository
	ProcessCh chan models.ProfileMessage
	Cache     map[uint8]uint8
	CacheLock sync.RWMutex
	WG        sync.WaitGroup
	Logger    *zap.Logger
}

func NewService(repo *repo.Repository, numWorkers int, logger *zap.Logger) *Service {
	return &Service{
		Repo:      repo,
		ProcessCh: make(chan models.ProfileMessage, 100),
		Cache:     make(map[uint8]uint8),
		WG:        sync.WaitGroup{},
		Logger:    logger,
	}
}

func (s *Service) LoadCache() error {
	cache, err := s.Repo.LoadCache(s.Logger)
	if err != nil {
		return err
	}

	s.CacheLock.Lock()
	s.Cache = cache
	s.CacheLock.Unlock()

	return nil
}

func (s *Service) StartWorkers(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		s.WG.Add(1)
		go s.processMessages()
	}
}

func (s *Service) processMessages() {
	defer s.WG.Done()
	for profileMsg := range s.ProcessCh {
		s.CacheLock.RLock()
		roadmapID, exists := s.Cache[profileMsg.ClassID]
		s.CacheLock.RUnlock()

		if !exists {
			s.Logger.Warn("ClassID not found in cache", zap.Uint8("ClassID", profileMsg.ClassID))
			continue
		}

		resultMsg := models.ResultMessage{
			ProfileID: profileMsg.ProfileID,
			ClassID:   profileMsg.ClassID,
			RoadmapID: roadmapID,
		}

		err := s.Repo.RMQ.SendMessage(resultMsg)
		if err != nil {
			s.Logger.Error("Failed to send result message", zap.Error(err))
		}
		s.Repo.RMQ.IncrementCounter()

		err = postgre.SaveMessage(s.Repo.DB, resultMsg, s.Logger)
		if err != nil {
			s.Logger.Error("Failed to save message to database", zap.Error(err))
		}
	}
}

func (s *Service) ProcessMessage(profileMsg models.ProfileMessage) {
	s.ProcessCh <- profileMsg
}

func (s *Service) Stop() {
	close(s.ProcessCh)
	s.WG.Wait()
	s.Repo.RMQ.SendStats(true)
}
