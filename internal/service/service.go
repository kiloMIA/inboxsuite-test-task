package service

import (
	"inboxsuite/internal/models"
	"inboxsuite/internal/repo"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Service struct {
	Repo       *repo.Repository
	ProcessCh  chan models.ProfileMessage
	Cache      map[uint8]uint8
	CacheLock  sync.RWMutex
	Processed  sync.Map
	WG         sync.WaitGroup
	Logger     *zap.Logger
	EventCount int32
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

	s.Logger.Info("Cache loaded", zap.Any("cache", s.Cache))

	return nil
}

func (s *Service) StartWorkers(numWorkers int) {
	s.Logger.Info("Starting workers", zap.Int("numWorkers", numWorkers))
	for i := 0; i < numWorkers; i++ {
		s.WG.Add(1)
		go s.processMessages()
	}
}

func (s *Service) processMessages() {
	defer s.WG.Done()
	for profileMsg := range s.ProcessCh {
		s.Logger.Info("Processing message", zap.Any("profileMsg", profileMsg))

		if _, loaded := s.Processed.LoadOrStore(profileMsg.ProfileID, true); loaded {
			s.Logger.Warn("Message already processed", zap.Int64("profileID", profileMsg.ProfileID))
			continue
		}

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
		s.IncrementCounter()

		err = s.Repo.SaveMessage(resultMsg, s.Logger)
		if err != nil {
			s.Logger.Error("Failed to save message to database", zap.Error(err))
		}
	}
}

func (s *Service) ProcessMessage(profileMsg models.ProfileMessage) {
	s.Logger.Info("Queuing message for processing", zap.Any("profileMsg", profileMsg))
	s.ProcessCh <- profileMsg
}

func (s *Service) StartAutomaticMessageGeneration() {
	go func() {
		s.Logger.Info("Starting automatic message generation")

		for classID := range s.Cache {
			profileID := rand.Int63n(100000)
			profileMsg := models.ProfileMessage{
				ProfileID: profileID,
				ClassID:   classID,
			}
			s.ProcessMessage(profileMsg)
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func (s *Service) IncrementCounter() {
	s.EventCount++
	if s.EventCount%10 == 0 {
		s.sendStats()
	}
}

func (s *Service) sendStats() {
	statsMsg := models.StatsMessage{
		Count: s.EventCount,
	}
	s.Repo.RMQ.SendStatsMessage(statsMsg)
}

func (s *Service) Stop() {
	close(s.ProcessCh)
	s.WG.Wait()
	s.sendStats()
	s.Logger.Info("All workers stopped")
}
