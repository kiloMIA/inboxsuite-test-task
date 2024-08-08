package models

type ProfileMessage struct {
	ProfileID int64 `json:"profileID"`
	ClassID   uint8 `json:"classID"`
}

type ResultMessage struct {
	ProfileID int64 `json:"profileID"`
	ClassID   uint8 `json:"classID"`
	RoadmapID uint8 `json:"roadmapID"`
}

type StatsMessage struct {
	Count int32 `json:"count"`
}
