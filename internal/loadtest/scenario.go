package loadtest

import "time"

type scenarioPreset struct {
	participants  int
	publishers    int
	publishAudio  bool
	publishVideo  bool
	subscribeAll  bool
	duration      time.Duration
	rampUp        time.Duration
	rampDown      time.Duration
	joinLeaveLoop bool
}

func ScenarioPreset(name string) (scenarioPreset, bool) {
	scenarios := map[string]scenarioPreset{
		"baseline": {
			participants: 2, publishers: 1, publishAudio: true, publishVideo: true,
			subscribeAll: true, duration: 5 * time.Minute,
		},
		"1pub-5sub": {
			participants: 6, publishers: 1, publishAudio: true, publishVideo: true,
			subscribeAll: true, duration: 5 * time.Minute,
		},
		"1pub-10sub": {
			participants: 11, publishers: 1, publishAudio: true, publishVideo: true,
			subscribeAll: true, duration: 5 * time.Minute,
		},
		"1pub-20sub": {
			participants: 21, publishers: 1, publishAudio: true, publishVideo: true,
			subscribeAll: true, duration: 5 * time.Minute,
		},
		"5pub-5sub": {
			participants: 10, publishers: 5, publishAudio: true, publishVideo: true,
			subscribeAll: true, duration: 5 * time.Minute,
		},
		"10pub-10sub": {
			participants: 20, publishers: 10, publishAudio: true, publishVideo: true,
			subscribeAll: true, duration: 10 * time.Minute,
		},
		"20-participants": {
			participants: 20, publishers: 20, publishAudio: true, publishVideo: true,
			subscribeAll: true, duration: 10 * time.Minute,
		},
		"ramp-up": {
			participants: 20, publishers: 20, publishAudio: true, publishVideo: true,
			subscribeAll: true, duration: 10 * time.Minute, rampUp: 2 * time.Minute,
		},
		"ramp-down": {
			participants: 20, publishers: 20, publishAudio: true, publishVideo: true,
			subscribeAll: true, duration: 10 * time.Minute, rampUp: time.Minute, rampDown: time.Minute,
		},
		"join-leave": {
			participants: 10, publishers: 10, publishAudio: true, publishVideo: true,
			subscribeAll: true, duration: 10 * time.Minute, joinLeaveLoop: true,
		},
	}
	p, ok := scenarios[name]
	return p, ok
}

func (p scenarioPreset) apply(c *Config) {
	c.Participants = p.participants
	c.Publishers = p.publishers
	c.PublishAudio = p.publishAudio
	c.PublishVideo = p.publishVideo
	c.SubscribeAll = p.subscribeAll
	c.Duration = p.duration
	c.RampUp = p.rampUp
	c.RampDown = p.rampDown
	c.JoinLeaveLoop = p.joinLeaveLoop
}
