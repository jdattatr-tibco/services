package messaging

import (
	"github.com/project-flogo/core/support/log"
	"github.com/project-flogo/core/support/service"
)

const (
	BrokerUrl        = "pulsarUrl"
	Messaging        = "messaging"
	PayloadTopic     = "payloadTopic"
	SubscriptionName = "subscriptionName"
)

const (
	DefaultPayloadTopic     = "SMServicePayloadTopic"
	DefaultSubscriptionName = "SMServiceSubscription"
)

var logger = log.ChildLogger(log.RootLogger(), "flow-state-messaging")

func init() {
	_ = service.RegisterFactory(&MessagingServiceFactory{})
}

type MessagingServiceFactory struct {
}

func (s *MessagingServiceFactory) NewService(config *service.Config) (service.Service, error) {
	ms := &MessagingService{}

	err := ms.init(config.Settings["messaging"].(map[string]interface{}))
	if err != nil {
		return nil, err
	}

	return ms, nil
}

type MessagingService struct {
	server *Server
}

func (ms *MessagingService) init(settings map[string]interface{}) error {

	var options []func(*Server)
	options = append(options, AddConsumerOptions(settings))

	server, err := newServer(settings, options...)
	if err != nil {
		return err
	}
	ms.server = server
	return nil
}

func (ms *MessagingService) Name() string {
	return "MessagingService"
}

func (s *MessagingService) Start() error {
	return s.server.Start()
}
func (s *MessagingService) Stop() error {
	return s.server.Stop()
}
