package messaging

import (
	"fmt"

	"github.com/project-flogo/core/data/coerce"
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

	url, set := settings[BrokerUrl]
	if !set {
		return fmt.Errorf("Messaging broker url not set")
	}
	brokerUrl, err := coerce.ToString(url)
	if err != nil {
		return fmt.Errorf("Unable to coerce broker url %v to string %s", url, err)
	}

	var options []func(*Server)
	options = append(options, AddConsumerOptions(settings))

	server, err := newServer(brokerUrl, options...)
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
