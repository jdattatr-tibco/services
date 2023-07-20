package messaging

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/project-flogo/core/data/coerce"
	"github.com/project-flogo/flow/state"
	"github.com/project-flogo/services/flow-state/store"
)

const (
	PAYLOAD_TYPE             = "payload_type"
	PAYLOAD_TYPE_STEP        = "step"
	PAYLOAD_TYPE_SNAPSHOT    = "snapshot"
	PAYLOAD_TYPE_STATE_START = "state_start"
	PAYLOAD_TYPE_STATE_END   = "state_end"
	AUTH_TYPE                = "authentication"
	JWT_TOKEN                = "jwt_token"
)

type Server struct {
	clientOpts      pulsar.ClientOptions
	client          pulsar.Client
	consumer        pulsar.Consumer
	consumerOptions pulsar.ConsumerOptions
	wg              sync.WaitGroup
	started         bool
	stepStore       store.Store
}

func newServer(settings map[string]interface{}, opts ...func(*Server)) (*Server, error) {
	url, set := settings[BrokerUrl]
	if !set {
		return nil, fmt.Errorf("Messaging broker url not set")
	}
	brokerUrl, err := coerce.ToString(url)
	if err != nil {
		return nil, fmt.Errorf("Unable to coerce broker url %v to string %s", url, err)
	}

	srv := &Server{
		stepStore: store.RegistedStore(),
	}

	authType, set := settings[AUTH_TYPE]
	jwtToken, set := settings[JWT_TOKEN]

	srv.clientOpts = pulsar.ClientOptions{
		URL: brokerUrl,
	}
	switch authType {
	case "JWT":
		srv.clientOpts.Authentication = pulsar.NewAuthenticationToken(fmt.Sprintf("%v", jwtToken))
	}

	for _, opt := range opts {
		opt(srv)
	}
	return srv, nil
}
func (s *Server) Start() error {
	var err error

	s.client, err = pulsar.NewClient(s.clientOpts)
	if err != nil {
		return err
	}

	s.consumer, err = s.client.Subscribe(s.consumerOptions)
	if err != nil {
		return err
	}
	for {
		select {
		case msg, ok := <-s.consumer.Chan():
			if !ok {
				logger.Errorf("Error recieving message")
			}
			s.wg.Add(1)
			go s.handleMessage(msg)
		}
	}
}

func (s *Server) handleMessage(msg pulsar.ConsumerMessage) {
	defer func() {
		s.wg.Done()
	}()

	payloadType := msg.Message.Properties()[PAYLOAD_TYPE]
	switch payloadType {
	case PAYLOAD_TYPE_STEP:
		step := &state.Step{}
		err := json.Unmarshal(msg.Payload(), step)
		if err != nil {
			logger.Errorf("Error parsing the message [%v]", msg.Payload())
			s.consumer.Nack(msg)
		}
		err = s.stepStore.SaveStep(step)
		if err != nil {
			logger.Errorf("Error saving step - %v", err)
			return
		}
		s.consumer.Ack(msg)
		break
	case PAYLOAD_TYPE_SNAPSHOT:
		snapshot := &state.Snapshot{}
		err := json.Unmarshal(msg.Payload(), snapshot)
		if err != nil {
			logger.Errorf("Error parsing the message [%v]", msg.Payload())
			s.consumer.Nack(msg)
		}
		err = s.stepStore.SaveSnapshot(snapshot)
		if err != nil {
			logger.Errorf("Error saving snapshot - %v", err)
			return
		}
		s.consumer.Ack(msg)
		break
	case PAYLOAD_TYPE_STATE_START:
		state := &state.FlowState{}
		err := json.Unmarshal(msg.Payload(), state)
		if err != nil {
			logger.Errorf("Error parsing the message [%v]", msg.Payload())
			s.consumer.Nack(msg)
		}
		err = s.stepStore.RecordStart(state)
		if err != nil {
			logger.Errorf("Error saving state start - %v", err)
			return
		}
		s.consumer.Ack(msg)
		break
	case PAYLOAD_TYPE_STATE_END:
		state := &state.FlowState{}
		err := json.Unmarshal(msg.Payload(), state)
		if err != nil {
			logger.Errorf("Error parsing the message [%v]", msg.Payload())
			s.consumer.Nack(msg)
		}
		err = s.stepStore.RecordEnd(state)
		if err != nil {
			logger.Errorf("Error saving state end - %v", err)
			return
		}
		s.consumer.Ack(msg)
		break
	default:
		logger.Errorf("unkown message type")
	}

}

func (s *Server) Stop() error {
	return nil
}

func AddConsumerOptions(settings map[string]interface{}) func(*Server) {
	var err error

	topic, set := settings[PayloadTopic]
	var payloadTopic string
	if !set {
		payloadTopic = DefaultPayloadTopic
		logger.Infof("Payload topic not specified, using default topic %s", payloadTopic)
	} else {
		payloadTopic, err = coerce.ToString(topic)
		if err != nil {
			payloadTopic = DefaultPayloadTopic
			logger.Infof("Unable to coerce payload topic , using default topic %s", payloadTopic)
		}
	}

	subName, set := settings[SubscriptionName]
	var subscriptionName string
	if !set {
		subscriptionName = DefaultSubscriptionName
		logger.Infof("Subscription name not specified, using default subscription name %s", subscriptionName)
	} else {
		subscriptionName, err = coerce.ToString(subName)
		if err != nil {
			subscriptionName = DefaultPayloadTopic
			logger.Infof("Unable to coerce subscription name, using default subscription name %s", subscriptionName)
		}
	}

	return func(s *Server) {
		s.consumerOptions = pulsar.ConsumerOptions{
			Topic:            payloadTopic,
			SubscriptionName: subscriptionName,
		}
	}
}
