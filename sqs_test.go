package localstacktest_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func TestSendReceive(t *testing.T) {
	const numberOfPollers = 5
	const numberOfGroups = 2
	const numberOfMessagesPerGroup = 100
	const numberOfMessages = numberOfGroups * numberOfMessagesPerGroup
	const randomReceiveDelayMS = 0
	const randomSendDelayMS = 0

	config := loadConfig(t)
	client := sqs.NewFromConfig(config)

	queueURL := createQueue(t, client)

	type Msg struct {
		Group string `json:"group"`
		Index int    `json:"index"`
	}
	ch := make(chan Msg, numberOfMessages)

	ctx, cancelPollers := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	for i := 0; i < numberOfPollers; i++ {
		wg.Add(1)
		go func(poller int) {
			defer wg.Done()
			for {
				messages, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
					QueueUrl:              aws.String(queueURL),
					WaitTimeSeconds:       int32(10),
					MessageAttributeNames: []string{"All"},
					MaxNumberOfMessages:   10,
				})
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					t.Error("failed to receive messages:", err)
					return
				}

				for _, message := range messages.Messages {
					var m Msg
					b := aws.ToString(message.Body)
					_ = json.Unmarshal([]byte(b), &m)

					t.Log("message received", m, "on", poller)

					if randomReceiveDelayMS != 0 {
						time.Sleep(time.Duration(rand.Intn(randomReceiveDelayMS)) * time.Millisecond)
					}

					ch <- m

					_, err = client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
						QueueUrl:      aws.String(queueURL),
						ReceiptHandle: message.ReceiptHandle,
					})
					if err != nil {
						if errors.Is(err, context.Canceled) {
							return
						}
						t.Error("failed to delete message:", err)
					}
				}
			}
		}(i)
	}

	messagesToSendPerGroup := make(map[string][]Msg, numberOfGroups)
	for i := 0; i < numberOfGroups; i++ {
		group := fmt.Sprintf("g%d", i)
		for j := 0; j < numberOfMessagesPerGroup; j++ {
			messagesToSendPerGroup[group] = append(messagesToSendPerGroup[group], Msg{
				Group: group,
				Index: j,
			})
		}
	}

	// WHEN
	for _, messages := range messagesToSendPerGroup {
		go func(messages []Msg) {
			for _, msg := range messages {
				if randomSendDelayMS != 0 {
					time.Sleep(time.Duration(rand.Intn(randomSendDelayMS)) * time.Millisecond)
				}

				b, err := json.Marshal(msg)
				if err != nil {
					t.Error("failed to marshal message:", err)
				}

				_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{
					QueueUrl:       aws.String(queueURL),
					MessageBody:    aws.String(string(b)),
					MessageGroupId: aws.String(msg.Group),
				})
				if err != nil {
					t.Error("failed to send message:", err)
				}
				t.Log("message sent", msg)
			}
		}(messages)
	}

	// THEN
	receivedMessagesPerGroup := make(map[string][]Msg)
	for i := 0; i < numberOfMessages; i++ {
		msg := <-ch
		receivedMessagesPerGroup[msg.Group] = append(receivedMessagesPerGroup[msg.Group], msg)
	}

	cancelPollers()
	wg.Wait()

	if !reflect.DeepEqual(messagesToSendPerGroup, receivedMessagesPerGroup) {
		t.Errorf("did not receive expected messages: expected=%v actual=%v", messagesToSendPerGroup, receivedMessagesPerGroup)
	}
}

func createQueue(t *testing.T, client *sqs.Client) string {
	t.Helper()

	queue, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: aws.String("messagingtest.fifo"),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
			// Set a long visibility timeout to ensure that processed message don't reappear
			"VisibilityTimeout": "43200",
		},
	})
	if err != nil {
		t.Fatal("failed to create queue:", err)
	}
	t.Cleanup(func() {
		_, err = client.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{
			QueueUrl: queue.QueueUrl,
		})
		if err != nil {
			t.Error("failed to delete queue:", err)
		}
	})

	return aws.ToString(queue.QueueUrl)
}
func loadConfig(t *testing.T) aws.Config {
	t.Helper()

	opts := []func(*config.LoadOptions) error{
		config.WithRegion("eu-central-1"),
	}
	if endpointURL := os.Getenv("AWS_ENDPOINT_URL"); endpointURL != "" {
		customResolver := aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           endpointURL,
					SigningRegion: "eu-central-1",
				}, nil
			})
		opts = append(opts, config.WithEndpointResolverWithOptions(customResolver))
	}

	c, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		t.Fatal("failed to load aws config")
	}
	return c
}
