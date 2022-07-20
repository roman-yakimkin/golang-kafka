package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"sync"
)

type Message struct {
	Text string
}

type Client struct {
	Reader *kafka.Reader
	Writer *kafka.Writer
}

func NewClient(brokers []string, topic string, groupId string) (*Client, error) {
	if len(brokers) == 0 || brokers[0] == "" || topic == "" || groupId == "" {
		return nil, errors.New("no Kafka params")
	}
	c := Client{}
	c.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupId,
		MinBytes: 10e1,
		MaxBytes: 10e6,
	})
	c.Writer = &kafka.Writer{
		Addr:     kafka.TCP(brokers[0]),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	return &c, nil
}

func (c *Client) sendMessages(messages []kafka.Message) error {
	err := c.Writer.WriteMessages(context.Background(), messages...)
	return err
}

func (c *Client) getMessage() (kafka.Message, error) {
	msg, err := c.Reader.ReadMessage(context.Background())
	return msg, err
}

func (c *Client) fetchProcessCommit() error {
	msg, err := c.Reader.FetchMessage(context.Background())
	if err != nil {
		return err
	}

	fmt.Printf("%+v\n%s\n%s", msg, string(msg.Key), string(msg.Value))

	err = c.Reader.CommitMessages(context.Background(), msg)
	return err
}

func main() {
	brokers := []string{
		"localhost:19092",
	}
	kfk, err := NewClient(brokers, "yrvtest", "yrv_consumer_group")
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(200)
	go func() {
		for i := 0; i < 100; i++ {
			messages := []kafka.Message{
				{
					Key:   []byte(fmt.Sprintf("Msg %d", i)),
					Value: []byte(fmt.Sprintf("Message #%d value", i)),
				},
			}
			err = kfk.sendMessages(messages)
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}
	}()

	go func() {
		for i := 0; i < 100; i++ {
			err = kfk.fetchProcessCommit()
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}
	}()
	wg.Wait()
}
