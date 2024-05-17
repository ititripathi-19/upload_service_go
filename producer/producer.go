package producer

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"upload/utils"

	"github.com/segmentio/kafka-go"
)

func StartProducers(numProducers int, kafkaRecords <-chan utils.KafkaRecord, status chan<- bool, wg *sync.WaitGroup) {
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go producer(i, kafkaRecords, status, wg)
	}
}

func producer(id int, kafkaRecords <-chan utils.KafkaRecord, status chan<- bool, wg *sync.WaitGroup) {
	defer wg.Done()

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "upload",
		Balancer: &kafka.LeastBytes{},
		Async:    true,
	})
	defer writer.Close()

	for record := range kafkaRecords {
		jsonRecord, err := json.Marshal(record)
		if err != nil {
			log.Fatalf("Error formating kafka record to json %v", err)
		}
		err = writer.WriteMessages(context.Background(), kafka.Message{Value: []byte(string(jsonRecord))})
		if err != nil {
			log.Printf("Producer %d failed to produce record: %s, %v\n", id, string(jsonRecord), err)
			status <- false
		} else {
			log.Printf("Producer %d successfully produced record: %s\n", id, string(jsonRecord))
			status <- true
		}
	}
}
