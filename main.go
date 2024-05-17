package main

import (
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"io"
	"log"
	"os"
	"sync"
	"upload/producer"
	"upload/utils"
)

func readCSV(filePath string, records chan<- utils.Record) {
	defer close(records)
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open CSV file: %v", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()

	if err != nil {
		log.Fatalf("Failed to read headers from CSV file: %v", err)
		return
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Failed to read record from CSV file: %v", err)
		}
		if len(record) != len(headers) {
			log.Fatalf("mismatched number of columns in row: %v", record)
			return
		}
		records <- utils.Record{Mobile: record[0]}
	}
}

func generateKafkaRecords(id int, records <-chan utils.Record, kafkaRecords chan<- utils.KafkaRecord, wg *sync.WaitGroup) {
	defer wg.Done()
	for record := range records {
		hash := md5.Sum([]byte(record.Mobile))
		hashString := hex.EncodeToString(hash[:])
		hashString = hashString[:16]
		kafkaRecords <- utils.KafkaRecord{
			Number: record.Mobile,
			UniqID: hashString,
		}
	}
}

func aggregateResults(status <-chan bool) {
	var successCount, failureCount int

	for result := range status {
		if result {
			successCount++
		} else {
			failureCount++
		}
	}
	log.Printf("Total Success: %d, Total Failure: %d\n", successCount, failureCount)
}

func main() {
	const numWorkers = 5
	filePath := "./data/1L.csv"

	records := make(chan utils.Record, 1000)
	kafkaRecords := make(chan utils.KafkaRecord, 1000)
	status := make(chan bool, 1000)

	var wg sync.WaitGroup
	var kwg sync.WaitGroup

	go readCSV(filePath, records)

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go generateKafkaRecords(i, records, kafkaRecords, &wg)
	}

	go producer.StartProducers(numWorkers, kafkaRecords, status, &kwg)

	go func() {
		wg.Wait()
		close(kafkaRecords)
		kwg.Wait()
		close(status)
	}()

	aggregateResults(status)
}
