package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

/* Idea :
// get all variables
// initialize all contexts
// consume message
// produce to topic

Impl :
// Consumer : Consumes message
	// and calls Producer to produce consumed message

*/

/*
var (
	chost  = "localhost:9092"
	phost  = "localhost:9092"
	ctopic = "consum"
	ptopic = "produce"
)
*/
var (
	chost  string
	phost  string
	ctopic string
	ptopic string
)

func produce(phost string, producer sarama.SyncProducer, toproducemsg []byte) bool {
	msg := &sarama.ProducerMessage{
		Topic: ptopic,
		Value: sarama.StringEncoder(toproducemsg),
	}

	_, _, err := producer.SendMessage(msg)
	if err != nil {
		log.Print(err)
		return false
	}
	return true
}

func main() {
	chos := flag.String("sh", "", "Source Host : List of source hosts and port: broker1:9092,broker2:9092")
	ctop := flag.String("st", "", "Source Topic : Topic info of source: source-topic")
	phos := flag.String("dh", "", "Desitnation Host : List of destination hosts and port: broker1:9092,broker2:9092")
	ptop := flag.String("dt", "", "Desitnation Topic : Topic info of destination: destn-topic")

	flag.Parse()

	chost = *chos
	phost = *phos
	ctopic = *ctop
	ptopic = *ptop

	if phost == "" || chost == "" || ctopic == "" || ptopic == "" {
		flag.Usage()
		os.Exit(1)
	}

	// INITIATING PRODUCER CONFIGS
	pconfig := sarama.NewConfig()
	pconfig.Producer.Return.Successes = true
	pconfig.Producer.Return.Errors = true
	pconfig.Producer.RequiredAcks = sarama.WaitForAll
	pconfig.Producer.Retry.Max = 5
	pbrokers := []string{phost}
	producer, err := sarama.NewSyncProducer(pbrokers, pconfig)
	if err != nil {
		log.Print(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Print(err)
		}
	}()

	// INITATING CONSUMER CONFIGS
	cconfig := sarama.NewConfig()
	cconfig.Consumer.Return.Errors = true
	brokers := []string{chost}
	master, err := sarama.NewConsumer(brokers, cconfig)
	if err != nil {
		log.Print(err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			log.Print(err)
		}
	}()
	consumer, err := master.ConsumePartition(ctopic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Print(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	msgCount := 0
	errorCount := 0
	doneCh := make(chan struct{})

	log.Println("Estb. connection between kafka end-points. Starting the kafka streams..")

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				log.Print(err)
			case msg := <-consumer.Messages():
				if produce(phost, producer, msg.Value) {
					msgCount++
				} else {
					errorCount++
				}
			case <-signals:
				log.Print("Stopping the streams..")
				log.Printf("Total messages processed successfully : %d \n", msgCount)
				log.Printf("Total messages failed : %d \n", errorCount)
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
	fmt.Println("Total count of messages processed : ", msgCount, "messages")

}
