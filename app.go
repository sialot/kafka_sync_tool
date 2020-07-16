package main

import (
	"strconv"

	"fmt"
	"os"
	"os/signal"

	"./ezlog"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/kylelemons/go-gypsy/yaml"
)

var SysConfig *yaml.File
var groupId = ""
var sourceBrokers []string
var sourceUser = ""
var sourcePassword = ""

var targetBrokers []string
var targetUser = ""
var targetPassword = ""

var logPath = ""

var topics []string
var logger ezlog.Log

func main() {
	initConfig()
	sync()
}

func sync() {

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// init producer
	targetConfig := sarama.NewConfig()
	targetConfig.Producer.RequiredAcks = sarama.WaitForAll
	targetConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	targetConfig.Producer.Return.Successes = true
	targetConfig.Producer.Return.Errors = true
	targetConfig.Producer.MaxMessageBytes = 19000000
	targetConfig.Net.SASL.Enable = true
	targetConfig.Net.SASL.User = targetUser
	targetConfig.Net.SASL.Password = targetPassword

	// 使用给定代理地址和配置创建一个同步生产者
	producer, err := sarama.NewSyncProducer(targetBrokers, targetConfig)
	if err != nil {
		panic(err)
	}

	defer producer.Close()

	// init (custom) config, enable errors and notifications
	sourceConfig := cluster.NewConfig()
	sourceConfig.Consumer.Return.Errors = true
	sourceConfig.Consumer.Offsets.Initial = -2
	sourceConfig.Group.Return.Notifications = true
	sourceConfig.Net.SASL.Enable = true
	sourceConfig.Net.SASL.User = sourceUser
	sourceConfig.Net.SASL.Password = sourcePassword

	// init consumer
	consumer, err := cluster.NewConsumer(sourceBrokers, groupId, topics, sourceConfig)
	if err != nil {
		panic(err)
	}

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			logger.Printf("Consumer Error: %s\n", err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			logger.Printf("Consumer Rebalanced: %+v\n", ntf)
		}
	}()

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {

				logger.Printf("Recive sucess > Topic=%s, Partition = %d, Offset=%d, Key=%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key)

				//构建发送的消息，
				newMsg := &sarama.ProducerMessage{
					Key:   sarama.ByteEncoder(msg.Key),
					Topic: msg.Topic,
					Value: sarama.ByteEncoder(msg.Value),
				}

				//SendMessage：该方法是生产者生产给定的消息
				//生产成功的时候返回该消息的分区和所在的偏移量
				//生产失败的时候返回error
				partition, offset, err := producer.SendMessage(newMsg)

				if err != nil {
					logger.Printf("Send message Fail > Topic=%s, Partition = %d, Offset=%d, Key=%s\n", newMsg.Topic, partition, offset, newMsg.Key)
					logger.Printf("    NEW MSG Topic %s\n", newMsg.Topic)
					logger.Printf("    returned %v\n", err)

					// 出错中断
					panic(err.Error())
				} else {
					logger.Printf("Send success > Topic=%s, Partition = %d, Offset=%d, Key=%s\n", msg.Topic, partition, offset, msg.Key)

					// mark message as processed
					consumer.MarkOffset(msg, "")
				}
			}
		case <-signals:
			return
		}
	}
}

// initConfig 准备配置文件
func initConfig() {

	var i int

	// 加载配置文件
	var err error
	SysConfig, err = yaml.ReadFile("./config/config.yaml")
	if err != nil {
		fmt.Println(err.Error())
		panic(err.Error())
	}

	logPath, err = SysConfig.Get("log_path")
	if err != nil {
		fmt.Println("Config logPath not exist!")
		panic(err.Error())
	}

	fmt.Println("log print to :" + logPath)

	// log
	logger = ezlog.Log{
		Filename: logPath,
		Pattern:  ""}

	// group id
	groupId, err = SysConfig.Get("group_id")
	if err != nil {
		logger.Error("Config group_id not exist!")
		panic(err.Error())
	}

	// ===================================== source start ================================
	sourceBrockerCount, err := SysConfig.Count("source.brokers")
	if err != nil {
		panic(err.Error())
	}
	for i = 0; i < sourceBrockerCount; i++ {

		broker, err := SysConfig.Get("source.brokers[" + strconv.Itoa(i) + "]")
		if err != nil {
			panic(err.Error())
		}

		sourceBrokers = append(sourceBrokers, broker)
	}
	logger.Printf("sourceBrokers: %v \n", sourceBrokers)

	sourceUser, err = SysConfig.Get("source.sasluser")
	if err != nil {
		logger.Error("Config source.sasluser not exist!")
		panic(err.Error())
	}
	logger.Printf("sourceUser: %s \n", sourceUser)

	sourcePassword, err = SysConfig.Get("source.saslpassword")
	if err != nil {
		logger.Error("Config source.sasluser not exist!")
		panic(err.Error())
	}
	logger.Printf("sourcePassword: %s \n", sourcePassword)

	// ===================================== source end ================================

	// ===================================== target start ================================
	targetBrockerCount, err := SysConfig.Count("target.brokers")
	if err != nil {
		panic(err.Error())
	}

	for i = 0; i < targetBrockerCount; i++ {
		broker, err := SysConfig.Get("target.brokers[" + strconv.Itoa(i) + "]")
		if err != nil {
			panic(err.Error())
		}

		targetBrokers = append(targetBrokers, broker)
	}
	logger.Printf("targetBrokers: %v \n", targetBrokers)

	targetUser, err = SysConfig.Get("target.sasluser")
	if err != nil {
		logger.Error("Config target.sasluser not exist!")
		panic(err.Error())
	}
	logger.Printf("targetUser: %s \n", targetUser)

	targetPassword, err = SysConfig.Get("target.saslpassword")
	if err != nil {
		logger.Error("Config target.sasluser not exist!")
		panic(err.Error())
	}
	logger.Printf("targetPassword: %s \n", targetPassword)

	// ===================================== target end ================================

	// topic
	topicCount, err := SysConfig.Count("topics")
	if err != nil {
		panic(err.Error())
	}

	for i = 0; i < topicCount; i++ {

		topic, err := SysConfig.Get("topics[" + strconv.Itoa(i) + "]")
		if err != nil {
			panic(err.Error())
		}

		topics = append(topics, topic)
	}
	logger.Printf("topics: %v \n", topics)
}
