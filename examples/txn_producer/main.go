package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/rcrowley/go-metrics"

	"github.com/IBM/sarama"
)

// Sarama configuration options
var (
	brokers   = ""
	version   = ""
	topic     = ""
	producers = 1
	verbose   = false

	recordsNumber int64 = 1

	recordsRate = metrics.GetOrRegisterMeter("records.rate", nil)
)

/*
Q: go 中init()是什么时候调用的？
A:
- 包初始化时：当一个包被导入时，Go 语言运行时会自动执行该包中的 init() 函数。如果一个包被多个地方导入，init() 函数只会在首次导入该包时被调用一次
- 先于 main() 函数：对于 main 包，init() 函数的执行顺序在 main() 函数之前
*/
func init() {
	flag.StringVar(&brokers, "brokers", "kafka9001:9092", "Kafka bootstrap brokers to connect to, as a comma separated list")
	// MinVersion     = V0_8_2_0
	// MaxVersion     = V4_0_0_0
	// DefaultVersion = V2_1_0_0
	flag.StringVar(&version, "version", sarama.DefaultVersion.String(), "Kafka cluster version")
	flag.StringVar(&topic, "topic", "EOC_ALERT_MESSAGE_TOPIC", "Kafka topics where records will be copied from topics.")
	flag.IntVar(&producers, "producers", 10, "Number of concurrent producers")
	flag.Int64Var(&recordsNumber, "records-number", 10000, "Number of records sent per loop")
	flag.BoolVar(&verbose, "verbose", false, "Sarama logging")
	flag.Parse()

	if len(brokers) == 0 {
		panic("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}

	if len(topic) == 0 {
		panic("no topic given to be consumed, please set the -topic flag")
	}
}

func main() {
	keepRunning := true
	log.Println("Starting a new Sarama producer")

	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	// 生成 producerProvider - 注意这是 2个 入参 哦，一个brokers， 一个 返回 Config 的 func()
	producerProvider := newProducerProvider(strings.Split(brokers, ","), func() *sarama.Config {
		// 生成Config
		config := sarama.NewConfig()
		// 填充配置
		config.Version = version
		// 幂等性
		config.Producer.Idempotent = true
		config.Producer.Return.Errors = false
		// 0 / 1 / -1
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
		config.Producer.Transaction.Retry.Backoff = 10
		config.Producer.Transaction.ID = "txn_producer"
		// 飞行中的 Request 数量
		config.Net.MaxOpenRequests = 1
		// 返回 *sarama.Config
		return config
	})

	go metrics.Log(metrics.DefaultRegistry, 5*time.Second, log.New(os.Stderr, "metrics: ", log.LstdFlags))

	// go中context的概念
	ctx, cancel := context.WithCancel(context.Background())
	// 声明了一个 sync.WaitGroup 类型的变量 wg。sync.WaitGroup 用于协调多个 goroutine 的同步，它可以用来等待一组 goroutine 完成工作
	var wg sync.WaitGroup

	// 并发数
	for i := 0; i < producers; i++ {
		// 在 WaitGroup 中增加一个计数，用来表示有一个新的 goroutine 开始工作
		wg.Add(1)
		// 创建并启动一个匿名的 goroutine
		go func() {
			// 使用 defer 关键字确保在这个 goroutine 结束时，调用 wg.Done() 来将 WaitGroup 的计数减 1 ，表示该 goroutine 工作完成
			defer wg.Done()

			// 一个无限循环，会一直执行循环体中的代码，直到遇到return语句跳出循环
			for {
				// select语句用于在多个通道操作中进行选择，类似于switch语句，但用于通道
				select {
				// "Done returns a channel that's closed when work done on behalf of this context should be canceled"
				case <-ctx.Done():
					return
				// 调用 produceTestRecord
				default:
					produceTestRecord(producerProvider)
				}
			}
		}() // 最后的括号表示立即调用这个匿名函数，并启动一个新的Goroutine来执行它
	}

	// 创建了一个缓冲通道 sigterm，用于接收操作系统信号
	sigterm := make(chan os.Signal, 1)

	// signal.Notify 函数用于注册要接收的信号。这里注册了两个信号：syscall.SIGINT（通常是用户按下 Ctrl+C 时发送的信号）和 syscall.SIGTERM（程序终止信号 ）。
	// 当这些信号发生时，对应的信号值会被发送到 sigterm 通道。
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	// 循环内部使用 <-sigterm 从信号通道接收信号。当接收到 SIGINT 或 SIGTERM 信号时，会打印一条日志信息 "terminating: via signal"，然后将 keepRunning 设置为 false ，从而退出循环
	for keepRunning {
		<-sigterm
		log.Println("terminating: via signal")
		keepRunning = false
	}

	// 会向基于 ctx 衍生出的所有相关协程发送取消信号
	cancel()
	// wg.Wait() 会阻塞当前 goroutine，直到所有通过 wg.Add 添加的计数都通过 wg.Done 完成，通常用于等待一组 goroutine 完成任务
	wg.Wait()

	producerProvider.clear()
}

func produceTestRecord(producerProvider *producerProvider) {
	// 1. 从 producerProvider 取得 AsyncProducer
	producer := producerProvider.borrow()
	defer producerProvider.release(producer)

	// 2. Start kafka transaction
	err := producer.BeginTxn()
	if err != nil {
		log.Printf("unable to start txn %s\n", err)
		return
	}

	// 3. Produce some records in transaction
	var i int64
	for i = 0; i < recordsNumber; i++ {
		// 看上去 只是吧 数据放进了 ”Channel“中，是怎么运转起来的呢 ？
		// 猜测 一定有另一个”线程“ 从 chan *ProducerMessage 中取走
		producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder("test")}

	}

	// 4. commit transaction
	err = producer.CommitTxn()

	// 5. err检测与重试
	if err != nil {
		log.Printf("Producer: unable to commit txn %s\n", err)
		for {
			if producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
				// fatal error. need to recreate producer.
				log.Printf("Producer: producer is in a fatal state, need to recreate it")
				break
			}
			// If producer is in abortable state, try to abort current transaction.
			if producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
				err = producer.AbortTxn()
				if err != nil {
					// If an error occured just retry it.
					log.Printf("Producer: unable to abort transaction: %+v", err)
					continue
				}
				break
			}
			// if not you can retry
			err = producer.CommitTxn()
			if err != nil {
				log.Printf("Producer: unable to commit txn %s\n", err)
				continue
			}
		}
		return
	}
	recordsRate.Mark(recordsNumber)
}

// pool of producers that ensure transactional-id is unique.
type producerProvider struct {
	// 事务ID-唯一
	transactionIdGenerator int32

	// Go 语言标准库中的互斥锁
	producersLock sync.Mutex

	// 用于存储多个 kafka AsyncProducer
	producers []sarama.AsyncProducer

	// 函数类型的字段，该函数不接受任何参数，返回一个 sarama.AsyncProducer 类型的对象
	producerProvider func() sarama.AsyncProducer
}

/*
*
param1 - brokers []string
param2 - producerConfigurationProvider func() *sarama.Config

	这是一个函数类型的参数，该函数不接受任何参数，返回一个指向 sarama.Config 结构体的指针

说人话就是 告诉 producerProvider 怎么利用 brokers 和 config 来生成 AsyncProducer
*/
func newProducerProvider(brokers []string, producerConfigurationProvider func() *sarama.Config) *producerProvider {
	// 创建了一个 producerProvider 结构体的实例，并将其地址赋值给变量 provider
	provider := &producerProvider{}

	// 初始化 producerProvider 中 producerProvider字段，说直白点就是生成 [sarama.AsyncProducer] 的函数
	provider.producerProvider = func() sarama.AsyncProducer {
		// config
		config := producerConfigurationProvider()
		// 生成 独立的 [ Transaction.ID ]
		suffix := provider.transactionIdGenerator
		// Append transactionIdGenerator to current config.Producer.Transaction.ID to ensure transaction-id uniqueness.
		if config.Producer.Transaction.ID != "" {
			provider.transactionIdGenerator++
			config.Producer.Transaction.ID = config.Producer.Transaction.ID + "-" + fmt.Sprint(suffix)
		}
		// 利用 brokers, config 生成 AsyncProducer
		producer, err := sarama.NewAsyncProducer(brokers, config)
		if err != nil {
			return nil
		}
		return producer
	}

	return provider
}

/*
*

	入参：producerProvide
	入参：producerProvider
	返回: AsyncProducer

	尝试从 []sarama.AsyncProducer 中取得 AsyncProducer，没有就创建返回
*/
func (p *producerProvider) borrow() (producer sarama.AsyncProducer) {
	// p.producersLock 对共享资源 [ producers []sarama.AsyncProducer ] 上锁
	p.producersLock.Lock()

	// defer 关键字确保无论函数以何种方式结束（正常返回或发生错误），都会执行 Unlock 操作来释放锁，保证资源的正确访问控制
	defer p.producersLock.Unlock()

	// producerProvider 池子中 AsyncProducer 为空
	if len(p.producers) == 0 {
		for {
			// 创建一个 producer， 并直接返回
			producer = p.producerProvider()
			if producer != nil {
				return
			}
		}
	}

	// 如果资源池不为空，获取资源池中最后一个生产者实例（通过计算索引 len(p.producers) - 1 ）
	index := len(p.producers) - 1
	// 按索引取到 AsyncProducer
	producer = p.producers[index]
	// 然后从资源池中移除该生产者（通过切片操作 p.producers[:index] ，丢弃最后一个元素）
	p.producers = p.producers[:index]
	return
}

func (p *producerProvider) release(producer sarama.AsyncProducer) {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	// If released producer is erroneous close it and don't return it to the producer pool.
	if producer.TxnStatus()&sarama.ProducerTxnFlagInError != 0 {
		// Try to close it
		_ = producer.Close()
		return
	}
	p.producers = append(p.producers, producer)
}

func (p *producerProvider) clear() {
	p.producersLock.Lock()
	defer p.producersLock.Unlock()

	for _, producer := range p.producers {
		producer.Close()
	}
	p.producers = p.producers[:0]
}
