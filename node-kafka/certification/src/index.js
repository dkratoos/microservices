import { Kafka, logLevel } from 'kafkajs'

const kafka = new Kafka({
  clientId: 'app-one',
  brokers: ['localhost:29092'],
  logLevel: logLevel.NOTHING
})

const topic = 'issue-certificate'
const consumer = kafka.consumer({ groupId: 'app-one-group'})
const producer = kafka.producer()

async function run() {
  await producer.connect()
  await consumer.connect()
  await consumer.subscribe({ topic })
  await consumer.run({
    eachMessage: async ({topic, partition, message}) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`

      console.log(`---${prefix} ${message.key}#${message.value}`)

      producer.send({
        topic: 'return-response',
        messages: [
          {
            value: prefix
          }
        ]
      })
    }
  })
}

run().catch(console.error)