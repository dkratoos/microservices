import express from 'express'
import { Kafka, logLevel } from 'kafkajs'

import routes from './routes'

const app = express()

/**
 * Faz conexao com o kafka
 */
const kafka = new Kafka({
  clientId: 'app-one',
  brokers: ['localhost:29092'],
  retry: {
    initialRetryTime: 30,
    retries: 2
  },
  logLevel: logLevel.NOTHING
})

/**
 * Disponibiliza o producer para todas as rotas
 */
const admin = kafka.admin()
const producer = kafka.producer()

app.use((req, res, next) => {
  req.producer = producer

  return next()
})

/**
 * Registra consumer
 */
const topic = 'return-response'
const consumer = kafka.consumer({ groupId: 'app-one-group-01'})

/**
 * Cadastra as rotas da aplicacao
 */
app.use(routes)


async function run() {
  await admin.connect()
  await producer.connect()
  await consumer.connect()
  await consumer.subscribe({ topic })
  await consumer.run({
    eachMessage: async ({topic, partition, message}) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`

      console.log(`---${prefix} ${message.key}#${message.value}`)
    }
  })

  app.listen(3333)
}

run().catch(console.error)
