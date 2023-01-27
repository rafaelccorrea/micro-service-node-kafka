import express, { Request, Response } from "express";
import cors from "cors";
import { Kafka, logLevel, Partitioners } from "kafkajs";
import * as routes from "../api/routes/routes";

export const app = express();

app.use(express.json());
app.use(cors());

const kafka = new Kafka({
  clientId: "api_producers",
  brokers: ["localhost:9092"],
  retry: {
    initialRetryTime: 300,
    retries: 10
  },
  logLevel: logLevel.NOTHING
});

const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});

const consumer = kafka.consumer({ groupId: "certificate-group-receiver" });

app.use((req, res, next) => {
  req.producer = producer;
  return next();
});

//Routes
app.use("", routes.certification);

app.get("*", (req: Request, res: Response) =>
  res.status(200).send({
    message: "Welcome to this API.",
  })
);

async function run() {
  await producer.connect()
  await consumer.connect()

  await consumer.subscribe({ topic: 'certification-response' });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log('Resposta', String(message.value));
    },
  });

  app.listen(3030, () => {
    console.log("HTTP server On !");
  });
}

run().catch(console.error);
