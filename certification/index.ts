import { Kafka, Partitioners } from "kafkajs";

const kafka = new Kafka({
  brokers: ["localhost: 9092"],
  clientId: "certificate",
});

const consumer = kafka.consumer({
  groupId: "certificate-group",
});

const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});

async function run() {
  await consumer.connect();
  await producer.connect()
  await consumer.subscribe({ topic: "issue-certificate" });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      console.log(`- ${prefix} ${message.key}#${message.value}`);

      const payload = JSON.parse(message.value);

      producer.send({
        topic: "certification-response",
        messages: [
          {
            value: `Certificado do usuário ${payload.user.name} do curso ${payload.certificate} gerado!`,
          },
        ],
      });
    },
  });
}

run().catch(console.error);
