import { Kafka, Producer } from "kafkajs";
import { mockedUUID } from "./mocks";
import { v4 as uuidv4 } from "uuid";

enum typeRequest {
  Deposit = "deposit",
  Withdraw = "withdraw",
}

interface Request {
  id: string;
  clientId: string;
  amount: number;
  type: typeRequest.Deposit | typeRequest.Withdraw;
  atTime: string;
}

const kafka = new Kafka({
  clientId: "client",
  brokers: [
    "kafka-0.kafka-svc.kafka.svc.cluster.local:9092",
    "kafka-1.kafka-svc.kafka.svc.cluster.local:9093",
    "kafka-2.kafka-svc.kafka.svc.cluster.local:9094",
  ],
});

class Client {
  producer: Producer;

  constructor() {
    this.producer = kafka.producer();
  }

  async produce() {
    const request: Request = {
      id: uuidv4(),
      clientId: mockedUUID[Math.floor(Math.random() * mockedUUID.length)],
      amount: Math.floor(Math.random() * 9999),
      type: Math.random() > 0.5 ? typeRequest.Withdraw : typeRequest.Deposit,
      atTime: new Date().toISOString(),
    };
    await this.producer.connect();
    await this.producer.send({
      topic: "billing",

      messages: [
        {
          key: request.id,
          value: JSON.stringify(request),
        },
      ],
    });
    console.log("Message sent");
    await this.producer.disconnect();
  }
}

const sendMessage = async () => {
  const client = new Client();
  await client.produce();
};

const run = async () => {
  setInterval(sendMessage, 3000);
};

run().catch(console.error);
