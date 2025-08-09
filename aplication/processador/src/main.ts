import cassandra from "cassandra-driver";
import { Kafka, Consumer } from "kafkajs";

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

const CASSANDRA_USER = process.env.CASSANDRA_USER;
const CASSANDRA_PASSWORD = process.env.CASSANDRA_PASSWORD;

// 1. Crie o provedor de autenticação
const authProvider = new cassandra.auth.PlainTextAuthProvider(
  CASSANDRA_USER || "",
  CASSANDRA_PASSWORD || ""
);

const client = new cassandra.Client({
  contactPoints: [
    "demo-dc1-all-pods-service.k8ssandra-operator.svc.cluster.local:9042",
  ],
  localDataCenter: "dc1",
  keyspace: "k1",
  authProvider: authProvider,
});

class Processador {
  request: Request;
  constructor(request: Request) {
    this.request = request;
  }

  async processar() {
    switch (this.request.type) {
      case typeRequest.Deposit:
        await this.deposit();
        break;
      case typeRequest.Withdraw:
        await this.withdraw();
        break;
      default:
        console.error("Invalid request type");
        break;
    }
  }

  private async deposit() {
    const queryBalance = `SELECT balance FROM k1.billing_request WHERE client_id = ? LIMIT 1`;
    const balance = await client.execute(
      queryBalance,
      [this.request.clientId],
      { prepare: true }
    );
    let currentBalance =
      balance.rows.length == 0 ? 0 : balance.rows[0].get("balance");

    const query = `INSERT INTO k1.billing_request (request_id, client_id, amount, type, balance, at_time) VALUES (?, ?, ?, ?, ?, ?)`;

    const params = [
      this.request.id,
      this.request.clientId,
      this.request.amount,
      this.request.type,
      currentBalance + this.request.amount,
      this.request.atTime,
    ];

    await client.execute(query, params, { prepare: true });
    console.log("Request processed successfully");
  }

  private async withdraw() {
    const queryBalance = `SELECT balance FROM k1.billing_request WHERE client_id = ? LIMIT 1`;
    const balance = await client.execute(
      queryBalance,
      [this.request.clientId],
      { prepare: true }
    );
    let currentBalance =
      balance.rows.length == 0 ? 0 : balance.rows[0].get("balance");

    const query = `INSERT INTO k1.billing_request (request_id, client_id, amount, type, balance, at_time) VALUES (?, ?, ?, ?, ?, ?)`;

    const params = [
      this.request.id,
      this.request.clientId,
      this.request.amount,
      this.request.type,
      currentBalance - this.request.amount,
      this.request.atTime,
    ];

    await client.execute(query, params, { prepare: true });
    console.log("Request processed successfully");
  }
}

const kafka = new Kafka({
  clientId: "processador",
  brokers: ["my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"],
});

class ConsumerKafka {
  consumer: Consumer;
  constructor() {
    this.consumer = kafka.consumer({ groupId: "consumer-group" });
  }

  async prepare() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: "billing", fromBeginning: false });
    const errorTypes = ["unhandledRejection", "uncaughtException"];
    const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];

    errorTypes.forEach((type) => {
      process.on(type, async (e) => {
        try {
          console.log(`process.on ${type}`);
          console.error(e);
          await this.consumer.disconnect();
          process.exit(0);
        } catch (_) {
          process.exit(1);
        }
      });
    });

    signalTraps.forEach((type) => {
      process.once(type, async () => {
        try {
          await this.consumer.disconnect();
        } finally {
          process.kill(process.pid, type);
        }
      });
    });
  }

  async consume() {
    await this.prepare();
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(
          `Received message: ${message.value}, partition: ${partition}, topic: ${topic}`
        );
        if (message.value) {
          const request: Request = JSON.parse(message.value.toString());
          const processador = new Processador(request);
          await processador.processar();
        }
      },
    });
  }
}

new ConsumerKafka().consume().catch(console.error);
