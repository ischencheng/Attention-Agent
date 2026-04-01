import { Kafka } from "kafkajs";

type AgentInput = {
  cid?: string;
  raw?: Record<string, unknown>;
  mid_agent_relevant_cids?: string[];
};

const kafka = new Kafka({ brokers: (process.env.KAFKA_BOOTSTRAP ?? "kafka1:19092").split(",") });
const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID ?? "strong-agent-ts-v1" });
const producer = kafka.producer();

async function run(): Promise<void> {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: process.env.INPUT_TOPIC ?? "annotations", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) {
        return;
      }
      let input: AgentInput;
      try {
        input = JSON.parse(message.value.toString("utf-8")) as AgentInput;
      } catch {
        return;
      }
      const cid = String(input.cid ?? "");
      if (!cid) {
        return;
      }
      const output = {
        cid,
        raw: input,
        report: `strong-agent reviewed cid=${cid}`,
      };

      await producer.send({
        topic: process.env.OUTPUT_TOPIC ?? "decision_packet",
        messages: [{ key: cid, value: JSON.stringify(output) }],
      });
    },
  });
}

run().catch((err) => {
  console.error("strong-agent-ts fatal:", err);
  process.exit(1);
});
