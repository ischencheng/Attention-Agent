import { Kafka } from "kafkajs";
import { Pool } from "pg";
import { PiMonoAgentService } from "../adapters/piMonoAgentService.js";
import { SearchSimilarVectorsByCidTool } from "../tools/dbTools.js";

type AgentInput = {
  cid?: string;
  raw?: Record<string, unknown>;
};

const kafka = new Kafka({ brokers: (process.env.KAFKA_BOOTSTRAP ?? "kafka1:19092").split(",") });
const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID ?? "mid-agent-ts-v1" });
const producer = kafka.producer();

const db = new Pool({
  host: process.env.DB_HOST ?? "postgres",
  port: Number(process.env.DB_PORT ?? 5432),
  database: process.env.DB_NAME ?? "bluesky",
  user: process.env.DB_USER ?? "bluesky",
  password: process.env.DB_PASSWORD ?? "bluesky",
});

const model = process.env.MID_AGENT_MODEL ?? "gemini-2.5-flash-lite";
const agentService = new PiMonoAgentService(model);
const searchSimilarVectorsTool = new SearchSimilarVectorsByCidTool(db);

function normalizeCidList(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => String(item ?? "").trim())
    .filter((item) => item.length > 0)
    .filter((item, index, arr) => arr.indexOf(item) === index);
}

function extractRelevantCidsFromText(outputText: string): string[] {
  const asJson = (() => {
    try {
      return JSON.parse(outputText) as Record<string, unknown>;
    } catch {
      return null;
    }
  })();
  if (asJson) {
    return normalizeCidList(
      asJson.relevant_cids ?? asJson.mid_agent_relevant_cids ?? asJson.relevant_ids ?? []
    );
  }

  const jsonMatch = outputText.match(/\{[\s\S]*\}/);
  if (!jsonMatch) {
    return [];
  }
  try {
    const parsed = JSON.parse(jsonMatch[0]) as Record<string, unknown>;
    return normalizeCidList(
      parsed.relevant_cids ?? parsed.mid_agent_relevant_cids ?? parsed.relevant_ids ?? []
    );
  } catch {
    return [];
  }
}

function extractText(input: AgentInput): string {
  const raw = input.raw;
  if (!raw || typeof raw !== "object") {
    return "";
  }
  const commit = raw.commit;
  if (!commit || typeof commit !== "object") {
    return "";
  }
  const record = (commit as Record<string, unknown>).record;
  if (!record || typeof record !== "object") {
    return "";
  }
  const text = (record as Record<string, unknown>).text;
  return typeof text === "string" ? text : "";
}

async function fallbackRelevantCids(cid: string): Promise<string[]> {
  const fallback = await searchSimilarVectorsTool.execute({
    cid,
    top_k: Number(process.env.MID_AGENT_TOP_K ?? 5),
    candidate_pool: Number(process.env.MID_AGENT_CANDIDATE_POOL ?? 500),
    min_score: Number(process.env.MID_AGENT_MIN_SCORE ?? 0.2),
  });
  const rows = Array.isArray(fallback.rows) ? fallback.rows : [];
  return rows
    .map((row) => {
      if (!row || typeof row !== "object") {
        return "";
      }
      return String((row as Record<string, unknown>).cid ?? "").trim();
    })
    .filter((candidate) => candidate.length > 0);
}

async function run(): Promise<void> {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: process.env.INPUT_TOPIC ?? "attention_ticket", fromBeginning: true });

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
      const cid = String(input.cid ?? "").trim();
      if (!cid) {
        return;
      }

      const postText = extractText(input);
      let relevantCids: string[] = [];
      try {
        const result = await agentService.runWithTools({
          messages: [
            {
              role: "system",
              content:
                "You are a market-intelligence mid-agent. Use tool searchSimilarVectorsByCid to retrieve semantically related posts. Return ONLY strict JSON with field relevant_cids:string[]. Prefer at most top 5 high-confidence related cids and avoid duplicates.",
            },
            {
              role: "user",
              content: `Find related cids for current cid=${cid}. post_text=${JSON.stringify(postText)}`,
            },
          ],
          tools: [searchSimilarVectorsTool],
        });
        relevantCids = extractRelevantCidsFromText(result.outputText);
      } catch (err) {
        console.error("mid-agent tool run failed:", err);
      }

      if (relevantCids.length === 0) {
        relevantCids = await fallbackRelevantCids(cid);
      }

      const finalRelevant = [cid, ...relevantCids.filter((candidate) => candidate !== cid)].filter(
        (candidate, index, arr) => arr.indexOf(candidate) === index
      );

      const output = {
        cid,
        raw: input,
        mid_agent_relevant_cids: finalRelevant,
        relevant_ids: finalRelevant,
      };

      await producer.send({
        topic: process.env.OUTPUT_TOPIC ?? "annotations",
        messages: [{ key: cid, value: JSON.stringify(output) }],
      });
    },
  });
}

run().catch((err) => {
  console.error("mid-agent-ts fatal:", err);
  process.exit(1);
});
