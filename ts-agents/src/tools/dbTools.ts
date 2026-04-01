import type { Pool } from "pg";
import type { ITool } from "../core/interfaces.js";

export class SearchRawMessagesTool implements ITool {
  readonly name = "searchRawMessages";

  constructor(private readonly db: Pool) {}

  async execute(input: Record<string, unknown>): Promise<Record<string, unknown>> {
    const cids = Array.isArray(input.cids) ? input.cids.map(String) : [];
    if (cids.length === 0) {
      return { rows: [] };
    }
    const { rows } = await this.db.query(
      `
      SELECT payload
      FROM raw_messages
      WHERE payload->>'cid' = ANY($1::text[])
      ORDER BY created_at DESC
      LIMIT 200
      `,
      [cids]
    );
    return { rows };
  }
}

export class SearchRelatedAnnotationsTool implements ITool {
  readonly name = "searchRelatedAnnotations";

  constructor(private readonly db: Pool) {}

  async execute(input: Record<string, unknown>): Promise<Record<string, unknown>> {
    const cid = String(input.cid ?? "");
    if (!cid) {
      return { rows: [] };
    }
    const { rows } = await this.db.query(
      `
      SELECT payload
      FROM annotations
      WHERE payload->>'cid' = $1
      ORDER BY created_at DESC
      LIMIT 200
      `,
      [cid]
    );
    return { rows };
  }
}

export class SearchVectorByUuidTool implements ITool {
  readonly name = "searchVectorByUuid";

  constructor(private readonly db: Pool) {}

  async execute(input: Record<string, unknown>): Promise<Record<string, unknown>> {
    const cids = Array.isArray(input.cids) ? input.cids.map(String) : [];
    if (cids.length === 0) {
      return { rows: [] };
    }
    const { rows } = await this.db.query(
      `
      SELECT payload
      FROM vector_embeddings
      WHERE payload->>'cid' = ANY($1::text[])
      LIMIT 200
      `,
      [cids]
    );
    return { rows };
  }
}

export class SearchSimilarVectorsByCidTool implements ITool {
  readonly name = "searchSimilarVectorsByCid";

  constructor(private readonly db: Pool) {}

  async execute(input: Record<string, unknown>): Promise<Record<string, unknown>> {
    const cid = String(input.cid ?? "").trim();
    if (!cid) {
      return { rows: [], reason: "missing_cid" };
    }
    const topK = Math.max(1, Math.min(20, Number(input.top_k ?? 5)));
    const minScore = Number(input.min_score ?? 0.2);

    const hasTarget = await this.db.query(
      `
      SELECT 1
      FROM vector_embeddings
      WHERE cid = $1
        AND embedding_vec IS NOT NULL
      ORDER BY created_at DESC
      LIMIT 1
      `,
      [cid]
    );
    if (hasTarget.rows.length === 0) {
      return { rows: [], reason: "target_embedding_not_found", target_cid: cid };
    }

    const { rows } = await this.db.query(
      `
      WITH target AS (
        SELECT embedding_vec
        FROM vector_embeddings
        WHERE cid = $1
          AND embedding_vec IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 1
      ),
      latest_candidates AS (
        SELECT DISTINCT ON (cid) cid, embedding_vec
        FROM vector_embeddings
        WHERE cid <> $1
          AND embedding_vec IS NOT NULL
        ORDER BY cid, created_at DESC
      )
      SELECT lc.cid, (1 - (lc.embedding_vec <=> t.embedding_vec)) AS score
      FROM latest_candidates lc
      CROSS JOIN target t
      WHERE (1 - (lc.embedding_vec <=> t.embedding_vec)) >= $2
      ORDER BY (lc.embedding_vec <=> t.embedding_vec) ASC
      LIMIT $3
      `,
      [cid, minScore, topK]
    );

    return {
      target_cid: cid,
      min_score: minScore,
      rows,
    };
  }
}

