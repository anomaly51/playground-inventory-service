import Fastify, { type FastifyInstance } from "fastify";
import {
  InventoryListResponseSchema,
  InventoryResetRequestSchema,
} from "./contracts.js";
import type { WorkerConfig } from "./config.js";
import type { WorkerMetrics } from "./metrics.js";
import type { RabbitWorker } from "./worker.js";
import type { InventoryStorage } from "./storage.js";

export async function createApp(options: {
  config: WorkerConfig;
  worker: RabbitWorker;
  storage: InventoryStorage;
  metrics: WorkerMetrics;
}): Promise<FastifyInstance> {
  const app = Fastify({
    logger: { level: options.config.logLevel },
    requestTimeout: 5_000,
    connectionTimeout: 5_000,
  });
  app.get("/healthz", async () => ({ status: "ok" }));
  app.get("/readyz", async (_request, reply) => {
    const brokerReadiness = options.worker.readiness();
    const rabbitReady = brokerReadiness.rabbitmq;
    const mysqlReady = await options.storage.isReady(options.config.readinessTimeoutMs);
    const ready = rabbitReady && mysqlReady;
    return reply.code(ready ? 200 : 503).send({
      status: ready ? "ready" : "not-ready",
      dependencies: {
        rabbitmq: rabbitReady,
        mysql: mysqlReady,
        kafkaTelemetry: brokerReadiness.kafka,
      },
    });
  });
  app.get("/operations/snapshot", async () => {
    const mysqlReady = await options.storage.isReady(options.config.readinessTimeoutMs);
    let reservations: number | null = null;
    try {
      reservations = mysqlReady
        ? (await options.storage.getRuntime()).reservations
        : null;
    } catch {
      reservations = null;
    }
    return {
      nodes: {
        "inventory-worker": {
          inFlight: options.worker.runtime().inFlight,
          total: reservations,
          healthy: options.worker.isReady() && mysqlReady,
        },
        mysql: {
          inFlight: null,
          total: reservations,
          healthy: mysqlReady,
        },
      },
    };
  });
  app.get("/metrics", async (_request, reply) => {
    reply.header("content-type", options.metrics.registry.contentType);
    return options.metrics.registry.metrics();
  });
  app.get("/api/v1/inventory", async (_request, reply) => {
    const response = InventoryListResponseSchema.parse({
      items: await options.storage.listInventory(),
    });
    reply.header("cache-control", "no-store");
    return response;
  });
  app.post("/api/v1/lab/inventory/reset", async (request, reply) => {
    if (!options.config.inventoryResetEnabled) {
      return reply.code(403).send({ error: "inventory_reset_disabled" });
    }
    const body = InventoryResetRequestSchema.parse(request.body ?? {});
    const response = InventoryListResponseSchema.parse({
      items: await options.storage.resetInventory(body.items),
    });
    reply.header("cache-control", "no-store");
    return response;
  });
  return app;
}
