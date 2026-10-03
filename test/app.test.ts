import type { InventoryItem } from "../src/contracts.js";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createApp } from "../src/app.js";
import { loadConfig } from "../src/config.js";
import { WorkerMetrics } from "../src/metrics.js";
import type { InventoryStorage } from "../src/storage.js";
import type { RabbitWorker } from "../src/worker.js";

const apps: Awaited<ReturnType<typeof createApp>>[] = [];
afterEach(async () => Promise.all(apps.splice(0).map((app) => app.close())));

const inventory: InventoryItem[] = [{
  sku: "DROP-CAP-LIME",
  available: 60,
  reserved: 0,
  updatedAt: "2026-08-30T10:00:00.000Z",
}];

function storage(ready: boolean): InventoryStorage {
  return {
    start: vi.fn(),
    stop: vi.fn(),
    isReady: vi.fn(async () => ready),
    reserve: vi.fn(async () => { throw new Error("unused"); }),
    recordFailure: vi.fn(async () => undefined),
    getRuntime: vi.fn(async () => ({ reservations: 9, availableStock: 116 })),
    listInventory: vi.fn(async () => inventory),
    resetInventory: vi.fn(async () => inventory),
  };
}

function worker(rabbitmq: boolean, kafka = false): RabbitWorker {
  return {
    isReady: () => rabbitmq,
    readiness: () => ({ rabbitmq, kafka }),
    runtime: () => ({ inFlight: 2 }),
  } as RabbitWorker;
}

describe("rabbit-worker HTTP API", () => {
  it("requires RabbitMQ and MySQL but treats Kafka trace telemetry as optional", async () => {
    const app = await createApp({
      config: loadConfig({ LOG_LEVEL: "silent" }),
      worker: worker(true, false),
      storage: storage(true),
      metrics: new WorkerMetrics(),
    });
    apps.push(app);

    const response = await app.inject({ method: "GET", url: "/readyz" });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      status: "ready",
      dependencies: { rabbitmq: true, mysql: true, kafkaTelemetry: false },
    });
  });

  it("reports real runtime contributions and inventory", async () => {
    const store = storage(true);
    const app = await createApp({
      config: loadConfig({ LOG_LEVEL: "silent" }),
      worker: worker(true),
      storage: store,
      metrics: new WorkerMetrics(),
    });
    apps.push(app);

    const snapshot = await app.inject({ method: "GET", url: "/operations/snapshot" });
    expect(snapshot.json()).toEqual({
      nodes: {
        "inventory-worker": { inFlight: 2, total: 9, healthy: true },
        mysql: { inFlight: null, total: 9, healthy: true },
      },
    });
    const response = await app.inject({ method: "GET", url: "/api/v1/inventory" });
    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ items: inventory });
  });

  it("validates and forwards inventory reset requests", async () => {
    const store = storage(true);
    const app = await createApp({
      config: loadConfig({ LOG_LEVEL: "silent", LAB_INVENTORY_RESET_ENABLED: "true" }),
      worker: worker(true),
      storage: store,
      metrics: new WorkerMetrics(),
    });
    apps.push(app);

    const items = [{ sku: "DROP-CAP-LIME", available: 12 }];
    const response = await app.inject({
      method: "POST",
      url: "/api/v1/lab/inventory/reset",
      payload: { items },
    });

    expect(response.statusCode).toBe(200);
    expect(store.resetInventory).toHaveBeenCalledWith(items);
  });
});
