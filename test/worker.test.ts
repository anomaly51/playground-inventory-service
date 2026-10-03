import { RABBITMQ, type InventoryReservationCommand, type InventoryReservationResult } from "../src/contracts.js";
import type { ConfirmChannel, ConsumeMessage } from "amqplib";
import { describe, expect, it, vi } from "vitest";
import { loadConfig } from "../src/config.js";
import { WorkerMetrics } from "../src/metrics.js";
import type { InventoryStorage } from "../src/storage.js";
import { enforceInventoryScenario, RabbitWorker } from "../src/worker.js";

const command: InventoryReservationCommand = {
  schemaVersion: 1,
  commandId: "01ARZ3NDEKTSV4RRFFQ69G5FAA",
  traceId: "01ARZ3NDEKTSV4RRFFQ69G5FAB",
  orderId: "01ARZ3NDEKTSV4RRFFQ69G5FAC",
  sku: "DROP-CAP-LIME",
  quantity: 2,
  scenario: "normal",
  createdAt: "2026-08-30T10:00:00.000Z",
};

function result(overrides: Partial<InventoryReservationResult> = {}): InventoryReservationResult {
  return {
    schemaVersion: 1,
    resultId: "01ARZ3NDEKTSV4RRFFQ69G5FAD",
    commandId: command.commandId,
    traceId: command.traceId,
    orderId: command.orderId,
    sku: command.sku,
    quantity: command.quantity,
    status: "reserved",
    remainingStock: 58,
    processedAt: "2026-08-30T10:00:01.000Z",
    ...overrides,
  };
}

function storage(overrides: Partial<InventoryStorage> = {}): InventoryStorage {
  return {
    start: vi.fn(),
    stop: vi.fn(),
    isReady: vi.fn(async () => true),
    reserve: vi.fn(async () => result()),
    recordFailure: vi.fn(async () => undefined),
    getRuntime: vi.fn(async () => ({ reservations: 0, availableStock: 125 })),
    listInventory: vi.fn(async () => []),
    resetInventory: vi.fn(async () => []),
    ...overrides,
  };
}

function message(value: InventoryReservationCommand, headers: Record<string, unknown> = {}): ConsumeMessage {
  return {
    content: Buffer.from(JSON.stringify(value)),
    properties: { headers },
    fields: {},
  } as ConsumeMessage;
}

function workerWith(store: InventoryStorage, maxAttempts = 3): RabbitWorker {
  return new RabbitWorker(
    loadConfig({
      LOG_LEVEL: "silent",
      PROCESSING_DELAY_MS: "0",
      RABBITMQ_MAX_ATTEMPTS: String(maxAttempts),
      MYSQL_URL: "mysql://lab:lab@mysql:3306/flashdrop",
    }),
    { error: vi.fn(), warn: vi.fn(), info: vi.fn() } as never,
    new WorkerMetrics(),
    store,
  );
}

describe("inventory scenarios", () => {
  it("retries the transient scenario once and permanently fails the DLQ scenario", () => {
    expect(() => enforceInventoryScenario("normal", 1)).not.toThrow();
    expect(() => enforceInventoryScenario("inventory-retry", 1)).toThrow(
      "injected transient inventory failure",
    );
    expect(() => enforceInventoryScenario("inventory-retry", 2)).not.toThrow();
    expect(() => enforceInventoryScenario("inventory-dlq", 3)).toThrow(
      "injected permanent inventory failure",
    );
  });
});

describe("RabbitWorker command handling", () => {
  it("persists and publishes the durable result before acknowledging", async () => {
    const order: string[] = [];
    const reserve = vi.fn(async (received: InventoryReservationCommand) => {
      order.push("reserve");
      return result({ runId: received.runId });
    });
    const store = storage({ reserve });
    const worker = workerWith(store);
    const internals = worker as unknown as {
      channel: ConfirmChannel;
      emitTrace: () => Promise<void>;
      publishResult: (_channel: ConfirmChannel, value: InventoryReservationResult) => Promise<void>;
      handle: (value: ConsumeMessage) => Promise<void>;
    };
    internals.channel = {
      ack: () => { order.push("ack"); },
    } as unknown as ConfirmChannel;
    internals.emitTrace = async () => undefined;
    internals.publishResult = async (_channel, value) => {
      expect(value.runId).toBe("load-42");
      order.push("publish");
    };

    await internals.handle(message(command, { "x-run-id": "load-42" }));

    expect(reserve).toHaveBeenCalledWith({ ...command, runId: "load-42" });
    expect(order).toEqual(["reserve", "publish", "ack"]);
  });

  it("publishes bounded retries and acknowledges only after confirm", async () => {
    const actions: string[] = [];
    const recordFailure = vi.fn(async () => undefined);
    const worker = workerWith(storage({ recordFailure }));
    const internals = worker as unknown as {
      channel: ConfirmChannel;
      emitTrace: () => Promise<void>;
      publishRaw: (
        _channel: ConfirmChannel,
        exchange: string,
        _routingKey: string,
        _content: Buffer,
        _properties: unknown,
        headers: Record<string, unknown>,
      ) => Promise<void>;
      handle: (value: ConsumeMessage) => Promise<void>;
    };
    internals.channel = {
      ack: () => { actions.push("ack"); },
      nack: () => { actions.push("nack"); },
    } as unknown as ConfirmChannel;
    internals.emitTrace = async () => undefined;
    internals.publishRaw = async (_channel, exchange, _routingKey, _content, _properties, headers) => {
      expect(exchange).toBe(RABBITMQ.exchanges.retry);
      expect(headers["x-attempt"]).toBe(1);
      actions.push("retry-confirmed");
    };

    await internals.handle(message({ ...command, scenario: "inventory-retry" }));

    expect(recordFailure).toHaveBeenCalledWith(
      expect.objectContaining({ commandId: command.commandId }),
      1,
      "injected transient inventory failure",
      false,
    );
    expect(actions).toEqual(["retry-confirmed", "ack"]);
  });

  it("persists a failed result, publishes DLQ and result, then acknowledges", async () => {
    const actions: string[] = [];
    const failed = result({ status: "failed", remainingStock: undefined, reason: "boom" });
    const recordFailure = vi.fn(async () => failed);
    const worker = workerWith(storage({ recordFailure }), 3);
    const internals = worker as unknown as {
      channel: ConfirmChannel;
      emitTrace: () => Promise<void>;
      publishRaw: (_channel: ConfirmChannel, exchange: string) => Promise<void>;
      publishResult: () => Promise<void>;
      handle: (value: ConsumeMessage) => Promise<void>;
    };
    internals.channel = {
      ack: () => { actions.push("ack"); },
      nack: () => { actions.push("nack"); },
    } as unknown as ConfirmChannel;
    internals.emitTrace = async () => undefined;
    internals.publishRaw = async (_channel, exchange) => {
      expect(exchange).toBe(RABBITMQ.exchanges.deadLetter);
      actions.push("dlq-confirmed");
    };
    internals.publishResult = async () => { actions.push("result-confirmed"); };

    await internals.handle(message(
      { ...command, scenario: "inventory-dlq" },
      { "x-attempt": 2 },
    ));

    expect(recordFailure).toHaveBeenCalledWith(
      expect.anything(),
      3,
      "injected permanent inventory failure",
      true,
    );
    expect(actions).toEqual(["dlq-confirmed", "result-confirmed", "ack"]);
  });

  it("keeps retries bounded when MySQL failure recording is unavailable", async () => {
    const actions: string[] = [];
    const mysqlUnavailable = vi.fn(async () => {
      throw new Error("mysql unavailable");
    });
    const worker = workerWith(storage({ recordFailure: mysqlUnavailable }), 3);
    const internals = worker as unknown as {
      channel: ConfirmChannel;
      emitTrace: () => Promise<void>;
      publishRaw: (_channel: ConfirmChannel, exchange: string) => Promise<void>;
      publishResult: (
        _channel: ConfirmChannel,
        value: InventoryReservationResult,
      ) => Promise<void>;
      handle: (value: ConsumeMessage) => Promise<void>;
    };
    internals.channel = {
      ack: () => { actions.push("ack"); },
      nack: () => { actions.push("nack"); },
    } as unknown as ConfirmChannel;
    internals.emitTrace = async () => undefined;
    internals.publishRaw = async (_channel, exchange) => {
      expect(exchange).toBe(RABBITMQ.exchanges.deadLetter);
      actions.push("dlq-confirmed");
    };
    internals.publishResult = async (_channel, value) => {
      expect(value).toMatchObject({
        commandId: command.commandId,
        orderId: command.orderId,
        status: "failed",
        reason: "injected permanent inventory failure",
      });
      actions.push("result-confirmed");
    };

    await internals.handle(message(
      { ...command, scenario: "inventory-dlq" },
      { "x-attempt": 2 },
    ));

    expect(mysqlUnavailable).toHaveBeenCalledWith(
      expect.anything(),
      3,
      "injected permanent inventory failure",
      true,
    );
    expect(actions).toEqual(["dlq-confirmed", "result-confirmed", "ack"]);
  });

  it("advances the Rabbit attempt header while MySQL remains down", async () => {
    const actions: string[] = [];
    const mysqlUnavailable = vi.fn(async () => {
      throw new Error("mysql unavailable");
    });
    const worker = workerWith(storage({
      reserve: mysqlUnavailable,
      recordFailure: mysqlUnavailable,
    }), 3);
    const internals = worker as unknown as {
      channel: ConfirmChannel;
      emitTrace: () => Promise<void>;
      publishRaw: (
        _channel: ConfirmChannel,
        exchange: string,
        _routingKey: string,
        _content: Buffer,
        _properties: unknown,
        headers: Record<string, unknown>,
      ) => Promise<void>;
      handle: (value: ConsumeMessage) => Promise<void>;
    };
    internals.channel = {
      ack: () => { actions.push("ack"); },
      nack: () => { actions.push("nack"); },
    } as unknown as ConfirmChannel;
    internals.emitTrace = async () => undefined;
    internals.publishRaw = async (_channel, exchange, _key, _content, _properties, headers) => {
      expect(exchange).toBe(RABBITMQ.exchanges.retry);
      expect(headers["x-attempt"]).toBe(1);
      actions.push("retry-confirmed");
    };

    await internals.handle(message(command));

    expect(actions).toEqual(["retry-confirmed", "ack"]);
  });
});
