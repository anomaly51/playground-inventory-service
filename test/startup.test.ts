import { EventEmitter } from "node:events";
import amqp, { type ChannelModel } from "amqplib";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { loadConfig } from "../src/config.js";
import { WorkerMetrics } from "../src/metrics.js";
import { RabbitWorker } from "../src/worker.js";

function connection() {
  const channel = {
    assertExchange: vi.fn(async () => undefined),
    assertQueue: vi.fn(async () => undefined),
    bindQueue: vi.fn(async () => undefined),
    prefetch: vi.fn(async () => undefined),
    consume: vi.fn(async () => ({ consumerTag: "flashdrop-inventory-worker" })),
    cancel: vi.fn(async () => undefined),
    close: vi.fn(async () => undefined),
  };
  const model = Object.assign(new EventEmitter(), {
    createConfirmChannel: vi.fn(async () => channel),
    close: vi.fn(async () => undefined),
  });
  return { channel, model, client: model as unknown as ChannelModel };
}

describe("RabbitWorker startup recovery", () => {
  let worker: RabbitWorker;

  beforeEach(() => {
    vi.useFakeTimers();
    worker = new RabbitWorker(
      loadConfig({ LOG_LEVEL: "silent", MYSQL_URL: "mysql://lab:lab@mysql:3306/flashdrop" }),
      { error: vi.fn(), warn: vi.fn(), info: vi.fn() } as never,
      new WorkerMetrics(),
      {} as never,
    );
    const internals = worker as unknown as { connectKafka: () => Promise<void> };
    vi.spyOn(internals, "connectKafka").mockResolvedValue(undefined);
  });

  afterEach(async () => {
    await worker.stop();
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("allows HTTP startup while unready and reconnects after the initial failure", async () => {
    const rabbit = connection();
    const connect = vi.spyOn(amqp, "connect")
      .mockRejectedValueOnce(new Error("connect ETIMEDOUT"))
      .mockResolvedValue(rabbit.client);

    await expect(worker.start()).resolves.toBeUndefined();
    expect(worker.readiness().rabbitmq).toBe(false);
    expect(connect).toHaveBeenCalledWith(
      expect.any(Object),
      expect.objectContaining({ timeout: 5_000, keepAlive: true }),
    );
    await vi.advanceTimersByTimeAsync(999);
    expect(connect).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(connect).toHaveBeenCalledTimes(2);
    expect(worker.readiness().rabbitmq).toBe(true);
    expect(rabbit.channel.consume).toHaveBeenCalledOnce();
  });

  it("caps repeated startup retry delays at 30 seconds", async () => {
    const connect = vi.spyOn(amqp, "connect").mockRejectedValue(new Error("broker unavailable"));
    await worker.start();

    let attempts = 1;
    for (const delay of [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000]) {
      await vi.advanceTimersByTimeAsync(delay - 1);
      expect(connect).toHaveBeenCalledTimes(attempts);
      await vi.advanceTimersByTimeAsync(1);
      expect(connect).toHaveBeenCalledTimes(++attempts);
    }
    expect(worker.readiness().rabbitmq).toBe(false);
  });

  it("cancels pending startup retries on shutdown", async () => {
    const connect = vi.spyOn(amqp, "connect").mockRejectedValue(new Error("broker unavailable"));
    await worker.start();
    await worker.stop();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(connect).toHaveBeenCalledOnce();
  });

  it("closes an incomplete connection before retrying topology setup", async () => {
    const failed = connection();
    failed.channel.assertExchange.mockRejectedValueOnce(new Error("topology unavailable"));
    const recovered = connection();
    vi.spyOn(amqp, "connect")
      .mockResolvedValueOnce(failed.client)
      .mockResolvedValue(recovered.client);

    await worker.start();
    expect(worker.readiness().rabbitmq).toBe(false);
    expect(failed.model.close).toHaveBeenCalledOnce();
    expect(failed.channel.consume).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(worker.readiness().rabbitmq).toBe(true);
    expect(recovered.channel.consume).toHaveBeenCalledOnce();
  });
});
