import amqp, {
  type ChannelModel,
  type ConfirmChannel,
  type ConsumeMessage,
  type Options,
} from "amqplib";
import {
  InventoryReservationCommandSchema,
  InventoryReservationResultSchema,
  RABBITMQ,
  TraceEventSchema,
  type InventoryReservationCommand,
  type InventoryReservationResult,
  type TraceEvent,
} from "./contracts.js";
import type { FastifyBaseLogger } from "fastify";
import { Kafka, Partitioners, logLevel as kafkaLogLevel, type Producer } from "kafkajs";
import { ulid } from "ulid";
import type { WorkerConfig } from "./config.js";
import type { WorkerMetrics } from "./metrics.js";
import { currentAttempt, failureDisposition } from "./retry.js";
import type { InventoryStorage } from "./storage.js";

function rabbitConnectionOptions(connectionUrl: string): Options.Connect {
  const url = new URL(connectionUrl);
  if (url.protocol !== "amqp:" && url.protocol !== "amqps:") {
    throw new Error("RABBITMQ_URL must use amqp:// or amqps://");
  }
  return {
    protocol: url.protocol.slice(0, -1),
    hostname: url.hostname,
    ...(url.port ? { port: Number(url.port) } : {}),
    ...(url.username || url.password
      ? {
          username: decodeURIComponent(url.username),
          password: decodeURIComponent(url.password),
        }
      : {}),
    vhost: url.pathname.length > 1 ? decodeURIComponent(url.pathname.slice(1)) : "/",
    frameMax: 131_072,
    heartbeat: 15,
  };
}

class InjectedInventoryFailure extends Error {}

function failedReservationResult(
  command: InventoryReservationCommand,
  error: string,
): InventoryReservationResult {
  return InventoryReservationResultSchema.parse({
    schemaVersion: 1,
    resultId: ulid(),
    commandId: command.commandId,
    traceId: command.traceId,
    orderId: command.orderId,
    ...(command.runId ? { runId: command.runId } : {}),
    sku: command.sku,
    quantity: command.quantity,
    status: "failed",
    reason: error.slice(0, 512),
    processedAt: new Date().toISOString(),
  });
}

export function enforceInventoryScenario(
  scenario: InventoryReservationCommand["scenario"],
  attempt: number,
): void {
  if (scenario === "inventory-retry" && attempt < 2) {
    throw new InjectedInventoryFailure("injected transient inventory failure");
  }
  if (scenario === "inventory-dlq") {
    throw new InjectedInventoryFailure("injected permanent inventory failure");
  }
}

export class RabbitWorker {
  private rabbitConnection?: ChannelModel;
  private channel?: ConfirmChannel;
  private consumerTag?: string;
  private readonly producer: Producer;
  private rabbitReady = false;
  private kafkaReady = false;
  private closing = false;
  private readonly inFlight = new Set<Promise<void>>();
  private reconnectTimer?: NodeJS.Timeout;
  private reconnectDelayMs = 1_000;
  private rabbitConnectPromise?: Promise<void>;
  private kafkaConnectPromise?: Promise<void>;
  private kafkaReconnectTimer?: NodeJS.Timeout;
  private kafkaReconnectDelayMs = 1_000;
  private readonly tracePublishes = new Set<Promise<void>>();

  constructor(
    private readonly config: WorkerConfig,
    private readonly logger: FastifyBaseLogger,
    private readonly metrics: WorkerMetrics,
    private readonly storage: InventoryStorage,
  ) {
    const kafka = new Kafka({
      clientId: config.kafkaClientId,
      brokers: config.kafkaBrokers,
      connectionTimeout: 5_000,
      requestTimeout: 10_000,
      retry: { retries: 8, initialRetryTime: 250, maxRetryTime: 5_000 },
      logLevel: kafkaLogLevel.NOTHING,
    });
    this.producer = kafka.producer({
      idempotent: true,
      maxInFlightRequests: 5,
      allowAutoTopicCreation: false,
      createPartitioner: Partitioners.DefaultPartitioner,
    });
  }

  async start(): Promise<void> {
    void this.connectKafka();
    await this.connectRabbit().catch((error: unknown) => {
      this.logger.warn(
        { err: error, retryInMs: this.reconnectDelayMs },
        "RabbitMQ unavailable at startup; reconnect scheduled",
      );
      this.scheduleReconnect();
    });
  }

  private connectKafka(): Promise<void> {
    if (this.kafkaConnectPromise) return this.kafkaConnectPromise;
    const operation = this.producer.connect()
      .then(() => {
        this.kafkaReady = true;
        this.kafkaReconnectDelayMs = 1_000;
      })
      .catch((error: unknown) => {
        this.kafkaReady = false;
        this.logger.warn({ err: error }, "Kafka trace telemetry unavailable");
        this.scheduleKafkaReconnect();
      })
      .finally(() => {
        if (this.kafkaConnectPromise === operation) this.kafkaConnectPromise = undefined;
      });
    this.kafkaConnectPromise = operation;
    return operation;
  }

  private scheduleKafkaReconnect(): void {
    if (this.closing || this.kafkaReconnectTimer) return;
    const delay = this.kafkaReconnectDelayMs;
    this.kafkaReconnectDelayMs = Math.min(this.kafkaReconnectDelayMs * 2, 30_000);
    this.kafkaReconnectTimer = setTimeout(() => {
      this.kafkaReconnectTimer = undefined;
      void this.connectKafka();
    }, delay);
    this.kafkaReconnectTimer.unref();
  }

  private connectRabbit(): Promise<void> {
    if (this.rabbitConnectPromise) return this.rabbitConnectPromise;
    const operation = this.openRabbit().finally(() => {
      if (this.rabbitConnectPromise === operation) this.rabbitConnectPromise = undefined;
    });
    this.rabbitConnectPromise = operation;
    return operation;
  }

  private async openRabbit(): Promise<void> {
    const connection = await amqp.connect(rabbitConnectionOptions(this.config.rabbitUrl), {
      clientProperties: { connection_name: "flashdrop-inventory-worker" },
      keepAlive: true,
      keepAliveDelay: 5_000,
      timeout: 5_000,
    });
    this.rabbitConnection = connection;
    connection.on("error", (error) => {
      if (this.rabbitConnection === connection) this.rabbitReady = false;
      this.logger.error({ err: error }, "RabbitMQ connection error");
    });
    connection.on("close", () => {
      if (this.rabbitConnection !== connection) return;
      this.rabbitReady = false;
      this.rabbitConnection = undefined;
      this.channel = undefined;
      this.consumerTag = undefined;
      if (!this.closing) {
        this.logger.warn("RabbitMQ connection closed; reconnect scheduled");
        this.scheduleReconnect();
      }
    });
    try {
      const channel = await connection.createConfirmChannel();
      await this.assertTopology(channel);
      await channel.prefetch(this.config.prefetch);
      this.channel = channel;
      const consumer = await channel.consume(
        RABBITMQ.queues.commands,
        (message) => this.track(message),
        { noAck: false, consumerTag: "flashdrop-inventory-worker" },
      );
      this.consumerTag = consumer.consumerTag;
      this.rabbitReady = true;
      this.reconnectDelayMs = 1_000;
    } catch (error) {
      if (this.rabbitConnection === connection) {
        this.rabbitReady = false;
        this.rabbitConnection = undefined;
        this.channel = undefined;
        this.consumerTag = undefined;
      }
      await connection.close().catch(() => undefined);
      throw error;
    }
    this.logger.info(
      { queue: RABBITMQ.queues.commands, prefetch: this.config.prefetch },
      "FlashDrop inventory worker consuming",
    );
  }

  private scheduleReconnect(): void {
    if (this.closing || this.reconnectTimer) return;
    const delay = this.reconnectDelayMs;
    this.reconnectDelayMs = Math.min(this.reconnectDelayMs * 2, 30_000);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.connectRabbit().catch((error) => {
        this.logger.error(
          { err: error, retryInMs: this.reconnectDelayMs },
          "RabbitMQ reconnect failed",
        );
        this.scheduleReconnect();
      });
    }, delay);
    this.reconnectTimer.unref();
  }

  private async assertTopology(channel: ConfirmChannel): Promise<void> {
    await Promise.all([
      channel.assertExchange(RABBITMQ.exchanges.commands, "direct", { durable: true }),
      channel.assertExchange(RABBITMQ.exchanges.retry, "direct", { durable: true }),
      channel.assertExchange(RABBITMQ.exchanges.deadLetter, "direct", { durable: true }),
      channel.assertExchange(RABBITMQ.exchanges.results, "fanout", { durable: true }),
    ]);
    const queueType = { "x-queue-type": this.config.queueType };
    await channel.assertQueue(RABBITMQ.queues.commands, {
      durable: true,
      arguments: queueType,
    });
    await channel.assertQueue(RABBITMQ.queues.retry, {
      durable: true,
      arguments: {
        ...queueType,
        "x-message-ttl": this.config.retryDelayMs,
        "x-dead-letter-exchange": RABBITMQ.exchanges.commands,
        "x-dead-letter-routing-key": RABBITMQ.routingKeys.process,
      },
    });
    await channel.assertQueue(RABBITMQ.queues.deadLetter, {
      durable: true,
      arguments: queueType,
    });
    await channel.assertQueue(RABBITMQ.queues.orderResults, {
      durable: true,
      arguments: queueType,
    });
    await Promise.all([
      channel.bindQueue(
        RABBITMQ.queues.commands,
        RABBITMQ.exchanges.commands,
        RABBITMQ.routingKeys.process,
      ),
      channel.bindQueue(
        RABBITMQ.queues.retry,
        RABBITMQ.exchanges.retry,
        RABBITMQ.routingKeys.retry,
      ),
      channel.bindQueue(
        RABBITMQ.queues.deadLetter,
        RABBITMQ.exchanges.deadLetter,
        RABBITMQ.routingKeys.deadLetter,
      ),
      channel.bindQueue(
        RABBITMQ.queues.orderResults,
        RABBITMQ.exchanges.results,
        "",
      ),
    ]);
  }

  private track(message: ConsumeMessage | null): void {
    if (!message) {
      this.rabbitReady = false;
      void this.rabbitConnection?.close().catch(() => undefined);
      return;
    }
    const operation = this.handle(message)
      .catch((error) => this.logger.error({ err: error }, "inventory command failed"))
      .finally(() => {
        this.inFlight.delete(operation);
        this.metrics.inFlight.dec();
      });
    this.metrics.inFlight.inc();
    this.inFlight.add(operation);
  }

  private async handle(message: ConsumeMessage): Promise<void> {
    const channel = this.channel;
    if (!channel) return;
    const attempt = currentAttempt(message.properties.headers);
    let command: InventoryReservationCommand;
    try {
      command = InventoryReservationCommandSchema.parse(
        JSON.parse(message.content.toString("utf8")),
      );
    } catch (error) {
      await this.deadLetterMalformed(channel, message, error);
      channel.ack(message);
      this.metrics.messages.inc({ outcome: "malformed" });
      return;
    }

    const runId = command.runId ?? this.runId(message);
    const effectiveCommand = runId && !command.runId
      ? { ...command, runId }
      : command;
    const identifiers = {
      commandId: command.commandId,
      orderId: command.orderId,
      sku: command.sku,
      quantity: command.quantity,
      attempt,
      ...(runId ? { runId } : {}),
    };
    const end = this.metrics.processingDuration.startTimer();
    await this.emitTrace({
      traceId: command.traceId,
      orderId: command.orderId,
      ...(runId ? { runId } : {}),
      source: "rabbitmq",
      target: "inventory-worker",
      transport: "rabbitmq",
      stage: "inventory.command.consume",
      status: "started",
      summary: `Inventory command delivery ${attempt}/${this.config.maxAttempts}`,
      payload: identifiers,
    });

    try {
      enforceInventoryScenario(command.scenario, attempt);
      await this.emitTrace({
        traceId: command.traceId,
        orderId: command.orderId,
        ...(runId ? { runId } : {}),
        source: "inventory-worker",
        target: "mysql",
        transport: "mysql",
        stage: "inventory.reserve",
        status: "started",
        summary: "Inventory worker started an atomic stock reservation",
        payload: identifiers,
      });
      const result = await this.storage.reserve(effectiveCommand);
      await this.emitTrace({
        traceId: command.traceId,
        orderId: command.orderId,
        ...(runId ? { runId } : {}),
        source: "mysql",
        target: "inventory-worker",
        transport: "mysql",
        stage: "inventory.reserve",
        status: "succeeded",
        summary: result.status === "reserved"
          ? "MySQL atomically reserved stock"
          : "MySQL atomically confirmed sold-out stock",
        payload: { ...identifiers, result },
      });
      await this.publishResult(channel, result);
      await this.emitTrace({
        traceId: command.traceId,
        orderId: command.orderId,
        ...(runId ? { runId } : {}),
        source: "inventory-worker",
        target: "rabbitmq",
        transport: "rabbitmq",
        stage: "inventory.result.publish",
        status: "succeeded",
        summary: "Inventory worker published a durable reservation result",
        payload: { ...identifiers, resultId: result.resultId, status: result.status },
      });
      channel.ack(message);
      this.metrics.messages.inc({ outcome: result.status });
      end({ outcome: "succeeded" });
    } catch (error) {
      await this.handleFailure(channel, message, effectiveCommand, attempt, runId, error);
      end({ outcome: "failed" });
    }
  }

  private async handleFailure(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    command: InventoryReservationCommand,
    attempt: number,
    runId: string | undefined,
    error: unknown,
  ): Promise<void> {
    const disposition = failureDisposition(attempt, this.config.maxAttempts);
    const errorMessage = error instanceof Error ? error.message : String(error);
    const identifiers = {
      commandId: command.commandId,
      orderId: command.orderId,
      attempt,
      error: errorMessage,
      ...(runId ? { runId } : {}),
    };
    await this.emitTrace({
      traceId: command.traceId,
      orderId: command.orderId,
      ...(runId ? { runId } : {}),
      source: error instanceof InjectedInventoryFailure ? "inventory-worker" : "mysql",
      target: "inventory-worker",
      transport: error instanceof InjectedInventoryFailure ? "rabbitmq" : "mysql",
      stage: "inventory.reserve",
      status: "failed",
      summary: `Inventory reservation attempt ${attempt} failed`,
      payload: identifiers,
    });
    const final = disposition.kind === "dead-letter";
    let failedResult: InventoryReservationResult | undefined;
    try {
      failedResult = await this.storage.recordFailure(
        command,
        attempt,
        errorMessage,
        final,
      );
    } catch (persistenceError) {
      // Rabbit headers are the retry source of truth. A MySQL outage must not
      // requeue the unchanged original command forever.
      this.logger.warn(
        {
          err: persistenceError,
          traceId: command.traceId,
          orderId: command.orderId,
          attempt,
        },
        "Unable to record inventory failure; continuing bounded Rabbit retry",
      );
    }

    try {
      if (disposition.kind === "retry") {
        await this.publishRaw(
          channel,
          RABBITMQ.exchanges.retry,
          RABBITMQ.routingKeys.retry,
          message.content,
          message.properties,
          { "x-attempt": disposition.nextAttemptHeader, "x-last-error": errorMessage },
        );
        await this.emitTrace({
          traceId: command.traceId,
          orderId: command.orderId,
          ...(runId ? { runId } : {}),
          source: "inventory-worker",
          target: "rabbitmq",
          transport: "rabbitmq",
          stage: "inventory.retry.publish",
          status: "retrying",
          summary: `Inventory command scheduled for retry ${attempt + 1}/${this.config.maxAttempts}`,
          payload: { ...identifiers, retryDelayMs: this.config.retryDelayMs },
        });
        this.metrics.messages.inc({ outcome: "retrying" });
      } else {
        failedResult ??= failedReservationResult(command, errorMessage);
        await this.publishRaw(
          channel,
          RABBITMQ.exchanges.deadLetter,
          RABBITMQ.routingKeys.deadLetter,
          message.content,
          message.properties,
          { "x-attempt": attempt, "x-last-error": errorMessage },
        );
        await this.publishResult(channel, failedResult);
        await this.emitTrace({
          traceId: command.traceId,
          orderId: command.orderId,
          ...(runId ? { runId } : {}),
          source: "inventory-worker",
          target: "rabbitmq-dlq",
          transport: "rabbitmq",
          stage: "inventory.dead-letter.publish",
          status: "failed",
          summary: `Inventory command moved to DLQ after ${attempt} attempts`,
          payload: { ...identifiers, resultId: failedResult.resultId },
        });
        this.metrics.messages.inc({ outcome: "dead_lettered" });
      }
      channel.ack(message);
    } catch (publishError) {
      this.logger.error(
        { err: publishError, traceId: command.traceId, orderId: command.orderId },
        "retry/DLQ/result publication failed; requeueing original",
      );
      channel.nack(message, false, true);
    }
  }

  private async deadLetterMalformed(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    error: unknown,
  ): Promise<void> {
    await this.publishRaw(
      channel,
      RABBITMQ.exchanges.deadLetter,
      RABBITMQ.routingKeys.deadLetter,
      message.content,
      message.properties,
      { "x-parse-error": error instanceof Error ? error.message : String(error) },
    );
  }

  private async publishResult(
    channel: ConfirmChannel,
    result: InventoryReservationResult,
  ): Promise<void> {
    await this.publishRaw(
      channel,
      RABBITMQ.exchanges.results,
      "",
      Buffer.from(JSON.stringify(result)),
      {
        contentType: "application/json",
        contentEncoding: "utf-8",
        correlationId: result.traceId,
        messageId: result.resultId,
        timestamp: Date.now(),
        type: "flashdrop.inventory.result.v1",
        headers: {},
      },
      {},
    );
  }

  private async publishRaw(
    channel: ConfirmChannel,
    exchange: string,
    routingKey: string,
    content: Buffer,
    properties: Options.Publish,
    extraHeaders: Record<string, unknown>,
  ): Promise<void> {
    const accepted = channel.publish(exchange, routingKey, content, {
      ...properties,
      persistent: true,
      headers: { ...properties.headers, ...extraHeaders },
    });
    if (!accepted) {
      await new Promise<void>((resolve) => channel.once("drain", resolve));
    }
    await channel.waitForConfirms();
  }

  private async emitTrace(input: Omit<TraceEvent, "id" | "timestamp">): Promise<void> {
    const event = TraceEventSchema.parse({
      ...input,
      id: ulid(),
      timestamp: new Date().toISOString(),
    });
    if (!this.kafkaReady || this.tracePublishes.size >= 1_000) return;
    const publication = this.producer.send({
        topic: this.config.kafkaTraceTopic,
        acks: -1,
        messages: [{ key: event.orderId ?? event.traceId, value: JSON.stringify(event) }],
      })
      .then(() => { this.kafkaReady = true; })
      .catch((error: unknown) => {
        this.kafkaReady = false;
        this.logger.error({ err: error, traceId: event.traceId }, "trace publication failed");
        this.scheduleKafkaReconnect();
      })
      .finally(() => { this.tracePublishes.delete(publication); });
    this.tracePublishes.add(publication);
  }

  private runId(message: ConsumeMessage): string | undefined {
    const value = message.properties.headers?.["x-run-id"];
    if (typeof value === "string" && value.length > 0) return value;
    if (Buffer.isBuffer(value) && value.length > 0) return value.toString("utf8");
    return undefined;
  }

  isReady(): boolean {
    return this.rabbitReady;
  }

  readiness(): { rabbitmq: boolean; kafka: boolean } {
    return { rabbitmq: this.rabbitReady, kafka: this.kafkaReady };
  }

  runtime(): { inFlight: number } {
    return { inFlight: this.inFlight.size };
  }

  async stop(): Promise<void> {
    this.closing = true;
    this.rabbitReady = false;
    this.kafkaReady = false;
    if (this.kafkaReconnectTimer) clearTimeout(this.kafkaReconnectTimer);
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.channel && this.consumerTag) {
      await this.channel.cancel(this.consumerTag).catch(() => undefined);
    }
    await Promise.allSettled([...this.inFlight, ...this.tracePublishes]);
    await Promise.allSettled([
      this.channel?.close(),
      this.rabbitConnection?.close(),
      this.producer.disconnect(),
    ]);
  }
}
