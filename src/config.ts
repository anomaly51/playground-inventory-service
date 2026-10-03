import { z } from "zod";

const schema = z.object({
  LOG_LEVEL: z.string().default("info"),
  HOST: z.string().optional(),
  PORT: z.coerce.number().int().min(1).max(65_535).optional(),
  METRICS_HOST: z.string().default("0.0.0.0"),
  METRICS_PORT: z.coerce.number().int().min(1).max(65_535).default(3_004),
  RABBITMQ_URL: z.string().default("amqp://guest:guest@rabbitmq:5672"),
  KAFKA_BROKERS: z.string().default("kafka:9092"),
  KAFKA_CLIENT_ID: z.string().default("flashdrop-inventory-worker"),
  KAFKA_TRACE_TOPIC: z.string().default("flashdrop.traces.v1"),
  MYSQL_URL: z.string().default("mysql://lab:lab@mysql:3306/flashdrop"),
  MYSQL_POOL_SIZE: z.coerce.number().int().min(1).max(100).default(10),
  MYSQL_CONNECTION_TIMEOUT_MS: z.coerce.number().int().min(100).max(30_000).default(5_000),
  READINESS_TIMEOUT_MS: z.coerce.number().int().min(50).max(5_000).default(500),
  PREFETCH: z.coerce.number().int().min(1).max(1_000).optional(),
  MAX_ATTEMPTS: z.coerce.number().int().min(1).max(20).optional(),
  RETRY_DELAY_MS: z.coerce.number().int().min(100).max(86_400_000).optional(),
  RABBITMQ_PREFETCH: z.coerce.number().int().min(1).max(1_000).default(10),
  RABBITMQ_MAX_ATTEMPTS: z.coerce.number().int().min(1).max(20).default(3),
  RABBITMQ_RETRY_DELAY_MS: z.coerce.number().int().min(100).max(86_400_000).default(3_000),
  PROCESSING_DELAY_MS: z.coerce.number().int().min(0).max(30_000).default(150),
  RABBIT_QUEUE_TYPE: z.enum(["classic", "quorum"]).default("classic"),
  LAB_INVENTORY_RESET_ENABLED: z.enum(["true", "false", "1", "0"]).default("true"),
});

export interface WorkerConfig {
  logLevel: string;
  host: string;
  port: number;
  rabbitUrl: string;
  kafkaBrokers: string[];
  kafkaClientId: string;
  kafkaTraceTopic: string;
  mysqlUrl: string;
  mysqlPoolSize: number;
  mysqlConnectionTimeoutMs: number;
  readinessTimeoutMs: number;
  prefetch: number;
  maxAttempts: number;
  retryDelayMs: number;
  processingDelayMs: number;
  queueType: "classic" | "quorum";
  inventoryResetEnabled: boolean;
}

export function loadConfig(environment: NodeJS.ProcessEnv = process.env): WorkerConfig {
  const parsed = schema.parse(environment);
  const mysqlUrl = new URL(parsed.MYSQL_URL);
  if (mysqlUrl.protocol !== "mysql:") {
    throw new Error("MYSQL_URL must use mysql://");
  }
  if (!mysqlUrl.pathname.slice(1)) {
    throw new Error("MYSQL_URL must include a database name");
  }
  return {
    logLevel: parsed.LOG_LEVEL,
    host: parsed.HOST ?? parsed.METRICS_HOST,
    port: parsed.PORT ?? parsed.METRICS_PORT,
    rabbitUrl: parsed.RABBITMQ_URL,
    kafkaBrokers: parsed.KAFKA_BROKERS.split(",").map((entry) => entry.trim()).filter(Boolean),
    kafkaClientId: parsed.KAFKA_CLIENT_ID,
    kafkaTraceTopic: parsed.KAFKA_TRACE_TOPIC,
    mysqlUrl: parsed.MYSQL_URL,
    mysqlPoolSize: parsed.MYSQL_POOL_SIZE,
    mysqlConnectionTimeoutMs: parsed.MYSQL_CONNECTION_TIMEOUT_MS,
    readinessTimeoutMs: parsed.READINESS_TIMEOUT_MS,
    prefetch: parsed.PREFETCH ?? parsed.RABBITMQ_PREFETCH,
    maxAttempts: parsed.MAX_ATTEMPTS ?? parsed.RABBITMQ_MAX_ATTEMPTS,
    retryDelayMs: parsed.RETRY_DELAY_MS ?? parsed.RABBITMQ_RETRY_DELAY_MS,
    processingDelayMs: parsed.PROCESSING_DELAY_MS,
    queueType: parsed.RABBIT_QUEUE_TYPE,
    inventoryResetEnabled: ["true", "1"].includes(parsed.LAB_INVENTORY_RESET_ENABLED),
  };
}
