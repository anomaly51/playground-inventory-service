import { createApp } from "./app.js";
import { loadConfig } from "./config.js";
import { WorkerMetrics } from "./metrics.js";
import { RabbitWorker } from "./worker.js";
import { MySqlInventoryStorage } from "./storage.js";
import pino from "pino";

const config = loadConfig();
const metrics = new WorkerMetrics();
const bootstrap = pino({ name: "rabbit-worker", level: config.logLevel });
const storage = new MySqlInventoryStorage(config);
const worker = new RabbitWorker(config, bootstrap, metrics, storage);
await storage.start();
await worker.start();
const app = await createApp({ config, worker, storage, metrics });
let shuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  app.log.info({ signal }, "graceful shutdown started");
  const forceExit = setTimeout(() => process.exit(1), 15_000).unref();
  await app.close();
  await worker.stop();
  await storage.stop();
  clearTimeout(forceExit);
  process.exit(0);
}

process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));
await app.listen({ host: config.host, port: config.port });
