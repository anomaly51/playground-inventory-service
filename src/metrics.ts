import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from "@prometheus-io/client";

export class WorkerMetrics {
  readonly registry = new Registry();
  readonly messages = new Counter({
    name: "lab_rabbit_worker_messages_total",
    help: "RabbitMQ commands by outcome",
    labelNames: ["outcome"] as const,
    registers: [this.registry],
  });
  readonly processingDuration = new Histogram({
    name: "lab_rabbit_worker_processing_duration_seconds",
    help: "Worker command processing duration",
    labelNames: ["outcome"] as const,
    buckets: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
    registers: [this.registry],
  });
  readonly inFlight = new Gauge({
    name: "lab_rabbit_worker_in_flight",
    help: "Commands currently being processed",
    registers: [this.registry],
  });

  constructor() {
    collectDefaultMetrics({ register: this.registry, prefix: "lab_rabbit_worker_process_" });
  }
}
