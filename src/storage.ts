import {
  InventoryReservationResultSchema,
  InventoryItemSchema,
  type FlashDropSku,
  type InventoryItem,
  type InventoryReservationCommand,
  type InventoryReservationResult,
} from "./contracts.js";
import { createPool, type Pool, type PoolConnection, type RowDataPacket } from "mysql2/promise";
import { ulid } from "ulid";
import type { WorkerConfig } from "./config.js";

export interface InventoryRuntime {
  reservations: number;
  availableStock: number;
}

export interface InventoryStorage {
  start(): Promise<void>;
  stop(): Promise<void>;
  isReady(timeoutMs: number): Promise<boolean>;
  reserve(command: InventoryReservationCommand): Promise<InventoryReservationResult>;
  recordFailure(
    command: InventoryReservationCommand,
    attempt: number,
    error: string,
    final: boolean,
  ): Promise<InventoryReservationResult | undefined>;
  getRuntime(): Promise<InventoryRuntime>;
  listInventory(): Promise<InventoryItem[]>;
  resetInventory(
    items?: readonly { sku: FlashDropSku; available: number }[],
  ): Promise<InventoryItem[]>;
}

const CREATE_INVENTORY = `
  CREATE TABLE IF NOT EXISTS inventory (
    sku VARCHAR(64) NOT NULL,
    available INT UNSIGNED NOT NULL,
    initial_stock INT UNSIGNED NOT NULL,
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
      ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (sku),
    CONSTRAINT inventory_available_nonnegative CHECK (available >= 0)
  ) ENGINE=InnoDB
`;

const CREATE_RESERVATIONS = `
  CREATE TABLE IF NOT EXISTS reservations (
    reservation_id CHAR(26) NOT NULL,
    command_id CHAR(26) NOT NULL,
    order_id CHAR(26) NOT NULL,
    trace_id CHAR(26) NOT NULL,
    sku VARCHAR(64) NOT NULL,
    quantity INT UNSIGNED NOT NULL,
    status VARCHAR(32) NOT NULL,
    remaining_stock INT UNSIGNED,
    reason VARCHAR(512),
    result_json JSON NOT NULL,
    processed_at DATETIME(3) NOT NULL,
    PRIMARY KEY (reservation_id),
    UNIQUE KEY reservations_command_id_uq (command_id),
    UNIQUE KEY reservations_order_id_uq (order_id),
    INDEX reservations_trace_id_idx (trace_id),
    INDEX reservations_processed_at_idx (processed_at)
  ) ENGINE=InnoDB
`;

const CREATE_INBOX = `
  CREATE TABLE IF NOT EXISTS inbox (
    command_id CHAR(26) NOT NULL,
    order_id CHAR(26) NOT NULL,
    trace_id CHAR(26) NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempts INT UNSIGNED NOT NULL DEFAULT 0,
    result_json JSON,
    last_error VARCHAR(512),
    received_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
      ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (command_id),
    INDEX inbox_order_id_idx (order_id),
    INDEX inbox_status_updated_at_idx (status, updated_at)
  ) ENGINE=InnoDB
`;

const SEED_INVENTORY = `
  INSERT INTO inventory (sku, available, initial_stock) VALUES
    ('DROP-SNEAKER-RED', 25, 25),
    ('DROP-HOODIE-BLACK', 40, 40),
    ('DROP-CAP-LIME', 60, 60)
  ON DUPLICATE KEY UPDATE sku = VALUES(sku)
`;

function poolConfig(config: WorkerConfig) {
  const url = new URL(config.mysqlUrl);
  return {
    host: url.hostname,
    port: url.port ? Number(url.port) : 3_306,
    user: decodeURIComponent(url.username),
    password: decodeURIComponent(url.password),
    database: decodeURIComponent(url.pathname.slice(1)),
    connectionLimit: config.mysqlPoolSize,
    connectTimeout: config.mysqlConnectionTimeoutMs,
    waitForConnections: true,
    queueLimit: 0,
    enableKeepAlive: true,
    keepAliveInitialDelay: 0,
    timezone: "Z",
  } as const;
}

interface JsonRow extends RowDataPacket {
  result_json: string | InventoryReservationResult | null;
}

interface StockRow extends RowDataPacket {
  available: number;
}

interface CountRow extends RowDataPacket {
  reservations: string | number;
  available_stock: string | number;
}

interface InventoryRow extends RowDataPacket {
  sku: FlashDropSku;
  available: number;
  reserved: number;
  updated_at: Date | string;
}

const DEFAULT_INVENTORY: Readonly<Record<FlashDropSku, number>> = {
  "DROP-SNEAKER-RED": 25,
  "DROP-HOODIE-BLACK": 40,
  "DROP-CAP-LIME": 60,
};

function parseStoredResult(value: JsonRow["result_json"]): InventoryReservationResult | undefined {
  if (value === null) return undefined;
  const document = typeof value === "string" ? JSON.parse(value) : value;
  return InventoryReservationResultSchema.parse(document);
}

function isDuplicateEntry(error: unknown): boolean {
  return typeof error === "object" && error !== null
    && "code" in error && error.code === "ER_DUP_ENTRY";
}

export class MySqlInventoryStorage implements InventoryStorage {
  private readonly pool: Pool;
  private started = false;
  private ready = false;

  constructor(config: WorkerConfig, pool?: Pool) {
    this.pool = pool ?? createPool(poolConfig(config));
  }

  async start(): Promise<void> {
    await this.pool.query(CREATE_INVENTORY);
    await this.pool.query(CREATE_RESERVATIONS);
    await this.pool.query(CREATE_INBOX);
    await this.pool.query(SEED_INVENTORY);
    this.started = true;
    this.ready = true;
  }

  async reserve(command: InventoryReservationCommand): Promise<InventoryReservationResult> {
    try {
      return await this.reserveOnce(command);
    } catch (error) {
      if (!isDuplicateEntry(error)) throw error;
      // A concurrent delivery for the same command/order may win the unique-key
      // race. reserveOnce has already rolled back and released its connection,
      // so a fresh transaction can safely return the durable winner.
      return this.reserveOnce(command);
    }
  }

  private async reserveOnce(
    command: InventoryReservationCommand,
  ): Promise<InventoryReservationResult> {
    const connection = await this.pool.getConnection();
    try {
      await connection.beginTransaction();
      const existing = await this.findExisting(connection, command);
      if (existing) {
        await connection.commit();
        this.ready = true;
        return existing;
      }

      await connection.execute(
        `INSERT INTO inbox (
           command_id, order_id, trace_id, status, attempts, last_error
         ) VALUES (?, ?, ?, 'processing', 1, NULL)
         ON DUPLICATE KEY UPDATE
           attempts = attempts + 1,
           status = IF(result_json IS NULL, 'processing', status),
           last_error = IF(result_json IS NULL, NULL, last_error)`,
        [command.commandId, command.orderId, command.traceId],
      );

      const [decrement] = await connection.execute(
        `UPDATE inventory
         SET available = available - ?
         WHERE sku = ? AND available >= ?`,
        [command.quantity, command.sku, command.quantity],
      );
      const affectedRows = Number((decrement as { affectedRows?: number }).affectedRows ?? 0);
      const [stockRows] = await connection.query<StockRow[]>(
        "SELECT available FROM inventory WHERE sku = ? FOR UPDATE",
        [command.sku],
      );
      if (!stockRows[0]) throw new Error(`inventory sku ${command.sku} is not seeded`);
      const remainingStock = Number(stockRows[0].available);
      const status = affectedRows === 1 ? "reserved" : "sold_out";
      const reason = status === "sold_out" ? "insufficient inventory" : undefined;
      const result = InventoryReservationResultSchema.parse({
        schemaVersion: 1,
        resultId: ulid(),
        commandId: command.commandId,
        traceId: command.traceId,
        orderId: command.orderId,
        ...(command.runId ? { runId: command.runId } : {}),
        sku: command.sku,
        quantity: command.quantity,
        status,
        remainingStock,
        ...(reason ? { reason } : {}),
        processedAt: new Date().toISOString(),
      });
      await this.insertReservation(connection, command, result);
      await connection.execute(
        `UPDATE inbox
         SET status = 'completed', result_json = ?, last_error = NULL
         WHERE command_id = ?`,
        [JSON.stringify(result), command.commandId],
      );
      await connection.commit();
      this.ready = true;
      return result;
    } catch (error) {
      await connection.rollback().catch(() => undefined);
      this.ready = false;
      throw error;
    } finally {
      connection.release();
    }
  }

  async recordFailure(
    command: InventoryReservationCommand,
    attempt: number,
    error: string,
    final: boolean,
  ): Promise<InventoryReservationResult | undefined> {
    const connection = await this.pool.getConnection();
    try {
      await connection.beginTransaction();
      const existing = await this.findExisting(connection, command);
      if (existing) {
        await connection.commit();
        return existing;
      }

      let result: InventoryReservationResult | undefined;
      if (final) {
        result = InventoryReservationResultSchema.parse({
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
        await this.insertReservation(connection, command, result);
      }
      await connection.execute(
        `INSERT INTO inbox (
           command_id, order_id, trace_id, status, attempts, result_json, last_error
         ) VALUES (?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           attempts = GREATEST(attempts, VALUES(attempts)),
           status = IF(result_json IS NULL, VALUES(status), status),
           result_json = COALESCE(result_json, VALUES(result_json)),
           last_error = IF(result_json IS NULL, VALUES(last_error), last_error)`,
        [
          command.commandId,
          command.orderId,
          command.traceId,
          final ? "dead_lettered" : "retrying",
          attempt,
          result ? JSON.stringify(result) : null,
          error.slice(0, 512),
        ],
      );
      await connection.commit();
      this.ready = true;
      return result;
    } catch (failure) {
      await connection.rollback().catch(() => undefined);
      this.ready = false;
      if (final && isDuplicateEntry(failure)) {
        const [rows] = await this.pool.query<JsonRow[]>(
          "SELECT result_json FROM reservations WHERE order_id = ? LIMIT 1",
          [command.orderId],
        );
        const existing = rows[0] ? parseStoredResult(rows[0].result_json) : undefined;
        if (existing) return existing;
      }
      throw failure;
    } finally {
      connection.release();
    }
  }

  private async findExisting(
    connection: PoolConnection,
    command: InventoryReservationCommand,
  ): Promise<InventoryReservationResult | undefined> {
    const [inboxRows] = await connection.query<JsonRow[]>(
      "SELECT result_json FROM inbox WHERE command_id = ? FOR UPDATE",
      [command.commandId],
    );
    const inboxResult = inboxRows[0]
      ? parseStoredResult(inboxRows[0].result_json)
      : undefined;
    if (inboxResult) return inboxResult;

    const [reservationRows] = await connection.query<JsonRow[]>(
      "SELECT result_json FROM reservations WHERE order_id = ? FOR UPDATE",
      [command.orderId],
    );
    return reservationRows[0]
      ? parseStoredResult(reservationRows[0].result_json)
      : undefined;
  }

  private async insertReservation(
    connection: PoolConnection,
    command: InventoryReservationCommand,
    result: InventoryReservationResult,
  ): Promise<void> {
    await connection.execute(
      `INSERT INTO reservations (
         reservation_id, command_id, order_id, trace_id, sku, quantity,
         status, remaining_stock, reason, result_json, processed_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        result.resultId,
        command.commandId,
        command.orderId,
        command.traceId,
        command.sku,
        command.quantity,
        result.status,
        result.remainingStock ?? null,
        result.reason ?? null,
        JSON.stringify(result),
        new Date(result.processedAt),
      ],
    );
  }

  async getRuntime(): Promise<InventoryRuntime> {
    const [rows] = await this.pool.query<CountRow[]>(
      `SELECT
         (SELECT COUNT(*) FROM reservations) AS reservations,
         (SELECT COALESCE(SUM(available), 0) FROM inventory) AS available_stock`,
    );
    return {
      reservations: Number(rows[0]?.reservations ?? 0),
      availableStock: Number(rows[0]?.available_stock ?? 0),
    };
  }

  async listInventory(): Promise<InventoryItem[]> {
    const [rows] = await this.pool.query<InventoryRow[]>(
      `SELECT sku, available,
              GREATEST(initial_stock - available, 0) AS reserved,
              updated_at
       FROM inventory
       ORDER BY sku`,
    );
    return rows.map((row) => InventoryItemSchema.parse({
      sku: row.sku,
      available: Number(row.available),
      reserved: Number(row.reserved),
      updatedAt: new Date(row.updated_at).toISOString(),
    }));
  }

  async resetInventory(
    items: readonly { sku: FlashDropSku; available: number }[] = [],
  ): Promise<InventoryItem[]> {
    const requested = new Map<FlashDropSku, number>(
      Object.entries(DEFAULT_INVENTORY) as [FlashDropSku, number][],
    );
    for (const item of items) requested.set(item.sku, item.available);

    const connection = await this.pool.getConnection();
    try {
      await connection.beginTransaction();
      await connection.query("DELETE FROM inbox");
      await connection.query("DELETE FROM reservations");
      for (const [sku, available] of requested) {
        await connection.execute(
          `INSERT INTO inventory (sku, available, initial_stock)
           VALUES (?, ?, ?)
           ON DUPLICATE KEY UPDATE
             available = VALUES(available),
             initial_stock = VALUES(initial_stock),
             updated_at = CURRENT_TIMESTAMP(3)`,
          [sku, available, available],
        );
      }
      await connection.commit();
      this.ready = true;
    } catch (error) {
      await connection.rollback().catch(() => undefined);
      this.ready = false;
      throw error;
    } finally {
      connection.release();
    }
    return this.listInventory();
  }

  async isReady(timeoutMs: number): Promise<boolean> {
    if (!this.started) return false;
    try {
      await this.pool.query({ sql: "SELECT 1", timeout: timeoutMs });
      this.ready = true;
      return true;
    } catch {
      this.ready = false;
      return false;
    }
  }

  async stop(): Promise<void> {
    this.started = false;
    this.ready = false;
    await this.pool.end();
  }
}

// Transitional type aliases for callers compiled before the FlashDrop migration.
export type WorkerResultStorage = InventoryStorage;
export { MySqlInventoryStorage as MySqlWorkerResultStorage };
