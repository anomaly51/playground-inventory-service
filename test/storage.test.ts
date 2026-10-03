import type { InventoryReservationCommand, InventoryReservationResult } from "../src/contracts.js";
import type { Pool, PoolConnection } from "mysql2/promise";
import { describe, expect, it, vi } from "vitest";
import { loadConfig } from "../src/config.js";
import { MySqlInventoryStorage } from "../src/storage.js";

const command: InventoryReservationCommand = {
  schemaVersion: 1,
  commandId: "01ARZ3NDEKTSV4RRFFQ69G5FAA",
  traceId: "01ARZ3NDEKTSV4RRFFQ69G5FAB",
  orderId: "01ARZ3NDEKTSV4RRFFQ69G5FAC",
  runId: "load-1",
  sku: "DROP-CAP-LIME",
  quantity: 2,
  scenario: "normal",
  createdAt: "2026-08-30T10:00:00.000Z",
};

function config() {
  return loadConfig({ MYSQL_URL: "mysql://lab:lab@mysql:3306/flashdrop" });
}

describe("MySqlInventoryStorage", () => {
  it("creates inventory, reservations and inbox tables", async () => {
    const query = vi.fn(async () => [[], []]);
    const end = vi.fn(async () => undefined);
    const pool = { query, end } as unknown as Pool;
    const storage = new MySqlInventoryStorage(config(), pool);

    await storage.start();

    const statements = query.mock.calls.map(([sql]) => String(sql));
    expect(statements).toHaveLength(4);
    expect(statements.some((sql) => sql.includes("CREATE TABLE IF NOT EXISTS inventory"))).toBe(true);
    expect(statements.some((sql) => sql.includes("CREATE TABLE IF NOT EXISTS reservations"))).toBe(true);
    expect(statements.some((sql) => sql.includes("CREATE TABLE IF NOT EXISTS inbox"))).toBe(true);
    expect(statements.some((sql) => sql.includes("INSERT INTO inventory"))).toBe(true);
    await expect(storage.isReady(100)).resolves.toBe(true);
    await storage.stop();
    expect(end).toHaveBeenCalledOnce();
  });

  it("uses one transaction and a conditional decrement that cannot oversell", async () => {
    const transactionOrder: string[] = [];
    const execute = vi.fn(async (sql: string) => {
      if (sql.includes("UPDATE inventory")) return [{ affectedRows: 1 }, []];
      return [{ affectedRows: 1 }, []];
    });
    const query = vi.fn(async (sql: string) => {
      if (sql.includes("FROM inbox")) return [[], []];
      if (sql.includes("FROM reservations")) return [[], []];
      if (sql.includes("FROM inventory")) return [[{ available: 58 }], []];
      return [[], []];
    });
    const connection = {
      beginTransaction: vi.fn(async () => { transactionOrder.push("begin"); }),
      execute,
      query,
      commit: vi.fn(async () => { transactionOrder.push("commit"); }),
      rollback: vi.fn(async () => { transactionOrder.push("rollback"); }),
      release: vi.fn(() => { transactionOrder.push("release"); }),
    } as unknown as PoolConnection;
    const pool = {
      getConnection: vi.fn(async () => connection),
    } as unknown as Pool;
    const storage = new MySqlInventoryStorage(config(), pool);

    const reserved = await storage.reserve(command);

    expect(reserved).toMatchObject({
      commandId: command.commandId,
      orderId: command.orderId,
      runId: "load-1",
      status: "reserved",
      remainingStock: 58,
    });
    const decrement = execute.mock.calls.find(([sql]) => String(sql).includes("UPDATE inventory"));
    expect(decrement?.[0]).toContain("available >= ?");
    expect(decrement?.[1]).toEqual([2, "DROP-CAP-LIME", 2]);
    expect(transactionOrder).toEqual(["begin", "commit", "release"]);
    expect(execute.mock.calls.some(([sql]) => String(sql).includes("INSERT INTO reservations"))).toBe(true);
    expect(execute.mock.calls.some(([sql]) => String(sql).includes("UPDATE inbox"))).toBe(true);
  });

  it("returns a durable duplicate without decrementing stock again", async () => {
    const existing: InventoryReservationResult = {
      schemaVersion: 1,
      resultId: "01ARZ3NDEKTSV4RRFFQ69G5FAD",
      commandId: command.commandId,
      traceId: command.traceId,
      orderId: command.orderId,
      runId: command.runId,
      sku: command.sku,
      quantity: command.quantity,
      status: "reserved",
      remainingStock: 58,
      processedAt: "2026-08-30T10:00:01.000Z",
    };
    const execute = vi.fn();
    const connection = {
      beginTransaction: vi.fn(),
      query: vi.fn(async (sql: string) => {
        if (sql.includes("FROM inbox")) return [[{ result_json: JSON.stringify(existing) }], []];
        return [[], []];
      }),
      execute,
      commit: vi.fn(),
      rollback: vi.fn(),
      release: vi.fn(),
    } as unknown as PoolConnection;
    const pool = { getConnection: vi.fn(async () => connection) } as unknown as Pool;
    const storage = new MySqlInventoryStorage(config(), pool);

    await expect(storage.reserve(command)).resolves.toEqual(existing);
    expect(execute).not.toHaveBeenCalled();
    expect(connection.commit).toHaveBeenCalledOnce();
    expect(connection.release).toHaveBeenCalledOnce();
  });

  it("retries a unique-key race only after releasing the first connection", async () => {
    const calls: string[] = [];
    const existing: InventoryReservationResult = {
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
    };
    const duplicate = Object.assign(new Error("duplicate"), { code: "ER_DUP_ENTRY" });
    const first = {
      beginTransaction: vi.fn(),
      query: vi.fn(async (sql: string) => {
        if (sql.includes("FROM inventory")) return [[{ available: 58 }], []];
        return [[], []];
      }),
      execute: vi.fn(async (sql: string) => {
        if (sql.includes("UPDATE inventory")) return [{ affectedRows: 1 }, []];
        if (sql.includes("INSERT INTO reservations")) throw duplicate;
        return [{ affectedRows: 1 }, []];
      }),
      commit: vi.fn(),
      rollback: vi.fn(async () => undefined),
      release: vi.fn(() => { calls.push("release-first"); }),
    } as unknown as PoolConnection;
    const second = {
      beginTransaction: vi.fn(),
      query: vi.fn(async (sql: string) => {
        if (sql.includes("FROM inbox")) return [[{ result_json: null }], []];
        return [[{ result_json: existing }], []];
      }),
      execute: vi.fn(),
      commit: vi.fn(),
      rollback: vi.fn(),
      release: vi.fn(),
    } as unknown as PoolConnection;
    const connections = [first, second];
    const pool = {
      getConnection: vi.fn(async () => {
        calls.push(`get-${calls.length}`);
        return connections.shift()!;
      }),
    } as unknown as Pool;
    const storage = new MySqlInventoryStorage(config(), pool);

    await expect(storage.reserve(command)).resolves.toEqual(existing);
    expect(calls.indexOf("release-first")).toBeLessThan(calls.lastIndexOf("get-2"));
    expect(second.execute).not.toHaveBeenCalled();
  });

  it("requires a mysql URL with a database name", () => {
    expect(() => loadConfig({ MYSQL_URL: "postgresql://lab:lab@postgres/flashdrop" }))
      .toThrow("MYSQL_URL must use mysql://");
    expect(() => loadConfig({ MYSQL_URL: "mysql://lab:lab@mysql" }))
      .toThrow("MYSQL_URL must include a database name");
  });
});
