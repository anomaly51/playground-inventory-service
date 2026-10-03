import { describe, expect, it } from "vitest";
import {
  InventoryListResponseSchema,
  InventoryReservationCommandSchema,
  InventoryReservationResultSchema,
  InventoryResetRequestSchema,
  TraceEventSchema,
} from "../src/contracts.js";

const command = {
  schemaVersion: 1,
  commandId: "01ARZ3NDEKTSV4RRFFQ69G5FAA",
  traceId: "01ARZ3NDEKTSV4RRFFQ69G5FAB",
  orderId: "01ARZ3NDEKTSV4RRFFQ69G5FAC",
  runId: "contract-test",
  sku: "DROP-CAP-LIME",
  quantity: 2,
  scenario: "normal",
  createdAt: "2026-08-30T10:00:00.000Z",
};

describe("inventory service wire contracts", () => {
  it("accepts a v1 command without changing correlation or routing fields", () => {
    expect(InventoryReservationCommandSchema.parse(command)).toEqual(command);
  });

  it.each([
    { schemaVersion: 2 },
    { quantity: 0 },
    { quantity: 6 },
    { quantity: "2" },
    { sku: "UNKNOWN" },
    { scenario: "unknown" },
    { commandId: "invalid" },
    { extra: true },
  ])("rejects an incompatible command: %j", (override) => {
    expect(InventoryReservationCommandSchema.safeParse({ ...command, ...override }).success).toBe(false);
  });

  it.each(["reserved", "sold_out", "failed"])("preserves the %s result envelope", (status) => {
    const result = {
      schemaVersion: 1,
      resultId: "01ARZ3NDEKTSV4RRFFQ69G5FAD",
      commandId: command.commandId,
      traceId: command.traceId,
      orderId: command.orderId,
      runId: command.runId,
      sku: command.sku,
      quantity: command.quantity,
      status,
      remainingStock: 58,
      processedAt: "2026-08-30T10:00:01.000Z",
    };
    expect(InventoryReservationResultSchema.parse(result)).toEqual(result);
    expect(InventoryReservationResultSchema.safeParse({ ...result, remainingStock: -1 }).success).toBe(false);
  });

  it("retains inventory response validation and reset limits", () => {
    const items = [{
      sku: command.sku, available: 58, reserved: 2, updatedAt: command.createdAt,
    }];
    expect(InventoryListResponseSchema.parse({ items })).toEqual({ items });
    expect(InventoryListResponseSchema.safeParse({ items: [{ ...items[0], available: -1 }] }).success).toBe(false);
    expect(InventoryResetRequestSchema.parse({})).toEqual({});
    expect(InventoryResetRequestSchema.parse({
      items: [{ sku: command.sku, available: 60 }],
    })).toEqual({ items: [{ sku: command.sku, available: 60 }] });
    expect(InventoryResetRequestSchema.safeParse({
      items: [{ sku: command.sku, available: 1_000_001 }],
    }).success).toBe(false);
  });

  it("preserves trace fields consumed by event-hub", () => {
    const trace = {
      id: "01ARZ3NDEKTSV4RRFFQ69G5FAE",
      traceId: command.traceId,
      orderId: command.orderId,
      runId: command.runId,
      correlationId: command.commandId,
      causationId: command.commandId,
      timestamp: command.createdAt,
      source: "inventory-worker",
      target: "mysql",
      stage: "inventory.reserve",
      status: "succeeded",
      transport: "mysql",
      summary: "Inventory reserved",
      payload: { quantity: command.quantity },
    };
    expect(TraceEventSchema.parse(trace)).toEqual(trace);
  });
});
