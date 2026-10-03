import { describe, expect, it } from "vitest";
import { currentAttempt, failureDisposition } from "../src/retry.js";

describe("retry policy", () => {
  it("counts the original delivery as attempt one", () => {
    expect(currentAttempt(undefined)).toBe(1);
    expect(currentAttempt({ "x-attempt": 1 })).toBe(2);
  });

  it("retries until the configured final attempt", () => {
    expect(failureDisposition(1, 3)).toEqual({ kind: "retry", nextAttemptHeader: 1 });
    expect(failureDisposition(2, 3)).toEqual({ kind: "retry", nextAttemptHeader: 2 });
    expect(failureDisposition(3, 3)).toEqual({ kind: "dead-letter", attempts: 3 });
  });
});
