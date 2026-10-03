export type FailureDisposition =
  | { kind: "retry"; nextAttemptHeader: number }
  | { kind: "dead-letter"; attempts: number };

export function currentAttempt(headers: Record<string, unknown> | undefined): number {
  const raw = headers?.["x-attempt"];
  const parsed = typeof raw === "number" ? raw : Number(raw ?? 0);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed + 1 : 1;
}

export function failureDisposition(
  attempt: number,
  maxAttempts: number,
): FailureDisposition {
  return attempt >= maxAttempts
    ? { kind: "dead-letter", attempts: attempt }
    : { kind: "retry", nextAttemptHeader: attempt };
}
