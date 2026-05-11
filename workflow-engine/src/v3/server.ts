import { randomUUID, timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { join } from "node:path";
import type { InputEnvelope } from "../v1/core/spec.ts";
import type { VersionedRunRequest, VersionedSimulateRequest, VersionedSpecRequest } from "./contracts.ts";
import { lintVersionedSpec, runVersioned, simulateVersioned, validateVersionedSpec } from "./runtime.ts";
import { getDatabaseUrl, query } from "./postgres.ts";
import { currentTraceId, initTracing, withRootSpan, withSpan } from "../tracing.ts";
import { createRequestLimiter, RateLimitError } from "./rateLimit.ts";

const PORT = Number(process.env.PORT ?? "8080");
const DATA_DIR = process.env.DATA_DIR ?? join(process.cwd(), "data");
const MAX_BODY_BYTES = Number(process.env.MAX_BODY_BYTES ?? "1048576");
const MAX_CONCURRENT_REQUESTS = Math.max(1, Number(process.env.MAX_CONCURRENT_REQUESTS ?? "64"));
const MAX_REQUESTS_PER_MINUTE = Math.max(1, Number(process.env.MAX_REQUESTS_PER_MINUTE ?? "120"));
const V3_API_KEY = process.env.V3_API_KEY?.trim() ?? "";
const REQUIRE_V3_API_KEY = process.env.REQUIRE_V3_API_KEY !== "false";
const ENABLE_V1_HTTP = process.env.ENABLE_V1_HTTP === "true";
const requestLimiter = createRequestLimiter(MAX_REQUESTS_PER_MINUTE);

let inFlightRequests = 0;

initTracing();

export async function startServer(port = PORT, host = process.env.BIND_HOST ?? "0.0.0.0") {
  assertServerConfig();
  const server = createServer(async (req, res) => {
    if (!acquireRequestSlot()) {
      writeJson(res, 503, { ok: false, error: "overloaded", message: "too many concurrent requests" }, randomUUID());
      return;
    }

    try {
      await withRootSpan("http.request", requestAttributes(req), async () => {
        await route(req, res);
      });
    } catch (err) {
      const httpError = normalizeHttpError(err);
      writeJson(res, httpError.status, { ok: false, error: httpError.code, message: httpError.message }, currentTraceId() ?? randomUUID());
    } finally {
      releaseRequestSlot();
    }
  });

  await new Promise<void>((resolve) => {
    server.listen(port, host, resolve);
  });

  return server;
}

async function route(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? "/", "http://localhost");
  const traceId = currentTraceId() ?? randomId();

  if (req.method === "GET" && url.pathname === "/healthz") {
    writeJson(res, 200, { ok: true, service: "workflow-engine-v3" }, traceId);
    return;
  }

  if (req.method === "GET" && url.pathname === "/readyz") {
    const databaseReady = await checkDatabaseReadiness();
    writeJson(
      res,
      databaseReady ? 200 : 503,
      {
        ok: databaseReady,
        dataDir: DATA_DIR,
        maxConcurrentRequests: MAX_CONCURRENT_REQUESTS,
        authRequired: REQUIRE_V3_API_KEY,
        databaseReady
      },
      traceId
    );
    return;
  }

  const match = url.pathname.match(/^\/(v1|v2)\/(validate|lint|simulate|run)$/);
  if (!match) {
    throw new HttpError(404, "not_found", "route not found");
  }
  if (req.method !== "POST") {
    throw new HttpError(405, "method_not_allowed", "use POST for versioned actions");
  }

  if (!isAuthorized(req)) {
    throw new HttpError(401, "unauthorized", "missing or invalid API key");
  }

  const version = match[1] === "v1" ? 1 : 2;
  const action = match[2];
  await requestLimiter.check(req);
  if (version === 1 && !ENABLE_V1_HTTP) {
    throw new HttpError(410, "v1_disabled", "v1 is disabled on the public API");
  }
  const body = await withSpan("http.read_body", { route: url.pathname, version }, async () => readJsonBody(req));
  if (!body || typeof body !== "object") {
    throw new HttpError(400, "invalid_request", "request body must be a JSON object");
  }

  if (action === "validate") {
    const request = body as VersionedSpecRequest;
    const result = await withSpan("spec.validate", { route: url.pathname, version }, async () =>
      validateVersionedSpec(version, request?.spec)
    );
    writeJson(res, result.issues.some((i) => i.level === "error") ? 400 : 200, { traceId, version, result }, traceId);
    return;
  }

  if (action === "lint") {
    const request = body as VersionedSpecRequest;
    const validation = await withSpan("spec.validate", { route: url.pathname, version }, async () =>
      validateVersionedSpec(version, request?.spec)
    );
    if (validation.issues.some((i) => i.level === "error")) {
      writeJson(res, 400, { traceId, version, result: validation }, traceId);
      return;
    }
    const result = await withSpan("spec.lint", { route: url.pathname, version }, async () => lintVersionedSpec(version, validation.spec));
    writeJson(res, 200, { traceId, version, result }, traceId);
    return;
  }

  if (action === "simulate") {
    const request = body as VersionedSimulateRequest;
    const validation = await withSpan("spec.validate", { route: url.pathname, version }, async () =>
      validateVersionedSpec(version, request?.spec)
    );
    if (validation.issues.some((i) => i.level === "error")) {
      writeJson(res, 400, { traceId, version, result: validation }, traceId);
      return;
    }
    const result = await withSpan("engine.simulate", { route: url.pathname, version }, async () =>
      simulateVersioned(version, validation.spec!, normalizeInputs(request?.inputs ?? [], request?.workflowId))
    );
    writeJson(res, 200, { traceId, version, result }, traceId);
    return;
  }

  if (action === "run") {
    const request = body as VersionedRunRequest;
    const result = await withSpan("engine.run", { route: url.pathname, version }, async () =>
      runVersioned(version, request, DATA_DIR)
    );
    writeJson(res, statusForRunResult(result.body), { traceId, version, result }, traceId);
    return;
  }

  throw new HttpError(404, "not_found", "route not found");
}

function normalizeInputs(inputs: InputEnvelope[], workflowId?: string): InputEnvelope[] {
  if (!workflowId) return inputs;
  return inputs.map((input) => ({ ...input, workflowId }));
}

async function readJsonBody(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of req) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    total += buf.length;
    if (total > MAX_BODY_BYTES) {
      throw new HttpError(413, "request_too_large", "request body exceeds limit");
    }
    chunks.push(buf);
  }
  if (chunks.length === 0) return {};
  const raw = Buffer.concat(chunks).toString("utf8");
  if (!raw.trim()) return {};
  try {
    return JSON.parse(raw) as unknown;
  } catch {
    throw new HttpError(400, "invalid_json", "request body must be valid JSON");
  }
}

function writeJson(res: ServerResponse, status: number, body: unknown, traceId?: string): void {
  res.statusCode = status;
  res.setHeader("content-type", "application/json; charset=utf-8");
  res.setHeader("cache-control", "no-store");
  res.setHeader("x-content-type-options", "nosniff");
  res.setHeader("referrer-policy", "no-referrer");
  res.setHeader("permissions-policy", "geolocation=(), microphone=(), camera=()");
  res.setHeader("strict-transport-security", "max-age=15552000; includeSubDomains");
  if (traceId) {
    res.setHeader("x-trace-id", traceId);
  }
  res.end(JSON.stringify(body));
}

function randomId(): string {
  return `req_${randomUUID().replaceAll("-", "")}`;
}

function requestAttributes(req: IncomingMessage) {
  const url = new URL(req.url ?? "/", "http://localhost");
  return {
    "http.method": req.method ?? "GET",
    "http.route": url.pathname,
    "workflow.version": url.pathname.startsWith("/v1/") ? 1 : url.pathname.startsWith("/v2/") ? 2 : 3
  } as const;
}

export function isAuthorized(req: IncomingMessage): boolean {
  if (!V3_API_KEY) return !REQUIRE_V3_API_KEY;
  const provided = requestApiKey(req);
  if (!provided) return false;
  return secureEqual(provided, V3_API_KEY);
}

function requestApiKey(req: IncomingMessage): string | undefined {
  const auth = req.headers.authorization?.trim();
  if (auth?.toLowerCase().startsWith("bearer ")) {
    return auth.slice(7).trim();
  }
  const header = req.headers["x-api-key"];
  if (Array.isArray(header)) return header[0]?.trim();
  return header?.trim();
}

function secureEqual(left: string, right: string): boolean {
  const a = Buffer.from(left);
  const b = Buffer.from(right);
  if (a.length !== b.length) return false;
  return timingSafeEqual(a, b);
}

function statusForRunResult(body: unknown): number {
  if (!body || typeof body !== "object") return 200;
  const result = body as { committed?: boolean; deduped?: boolean; rejected?: boolean; reason?: string; issues?: unknown[] };
  if (Array.isArray(result.issues) && result.issues.some((issue) => typeof issue === "object" && issue !== null && "level" in issue && (issue as { level?: string }).level === "error")) {
    return 400;
  }
  if (result.committed || result.deduped) return 200;
  if (result.rejected) {
    if (result.reason === "invalid_spec" || result.reason === "missing_workflow_id") return 400;
    if (result.reason === "store-append-failed") return 503;
    if (result.reason === "spec-mismatch") return 409;
    return 422;
  }
  return 200;
}

function acquireRequestSlot(): boolean {
  if (inFlightRequests >= MAX_CONCURRENT_REQUESTS) return false;
  inFlightRequests += 1;
  return true;
}

function releaseRequestSlot(): void {
  inFlightRequests = Math.max(0, inFlightRequests - 1);
}

class HttpError extends Error {
  status: number;
  code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
    this.name = "HttpError";
  }
}

function normalizeHttpError(err: unknown): HttpError {
  if (err instanceof HttpError) return err;
  if (err instanceof RateLimitError) return new HttpError(429, "rate_limited", err.message);
  if (err instanceof SyntaxError) return new HttpError(400, "invalid_json", err.message);
  return new HttpError(500, "internal_error", String(err));
}

function assertServerConfig(): void {
  if (REQUIRE_V3_API_KEY && !V3_API_KEY) {
    throw new Error("V3_API_KEY is required for public v3 traffic");
  }
}

async function checkDatabaseReadiness(): Promise<boolean> {
  const databaseUrl = getDatabaseUrl();
  if (!databaseUrl) return true;
  try {
    await query("select 1", [], databaseUrl);
    return true;
  } catch {
    return false;
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await startServer();
}
