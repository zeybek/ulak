#!/usr/bin/env node
/**
 * ulak E2E Test — Webhook Receiver Server
 *
 * Records all incoming requests for verification.
 * Configurable behavior per path:
 *   /ok/*           → 200 OK
 *   /fail/*         → 500 Internal Server Error
 *   /timeout/*      → 30s delay then 200
 *   /slow/*         → 5s delay then 200
 *   /flaky/*        → 500 first 3 times, then 200
 *   /permanent/*    → 400 Bad Request (permanent error)
 *   /auth/*         → validates auth headers
 *   /echo/*         → 200 + echoes request back
 *   /api/received   → GET: returns all received requests
 *   /api/received/:tag → GET: returns requests matching tag
 *   /api/clear      → POST: clears all recorded requests
 *   /api/stats      → GET: summary statistics
 */

const http = require("http");

const received = [];         // All received requests
const flakyCounters = {};    // Track retry counts per path

const PORT = process.env.PORT || 9876;

function parseBody(req) {
  return new Promise((resolve) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });
}

function recordRequest(req, body, tag) {
  const entry = {
    id: received.length + 1,
    timestamp: new Date().toISOString(),
    method: req.method,
    url: req.url,
    tag: tag || "unknown",
    headers: { ...req.headers },
    body: body,
    bodyJson: null,
    bodySize: Buffer.byteLength(body, "utf8"),
  };
  try {
    entry.bodyJson = JSON.parse(body);
  } catch {}
  received.push(entry);
  return entry;
}

function extractTag(url) {
  // /ok/my-tag → "ok/my-tag" (full path as tag for easy filtering)
  const clean = url.replace(/^\/+/, "").replace(/\/+$/, "");
  return clean || "root";
}

const server = http.createServer(async (req, res) => {
  const body = await parseBody(req);
  const url = req.url.split("?")[0]; // strip query string
  const tag = extractTag(url);

  // ─── API endpoints ───
  if (url === "/api/received" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ count: received.length, requests: received }));
  }

  if (url.startsWith("/api/received/") && req.method === "GET") {
    const filterTag = url.replace("/api/received/", "");
    const filtered = received.filter((r) => r.tag === filterTag || r.tag.startsWith(filterTag + "/"));
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ count: filtered.length, requests: filtered }));
  }

  if (url === "/api/clear" && req.method === "POST") {
    received.length = 0;
    Object.keys(flakyCounters).forEach((k) => delete flakyCounters[k]);
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: true }));
  }

  if (url === "/api/stats" && req.method === "GET") {
    const byTag = {};
    for (const r of received) {
      byTag[r.tag] = (byTag[r.tag] || 0) + 1;
    }
    const byMethod = {};
    for (const r of received) {
      byMethod[r.method] = (byMethod[r.method] || 0) + 1;
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ total: received.length, byTag, byMethod }));
  }

  if (url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: true, received: received.length }));
  }

  // ─── Record the request ───
  const entry = recordRequest(req, body, tag);

  // ─── Behavior routing ───

  // OK — always 200
  if (url.startsWith("/ok/")) {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ status: "ok", id: entry.id }));
  }

  // FAIL — always 500
  if (url.startsWith("/fail/")) {
    res.writeHead(500, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ status: "error", message: "Simulated 500" }));
  }

  // PERMANENT — 400 (triggers DLQ)
  if (url.startsWith("/permanent/")) {
    res.writeHead(400, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ status: "error", message: "Permanent failure" }));
  }

  // TIMEOUT — 30s delay
  if (url.startsWith("/timeout/")) {
    await new Promise((r) => setTimeout(r, 30000));
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ status: "ok", delayed: true }));
  }

  // SLOW — 5s delay
  if (url.startsWith("/slow/")) {
    await new Promise((r) => setTimeout(r, 5000));
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ status: "ok", delayed: true }));
  }

  // FLAKY — fail first 3 times, then succeed
  if (url.startsWith("/flaky/")) {
    const key = url;
    flakyCounters[key] = (flakyCounters[key] || 0) + 1;
    if (flakyCounters[key] <= 3) {
      res.writeHead(500, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ status: "error", attempt: flakyCounters[key] }));
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ status: "ok", attempt: flakyCounters[key] }));
  }

  // AUTH — validate auth headers
  if (url.startsWith("/auth/")) {
    const authHeader = req.headers["authorization"] || "";
    const apiKey = req.headers["x-api-key"] || "";

    if (url.startsWith("/auth/bearer")) {
      if (!authHeader.startsWith("Bearer ")) {
        res.writeHead(401, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ error: "Missing Bearer token" }));
      }
    } else if (url.startsWith("/auth/basic")) {
      if (!authHeader.startsWith("Basic ")) {
        res.writeHead(401, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ error: "Missing Basic auth" }));
      }
    } else if (url.startsWith("/auth/apikey")) {
      if (!apiKey) {
        res.writeHead(401, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ error: "Missing X-API-Key" }));
      }
    }

    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({
      status: "ok",
      auth: { authorization: authHeader, apiKey },
    }));
  }

  // ECHO — return everything back
  if (url.startsWith("/echo/")) {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({
      method: req.method,
      url: req.url,
      headers: req.headers,
      body: entry.bodyJson || body,
      bodySize: entry.bodySize,
    }));
  }

  // Default — 200
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ status: "ok", id: entry.id }));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[webhook-server] Listening on port ${PORT}`);
  console.log(`[webhook-server] Paths: /ok/ /fail/ /timeout/ /slow/ /flaky/ /permanent/ /auth/ /echo/`);
  console.log(`[webhook-server] API: /api/received /api/received/:tag /api/clear /api/stats /health`);
});
