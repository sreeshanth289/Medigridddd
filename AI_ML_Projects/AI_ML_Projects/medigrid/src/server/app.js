/**
 * MediGrid - High Performance Stateless Application Instance
 * Built with native Node.js HTTP & SSE (Server-Sent Events) for real-time reactivity.
 * Can run as Instance 1 (port 3001), Instance 2 (port 3002), or standalone.
 */

const http = require('node:http');
const url = require('node:url');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const { getDatabase } = require('../database/db');
const { getRedis } = require('../middleware/redis_engine');
const { getRabbitMQ } = require('../middleware/rabbitmq_engine');
const { getBedTrackingService } = require('../services/bed_tracking_service');
const { getTransferSagaService } = require('../services/transfer_saga_service');
const { getNotificationService } = require('../services/notification_service');
const { getAuditService } = require('../services/audit_service');

function createServerInstance({ instanceId = 'instance-1', port = 3001 } = {}) {
  const db = getDatabase();
  const redis = getRedis();
  const rmq = getRabbitMQ();
  const bedTracker = getBedTrackingService();
  const saga = getTransferSagaService();
  const notifs = getNotificationService();
  const audit = getAuditService();

  // Connected SSE clients for real-time live map & console fan-out
  const sseClients = new Set();

  function broadcastSSE(eventType, data) {
    const payload = `event: ${eventType}\ndata: ${JSON.stringify(data)}\n\n`;
    for (const res of sseClients) {
      try {
        res.write(payload);
      } catch (e) {
        sseClients.delete(res);
      }
    }
  }

  // Hook RabbitMQ events to SSE broadcast
  rmq.on('message_published', ({ routingKey, message }) => {
    broadcastSSE('event_published', { routingKey, message, instanceId });
  });

  rmq.on('message_dead_lettered', (dlqEntry) => {
    broadcastSSE('dead_letter_added', dlqEntry);
  });

  notifs.on('alert', (alert) => {
    broadcastSSE('incoming_alert', alert);
  });

  const server = http.createServer(async (req, res) => {
    // Enable CORS and Security Headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Client-Id, X-Idempotency-Key');
    res.setHeader('X-Served-By', instanceId);

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      return res.end();
    }

    const parsedUrl = url.parse(req.url, true);
    const pathname = parsedUrl.pathname;

    // Helper: JSON response
    const sendJson = (statusCode, data) => {
      res.writeHead(statusCode, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data));
    };

    // Helper: Parse JSON body
    const parseBody = () => new Promise((resolve, reject) => {
      let body = '';
      req.on('data', chunk => { body += chunk; });
      req.on('end', () => {
        try {
          resolve(body ? JSON.parse(body) : {});
        } catch (e) {
          reject(e);
        }
      });
      req.on('error', reject);
    });

    try {
      // 1. Static Files (SPA Web Frontend)
      if (req.method === 'GET' && (pathname === '/' || pathname === '/index.html' || pathname.startsWith('/public/'))) {
        let filePath = pathname === '/' || pathname === '/index.html'
          ? path.join(__dirname, '..', 'public', 'index.html')
          : path.join(__dirname, '..', 'public', pathname.replace('/public/', ''));

        if (fs.existsSync(filePath)) {
          const ext = path.extname(filePath);
          const mimeTypes = {
            '.html': 'text/html',
            '.js': 'application/javascript',
            '.css': 'text/css',
            '.json': 'application/json',
            '.png': 'image/png',
            '.svg': 'image/svg+xml'
          };
          res.writeHead(200, { 'Content-Type': mimeTypes[ext] || 'text/plain' });
          return fs.createReadStream(filePath).pipe(res);
        }
      }

      // 2. Real-Time Server-Sent Events (SSE) Stream
      if (req.method === 'GET' && pathname === '/api/stream') {
        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive',
          'X-Accel-Buffering': 'no'
        });
        res.write(`data: ${JSON.stringify({ type: 'connected', instanceId, timestamp: Date.now() })}\n\n`);
        sseClients.add(res);

        // Keep-alive heartbeat every 15s
        const heartbeat = setInterval(() => {
          res.write(`: heartbeat\n\n`);
        }, 15000);

        req.on('close', () => {
          clearInterval(heartbeat);
          sseClients.delete(res);
        });
        return;
      }

      // 3. Health & Readiness Probe
      if (req.method === 'GET' && pathname === '/api/health') {
        return sendJson(200, {
          status: 'UP',
          instanceId,
          port,
          timestamp: Date.now(),
          uptime: process.uptime(),
          memory: process.memoryUsage(),
          sseConnectedClients: sseClients.size
        });
      }

      // 4. City-Wide Bed Aggregates & Latency
      if (req.method === 'GET' && pathname === '/api/city-stats') {
        const stats = db.getCityStats();
        const latency = redis.getLatencyMetrics();
        return sendJson(200, {
          ...stats,
          latencyMetrics: latency,
          instanceId
        });
      }

      // 5. Hospital Directory
      if (req.method === 'GET' && pathname === '/api/hospitals') {
        const hospitals = db.getAllHospitals();
        return sendJson(200, { hospitals, instanceId });
      }

      // 6. Hospital Beds
      if (req.method === 'GET' && pathname.startsWith('/api/hospitals/') && pathname.endsWith('/beds')) {
        const parts = pathname.split('/');
        const hospitalId = parts[3];
        const bedType = parsedUrl.query.type || null;
        const status = parsedUrl.query.status || null;
        const beds = db.getBedsByHospital(hospitalId, bedType, status);
        const hospital = db.getHospitalById(hospitalId);
        return sendJson(200, { hospital, beds, count: beds.length, instanceId });
      }

      // 7. Rate-Limited Ambulance Geospatial Search (< 100ms requirement)
      if (req.method === 'POST' && pathname === '/api/dispatch/find-nearest') {
        const body = await parseBody();
        const { ambulanceId = 'AMB-108', lat, lon, bedType = 'ICU', maxRadiusKm = 25 } = body;

        // Redis Sliding-Window Rate Limit check (30 requests/min per ambulance)
        const rateCheck = await redis.checkRateLimit(`ratelimit:${ambulanceId}`, 30, 60000);
        if (!rateCheck.allowed) {
          return sendJson(429, {
            error: 'RATE_LIMIT_EXCEEDED',
            message: `Ambulance ${ambulanceId} exceeded request rate limit. Retry in ${Math.ceil(rateCheck.retryAfterMs / 1000)}s.`,
            retryAfterMs: rateCheck.retryAfterMs
          });
        }

        // Geospatial search in Redis
        const startTime = performance.now();
        const geoResults = redis.geoSearch('hospitals:geo', lon || 78.4867, lat || 17.3850, maxRadiusKm, 10);

        // Filter and enrich with live DB availability
        const enriched = [];
        for (const r of geoResults) {
          const hosp = db.getHospitalById(r.member);
          if (hosp) {
            const freeCount = bedType === 'ICU' ? hosp.free_icu
              : (bedType === 'VENTILATOR' ? hosp.free_ventilator
              : (bedType === 'GENERAL' ? hosp.free_general : hosp.free_beds));

            enriched.push({
              hospitalId: hosp.id,
              name: hosp.name,
              code: hosp.code,
              zone: hosp.zone,
              lat: hosp.lat,
              lon: hosp.lon,
              distanceKm: r.distanceKm,
              estimatedMinutes: r.estimatedMinutes,
              freeBedsOfType: freeCount,
              freeTotal: hosp.free_beds,
              phone: hosp.phone
            });
          }
        }

        const elapsedMs = Number((performance.now() - startTime).toFixed(2));

        return sendJson(200, {
          searchLatencyMs: elapsedMs,
          p95TargetMet: elapsedMs < 100,
          rateLimitRemaining: rateCheck.remaining,
          ambulanceId,
          bedType,
          matches: enriched,
          instanceId
        });
      }

      // 8. Atomic Bed Reservation (Distributed Lock with 15-Minute TTL)
      if (req.method === 'POST' && pathname === '/api/beds/reserve') {
        const body = await parseBody();
        const { bedId, hospitalId, ambulanceId = 'AMB-108', patientTriage = 'CRITICAL' } = body;

        if (!bedId) {
          return sendJson(400, { error: 'Missing bedId' });
        }

        const reservationId = 'RES-' + crypto.randomUUID().substring(0, 8);
        const ttlMs = 15 * 60 * 1000; // 15 minutes

        // 1. Acquire Redis Atomic Distributed Lock
        const lockAcquired = await redis.setNX(`bed:lock:${bedId}`, reservationId, ttlMs);
        if (!lockAcquired) {
          return sendJson(409, {
            success: false,
            error: 'DOUBLE_ALLOCATION_PREVENTED',
            message: `Bed ${bedId} is already locked/reserved by another concurrent ambulance or instance.`
          });
        }

        // 2. Perform SQLite atomic state transition
        const dbResult = db.reserveBedAtomic(bedId, reservationId, ambulanceId, patientTriage, ttlMs);
        if (!dbResult.success) {
          // Roll back Redis lock on DB conflict
          await redis.del(`bed:lock:${bedId}`);
          return sendJson(409, {
            success: false,
            error: 'DOUBLE_ALLOCATION_PREVENTED',
            message: dbResult.reason
          });
        }

        // 3. Publish bed.reserved event to RabbitMQ
        await rmq.publish('medigrid.events', 'bed.reserved', {
          bedId,
          hospitalId: dbResult.hospitalId,
          ambulanceId,
          patientTriage,
          reservationId,
          ttlMs,
          timestamp: Date.now()
        }, { priority: patientTriage === 'CRITICAL' ? 10 : 5 });

        return sendJson(200, {
          success: true,
          reservationId,
          bedId,
          expiresAt: dbResult.expiresAt,
          ttlMinutes: 15,
          instanceId
        });
      }

      // 9. Admit Patient (Optimistic with rollback support)
      if (req.method === 'POST' && pathname === '/api/beds/admit') {
        const body = await parseBody();
        const { bedId, patientName, patientId, simulateFailure = false } = body;

        if (simulateFailure) {
          return sendJson(500, {
            error: 'SIMULATED_ADMISSION_FAILURE',
            message: 'Optimistic UI Rollback: Simulated network error during patient admission.'
          });
        }

        const bed = db.getBedById(bedId);
        if (!bed) return sendJson(404, { error: 'Bed not found' });

        await rmq.publish('medigrid.events', 'bed.admitted', {
          bedId,
          hospitalId: bed.hospital_id,
          patientId: patientId || ('PAT-' + Math.floor(10000 + Math.random() * 90000)),
          patientName: patientName || 'Walk-in Patient',
          timestamp: Date.now()
        }, { priority: 7 });

        return sendJson(200, { success: true, bedId, instanceId });
      }

      // 10. Discharge Patient
      if (req.method === 'POST' && pathname === '/api/beds/discharge') {
        const body = await parseBody();
        const { bedId } = body;
        const bed = db.getBedById(bedId);
        if (!bed) return sendJson(404, { error: 'Bed not found' });

        await rmq.publish('medigrid.events', 'bed.discharged', {
          bedId,
          hospitalId: bed.hospital_id,
          patientId: bed.patient_id,
          timestamp: Date.now()
        }, { priority: 3 });

        return sendJson(200, { success: true, bedId, instanceId });
      }

      // 11. Inter-Hospital Transfer Saga API
      if (req.method === 'POST' && pathname === '/api/transfers/initiate') {
        const body = await parseBody();
        const result = await saga.initiateTransfer(body);
        if (!result.success) {
          return sendJson(400, result);
        }
        return sendJson(200, { ...result, instanceId });
      }

      if (req.method === 'POST' && pathname.startsWith('/api/transfers/') && pathname.endsWith('/accept')) {
        const transferId = pathname.split('/')[3];
        const body = await parseBody();
        const result = await saga.acceptTransfer(transferId, body.staffActor);
        return sendJson(result.success ? 200 : 400, { ...result, instanceId });
      }

      if (req.method === 'POST' && pathname.startsWith('/api/transfers/') && pathname.endsWith('/reject')) {
        const transferId = pathname.split('/')[3];
        const body = await parseBody();
        const result = await saga.rejectTransfer(transferId, body.reason, body.actor);
        return sendJson(result.success ? 200 : 400, { ...result, instanceId });
      }

      if (req.method === 'GET' && pathname.startsWith('/api/transfers/pending/')) {
        const hospitalId = pathname.split('/')[4];
        const pending = saga.getPendingTransfersForHospital(hospitalId);
        return sendJson(200, { pending, instanceId });
      }

      // 12. Operations & Observability Metrics
      if (req.method === 'GET' && pathname === '/api/metrics') {
        const metrics = rmq.getMetrics();
        const latency = redis.getLatencyMetrics();
        return sendJson(200, {
          ...metrics,
          bedSearchLatency: latency,
          instanceId
        });
      }

      // 13. Dead-Letter Queue Inspector & Replay
      if (req.method === 'GET' && pathname === '/api/dlq') {
        const deadLetters = db.getDeadLetters();
        return sendJson(200, { deadLetters, instanceId });
      }

      if (req.method === 'POST' && pathname.startsWith('/api/dlq/') && pathname.endsWith('/replay')) {
        const dlqId = pathname.split('/')[3];
        try {
          const replayResult = await rmq.replayDeadLetter(dlqId);
          db.markDeadLetterReplayed(dlqId);
          return sendJson(200, { success: true, dlqId, instanceId });
        } catch (e) {
          return sendJson(404, { error: e.message });
        }
      }

      // 14. Audit Trail
      if (req.method === 'GET' && pathname === '/api/audit') {
        const logs = audit.getAuditLogs(50);
        return sendJson(200, { logs, instanceId });
      }

      // 15. Live Stress & Failure Simulation Scenarios (For Judges Demo!)
      if (req.method === 'POST' && pathname.startsWith('/api/simulate/')) {
        const scenario = pathname.replace('/api/simulate/', '');
        const body = await parseBody();
        const { runScenario } = require('../simulator/stress_tester');
        const simResult = await runScenario(scenario, body);
        return sendJson(200, { scenario, ...simResult, instanceId });
      }

      // 404 Catch-all
      return sendJson(404, { error: 'Not Found', path: pathname });
    } catch (err) {
      console.error(`[${instanceId}] Internal Server Error on ${req.method} ${pathname}:`, err);
      return sendJson(500, { error: 'Internal Server Error', message: err.message, instanceId });
    }
  });

  return {
    server,
    start: () => new Promise((resolve) => {
      server.listen(port, '127.0.0.1', () => {
        console.log(`[MediGrid] ${instanceId} listening on http://localhost:${port}`);
        resolve(server);
      });
    }),
    stop: () => new Promise((resolve) => server.close(resolve))
  };
}

module.exports = { createServerInstance };
