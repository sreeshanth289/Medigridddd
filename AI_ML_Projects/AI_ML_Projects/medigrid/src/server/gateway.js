/**
 * MediGrid - Reverse Proxy & Resilient Gateway
 * Runs on Port 3000.
 * Load-balances traffic across Instance 1 (port 3001) and Instance 2 (port 3002).
 * Provides sub-second health-check monitoring and transparent automatic failover within < 5s.
 */

const http = require('node:http');

const GATEWAY_PORT = parseInt(process.env.GATEWAY_PORT || '3000', 10);
const PORT_1 = parseInt(process.env.PORT_1 || '4001', 10);
const PORT_2 = parseInt(process.env.PORT_2 || '4002', 10);

const instances = [
  { id: 'instance-1', host: '127.0.0.1', port: PORT_1, healthy: true, requestCount: 0, lastCheck: 0 },
  { id: 'instance-2', host: '127.0.0.1', port: PORT_2, healthy: true, requestCount: 0, lastCheck: 0 }
];

let roundRobinIndex = 0;
const failoverEvents = [];

function checkInstanceHealth(inst) {
  const req = http.request({
    hostname: inst.host,
    port: inst.port,
    path: '/api/health',
    method: 'GET',
    timeout: 1000
  }, (res) => {
    inst.lastCheck = Date.now();
    const isNowHealthy = res.statusCode === 200;
    if (inst.healthy !== isNowHealthy) {
      inst.healthy = isNowHealthy;
      recordFailover(`Instance ${inst.id} state changed to ${isNowHealthy ? 'HEALTHY' : 'UNHEALTHY'}`);
    }
  });

  req.on('error', () => {
    inst.lastCheck = Date.now();
    if (inst.healthy) {
      inst.healthy = false;
      recordFailover(`Instance ${inst.id} is DOWN / unreachable.`);
    }
  });

  req.on('timeout', () => {
    req.destroy();
    if (inst.healthy) {
      inst.healthy = false;
      recordFailover(`Instance ${inst.id} timed out.`);
    }
  });

  req.end();
}

function recordFailover(eventDesc) {
  console.log(`[MediGrid Gateway] [FAILOVER ALERT] ${eventDesc}`);
  failoverEvents.unshift({
    timestamp: Date.now(),
    description: eventDesc
  });
  if (failoverEvents.length > 50) failoverEvents.pop();
}

// Active health-checking every 1000ms
setInterval(() => {
  for (const inst of instances) {
    checkInstanceHealth(inst);
  }
}, 1000);

function getNextHealthyInstance() {
  const healthyList = instances.filter(i => i.healthy);
  if (healthyList.length === 0) return null;
  const target = healthyList[roundRobinIndex % healthyList.length];
  roundRobinIndex++;
  return target;
}

const server = http.createServer((clientReq, clientRes) => {
  // Gateway Diagnostic API
  if (clientReq.url === '/gateway/status') {
    clientRes.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return clientRes.end(JSON.stringify({
      gatewayPort: GATEWAY_PORT,
      instances,
      activeHealthyCount: instances.filter(i => i.healthy).length,
      failoverEvents,
      timestamp: Date.now()
    }));
  }

  const target = getNextHealthyInstance();
  if (!target) {
    clientRes.writeHead(503, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return clientRes.end(JSON.stringify({
      error: 'ALL_INSTANCES_UNAVAILABLE',
      message: 'No healthy MediGrid application instances are currently available.'
    }));
  }

  target.requestCount++;

  const proxyReq = http.request({
    hostname: target.host,
    port: target.port,
    path: clientReq.url,
    method: clientReq.method,
    headers: {
      ...clientReq.headers,
      'x-forwarded-for': clientReq.socket.remoteAddress,
      'x-gateway-proxy': 'MediGrid-Gateway',
      'x-routed-instance': target.id
    }
  }, (proxyRes) => {
    // Add Gateway headers
    proxyRes.headers['x-gateway-served-by'] = target.id;
    clientRes.writeHead(proxyRes.statusCode, proxyRes.headers);
    proxyRes.pipe(clientRes);
  });

  proxyReq.on('error', (err) => {
    console.error(`[MediGrid Gateway] Proxy error on ${target.id}:`, err.message);
    target.healthy = false;
    recordFailover(`Transparent failover triggered away from ${target.id}`);

    // Transparently retry on alternate instance
    const alternate = instances.find(i => i.id !== target.id && i.healthy);
    if (alternate) {
      console.log(`[MediGrid Gateway] Retrying client request on ${alternate.id}...`);
      alternate.requestCount++;
      const retryReq = http.request({
        hostname: alternate.host,
        port: alternate.port,
        path: clientReq.url,
        method: clientReq.method,
        headers: clientReq.headers
      }, (retryRes) => {
        retryRes.headers['x-gateway-served-by'] = alternate.id;
        retryRes.headers['x-failover-recovered'] = 'true';
        clientRes.writeHead(retryRes.statusCode, retryRes.headers);
        retryRes.pipe(clientRes);
      });
      retryReq.on('error', () => {
        clientRes.writeHead(502, { 'Content-Type': 'application/json' });
        clientRes.end(JSON.stringify({ error: 'FAILOVER_EXHAUSTED' }));
      });
      clientReq.pipe(retryReq);
    } else {
      clientRes.writeHead(502, { 'Content-Type': 'application/json' });
      clientRes.end(JSON.stringify({ error: 'BAD_GATEWAY', message: err.message }));
    }
  });

  clientReq.pipe(proxyReq);
});

server.listen(GATEWAY_PORT, '127.0.0.1', () => {
  console.log(`[MediGrid Gateway] Load Balancer listening on http://localhost:${GATEWAY_PORT}`);
  console.log(`[MediGrid Gateway] Routing across Instance 1 (port ${PORT_1}) and Instance 2 (port ${PORT_2})`);
});
