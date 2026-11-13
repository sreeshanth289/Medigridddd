/**
 * MediGrid - Cluster Orchestrator
 * Starts Instance 1 (port 3001), Instance 2 (port 3002), and Gateway Load Balancer (port 3000).
 */

const { createServerInstance } = require('../src/server/app');
const { seedDatabase } = require('../src/database/seed');
const http = require('node:http');

async function startCluster() {
  console.log('================================================================');
  console.log('  MediGrid: City Emergency Bed & Patient Transfer Network');
  console.log('  Yuktik \'26 Challenge PS04 · Team EL-DORADO');
  console.log('================================================================\n');

  // 1. Seed database with 42 hospitals & 4,200 beds
  console.log('[Step 1/3] Initializing persistent database & geospatial index...');
  seedDatabase();

  // 2. Start Application Instances
  console.log('[Step 2/3] Launching stateless application instances...');
  const port1 = parseInt(process.env.PORT_1 || '4001', 10);
  const port2 = parseInt(process.env.PORT_2 || '4002', 10);

  const inst1 = createServerInstance({ instanceId: 'instance-1', port: port1 });
  await inst1.start();

  const inst2 = createServerInstance({ instanceId: 'instance-2', port: port2 });
  await inst2.start();

  // 3. Start Gateway Load Balancer
  console.log('[Step 3/3] Launching Gateway Reverse Proxy & Failover Monitor...');
  require('../src/server/gateway');

  console.log('\n================================================================');
  console.log('  ✓ CLUSTER READY FOR JUDGES DEMONSTRATION');
  console.log('  --------------------------------------------------------------');
  console.log('  🌐 Gateway (Load Balancer): http://localhost:3000');
  console.log(`  🏥 Instance 1:              http://localhost:${port1}`);
  console.log(`  🏥 Instance 2:              http://localhost:${port2}`);
  console.log('  📊 Gateway Health Status:   http://localhost:3000/gateway/status');
  console.log('================================================================\n');
}

if (require.main === module) {
  startCluster().catch(err => {
    console.error('Fatal cluster start error:', err);
    process.exit(1);
  });
}

module.exports = { startCluster };
