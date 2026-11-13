/**
 * MediGrid - Instance Runner
 */

const { createServerInstance } = require('./app');
const { seedDatabase } = require('../database/seed');

// Ensure database is seeded
seedDatabase();

const port = parseInt(process.env.PORT || '3001', 10);
const instanceId = process.env.INSTANCE_ID || `instance-${port === 3001 ? 1 : 2}`;

const app = createServerInstance({ instanceId, port });

app.start().then(() => {
  console.log(`[MediGrid Instance] Started ${instanceId} on port ${port}`);
});

process.on('SIGINT', async () => {
  console.log(`[MediGrid Instance] Shutting down ${instanceId}...`);
  await app.stop();
  process.exit(0);
});
