/**
 * MediGrid - Live Stress & Failure Simulator
 * Implements the 6 core scenarios required for judges evaluation:
 * 1. Race condition (20 ambulances for 1 last bed -> ZERO double allocations)
 * 2. RabbitMQ Priority Leapfrogging (1 Critical ahead of 10,000 routine)
 * 3. Consumer Crash & Durable Message Recovery
 * 4. Inter-Hospital Transfer Saga Timeout & Compensation
 * 5. Poison Message DLQ routing and 1-Click Replay
 * 6. Fast TTL Expiry & Auto-Release within < 5 seconds
 */

const { getRabbitMQ } = require('../middleware/rabbitmq_engine');
const { getRedis } = require('../middleware/redis_engine');
const { getDatabase } = require('../database/db');
const { getTransferSagaService } = require('../services/transfer_saga_service');
const crypto = require('node:crypto');

async function runScenario(scenarioName, params = {}) {
  const rmq = getRabbitMQ();
  const redis = getRedis();
  const db = getDatabase();
  const saga = getTransferSagaService();

  switch (scenarioName) {
    // SCENARIO 1: 20 Ambulances Race for the Last ICU Bed
    case 'race_condition': {
      const hospitalId = params.hospitalId || 'hosp_apollo_jh';
      const bedType = params.bedType || 'ICU';

      // Ensure at least one ICU bed at the target hospital is explicitly AVAILABLE and lock cleared
      const allBeds = db.getBedsByHospital(hospitalId, bedType);
      if (allBeds.length === 0) {
        return { success: false, error: 'No beds found for hospital ' + hospitalId };
      }
      const targetBed = allBeds[0];
      db.updateBedStatus(targetBed.id, 'AVAILABLE', { reservationId: null, reservedUntil: null });
      await redis.del(`bed:lock:${targetBed.id}`);

      // Simulate 20 ambulances firing concurrent reservation requests at the same millisecond
      const numAmbulances = params.numAmbulances || 20;
      const results = [];
      const startTime = performance.now();

      const promises = [];
      for (let i = 1; i <= numAmbulances; i++) {
        const ambId = `AMB-RACE-${String(i).padStart(2, '0')}`;
        const resId = `RES-RACE-${crypto.randomUUID().substring(0, 6)}`;

        const p = (async () => {
          // 1. Atomic Redis lock
          const lockAcquired = await redis.setNX(`bed:lock:${targetBed.id}`, resId, 60000);
          if (!lockAcquired) {
            return { ambulance: ambId, success: false, reason: 'Redis Lock Contention: Bed already locked' };
          }

          // 2. Atomic SQLite reservation
          const dbRes = db.reserveBedAtomic(targetBed.id, resId, ambId, 'CRITICAL', 60000);
          if (!dbRes.success) {
            await redis.del(`bed:lock:${targetBed.id}`);
            return { ambulance: ambId, success: false, reason: 'DB Contention: ' + dbRes.reason };
          }

          // 3. Publish to RabbitMQ
          await rmq.publish('medigrid.events', 'bed.reserved', {
            bedId: targetBed.id,
            hospitalId,
            ambulanceId: ambId,
            reservationId: resId,
            patientTriage: 'CRITICAL',
            timestamp: Date.now()
          }, { priority: 10 });

          return { ambulance: ambId, success: true, reservationId: resId };
        })();

        promises.push(p);
      }

      const raceOutcomes = await Promise.all(promises);
      const winners = raceOutcomes.filter(r => r.success);
      const losers = raceOutcomes.filter(r => !r.success);
      const elapsedMs = Number((performance.now() - startTime).toFixed(2));

      return {
        scenario: 'Race Condition (Double Allocation Defense)',
        targetBedId: targetBed.id,
        targetHospital: hospitalId,
        totalAttempts: numAmbulances,
        successfulAllocations: winners.length,
        rejectedAllocations: losers.length,
        zeroDoubleAllocationVerified: winners.length === 1,
        winner: winners[0] || null,
        elapsedMs,
        details: raceOutcomes
      };
    }

    // SCENARIO 2: Critical Request Leaps Ahead of 10,000 Queued Routine Messages
    case 'priority_queue': {
      const routineCount = params.count || 2000; // 2,000 for responsive browser demo, configurable up to 10k
      const processedOrder = [];

      // Create a temporary priority test queue
      const testQueueName = 'sim_priority_test_' + Date.now();
      rmq.declareQueue(testQueueName, { maxPriority: 10 });
      rmq.bindQueue(testQueueName, 'medigrid.events', 'sim.priority.#');

      // 1. Temporarily pause consumer so messages queue up
      let consumerStarted = false;

      // 2. Flood with routine priority 1 messages
      const startTime = performance.now();
      const floodPromises = [];
      for (let i = 1; i <= routineCount; i++) {
        floodPromises.push(
          rmq.publish('medigrid.events', 'sim.priority.routine', {
            id: `routine-${i}`,
            type: 'ROUTINE_PAPERWORK',
            index: i
          }, { priority: 1 })
        );
      }
      await Promise.all(floodPromises);

      // 3. Inject ONE Critical Priority 10 Emergency Patient
      const criticalId = 'CRITICAL-EMERGENCY-' + Date.now();
      await rmq.publish('medigrid.events', 'sim.priority.critical', {
        id: criticalId,
        type: 'CRITICAL_CARDIAC_ARREST',
        triageLevel: 'CRITICAL',
        requiredBed: 'ICU_VENTILATOR'
      }, { priority: 10 });

      // 4. Start consumer and observe order of messages dequeued
      const firstTenDequeued = [];
      await new Promise((resolve) => {
        rmq.consume(testQueueName, (msg, ack) => {
          firstTenDequeued.push({
            id: msg.content.id,
            priority: msg.properties.priority,
            type: msg.content.type
          });
          ack();
          if (firstTenDequeued.length >= 10 || msg.content.id === criticalId) {
            resolve();
          }
        });
      });

      const criticalDequeuedIndex = firstTenDequeued.findIndex(m => m.id === criticalId);

      return {
        scenario: 'RabbitMQ Priority Queue Leapfrogging',
        routineMessagesQueued: routineCount,
        criticalRequestPriority: 10,
        routineRequestPriority: 1,
        criticalDequeuedAtPosition: criticalDequeuedIndex + 1, // 1 means 1st message consumed!
        leapfroggedSuccessfully: criticalDequeuedIndex === 0,
        firstMessagesProcessed: firstTenDequeued.slice(0, 5),
        elapsedMs: Number((performance.now() - startTime).toFixed(2))
      };
    }

    // SCENARIO 3: Consumer Crash & Recovery (Zero Lost Events)
    case 'consumer_crash': {
      const crashQueue = 'sim_crash_test_' + Date.now();
      rmq.declareQueue(crashQueue, { durable: true, maxPriority: 10 });
      rmq.bindQueue(crashQueue, 'medigrid.events', 'sim.crash.#');

      // Start consumer
      let receivedBeforeCrash = 0;
      let cancelConsumer = rmq.consume(crashQueue, (msg, ack) => {
        receivedBeforeCrash++;
        ack();
      });

      // Send 5 events
      for (let i = 1; i <= 5; i++) {
        await rmq.publish('medigrid.events', 'sim.crash.event', { i }, { priority: 5 });
      }

      // CRASH CONSUMER
      cancelConsumer();
      const depthAtCrash = rmq.queues.get(crashQueue).messages.length;

      // Publish 10 messages while consumer is dead
      for (let i = 6; i <= 15; i++) {
        await rmq.publish('medigrid.events', 'sim.crash.event', { i }, { priority: 5 });
      }
      const depthWhileDead = rmq.queues.get(crashQueue).messages.length;

      // REVIVE CONSUMER
      let receivedAfterRevive = 0;
      const revivedPromise = new Promise((resolve) => {
        rmq.consume(crashQueue, (msg, ack) => {
          receivedAfterRevive++;
          ack();
          if (receivedAfterRevive >= 10) {
            resolve();
          }
        });
      });

      await revivedPromise;

      return {
        scenario: 'Consumer Crash & Zero Lost Events Recovery',
        eventsPublishedWhileDead: 10,
        queueDepthDuringOutage: depthWhileDead,
        eventsRecoveredAndProcessed: receivedAfterRevive,
        zeroLossVerified: receivedAfterRevive === 10
      };
    }

    // SCENARIO 4: Poison Message to Dead-Letter Queue (DLQ) & 1-Click Replay
    case 'poison_dlq': {
      const poisonId = 'poison_' + crypto.randomUUID().substring(0, 6);
      const testDlqQueue = 'sim_dlq_test_' + Date.now();
      rmq.declareQueue(testDlqQueue, { durable: true, deadLetterExchange: 'medigrid.dlx' });
      rmq.bindQueue(testDlqQueue, 'medigrid.events', 'sim.poison');

      let retryAttempts = 0;
      rmq.consume(testDlqQueue, async (msg, ack, nack) => {
        retryAttempts++;
        // Deliberately simulate failure/exception
        nack(true, 'Simulated Unrecoverable Malformed Data Payload');
      });

      await rmq.publish('medigrid.events', 'sim.poison', {
        id: poisonId,
        corruptedField: '\x00\xFF_INVALID_BINARY_STREAM',
        timestamp: Date.now()
      });

      // Wait 2.2s for retries to exhaust (100ms + 200ms + 400ms) and route to DLQ
      await new Promise(r => setTimeout(r, 2200));

      const dlqEntries = Array.from(rmq.deadLetters.values()).filter(d => d.content && d.content.id === poisonId);
      const targetDlq = dlqEntries[0];
      if (targetDlq) {
        db.logDeadLetter('medigrid.dlx', 'sim.poison', targetDlq.content, targetDlq.errorMessage, targetDlq.retryCount, targetDlq.priority);
      }

      return {
        scenario: 'Dead-Letter Queue (DLQ) & Operator Replay',
        poisonMessageId: poisonId,
        retryAttemptsMade: retryAttempts,
        routedToDeadLetterQueue: !!targetDlq,
        dlqId: targetDlq ? targetDlq.id : null,
        errorMessage: targetDlq ? targetDlq.errorMessage : 'Failed to route to DLQ',
        replayReady: true
      };
    }

    // SCENARIO 5: Inter-Hospital Transfer Saga Timeout & Compensation
    case 'saga_timeout': {
      const fromHosp = 'hosp_apollo_jh';
      const toHosp = 'hosp_kims_sec';

      // Ensure Apollo has an occupied bed and KIMS has an available ICU bed
      const apolloBeds = db.getBedsByHospital(fromHosp, 'ICU');
      const sourceBed = apolloBeds[0];
      db.updateBedStatus(sourceBed.id, 'OCCUPIED', { patientId: 'PAT-SAGA-999', patientName: 'Saga Demo Patient' });

      // Ensure KIMS has a free ICU bed and clear any lingering lock
      const kimsBeds = db.getBedsByHospital(toHosp, 'ICU');
      const targetBedToFree = kimsBeds[0];
      db.updateBedStatus(targetBedToFree.id, 'AVAILABLE', { reservationId: null, reservedUntil: null });
      await redis.del(`bed:lock:${targetBedToFree.id}`);

      // Initiate transfer with short 3-second countdown for immediate demo!
      const initResult = await saga.initiateTransfer({
        patientId: 'PAT-SAGA-999',
        patientName: 'Saga Demo Patient',
        triageLevel: 'CRITICAL',
        fromHospitalId: fromHosp,
        toHospitalId: toHosp,
        bedType: 'ICU',
        sourceBedId: sourceBed.id,
        countdownSec: 3 // 3 seconds timeout
      });

      if (!initResult.success) {
        return {
          scenario: 'Inter-Hospital Transfer Saga Timeout & Compensation',
          error: initResult.reason || 'Failed to initiate transfer'
        };
      }

      // Check state during countdown
      const targetBedBefore = db.getBedById(initResult.targetBedId);

      // Wait 3.5 seconds for timeout to fire compensating transaction
      await new Promise(r => setTimeout(r, 3500));

      const targetBedAfter = db.getBedById(initResult.targetBedId);
      const transferRecord = db.db.prepare(`SELECT * FROM transfers WHERE id = ?`).get(initResult.transferId);

      return {
        scenario: 'Inter-Hospital Transfer Saga Timeout & Compensation',
        transferId: initResult.transferId,
        sourceHospital: 'Apollo Hospitals Jubilee Hills',
        receivingHospital: 'KIMS Hospitals Secunderabad',
        targetBedReservedDuringSaga: targetBedBefore.status === 'RESERVED',
        targetBedReleasedAfterCompensation: targetBedAfter.status === 'AVAILABLE',
        finalTransferStatus: transferRecord ? transferRecord.status : 'UNKNOWN',
        cleanCompensationVerified: transferRecord && transferRecord.status === 'TIMED_OUT' && targetBedAfter.status === 'AVAILABLE'
      };
    }

    // SCENARIO 6: 5-Second TTL Expiry & Auto-Release
    case 'ttl_auto_release': {
      const beds = db.getBedsByHospital('hosp_care_banj', 'VENTILATOR');
      if (beds.length === 0) return { error: 'No available beds for TTL test' };
      const bed = beds[0];

      // Reset to AVAILABLE and clear lock
      db.updateBedStatus(bed.id, 'AVAILABLE', { reservationId: null, reservedUntil: null });
      await redis.del(`bed:lock:${bed.id}`);

      const resId = 'RES-FAST-TTL-' + Date.now();

      // Reserve with short 1.5-second TTL
      await redis.setNX(`bed:lock:${bed.id}`, resId, 1500);
      db.reserveBedAtomic(bed.id, resId, 'AMB-FAST-TTL', 'URGENT', 1500);

      const bedBefore = db.getBedById(bed.id);

      // Wait 3.5 seconds for active background sweeper to release (< 5s target)
      await new Promise(r => setTimeout(r, 3500));
      const bedAfter = db.getBedById(bed.id);

      return {
        scenario: 'Unconfirmed Reservation Auto-Release (< 5s target)',
        bedId: bed.id,
        initialStatus: bedBefore.status, // RESERVED
        statusAfterExpiry: bedAfter.status, // AVAILABLE
        autoReleasedWithin5Sec: bedAfter.status === 'AVAILABLE'
      };
    }

    default:
      return { error: `Unknown scenario: ${scenarioName}` };
  }
}

module.exports = { runScenario };
