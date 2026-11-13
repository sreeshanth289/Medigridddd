/**
 * MediGrid - Inter-Hospital Transfer Saga Orchestrator
 * Implements the Distributed Saga Pattern across two hospitals:
 * 1. Step 1: Forward Action -> Hospital A requests transfer, source bed held.
 * 2. Step 2: Target Bed Atomic Lock in Redis at Hospital B.
 * 3. Step 3: Priority Message in RabbitMQ (Priority 10 for Critical, 5 for Urgent).
 * 4. Step 4: Hospital B Alert with Countdown (e.g. 180s in production, 30s in demo).
 * 5. Step 5: Acceptance -> Patient en-route, handoff complete, beds reconciled.
 * 6. Compensating Transaction -> On rejection or timeout:
 *    - Release reserved bed at Hospital B immediately
 *    - Revert source bed at Hospital A back to OCCUPIED
 *    - Log compensating event and notify dispatch
 */

const { getRabbitMQ } = require('../middleware/rabbitmq_engine');
const { getRedis } = require('../middleware/redis_engine');
const { getDatabase } = require('../database/db');
const crypto = require('node:crypto');

class TransferSagaService {
  constructor() {
    this.rmq = getRabbitMQ();
    this.redis = getRedis();
    this.db = getDatabase();
    this.activeSagas = new Map(); // transferId -> { timeoutTimer, ... }

    this.startConsumer();
    this.resumePendingTransfers();
  }

  resumePendingTransfers() {
    // Check if any transfers in DB were left in REQUESTED state
    const pending = this.db.db.prepare(`SELECT * FROM transfers WHERE status = 'REQUESTED'`).all();
    const now = Date.now();
    for (const t of pending) {
      const remainingMs = Math.max(0, t.expires_at - now);
      if (remainingMs === 0) {
        this.executeCompensatingTransaction(t.id, 'TIMEOUT_DURING_RECOVERY');
      } else {
        this.scheduleSagaTimeout(t.id, remainingMs);
      }
    }
  }

  startConsumer() {
    this.rmq.consume('transfer_saga_queue', async (msg, ack, nack) => {
      const { routingKey, content } = msg;

      try {
        switch (routingKey) {
          case 'transfer.requested':
            await this.handleTransferRequested(content);
            break;
          case 'transfer.accepted':
            await this.handleTransferAccepted(content);
            break;
          case 'transfer.rejected':
            await this.handleTransferRejected(content);
            break;
          case 'transfer.timeout':
            await this.handleTransferTimeout(content);
            break;
          default:
            console.log(`[TransferSagaService] Unknown transfer routing key: ${routingKey}`);
        }
        ack();
      } catch (err) {
        console.error(`[TransferSagaService] Error handling ${routingKey}:`, err);
        nack(true, err.message);
      }
    });
  }

  /**
   * Step 1: Initiate Transfer Saga
   */
  async initiateTransfer({
    patientId,
    patientName,
    triageLevel = 'CRITICAL', // 'CRITICAL', 'URGENT', 'ROUTINE'
    fromHospitalId,
    toHospitalId,
    bedType = 'ICU',
    sourceBedId = null,
    countdownSec = 60
  }) {
    const transferId = 'trf_' + crypto.randomUUID().substring(0, 8);
    const priority = triageLevel === 'CRITICAL' ? 10 : (triageLevel === 'URGENT' ? 5 : 1);
    const now = Date.now();
    const expiresAt = now + (countdownSec * 1000);

    // Step 1a: Find a free target bed at Hospital B
    const availableBeds = this.db.getBedsByHospital(toHospitalId, bedType, 'AVAILABLE');
    if (availableBeds.length === 0) {
      return {
        success: false,
        reason: `Target Hospital has no free ${bedType} beds available.`
      };
    }
    const targetBed = availableBeds[0];

    // Step 1b: Acquire atomic Redis lock on target bed
    const lockAcquired = await this.redis.setNX(`bed:lock:${targetBed.id}`, transferId, countdownSec * 1000);
    if (!lockAcquired) {
      return {
        success: false,
        reason: `Target bed ${targetBed.id} was just locked by another concurrent transfer.`
      };
    }

    // Step 1c: Mark target bed as RESERVED in DB
    this.db.updateBedStatus(targetBed.id, 'RESERVED', {
      reservationId: transferId,
      reservedUntil: expiresAt
    });

    // Step 1d: Insert transfer record into DB
    this.db.db.prepare(`
      INSERT INTO transfers (id, patient_id, patient_name, triage_level, priority, from_hospital_id, to_hospital_id, bed_type, status, source_bed_id, target_bed_id, created_at, expires_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'REQUESTED', ?, ?, ?, ?)
    `).run(transferId, patientId, patientName, triageLevel, priority, fromHospitalId, toHospitalId, bedType, sourceBedId, targetBed.id, now, expiresAt);

    // Step 1e: Publish high-priority event to RabbitMQ
    await this.rmq.publish('medigrid.events', 'transfer.requested', {
      transferId,
      patientId,
      patientName,
      triageLevel,
      priority,
      fromHospitalId,
      toHospitalId,
      bedType,
      sourceBedId,
      targetBedId: targetBed.id,
      expiresAt,
      countdownSec
    }, { priority });

    // Schedule automatic timeout for saga
    this.scheduleSagaTimeout(transferId, countdownSec * 1000);

    return {
      success: true,
      transferId,
      targetBedId: targetBed.id,
      expiresAt,
      priority
    };
  }

  scheduleSagaTimeout(transferId, timeoutMs) {
    if (this.activeSagas.has(transferId)) {
      clearTimeout(this.activeSagas.get(transferId).timer);
    }

    const timer = setTimeout(async () => {
      console.log(`[TransferSagaService] Transfer ${transferId} timed out waiting for receiving hospital.`);
      await this.rmq.publish('medigrid.events', 'transfer.timeout', {
        transferId,
        reason: 'RECEIVING_HOSPITAL_TIMEOUT'
      }, { priority: 9 });
    }, timeoutMs);

    this.activeSagas.set(transferId, { timer, transferId });
  }

  async handleTransferRequested(content) {
    console.log(`[TransferSagaService] Transfer requested: ${content.transferId} (${content.triageLevel}, Priority ${content.priority})`);
  }

  /**
   * Step 4a: Hospital B staff clicks "Accept Transfer"
   */
  async acceptTransfer(transferId, staffActor = 'Dr. Staff') {
    const transfer = this.db.db.prepare(`SELECT * FROM transfers WHERE id = ?`).get(transferId);
    if (!transfer || transfer.status !== 'REQUESTED') {
      return { success: false, reason: 'Transfer is no longer pending or already processed.' };
    }

    // Clear timeout
    if (this.activeSagas.has(transferId)) {
      clearTimeout(this.activeSagas.get(transferId).timer);
      this.activeSagas.delete(transferId);
    }

    // Publish accepted event
    await this.rmq.publish('medigrid.events', 'transfer.accepted', {
      transferId,
      patientId: transfer.patient_id,
      patientName: transfer.patient_name,
      fromHospitalId: transfer.from_hospital_id,
      toHospitalId: transfer.to_hospital_id,
      sourceBedId: transfer.source_bed_id,
      targetBedId: transfer.target_bed_id,
      acceptedBy: staffActor
    }, { priority: transfer.priority });

    return { success: true };
  }

  async handleTransferAccepted(content) {
    const { transferId, patientId, patientName, sourceBedId, targetBedId, acceptedBy } = content;
    const now = Date.now();

    // 1. Update transfer record to COMPLETED
    this.db.db.prepare(`
      UPDATE transfers
      SET status = 'COMPLETED', completed_at = ?
      WHERE id = ?
    `).run(now, transferId);

    // 2. Commit target bed to OCCUPIED with patient info
    if (targetBedId) {
      await this.redis.del(`bed:lock:${targetBedId}`);
      this.db.updateBedStatus(targetBedId, 'OCCUPIED', {
        patientId,
        patientName,
        reservationId: null,
        reservedUntil: null
      });
    }

    // 3. Release source bed at transferring hospital to CLEANING / AVAILABLE
    if (sourceBedId) {
      await this.redis.del(`bed:lock:${sourceBedId}`);
      this.db.updateBedStatus(sourceBedId, 'CLEANING', {
        patientId: null,
        patientName: null
      });
    }

    // 4. Audit Log
    this.db.logAudit('TRANSFER_COMPLETED', 'transfer', transferId, content.toHospitalId, acceptedBy || 'staff', {
      sourceBedId,
      targetBedId,
      patientId
    });

    console.log(`[TransferSagaService] Transfer ${transferId} successfully completed!`);
  }

  /**
   * Step 4b: Hospital B rejects transfer (e.g. specialized surgeon unavailable)
   */
  async rejectTransfer(transferId, reason = 'Hospital at peak capacity', actor = 'Receiving Hospital') {
    const transfer = this.db.db.prepare(`SELECT * FROM transfers WHERE id = ?`).get(transferId);
    if (!transfer || transfer.status !== 'REQUESTED') {
      return { success: false, reason: 'Transfer is no longer pending.' };
    }

    if (this.activeSagas.has(transferId)) {
      clearTimeout(this.activeSagas.get(transferId).timer);
      this.activeSagas.delete(transferId);
    }

    await this.rmq.publish('medigrid.events', 'transfer.rejected', {
      transferId,
      reason,
      actor
    }, { priority: 9 });

    return { success: true };
  }

  async handleTransferRejected(content) {
    await this.executeCompensatingTransaction(content.transferId, content.reason || 'REJECTED_BY_RECEIVING_HOSPITAL');
  }

  async handleTransferTimeout(content) {
    await this.executeCompensatingTransaction(content.transferId, content.reason || 'TIMEOUT_NO_RESPONSE');
  }

  /**
   * SAGA COMPENSATING TRANSACTION:
   * Reverses distributed effects cleanly:
   * - Releases reserved target bed at Hospital B
   * - Restores source bed at Hospital A
   * - Emits audit trail entry
   */
  async executeCompensatingTransaction(transferId, failureReason) {
    const transfer = this.db.db.prepare(`SELECT * FROM transfers WHERE id = ?`).get(transferId);
    if (!transfer || (transfer.status !== 'REQUESTED' && transfer.status !== 'COMPLETED')) {
      return;
    }

    console.log(`[TransferSagaService] EXECUTING COMPENSATING TRANSACTION for ${transferId}. Reason: ${failureReason}`);

    // 1. Release target bed lock in Redis and reset status to AVAILABLE
    if (transfer.target_bed_id) {
      await this.redis.del(`bed:lock:${transfer.target_bed_id}`);
      this.db.updateBedStatus(transfer.target_bed_id, 'AVAILABLE', {
        reservationId: null,
        reservedUntil: null
      });
    }

    // 2. Restore source bed status if needed
    if (transfer.source_bed_id) {
      this.db.updateBedStatus(transfer.source_bed_id, 'OCCUPIED', {
        patientId: transfer.patient_id,
        patientName: transfer.patient_name
      });
    }

    // 3. Mark transfer as REJECTED or TIMED_OUT
    const finalStatus = failureReason.includes('TIMEOUT') ? 'TIMED_OUT' : 'REJECTED';
    this.db.db.prepare(`
      UPDATE transfers
      SET status = ?, rejection_reason = ?, completed_at = ?
      WHERE id = ?
    `).run(finalStatus, failureReason, Date.now(), transferId);

    // 4. Record tamper-evident audit
    this.db.logAudit('TRANSFER_COMPENSATED', 'transfer', transferId, transfer.from_hospital_id, 'saga_orchestrator', {
      failureReason,
      targetBedReleased: transfer.target_bed_id,
      sourceBedRestored: transfer.source_bed_id
    });
  }

  getPendingTransfersForHospital(hospitalId) {
    return this.db.db.prepare(`
      SELECT * FROM transfers
      WHERE to_hospital_id = ? AND status = 'REQUESTED'
      ORDER BY priority DESC, created_at ASC
    `).all(hospitalId);
  }
}

let sagaInstance = null;
function getTransferSagaService() {
  if (!sagaInstance) {
    sagaInstance = new TransferSagaService();
  }
  return sagaInstance;
}

module.exports = { TransferSagaService, getTransferSagaService };
