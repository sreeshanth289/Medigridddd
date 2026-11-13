/**
 * MediGrid - Audit & Compliance Microservice
 * Consumes all events from RabbitMQ 'audit_queue'.
 * Writes tamper-evident records to SQLite for audit compliance and operational review.
 */

const { getRabbitMQ } = require('../middleware/rabbitmq_engine');
const { getDatabase } = require('../database/db');

class AuditService {
  constructor() {
    this.rmq = getRabbitMQ();
    this.db = getDatabase();
    this.startConsumer();
  }

  startConsumer() {
    this.rmq.consume('audit_queue', async (msg, ack) => {
      const { routingKey, content, properties } = msg;

      try {
        const eventType = routingKey.toUpperCase().replace(/\./g, '_');
        const entityType = routingKey.split('.')[0] || 'unknown';
        const entityId = content.bedId || content.transferId || content.reservationId || 'system';
        const hospitalId = content.hospitalId || content.toHospitalId || content.fromHospitalId || null;
        const actor = content.actor || content.ambulanceId || content.acceptedBy || 'system_service';

        this.db.logAudit(
          eventType,
          entityType,
          entityId,
          hospitalId,
          actor,
          {
            priority: properties.priority,
            timestamp: properties.timestamp,
            ...content
          }
        );

        ack();
      } catch (err) {
        console.error('[AuditService] Failed to record audit log:', err);
        ack(); // Do not block audit queue on format errors
      }
    });
  }

  getAuditLogs(limit = 100) {
    return this.db.getAuditLogs(limit);
  }
}

let auditInstance = null;
function getAuditService() {
  if (!auditInstance) {
    auditInstance = new AuditService();
  }
  return auditInstance;
}

module.exports = { AuditService, getAuditService };
