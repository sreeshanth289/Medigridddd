/**
 * MediGrid - Notification & Alert Microservice
 * Consumes from RabbitMQ 'notifications_queue'.
 * Dispatches real-time alerts, incoming transfer countdown warnings, and control-room surges.
 */

const { getRabbitMQ } = require('../middleware/rabbitmq_engine');
const EventEmitter = require('node:events');

class NotificationService extends EventEmitter {
  constructor() {
    super();
    this.rmq = getRabbitMQ();
    this.recentAlerts = [];
    this.startConsumer();
  }

  startConsumer() {
    this.rmq.consume('notifications_queue', async (msg, ack) => {
      const { routingKey, content, properties } = msg;

      const alert = {
        id: 'notif_' + Math.random().toString(36).substring(2, 9),
        timestamp: properties.timestamp || Date.now(),
        routingKey,
        priority: properties.priority || 1,
        title: this.formatAlertTitle(routingKey, content),
        message: this.formatAlertMessage(routingKey, content),
        targetHospitalId: content.toHospitalId || content.hospitalId,
        metadata: content
      };

      this.recentAlerts.unshift(alert);
      if (this.recentAlerts.length > 100) {
        this.recentAlerts.pop();
      }

      // Emit for live SSE / WebSocket broadcast
      this.emit('alert', alert);

      ack();
    });
  }

  formatAlertTitle(routingKey, content) {
    if (routingKey.includes('transfer.requested')) {
      return `🚨 INCOMING TRANSFER ALERT: ${content.triageLevel || 'CRITICAL'} (Priority ${content.priority || 10})`;
    }
    if (routingKey.includes('bed.reserved')) {
      return `🚑 AMBULANCE RESERVATION: Bed ${content.bedId}`;
    }
    return `Notification: ${routingKey}`;
  }

  formatAlertMessage(routingKey, content) {
    if (routingKey.includes('transfer.requested')) {
      return `Patient ${content.patientName || content.patientId} requires ${content.bedType} bed. Countdown: ${content.countdownSec || 60}s to accept.`;
    }
    if (routingKey.includes('bed.reserved')) {
      return `Ambulance ${content.ambulanceId} en route with ${content.patientTriage} patient. 15-minute bed hold activated.`;
    }
    return JSON.stringify(content);
  }

  getRecentAlerts(hospitalId = null) {
    if (!hospitalId) return this.recentAlerts.slice(0, 30);
    return this.recentAlerts.filter(a => !a.targetHospitalId || a.targetHospitalId === hospitalId).slice(0, 30);
  }
}

let notifInstance = null;
function getNotificationService() {
  if (!notifInstance) {
    notifInstance = new NotificationService();
  }
  return notifInstance;
}

module.exports = { NotificationService, getNotificationService };
