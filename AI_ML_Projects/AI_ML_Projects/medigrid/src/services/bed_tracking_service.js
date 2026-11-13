/**
 * MediGrid - Bed Tracking Microservice
 * Consumes events from RabbitMQ 'bed_tracking_queue'.
 * Maintains live bed state in SQLite and synchronizes Redis cache & Geospatial indices.
 */

const { getRabbitMQ } = require('../middleware/rabbitmq_engine');
const { getRedis } = require('../middleware/redis_engine');
const { getDatabase } = require('../database/db');

class BedTrackingService {
  constructor() {
    this.rmq = getRabbitMQ();
    this.redis = getRedis();
    this.db = getDatabase();
    this.consumerCancel = null;
    this.idempotencyCache = new Set(); // Prevent duplicate processing of same event ID

    this.initGeospatialIndex();
    this.startConsumer();
    this.listenToRedisExpiry();
    this.startActiveReservationSweeper();
  }

  startActiveReservationSweeper() {
    // Actively sweep expired reservations every 1000ms (< 5s requirement)
    setInterval(() => {
      const expired = this.db.releaseExpiredReservations();
      for (const b of expired) {
        this.redis.del(`bed:lock:${b.id}`);
        this.syncHospitalRedisGeo(b.hospital_id);
      }
    }, 1000);
  }

  // Populate Redis Geospatial index with all hospitals from DB
  initGeospatialIndex() {
    const hospitals = this.db.getAllHospitals();
    for (const h of hospitals) {
      this.redis.geoAdd('hospitals:geo', h.lon, h.lat, h.id, {
        name: h.name,
        code: h.code,
        zone: h.zone,
        freeBeds: h.free_beds,
        freeIcu: h.free_icu,
        freeVentilator: h.free_ventilator,
        freeGeneral: h.free_general
      });
    }
    console.log(`[BedTrackingService] Populated Redis Geospatial index with ${hospitals.length} hospitals.`);
  }

  startConsumer() {
    this.consumerCancel = this.rmq.consume('bed_tracking_queue', async (msg, ack, nack) => {
      const { routingKey, content, id } = msg;

      // Exactly-Once Check: Idempotency Key
      const idempotencyKey = content.idempotencyKey || id;
      if (this.idempotencyCache.has(idempotencyKey)) {
        console.log(`[BedTrackingService] Duplicate event detected (${idempotencyKey}). Acknowledging without duplicate effect.`);
        return ack();
      }
      this.idempotencyCache.add(idempotencyKey);
      if (this.idempotencyCache.size > 10000) {
        const oldest = this.idempotencyCache.values().next().value;
        this.idempotencyCache.delete(oldest);
      }

      try {
        switch (routingKey) {
          case 'bed.reserved':
            await this.handleBedReserved(content);
            break;
          case 'bed.admitted':
            await this.handleBedAdmitted(content);
            break;
          case 'bed.discharged':
            await this.handleBedDischarged(content);
            break;
          case 'bed.cleaning':
            await this.handleBedCleaning(content);
            break;
          case 'bed.expired':
            await this.handleBedExpired(content);
            break;
          default:
            console.log(`[BedTrackingService] Unhandled routing key: ${routingKey}`);
        }

        // Synchronize Redis Geo metadata for the affected hospital
        if (content.hospitalId) {
          this.syncHospitalRedisGeo(content.hospitalId);
        }

        ack();
      } catch (err) {
        console.error(`[BedTrackingService] Error processing ${routingKey}:`, err);
        nack(true, err.message); // Requeue for retry
      }
    });
  }

  syncHospitalRedisGeo(hospitalId) {
    const h = this.db.getHospitalById(hospitalId);
    if (h) {
      this.redis.geoAdd('hospitals:geo', h.lon, h.lat, h.id, {
        name: h.name,
        code: h.code,
        zone: h.zone,
        freeBeds: h.free_beds,
        freeIcu: h.free_icu,
        freeVentilator: h.free_ventilator,
        freeGeneral: h.free_general
      });
    }
  }

  async handleBedReserved(content) {
    const { bedId, hospitalId, ambulanceId, patientTriage, reservationId, ttlMs } = content;
    this.db.updateBedStatus(bedId, 'RESERVED', {
      reservationId,
      reservedUntil: Date.now() + (ttlMs || 15 * 60 * 1000)
    });
    console.log(`[BedTrackingService] Bed ${bedId} reserved for ambulance ${ambulanceId}`);
  }

  async handleBedAdmitted(content) {
    const { bedId, hospitalId, patientId, patientName } = content;
    // Release reservation lock in Redis
    await this.redis.del(`bed:lock:${bedId}`);
    this.db.updateBedStatus(bedId, 'OCCUPIED', {
      patientId: patientId || ('PAT-' + Math.floor(10000 + Math.random() * 90000)),
      patientName: patientName || 'Admitted Patient',
      reservationId: null,
      reservedUntil: null
    });
    console.log(`[BedTrackingService] Patient ${patientId || ''} admitted to Bed ${bedId}`);
  }

  async handleBedDischarged(content) {
    const { bedId } = content;
    await this.redis.del(`bed:lock:${bedId}`);
    // Transition to CLEANING
    this.db.updateBedStatus(bedId, 'CLEANING', {
      patientId: null,
      patientName: null,
      reservationId: null,
      reservedUntil: null
    });
    console.log(`[BedTrackingService] Bed ${bedId} discharged and moved to CLEANING`);

    // Simulate sanitation completion after 10 seconds or immediate release
    setTimeout(() => {
      const current = this.db.getBedById(bedId);
      if (current && current.status === 'CLEANING') {
        this.db.updateBedStatus(bedId, 'AVAILABLE');
        this.syncHospitalRedisGeo(current.hospital_id);
      }
    }, 10000);
  }

  async handleBedCleaning(content) {
    const { bedId, status } = content;
    this.db.updateBedStatus(bedId, status || 'AVAILABLE');
  }

  async handleBedExpired(content) {
    const { bedId } = content;
    await this.redis.del(`bed:lock:${bedId}`);
    this.db.updateBedStatus(bedId, 'AVAILABLE', {
      reservationId: null,
      reservedUntil: null
    });
    console.log(`[BedTrackingService] Expired reservation on Bed ${bedId} auto-released to AVAILABLE`);
  }

  listenToRedisExpiry() {
    this.redis.on('reservation_expired', async ({ bedId, reservationId }) => {
      console.log(`[BedTrackingService] TTL expired for bed ${bedId}. Publishing bed.expired event.`);
      await this.rmq.publish('medigrid.events', 'bed.expired', {
        bedId,
        reservationId,
        timestamp: Date.now()
      }, { priority: 8 });
    });
  }
}

let serviceInstance = null;
function getBedTrackingService() {
  if (!serviceInstance) {
    serviceInstance = new BedTrackingService();
  }
  return serviceInstance;
}

module.exports = { BedTrackingService, getBedTrackingService };
