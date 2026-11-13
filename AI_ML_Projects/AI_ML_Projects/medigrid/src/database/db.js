/**
 * MediGrid - SQLite Persistent Database Engine
 * Node 22+ native SQLite with synchronous ACID transactions.
 */

const { DatabaseSync } = require('node:sqlite');
const path = require('node:path');
const fs = require('node:fs');

const DB_PATH = process.env.MEDIGRID_DB_PATH || path.join(__dirname, '..', '..', 'medigrid.sqlite');

class DatabaseService {
  constructor(dbPath = DB_PATH) {
    this.dbPath = dbPath;
    this.db = new DatabaseSync(this.dbPath);
    this.initPragmas();
    this.createTables();
  }

  initPragmas() {
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA foreign_keys = ON;
      PRAGMA busy_timeout = 5000;
    `);
  }

  createTables() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS hospitals (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        code TEXT UNIQUE NOT NULL,
        zone TEXT NOT NULL,
        lat REAL NOT NULL,
        lon REAL NOT NULL,
        address TEXT NOT NULL,
        phone TEXT NOT NULL,
        contact_person TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS beds (
        id TEXT PRIMARY KEY,
        hospital_id TEXT NOT NULL REFERENCES hospitals(id) ON DELETE CASCADE,
        ward_name TEXT NOT NULL,
        bed_number TEXT NOT NULL,
        bed_type TEXT NOT NULL, -- 'ICU', 'VENTILATOR', 'GENERAL', 'OXYGEN', 'ISOLATION'
        status TEXT NOT NULL DEFAULT 'AVAILABLE', -- 'AVAILABLE', 'RESERVED', 'OCCUPIED', 'CLEANING'
        patient_id TEXT,
        patient_name TEXT,
        reserved_until INTEGER, -- Unix timestamp ms
        reservation_id TEXT,
        updated_at INTEGER NOT NULL,
        UNIQUE(hospital_id, ward_name, bed_number)
      );

      CREATE INDEX IF NOT EXISTS idx_beds_lookup ON beds(hospital_id, bed_type, status);
      CREATE INDEX IF NOT EXISTS idx_beds_status ON beds(status);
      CREATE INDEX IF NOT EXISTS idx_beds_reserved_until ON beds(reserved_until);

      CREATE TABLE IF NOT EXISTS reservations (
        id TEXT PRIMARY KEY,
        bed_id TEXT NOT NULL REFERENCES beds(id),
        hospital_id TEXT NOT NULL REFERENCES hospitals(id),
        ambulance_id TEXT NOT NULL,
        patient_triage TEXT NOT NULL, -- 'CRITICAL', 'URGENT', 'ROUTINE'
        reserved_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'ACTIVE', -- 'ACTIVE', 'CONFIRMED', 'EXPIRED', 'CANCELLED'
        created_by TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS transfers (
        id TEXT PRIMARY KEY,
        patient_id TEXT NOT NULL,
        patient_name TEXT NOT NULL,
        triage_level TEXT NOT NULL, -- 'CRITICAL' (Priority 10), 'URGENT' (Priority 5), 'ROUTINE' (Priority 1)
        priority INTEGER NOT NULL,
        from_hospital_id TEXT NOT NULL REFERENCES hospitals(id),
        to_hospital_id TEXT NOT NULL REFERENCES hospitals(id),
        bed_type TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'REQUESTED', -- 'REQUESTED', 'ACCEPTED', 'REJECTED', 'TIMED_OUT', 'COMPLETED'
        source_bed_id TEXT,
        target_bed_id TEXT,
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        completed_at INTEGER,
        rejection_reason TEXT
      );

      CREATE TABLE IF NOT EXISTS audit_logs (
        id TEXT PRIMARY KEY,
        timestamp INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        hospital_id TEXT,
        actor TEXT NOT NULL,
        details_json TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_logs(timestamp DESC);

      CREATE TABLE IF NOT EXISTS dead_letters (
        id TEXT PRIMARY KEY,
        timestamp INTEGER NOT NULL,
        exchange TEXT NOT NULL,
        routing_key TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        error_message TEXT NOT NULL,
        retry_count INTEGER NOT NULL DEFAULT 0,
        priority INTEGER NOT NULL DEFAULT 1,
        status TEXT NOT NULL DEFAULT 'DEAD' -- 'DEAD', 'REPLAYED', 'DISCARDED'
      );
    `);
  }

  // Hospital operations
  getAllHospitals() {
    const query = this.db.prepare(`
      SELECT h.*,
        COUNT(b.id) as total_beds,
        SUM(CASE WHEN b.status = 'AVAILABLE' THEN 1 ELSE 0 END) as free_beds,
        SUM(CASE WHEN b.status = 'AVAILABLE' AND b.bed_type = 'ICU' THEN 1 ELSE 0 END) as free_icu,
        SUM(CASE WHEN b.status = 'AVAILABLE' AND b.bed_type = 'VENTILATOR' THEN 1 ELSE 0 END) as free_ventilator,
        SUM(CASE WHEN b.status = 'AVAILABLE' AND b.bed_type = 'GENERAL' THEN 1 ELSE 0 END) as free_general,
        SUM(CASE WHEN b.status = 'AVAILABLE' AND b.bed_type = 'OXYGEN' THEN 1 ELSE 0 END) as free_oxygen,
        SUM(CASE WHEN b.status = 'RESERVED' THEN 1 ELSE 0 END) as reserved_beds,
        SUM(CASE WHEN b.status = 'OCCUPIED' THEN 1 ELSE 0 END) as occupied_beds
      FROM hospitals h
      LEFT JOIN beds b ON h.id = b.hospital_id
      GROUP BY h.id
      ORDER BY h.name ASC
    `);
    return query.all();
  }

  getHospitalById(id) {
    const query = this.db.prepare(`
      SELECT h.*,
        COUNT(b.id) as total_beds,
        SUM(CASE WHEN b.status = 'AVAILABLE' THEN 1 ELSE 0 END) as free_beds,
        SUM(CASE WHEN b.status = 'AVAILABLE' AND b.bed_type = 'ICU' THEN 1 ELSE 0 END) as free_icu,
        SUM(CASE WHEN b.status = 'AVAILABLE' AND b.bed_type = 'VENTILATOR' THEN 1 ELSE 0 END) as free_ventilator,
        SUM(CASE WHEN b.status = 'AVAILABLE' AND b.bed_type = 'GENERAL' THEN 1 ELSE 0 END) as free_general,
        SUM(CASE WHEN b.status = 'AVAILABLE' AND b.bed_type = 'OXYGEN' THEN 1 ELSE 0 END) as free_oxygen
      FROM hospitals h
      LEFT JOIN beds b ON h.id = b.hospital_id
      WHERE h.id = ?
      GROUP BY h.id
    `);
    return query.get(id);
  }

  getBedsByHospital(hospitalId, bedType = null, status = null) {
    let sql = `SELECT * FROM beds WHERE hospital_id = ?`;
    const params = [hospitalId];
    if (bedType) {
      sql += ` AND bed_type = ?`;
      params.push(bedType);
    }
    if (status) {
      sql += ` AND status = ?`;
      params.push(status);
    }
    sql += ` ORDER BY ward_name ASC, bed_number ASC`;
    return this.db.prepare(sql).all(...params);
  }

  getBedById(id) {
    return this.db.prepare(`SELECT * FROM beds WHERE id = ?`).get(id);
  }

  updateBedStatus(bedId, status, { patientId = null, patientName = null, reservedUntil = null, reservationId = null } = {}) {
    const stmt = this.db.prepare(`
      UPDATE beds
      SET status = ?,
          patient_id = ?,
          patient_name = ?,
          reserved_until = ?,
          reservation_id = ?,
          updated_at = ?
      WHERE id = ?
    `);
    stmt.run(status, patientId, patientName, reservedUntil, reservationId, Date.now(), bedId);
    return this.getBedById(bedId);
  }

  // Atomic reservation update
  reserveBedAtomic(bedId, reservationId, ambulanceId, patientTriage, ttlMs = 15 * 60 * 1000) {
    const now = Date.now();
    const expiresAt = now + ttlMs;

    // Check if bed is available
    const checkStmt = this.db.prepare(`SELECT * FROM beds WHERE id = ? AND status = 'AVAILABLE'`);
    const bed = checkStmt.get(bedId);
    if (!bed) {
      return { success: false, reason: 'Bed is not available or already reserved' };
    }

    // Atomically transition to RESERVED
    const updateStmt = this.db.prepare(`
      UPDATE beds
      SET status = 'RESERVED',
          reservation_id = ?,
          reserved_until = ?,
          updated_at = ?
      WHERE id = ? AND status = 'AVAILABLE'
    `);
    const result = updateStmt.run(reservationId, expiresAt, now, bedId);
    if (result.changes === 0) {
      return { success: false, reason: 'Race condition: Bed was taken by another request' };
    }

    // Insert reservation record
    const resStmt = this.db.prepare(`
      INSERT INTO reservations (id, bed_id, hospital_id, ambulance_id, patient_triage, reserved_at, expires_at, status, created_by)
      VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?)
    `);
    resStmt.run(reservationId, bedId, bed.hospital_id, ambulanceId, patientTriage, now, expiresAt, ambulanceId);

    return {
      success: true,
      reservationId,
      bedId,
      hospitalId: bed.hospital_id,
      expiresAt,
      bed
    };
  }

  // Release expired reservations
  releaseExpiredReservations() {
    const now = Date.now();
    const expiredBeds = this.db.prepare(`
      SELECT id, hospital_id, reservation_id FROM beds
      WHERE status = 'RESERVED' AND reserved_until IS NOT NULL AND reserved_until < ?
    `).all(now);

    for (const b of expiredBeds) {
      this.db.prepare(`
        UPDATE beds
        SET status = 'AVAILABLE', reservation_id = NULL, reserved_until = NULL, updated_at = ?
        WHERE id = ? AND status = 'RESERVED'
      `).run(now, b.id);

      if (b.reservation_id) {
        this.db.prepare(`
          UPDATE reservations
          SET status = 'EXPIRED'
          WHERE id = ? AND status = 'ACTIVE'
        `).run(b.reservation_id);
      }
    }

    return expiredBeds;
  }

  // Audit logging
  logAudit(eventType, entityType, entityId, hospitalId, actor, details = {}) {
    const id = 'aud_' + Math.random().toString(36).substring(2, 9) + '_' + Date.now();
    this.db.prepare(`
      INSERT INTO audit_logs (id, timestamp, event_type, entity_type, entity_id, hospital_id, actor, details_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(id, Date.now(), eventType, entityType, entityId, hospitalId || null, actor || 'system', JSON.stringify(details));
  }

  getAuditLogs(limit = 100) {
    return this.db.prepare(`SELECT * FROM audit_logs ORDER BY timestamp DESC LIMIT ?`).all(limit);
  }

  // Dead Letter storage
  logDeadLetter(exchange, routingKey, payload, errorMessage, retryCount = 0, priority = 1) {
    const id = 'dlq_' + Math.random().toString(36).substring(2, 9) + '_' + Date.now();
    this.db.prepare(`
      INSERT INTO dead_letters (id, timestamp, exchange, routing_key, payload_json, error_message, retry_count, priority, status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'DEAD')
    `).run(id, Date.now(), exchange, routingKey, JSON.stringify(payload), errorMessage, retryCount, priority);
    return id;
  }

  getDeadLetters(limit = 50) {
    return this.db.prepare(`SELECT * FROM dead_letters WHERE status = 'DEAD' ORDER BY timestamp DESC LIMIT ?`).all(limit);
  }

  markDeadLetterReplayed(id) {
    return this.db.prepare(`UPDATE dead_letters SET status = 'REPLAYED' WHERE id = ?`).run(id);
  }

  // Stats across the city
  getCityStats() {
    const row = this.db.prepare(`
      SELECT
        COUNT(DISTINCT h.id) as total_hospitals,
        COUNT(b.id) as total_beds,
        SUM(CASE WHEN b.status = 'AVAILABLE' THEN 1 ELSE 0 END) as available_beds,
        SUM(CASE WHEN b.status = 'RESERVED' THEN 1 ELSE 0 END) as reserved_beds,
        SUM(CASE WHEN b.status = 'OCCUPIED' THEN 1 ELSE 0 END) as occupied_beds,
        SUM(CASE WHEN b.status = 'CLEANING' THEN 1 ELSE 0 END) as cleaning_beds,
        SUM(CASE WHEN b.status = 'AVAILABLE' AND b.bed_type = 'ICU' THEN 1 ELSE 0 END) as available_icu,
        SUM(CASE WHEN b.status = 'AVAILABLE' AND b.bed_type = 'VENTILATOR' THEN 1 ELSE 0 END) as available_ventilator,
        SUM(CASE WHEN b.status = 'AVAILABLE' AND b.bed_type = 'GENERAL' THEN 1 ELSE 0 END) as available_general,
        SUM(CASE WHEN b.status = 'AVAILABLE' AND b.bed_type = 'OXYGEN' THEN 1 ELSE 0 END) as available_oxygen
      FROM hospitals h
      LEFT JOIN beds b ON h.id = b.hospital_id
    `).get();
    return row;
  }
}

let instance = null;
function getDatabase() {
  if (!instance) {
    instance = new DatabaseService();
  }
  return instance;
}

module.exports = { DatabaseService, getDatabase };
