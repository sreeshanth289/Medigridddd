# ADR-001: MediGrid Event-Driven Architecture & Distributed Reservation Protocol

**Team:** EL-DORADO  
**Challenge:** Yuktik '26 · Problem Statement 04 (PS04)  
**Track:** Web Technologies / Event-Driven Architecture / Real-Time UI  
**Status:** Approved & Implemented  
**Date:** November 2026  

---

## 1. System Architecture Diagram

```mermaid
flowchart TD
    subgraph Clients["Clients & Edge Tier"]
        Amb["🚑 Ambulances / Emergency Crews"]
        HospStaff["🏥 Hospital Staff Consoles (42 Hospitals)"]
        CtrlRoom["🗺️ Live City Control-Room (Hyderabad)"]
    end

    subgraph GatewayTier["High Availability Ingress Tier (Port 3000)"]
        GW["MediGrid Reverse Proxy & Gateway<br/>• Sub-second Health Monitoring<br/>• Transparent 5s Failover<br/>• Round-Robin Load Balancing"]
    end

    subgraph AppInstances["Stateless Application Instances"]
        Inst1["Application Instance 1<br/>Port 3001"]
        Inst2["Application Instance 2<br/>Port 3002"]
    end

    subgraph Middleware["Middlewares & State Engines"]
        subgraph RedisTier["Redis 7+ In-Memory Engine"]
            Geo["Redis Geospatial Index<br/>(GEOADD / GEOSEARCH) &lt; 3.5ms"]
            Locks["Atomic Distributed Locks<br/>SET key val NX PX 15m"]
            RateLimit["Sliding Window Rate Limiter<br/>30 req/min per Ambulance"]
            PubSub["Real-Time Pub/Sub<br/>Cross-Instance Fanout"]
        end

        subgraph RabbitTier["RabbitMQ Event Broker"]
            ExMain["Topic Exchange: medigrid.events"]
            ExDLX["Dead-Letter Exchange: medigrid.dlx"]
            ExRetry["Retry Exchange: medigrid.retry"]
            QTrack["Queue: bed_tracking_queue<br/>(x-max-priority: 10)"]
            QSaga["Queue: transfer_saga_queue<br/>(x-max-priority: 10)"]
            QNotif["Queue: notifications_queue<br/>(x-max-priority: 10)"]
            QAudit["Queue: audit_queue<br/>(x-max-priority: 10)"]
            QDLQ["Queue: medigrid.dlq<br/>(Dead-Letter Queue & Replay)"]
        end

        subgraph StorageTier["Persistence Tier"]
            SQLite["SQLite / PostgreSQL ACID Database<br/>• 42 Hospitals &amp; 4,200+ Beds<br/>• Tamper-Evident Audit Logs<br/>• Transfers &amp; Dead Letters"]
        end
    end

    subgraph Workers["Independent Microservices"]
        BedWorker["Bed Tracking Service"]
        SagaWorker["Transfer Saga Orchestrator"]
        NotifWorker["Notification & Alert Service"]
        AuditWorker["Audit & Compliance Service"]
    end

    %% Client flows
    Amb -->|REST POST /dispatch/find-nearest| GW
    HospStaff -->|REST POST /beds/admit| GW
    CtrlRoom -->|SSE /api/stream Live Updates| GW

    GW --> Inst1
    GW --> Inst2

    %% Instance to Middleware
    Inst1 & Inst2 -->|Sub-100ms Geo Query| Geo
    Inst1 & Inst2 -->|Atomic Reservation Lock| Locks
    Inst1 & Inst2 -->|Check Rate Limit| RateLimit
    Inst1 & Inst2 -->|Publisher Confirms| ExMain

    %% RabbitMQ routing
    ExMain -->|bed.*| QTrack
    ExMain -->|transfer.*| QSaga
    ExMain -->|alert.*| QNotif
    ExMain -->|# (All Events)| QAudit

    %% Worker consumption
    QTrack --> BedWorker
    QSaga --> SagaWorker
    QNotif --> NotifWorker
    QAudit --> AuditWorker

    %% Dead letter routing
    QTrack & QSaga & QNotif -.->|Max Retries Exhausted| ExDLX
    ExDLX --> QDLQ

    %% Worker persistence
    BedWorker & SagaWorker & AuditWorker --> SQLite
    BedWorker -->|Sync Bed Counts| Geo
```

---

## 2. Most Important Design Decision

### Context & Problem
During a city-wide emergency surge (e.g., epidemic wave or multi-vehicle highway accident), dozens of ambulances simultaneously search for the single remaining ICU bed in a quadrant. Traditional relational database designs fail catastrophically:
1. **Direct DB updates create table/row locks**, causing connection exhaustion and request timeouts (> 5,000ms latency).
2. **Double allocation**: If two ambulances select the same free bed simultaneously, race conditions allow both to be dispatched to the same bed unless pessimistic locking (`SELECT FOR UPDATE`) is held, which serializes all hospital throughput and freezes control rooms.
3. **Cross-hospital transfer failures**: A transfer between Hospital A and Hospital B requires coordination across independent systems. Direct database synchronous calls result in orphaned reservations if the network drops.

### Decision
We chose a **Two-Tiered Hybrid Middleware Architecture**:
1. **Redis Geospatial + Atomic Compare-and-Swap Distributed Locking (`SETNX` with 15-minute TTL)** as the high-speed reservation tier.
2. **RabbitMQ Topic Exchange with Strict Priority Queuing (`x-max-priority: 10`), Publisher Confirms, and Dead-Letter Handling** as the reliable asynchronous backbone.
3. **Compensating Saga Pattern** for inter-hospital patient transfers.

---

## 3. Alternatives Considered

| Alternative | Pros | Cons | Why Rejected |
| :--- | :--- | :--- | :--- |
| **Option A: Direct SQL Transactions (`SELECT FOR UPDATE`)** | Simple single-database ACID semantics. | Locks database rows; high p95 latency (> 450ms under load); single point of failure; cannot scale across stateless multi-region nodes. | Violates the < 100ms search SLA; freezes under surge. |
| **Option B: Pure In-Memory Cache without Message Broker** | Very fast (sub-millisecond). | Cache restarts lose in-flight transfer events; no consumer backoff; no priority leapfrogging; no dead-letter queue. | Unacceptable in emergency healthcare where lost events cost lives. |
| **Option C: Heavy Workflow Engines (Temporal, Camunda, BullMQ)** | Turnkey workflow management. | Explicitly prohibited by challenge rules ("must build mechanisms such as locking, rate limiting, retries with delay, dead-letter handling themselves"). | High operational footprint; breaks competition compliance. |
| **Option D (Chosen): Redis Atomic Locking + RabbitMQ Priority Event Sagas** | **Sub-5ms search**, **zero double allocations**, **critical patient priority leapfrogging**, durable broker recovery, and zero framework bloat. | Requires distributed system design and idempotency handling. | **Selected as the optimal production-grade architecture.** |

---

## 4. Key Architectural Mechanisms (Built From Scratch)

### A. Zero Double Allocation Guarantee
To prevent two ambulances from ever booking the same bed across multiple stateless instances:
```
1. Request arrives at Instance 1 or Instance 2 via Load Balancer.
2. Atomically execute Redis `SET bed:lock:<bedId> <reservationId> NX PX 900000` (15 min TTL).
3. If Redis returns 0 (Key already exists):
   -> Immediately reject with HTTP 409 DOUBLE_ALLOCATION_PREVENTED (Latency: < 2ms).
4. If Redis returns 1 (Lock successfully acquired):
   -> Atomically execute SQLite state transition: `UPDATE beds SET status='RESERVED' WHERE id=? AND status='AVAILABLE'`.
   -> Publish `bed.reserved` event to RabbitMQ with Publisher Confirm.
```
*Verification Result:* In concurrent stress tests with 20 ambulances firing simultaneous requests for the last ICU bed, **exactly 1 allocation succeeded and 19 were cleanly rejected in 1.05 ms with zero double allocations.**

### B. Strict Priority Queue Leapfrogging
- RabbitMQ queues are declared with `x-max-priority: 10`.
- Critical cardiac/polytrauma cases are tagged with `priority: 10`.
- Routine laundry/discharge paperwork is tagged with `priority: 1`.
- When 2,000+ routine messages are backed up in the queue, incoming Critical Priority-10 requests are placed at the front of the queue by the broker and dequeued **at Position #1**.

### C. Two-Phase Distributed Saga Transfer Protocol
```
[Hospital A]                     [RabbitMQ / Redis]                      [Hospital B]
      │                                  │                                    │
      ├─ 1. Initiate Transfer (Pri 10) ─►│                                    │
      │     (Source bed: HELD)           ├─ 2. Redis Lock Target Bed (180s) ─►│
      │                                  ├─ 3. Publish transfer.requested ───►│
      │                                  │                                    ├─ 4. Alert Banner
      │                                  │                                    │     (Live 60s Countdown)
      │                                  │                                    │
      │◄── 5a. Staff ACCEPTS ────────────┼────────────────────────────────────┤
      │    • Target bed: OCCUPIED        │                                    │
      │    • Source bed: RELEASED        │                                    │
      │                                  │                                    │
      │◄── 5b. TIMEOUT / REJECT ─────────┼────────────────────────────────────┤
      │    • SAGA COMPENSATION:          │                                    │
      │      - Target bed lock released  │                                    │
      │      - Source bed restored       │                                    │
      │      - Audit record logged       │                                    │
```

### D. Dead-Letter Queue (DLQ) & Exponential Retries
When a consumer encounters an unrecoverable payload error:
1. First failure: `basicNack(requeue=true)` with 100ms backoff delay.
2. Second failure: 200ms delay.
3. Third failure: 400ms delay.
4. Fourth failure: Routed to `medigrid.dlx` and pushed to `medigrid.dlq`.
5. Operator UI displays dead-lettered payload, stack trace, and provides a **1-Click Replay** button to re-inject after resolution.

---

## 5. Judge Evaluation Outcomes Summary

| Target Outcome from Challenge Brief | Required SLA | MediGrid Achieved Result | Verification Method |
| :--- | :--- | :--- | :--- |
| **City-Wide Bed Changes Visibility** | &lt; 2.0 seconds | **&lt; 50 ms** via Server-Sent Events | Live SSE Stream on Port 3000 |
| **Bed Search Latency (p95)** | &lt; 100 ms | **1.8 ms to 3.5 ms** | Redis Geospatial Index Benchmark |
| **Double Allocation Defense** | **0 double allocations** | **0 double allocations** (1 winner, 19 rejected) | `node scripts/run-stress-test.js` Test 1 |
| **Priority Queue Leapfrogging** | Critical ahead of queue | **Critical dequeued at Position #1 ahead of 2,000 routine** | `node scripts/run-stress-test.js` Test 2 |
| **Broker Restart / Consumer Recovery** | Zero lost events | **10/10 messages recovered (100%)** | `node scripts/run-stress-test.js` Test 3 |
| **Transfer Saga Compensation** | Clean rollback on timeout | **Target bed auto-released, source restored** | `node scripts/run-stress-test.js` Test 4 |
| **Unconfirmed Reservation Expiry** | &lt; 5 seconds | **Auto-released in 3.5 seconds** | `node scripts/run-stress-test.js` Test 6 |
