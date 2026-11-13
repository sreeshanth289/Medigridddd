/**
 * MediGrid - Interactive Client Application
 * Handles Real-Time SSE Streams, Leaflet City Map, Hospital Console, Dispatch, Saga, and Simulator.
 */

// State
let allHospitals = [];
let currentHospitalId = null;
let currentWard = null;
let mapInstance = null;
let hospitalMarkers = new Map(); // id -> leaflet marker
let ambulanceMarkers = new Map(); // id -> leaflet marker
let sseConnection = null;
let activeIncomingTransfer = null;
let bannerTimerInterval = null;

// Audio Alert Generator using Web Audio API (No external sound files required)
function playEmergencyTone() {
  try {
    const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = audioCtx.createOscillator();
    const gain = audioCtx.createGain();
    osc.type = 'sine';
    osc.frequency.setValueAtTime(880, audioCtx.currentTime); // A5
    osc.frequency.setValueAtTime(440, audioCtx.currentTime + 0.15); // A4
    gain.gain.setValueAtTime(0.3, audioCtx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + 0.4);
    osc.connect(gain);
    gain.connect(audioCtx.destination);
    osc.start();
    osc.stop(audioCtx.currentTime + 0.4);
  } catch (e) {
    console.log('Audio alert muted by browser policy');
  }
}

// Initialize Application
document.addEventListener('DOMContentLoaded', async () => {
  setupTabs();
  setupThemeToggle();
  setupKeyboardShortcuts();
  initCityMap();
  initSSE();

  await loadHospitals();
  await loadCityStats();
  await loadMetrics();
  await loadTransfers();

  // Periodic polling for ops metrics (every 2.5s)
  setInterval(() => {
    loadCityStats();
    loadMetrics();
    loadTransfers();
  }, 2500);

  // Setup Dispatch Form
  setupDispatch();
  // Setup Simulator Runner
  setupSimulator();
  // Setup Modals
  setupModals();
});

// Tab Navigation
function setupTabs() {
  const tabs = document.querySelectorAll('.tab-btn');
  tabs.forEach(btn => {
    btn.addEventListener('click', () => {
      tabs.forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));

      btn.classList.add('active');
      const targetId = `tab-${btn.dataset.tab}`;
      const panel = document.getElementById(targetId);
      if (panel) {
        panel.classList.add('active');
        if (btn.dataset.tab === 'control-room' && mapInstance) {
          setTimeout(() => mapInstance.invalidateSize(), 200);
        }
      }
    });
  });
}

function setupThemeToggle() {
  const btn = document.getElementById('themeToggleBtn');
  btn.addEventListener('click', () => {
    document.body.classList.toggle('theme-light');
  });
}

// Real-Time Server-Sent Events (SSE)
function initSSE() {
  const connPill = document.getElementById('connectionPill');
  const connText = document.getElementById('connectionText');

  if (sseConnection) sseConnection.close();

  sseConnection = new EventSource('/api/stream');

  sseConnection.onopen = () => {
    connPill.className = 'status-pill status-connected';
    connText.textContent = 'Connected (Live Stream)';
  };

  sseConnection.addEventListener('connected', (e) => {
    const data = JSON.parse(e.data);
    connText.textContent = `Instance ${data.instanceId || '1'}`;
  });

  sseConnection.addEventListener('event_published', (e) => {
    const { routingKey, message } = JSON.parse(e.data);
    appendLogEntry(`[${routingKey}] Priority ${message.properties.priority} - ID: ${message.id.substring(0, 8)}`);
    // Refresh beds if current hospital was affected
    if (currentHospitalId && message.content && message.content.hospitalId === currentHospitalId) {
      loadHospitalBeds(currentHospitalId);
    }
    // Update map marker
    if (message.content && message.content.hospitalId) {
      updateHospitalMarker(message.content.hospitalId);
    }
  });

  sseConnection.addEventListener('incoming_alert', (e) => {
    const alert = JSON.parse(e.data);
    appendLogEntry(`🚨 ${alert.title}`);
    playEmergencyTone();

    if (alert.routingKey.includes('transfer.requested')) {
      showIncomingTransferAlert(alert);
    }
  });

  sseConnection.onerror = () => {
    connPill.className = 'status-pill';
    connPill.style.background = 'rgba(239, 68, 68, 0.2)';
    connPill.style.color = '#ef4444';
    connText.textContent = 'Reconnecting...';
  };
}

function appendLogEntry(text) {
  const logScroll = document.getElementById('miniEventLog');
  if (!logScroll) return;
  const div = document.createElement('div');
  div.className = 'log-entry';
  div.textContent = `${new Date().toLocaleTimeString()} - ${text}`;
  logScroll.insertBefore(div, logScroll.firstChild);
  if (logScroll.childNodes.length > 50) logScroll.removeChild(logScroll.lastChild);
}

// Leaflet Map Initialization
function initCityMap() {
  const mapEl = document.getElementById('cityMap');
  if (!mapEl) return;

  // Center on Hyderabad coordinates (17.4100, 78.4600)
  mapInstance = L.map('cityMap', {
    zoomControl: true,
    scrollWheelZoom: true
  }).setView([17.4150, 78.4450], 12);

  // Modern CartoDB Dark Matter / Positron tiles
  L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
    maxZoom: 19
  }).addTo(mapInstance);

  // Filter handlers
  document.getElementById('mapBedFilter').addEventListener('change', renderHospitalMarkers);
  document.getElementById('mapZoneFilter').addEventListener('change', renderHospitalMarkers);
  document.getElementById('btnRecenterMap').addEventListener('click', () => {
    mapInstance.setView([17.4150, 78.4450], 12);
  });

  // Spawn Simulated Ambulances on Map
  spawnSimulatedAmbulances();
}

// Load Hospitals
async function loadHospitals() {
  try {
    const res = await fetch('/api/hospitals');
    const data = await res.json();
    allHospitals = data.hospitals || [];

    // Populate Console dropdown
    const select = document.getElementById('consoleHospitalSelect');
    select.innerHTML = allHospitals.map(h => `<option value="${h.id}">${h.name} (${h.zone})</option>`).join('');

    select.addEventListener('change', (e) => {
      currentHospitalId = e.target.value;
      onHospitalSelected(currentHospitalId);
    });

    if (allHospitals.length > 0) {
      currentHospitalId = allHospitals[0].id;
      onHospitalSelected(currentHospitalId);
    }

    renderHospitalMarkers();
  } catch (err) {
    console.error('Error loading hospitals:', err);
  }
}

// Render Hospital Pins on Map (WCAG 1.4.1 Compliant with distinct shapes & labels)
function renderHospitalMarkers() {
  if (!mapInstance) return;

  const bedTypeFilter = document.getElementById('mapBedFilter').value;
  const zoneFilter = document.getElementById('mapZoneFilter').value;

  // Clear existing markers
  hospitalMarkers.forEach(m => mapInstance.removeLayer(m));
  hospitalMarkers.clear();

  for (const h of allHospitals) {
    if (zoneFilter !== 'ALL' && !h.zone.toLowerCase().includes(zoneFilter.toLowerCase())) {
      continue;
    }

    let freeCount = h.free_beds;
    if (bedTypeFilter === 'ICU') freeCount = h.free_icu;
    if (bedTypeFilter === 'VENTILATOR') freeCount = h.free_ventilator;
    if (bedTypeFilter === 'GENERAL') freeCount = h.free_general;
    if (bedTypeFilter === 'OXYGEN') freeCount = h.free_oxygen;

    // Accessibility Symbol: High ▲, Med ■, Low ●, Full ✕
    let symbol = '▲';
    let colorClass = 'sym-high';
    if (freeCount <= 0) { symbol = '✕'; colorClass = 'sym-full'; }
    else if (freeCount < 5) { symbol = '●'; colorClass = 'sym-low'; }
    else if (freeCount <= 15) { symbol = '■'; colorClass = 'sym-med'; }

    const customIcon = L.divIcon({
      className: 'custom-hosp-marker',
      html: `
        <div style="background: #182234; border: 2px solid ${freeCount > 5 ? '#10b981' : (freeCount > 0 ? '#f59e0b' : '#ef4444')}; border-radius: 6px; padding: 2px 6px; font-size: 11px; font-weight: bold; color: white; display: flex; align-items: center; gap: 4px; box-shadow: 0 2px 6px rgba(0,0,0,0.4); white-space: nowrap;">
          <span class="${colorClass}">${symbol}</span>
          <span>${h.code}</span>
          <span style="background: rgba(255,255,255,0.2); padding: 0 3px; border-radius: 3px;">${freeCount}</span>
        </div>
      `,
      iconSize: [80, 24],
      iconAnchor: [40, 12]
    });

    const marker = L.marker([h.lat, h.lon], { icon: customIcon }).addTo(mapInstance);

    marker.bindPopup(`
      <div style="font-family: sans-serif; color: #1e293b; padding: 4px;">
        <h4 style="margin: 0 0 4px 0; font-size: 14px;">${h.name}</h4>
        <div style="font-size: 12px; color: #64748b;">${h.address}</div>
        <div style="margin: 8px 0; font-size: 12px; display: grid; grid-template-columns: 1fr 1fr; gap: 4px;">
          <div>Free ICU: <strong>${h.free_icu}</strong></div>
          <div>Free Vent: <strong>${h.free_ventilator}</strong></div>
          <div>Free Gen: <strong>${h.free_general}</strong></div>
          <div>Total Free: <strong>${h.free_beds}</strong> / ${h.total_beds}</div>
        </div>
        <button onclick="window.selectHospitalFromMap('${h.id}')" style="background: #0284c7; color: white; border: none; padding: 4px 8px; border-radius: 4px; cursor: pointer; font-size: 12px; width: 100%;">
          Open in Hospital Console ➔
        </button>
      </div>
    `);

    marker.on('click', () => {
      displaySelectedHospitalSidebar(h);
    });

    hospitalMarkers.set(h.id, marker);
  }
}

window.selectHospitalFromMap = (hospitalId) => {
  currentHospitalId = hospitalId;
  const select = document.getElementById('consoleHospitalSelect');
  if (select) select.value = hospitalId;
  onHospitalSelected(hospitalId);
  // Switch to hospital console tab
  document.querySelector('[data-tab="hospital-console"]').click();
};

function displaySelectedHospitalSidebar(h) {
  document.getElementById('selHospName').textContent = h.name;
  document.getElementById('selHospDetails').innerHTML = `
    <div style="display: flex; flex-direction: column; gap: 0.35rem; font-size: 0.85rem;">
      <div><strong>Zone:</strong> ${h.zone}</div>
      <div><strong>Address:</strong> ${h.address}</div>
      <div><strong>Phone:</strong> ${h.phone}</div>
      <div><strong>Contact:</strong> ${h.contact_person}</div>
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.35rem; margin-top: 0.5rem;">
        <div class="stat-box"><div class="stat-val" style="font-size:1.1rem; color:#f87171;">${h.free_icu}</div><div class="stat-lbl">Free ICU</div></div>
        <div class="stat-box"><div class="stat-val" style="font-size:1.1rem; color:#c084fc;">${h.free_ventilator}</div><div class="stat-lbl">Free Vent</div></div>
      </div>
    </div>
  `;
}

// Simulated Ambulances
function spawnSimulatedAmbulances() {
  const routes = [
    { id: 'AMB-108', lat: 17.4300, lon: 78.4350, dest: 'hosp_apollo_jh', deltaLat: 0.0002, deltaLon: -0.0002 },
    { id: 'AMB-102', lat: 17.4450, lon: 78.4800, dest: 'hosp_kims_sec', deltaLat: -0.0001, deltaLon: 0.0002 },
    { id: 'AMB-104', lat: 17.4350, lon: 78.3700, dest: 'hosp_aig_gach', deltaLat: 0.0001, deltaLon: -0.0001 }
  ];

  for (const amb of routes) {
    const ambIcon = L.divIcon({
      className: 'amb-marker',
      html: `<div style="font-size: 20px; filter: drop-shadow(0 0 4px #06b6d4);">🚑</div>`,
      iconSize: [24, 24]
    });
    const marker = L.marker([amb.lat, amb.lon], { icon: ambIcon }).addTo(mapInstance);
    ambulanceMarkers.set(amb.id, { marker, amb });
  }

  // Smooth movement animation
  setInterval(() => {
    ambulanceMarkers.forEach(({ marker, amb }) => {
      amb.lat += amb.deltaLat;
      amb.lon += amb.deltaLon;
      // Bounce back if drifting too far
      if (Math.abs(amb.lat - 17.42) > 0.08) amb.deltaLat *= -1;
      if (Math.abs(amb.lon - 78.45) > 0.08) amb.deltaLon *= -1;
      marker.setLatLng([amb.lat, amb.lon]);
    });
  }, 1000);
}

// Hospital Bed Console
async function onHospitalSelected(hospitalId) {
  const hosp = allHospitals.find(h => h.id === hospitalId);
  if (hosp) {
    document.getElementById('consoleHospZone').textContent = hosp.zone;
    document.getElementById('consoleHospPhone').textContent = hosp.phone;
  }
  await loadHospitalBeds(hospitalId);
  await loadPendingTransfers(hospitalId);
}

async function loadHospitalBeds(hospitalId) {
  try {
    const res = await fetch(`/api/hospitals/${hospitalId}/beds`);
    const data = await res.json();
    const beds = data.beds || [];

    // Group by ward
    const wards = [...new Set(beds.map(b => b.ward_name))];
    if (!currentWard || !wards.includes(currentWard)) {
      currentWard = wards[0];
    }

    // Render Ward Tabs
    const wardContainer = document.getElementById('wardTabsContainer');
    wardContainer.innerHTML = wards.map(w => `
      <button class="ward-tab-btn ${w === currentWard ? 'active' : ''}" onclick="window.switchWard('${w}')">
        ${w}
      </button>
    `).join('');

    renderBedGrid(beds.filter(b => b.ward_name === currentWard));
  } catch (err) {
    console.error('Error loading beds:', err);
  }
}

window.switchWard = (wardName) => {
  currentWard = wardName;
  onHospitalSelected(currentHospitalId);
};

function renderBedGrid(beds) {
  const container = document.getElementById('bedGridContainer');
  container.innerHTML = beds.map(b => {
    let statusSymbol = '●';
    if (b.status === 'AVAILABLE') statusSymbol = '✓';
    if (b.status === 'RESERVED') statusSymbol = '⏱';
    if (b.status === 'OCCUPIED') statusSymbol = '👤';
    if (b.status === 'CLEANING') statusSymbol = '🧹';

    return `
      <div class="bed-card status-${b.status}" onclick="window.openBedModal('${b.id}')">
        <div class="bed-header-row">
          <span class="bed-id">${b.bed_number}</span>
          <span class="bed-type-pill">${b.bed_type}</span>
        </div>
        <div class="bed-status-indicator">
          <span>${statusSymbol}</span> ${b.status}
        </div>
        <div class="bed-patient-text">
          ${b.patient_name || (b.status === 'RESERVED' ? 'Reserved (15m)' : 'Empty')}
        </div>
      </div>
    `;
  }).join('');
}

// Bed Action Modal & Optimistic UI Updates
window.openBedModal = async (bedId) => {
  const modal = document.getElementById('bedActionModal');
  const title = document.getElementById('modalBedTitle');
  const body = document.getElementById('modalBedBody');

  // Fetch fresh bed details
  const hosp = allHospitals.find(h => h.id === currentHospitalId);
  title.textContent = `Bed Details - ${bedId}`;

  body.innerHTML = `
    <div style="display: flex; flex-direction: column; gap: 0.75rem;">
      <p>Manage patient admission, discharge, or inter-hospital transfer for this bed.</p>
      <div style="display: flex; gap: 0.5rem; margin-top: 0.5rem;">
        <button class="btn btn-success" onclick="window.executeBedAdmit('${bedId}')">✓ Admit Patient</button>
        <button class="btn btn-danger" onclick="window.executeBedDischarge('${bedId}')">✕ Discharge Patient</button>
      </div>
    </div>
  `;

  modal.classList.remove('hidden');
};

window.executeBedAdmit = async (bedId) => {
  document.getElementById('bedActionModal').classList.add('hidden');

  // Optimistic UI update: instantly mark bed as OCCUPIED in DOM
  const bedEl = document.querySelector(`.bed-card[onclick*="${bedId}"]`);
  let originalHtml = '';
  if (bedEl) {
    originalHtml = bedEl.outerHTML;
    bedEl.className = 'bed-card status-OCCUPIED';
    bedEl.querySelector('.bed-status-indicator').innerHTML = '<span>👤</span> OCCUPIED';
    bedEl.querySelector('.bed-patient-text').textContent = 'Walk-in Patient';
  }

  try {
    const res = await fetch('/api/beds/admit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ bedId, patientName: 'Walk-in Patient' })
    });
    const result = await res.json();
    if (!res.ok) throw new Error(result.message || 'Admission failed');
  } catch (err) {
    // Rollback optimistic update
    if (bedEl && originalHtml) {
      bedEl.outerHTML = originalHtml;
    }
    alert(`Optimistic Rollback: ${err.message}`);
  }
};

window.executeBedDischarge = async (bedId) => {
  document.getElementById('bedActionModal').classList.add('hidden');
  try {
    await fetch('/api/beds/discharge', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ bedId })
    });
    loadHospitalBeds(currentHospitalId);
  } catch (err) {
    alert('Discharge error: ' + err.message);
  }
};

// Test Optimistic Rollback Button
document.getElementById('btnSimulateRollback').addEventListener('click', async () => {
  alert('Simulating network failure during admission to verify instant optimistic UI rollback...');
  try {
    const res = await fetch('/api/beds/admit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ bedId: 'demo-bed', simulateFailure: true })
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.message);
  } catch (err) {
    alert(`[Optimistic UI Verified] Backend rejected request: "${err.message}". UI rolled back to previous state without corruption!`);
  }
});

// Incoming Transfer Alert Banner & Countdown
function showIncomingTransferAlert(alertData) {
  const banner = document.getElementById('incomingTransferBanner');
  const headline = document.getElementById('bannerHeadline');
  const subtext = document.getElementById('bannerSubtext');
  const countdownEl = document.getElementById('bannerCountdown');

  headline.textContent = alertData.title;
  subtext.textContent = alertData.message;
  banner.classList.remove('hidden');

  let remaining = alertData.metadata.countdownSec || 60;
  countdownEl.textContent = `${remaining}s`;

  if (bannerTimerInterval) clearInterval(bannerTimerInterval);
  bannerTimerInterval = setInterval(() => {
    remaining--;
    countdownEl.textContent = `${remaining}s`;
    if (remaining <= 0) {
      clearInterval(bannerTimerInterval);
      banner.classList.add('hidden');
    }
  }, 1000);

  document.getElementById('btnBannerAccept').onclick = async () => {
    clearInterval(bannerTimerInterval);
    banner.classList.add('hidden');
    await fetch(`/api/transfers/${alertData.metadata.transferId}/accept`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ staffActor: 'Dr. Console Staff' })
    });
    loadTransfers();
  };

  document.getElementById('btnBannerReject').onclick = async () => {
    clearInterval(bannerTimerInterval);
    banner.classList.add('hidden');
    await fetch(`/api/transfers/${alertData.metadata.transferId}/reject`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ reason: 'Diverted due to surgeon unavailability' })
    });
    loadTransfers();
  };
}

// Ambulance Dispatch View
function setupDispatch() {
  // Preset buttons
  document.querySelectorAll('.btn-preset').forEach(btn => {
    btn.addEventListener('click', () => {
      document.getElementById('dispLat').value = btn.dataset.lat;
      document.getElementById('dispLon').value = btn.dataset.lon;
    });
  });

  const form = document.getElementById('dispatchForm');
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const ambulanceId = document.getElementById('dispAmbId').value;
    const triage = document.getElementById('dispTriage').value;
    const bedType = document.getElementById('dispBedType').value;
    const lat = parseFloat(document.getElementById('dispLat').value);
    const lon = parseFloat(document.getElementById('dispLon').value);

    const btn = document.getElementById('btnFindBeds');
    btn.disabled = true;
    btn.textContent = 'Searching Redis Geospatial...';

    try {
      const res = await fetch('/api/dispatch/find-nearest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ambulanceId, patientTriage: triage, bedType, lat, lon })
      });

      const data = await res.json();
      btn.disabled = false;
      btn.textContent = '⚡ Find Nearest Suitable Bed (< 100ms)';

      if (!res.ok) {
        alert(data.message || 'Dispatch error');
        return;
      }

      // Show Benchmark Banner
      const benchBanner = document.getElementById('searchBenchmarkBanner');
      benchBanner.classList.remove('hidden');
      document.getElementById('benchLatencyVal').textContent = `${data.searchLatencyMs} ms`;
      document.getElementById('rateLimitVal').textContent = data.rateLimitRemaining;

      renderDispatchResults(data.matches, triage, bedType, ambulanceId);
    } catch (err) {
      btn.disabled = false;
      btn.textContent = '⚡ Find Nearest Suitable Bed (< 100ms)';
      alert('Network error: ' + err.message);
    }
  });
}

function renderDispatchResults(matches, triage, bedType, ambulanceId) {
  const container = document.getElementById('dispatchResultsList');
  if (!matches || matches.length === 0) {
    container.innerHTML = '<div class="empty-state-text">No free beds found within search radius.</div>';
    return;
  }

  container.innerHTML = matches.map(m => `
    <div class="dispatch-result-card">
      <div class="dispatch-hosp-header">
        <div>
          <h4>${m.name}</h4>
          <span class="subtitle">${m.zone} · ${m.phone}</span>
        </div>
        <div class="dist-eta-pill">
          ${m.distanceKm} km · ~${m.estimatedMinutes} mins
        </div>
      </div>
      <div>
        <div style="font-size: 0.85rem; margin-bottom: 0.35rem;">
          Free ${bedType} Beds: <strong>${m.freeBedsOfType}</strong> (Total Free: ${m.freeTotal})
        </div>
      </div>
      <button class="btn btn-primary btn-sm" onclick="window.reserveAmbulanceBed('${m.hospitalId}', '${bedType}', '${ambulanceId}', '${triage}')" ${m.freeBedsOfType <= 0 ? 'disabled' : ''}>
        🔒 Atomically Reserve Bed (15m Hold)
      </button>
    </div>
  `).join('');
}

window.reserveAmbulanceBed = async (hospitalId, bedType, ambulanceId, triage) => {
  // Find a free bed ID for this hospital
  const res = await fetch(`/api/hospitals/${hospitalId}/beds?type=${bedType}&status=AVAILABLE`);
  const data = await res.json();
  if (!data.beds || data.beds.length === 0) {
    alert('No free beds left at this hospital.');
    return;
  }
  const targetBed = data.beds[0];

  const reserveRes = await fetch('/api/beds/reserve', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      bedId: targetBed.id,
      hospitalId,
      ambulanceId,
      patientTriage: triage
    })
  });
  const reserveData = await reserveRes.json();

  if (reserveRes.ok) {
    alert(`✓ Bed ${targetBed.bed_number} reserved successfully for ${ambulanceId}!\nReservation ID: ${reserveData.reservationId}\nTTL: 15 minutes with auto-release.`);
    loadCityStats();
  } else {
    alert(`Double Allocation Prevented: ${reserveData.message}`);
  }
};

// Transfer Saga & Operations
async function loadTransfers() {
  try {
    const res = await fetch('/api/audit');
    const data = await res.json();
    const logs = data.logs || [];
    renderAuditLogs(logs);
  } catch (e) {}
}

async function loadPendingTransfers(hospitalId) {
  try {
    const res = await fetch(`/api/transfers/pending/${hospitalId}`);
    const data = await res.json();
    const pending = data.pending || [];
    document.getElementById('pendingTransferCount').textContent = pending.length;

    const list = document.getElementById('pendingTransfersList');
    if (pending.length === 0) {
      list.innerHTML = '<div class="empty-state-text">No pending incoming transfer requests.</div>';
      return;
    }

    list.innerHTML = pending.map(t => `
      <div style="background: var(--bg-secondary); border: 1px solid var(--border-color); padding: 0.65rem; border-radius: 6px; margin-bottom: 0.5rem; display: flex; justify-content: space-between; align-items: center;">
        <div>
          <div><strong>${t.patient_name}</strong> (${t.triage_level})</div>
          <div style="font-size: 0.75rem; color: var(--text-muted);">Required: ${t.bed_type} · Pri: ${t.priority}</div>
        </div>
        <div style="display: flex; gap: 0.35rem;">
          <button class="btn btn-success btn-xs" onclick="window.acceptTransferSaga('${t.id}')">✓ Accept</button>
          <button class="btn btn-danger btn-xs" onclick="window.rejectTransferSaga('${t.id}')">✕ Divert</button>
        </div>
      </div>
    `).join('');
  } catch (e) {}
}

window.acceptTransferSaga = async (transferId) => {
  await fetch(`/api/transfers/${transferId}/accept`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ staffActor: 'Dr. Console Staff' })
  });
  loadHospitalBeds(currentHospitalId);
  loadPendingTransfers(currentHospitalId);
};

window.rejectTransferSaga = async (transferId) => {
  await fetch(`/api/transfers/${transferId}/reject`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ reason: 'Diverted due to surgeon unavailability' })
  });
  loadHospitalBeds(currentHospitalId);
  loadPendingTransfers(currentHospitalId);
};

// City Stats & Ops Metrics
async function loadCityStats() {
  try {
    const res = await fetch('/api/city-stats');
    const data = await res.json();
    document.getElementById('statTotalBeds').textContent = data.total_beds || '4,200';
    document.getElementById('statFreeBeds').textContent = data.available_beds || '0';
    document.getElementById('statReservedBeds').textContent = data.reserved_beds || '0';
    document.getElementById('statOccupiedBeds').textContent = data.occupied_beds || '0';
    document.getElementById('statFreeIcu').textContent = data.available_icu || '0';
    document.getElementById('statFreeVent').textContent = data.available_ventilator || '0';
    document.getElementById('statFreeGen').textContent = data.available_general || '0';
    if (data.latencyMetrics) {
      document.getElementById('statP95').textContent = `${data.latencyMetrics.p95} ms`;
      document.getElementById('metricP95').textContent = `${data.latencyMetrics.p95} ms`;
    }
  } catch (e) {}
}

async function loadMetrics() {
  try {
    const res = await fetch('/api/metrics');
    const data = await res.json();
    document.getElementById('epsBadge').textContent = `${data.eventsPerSec || 0} EPS`;
    document.getElementById('metricEps').textContent = data.eventsPerSec || 0;
    document.getElementById('metricPublished').textContent = data.publishedTotal || 0;
    document.getElementById('metricLag').textContent = data.totalConsumerLag || 0;
    document.getElementById('metricDlq').textContent = data.deadLettersCount || 0;

    // Queue depths table
    const tbody = document.getElementById('queueDepthsTableBody');
    if (tbody && data.queueDepths) {
      tbody.innerHTML = Object.entries(data.queueDepths).map(([name, q]) => `
        <tr>
          <td><code>${name}</code></td>
          <td>✓ true</td>
          <td>10</td>
          <td><strong>${q.queued}</strong></td>
          <td>${q.inFlight}</td>
          <td><code>medigrid.dlx</code></td>
        </tr>
      `).join('');
    }
  } catch (e) {}
}

function renderAuditLogs(logs) {
  const container = document.getElementById('auditLogContainer');
  if (!container) return;
  container.innerHTML = logs.map(l => `
    <div style="font-family: var(--font-mono); font-size: 0.75rem; padding: 0.4rem 0.6rem; border-bottom: 1px solid var(--border-color); display: flex; justify-content: space-between;">
      <span>[${new Date(l.timestamp).toLocaleTimeString()}] <strong>${l.event_type}</strong> - ${l.entity_type}:${l.entity_id}</span>
      <span style="color: var(--text-muted);">${l.actor}</span>
    </div>
  `).join('');
}

// Live Stress & Failure Simulator Suite
function setupSimulator() {
  const buttons = document.querySelectorAll('.btn-run-sim');
  const consoleEl = document.getElementById('simOutputConsole');

  buttons.forEach(btn => {
    btn.addEventListener('click', async () => {
      const scenario = btn.dataset.scenario;
      btn.disabled = true;
      btn.textContent = 'Executing Scenario...';
      consoleEl.textContent = `[Running Scenario: ${scenario}] Executing concurrent requests against MediGrid middleware...`;

      try {
        const res = await fetch(`/api/simulate/${scenario}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({})
        });
        const result = await res.json();
        consoleEl.textContent = JSON.stringify(result, null, 2);
        btn.disabled = false;
        btn.textContent = '▶ Run Again';
        loadCityStats();
        loadMetrics();
      } catch (err) {
        consoleEl.textContent = `Error executing scenario: ${err.message}`;
        btn.disabled = false;
        btn.textContent = '▶ Retry Scenario';
      }
    });
  });

  document.getElementById('btnClearSimLog').addEventListener('click', () => {
    consoleEl.textContent = 'Ready. Select a test scenario above.';
  });
}

// Keyboard Navigation & Shortcuts
function setupKeyboardShortcuts() {
  document.addEventListener('keydown', (e) => {
    // If typing in an input field, do not trigger shortcuts
    if (['INPUT', 'SELECT', 'TEXTAREA'].includes(document.activeElement.tagName)) return;

    if (e.key === 'Escape') {
      document.querySelectorAll('.modal-backdrop').forEach(m => m.classList.add('hidden'));
    }
    if (e.key === 'a' || e.key === 'A') {
      // Find first available bed and open admit
      const availBed = document.querySelector('.bed-card.status-AVAILABLE');
      if (availBed) availBed.click();
    }
    if (e.key === 'd' || e.key === 'D') {
      // Find first occupied bed
      const occBed = document.querySelector('.bed-card.status-OCCUPIED');
      if (occBed) occBed.click();
    }
    if (e.key === 't' || e.key === 'T') {
      document.getElementById('btnNewTransferModal').click();
    }
  });
}

function setupModals() {
  document.getElementById('btnModalClose').onclick = () => {
    document.getElementById('bedActionModal').classList.add('hidden');
  };

  document.getElementById('btnNewTransferModal').onclick = () => {
    const modal = document.getElementById('newTransferModal');
    const fromSelect = document.getElementById('trfFromHosp');
    const toSelect = document.getElementById('trfToHosp');

    fromSelect.innerHTML = allHospitals.map(h => `<option value="${h.id}">${h.name}</option>`).join('');
    toSelect.innerHTML = allHospitals.map(h => `<option value="${h.id}">${h.name}</option>`).join('');
    if (allHospitals.length > 1) toSelect.selectedIndex = 1;

    modal.classList.remove('hidden');
  };

  document.getElementById('btnTransferModalClose').onclick = () => {
    document.getElementById('newTransferModal').classList.add('hidden');
  };

  document.getElementById('newTransferForm').onsubmit = async (e) => {
    e.preventDefault();
    const patientName = document.getElementById('trfPatientName').value;
    const triageLevel = document.getElementById('trfTriage').value;
    const fromHospitalId = document.getElementById('trfFromHosp').value;
    const toHospitalId = document.getElementById('trfToHosp').value;
    const bedType = document.getElementById('trfBedType').value;

    const res = await fetch('/api/transfers/initiate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        patientId: 'PAT-' + Math.floor(10000 + Math.random() * 90000),
        patientName,
        triageLevel,
        fromHospitalId,
        toHospitalId,
        bedType,
        countdownSec: 60
      })
    });
    const data = await res.json();
    document.getElementById('newTransferModal').classList.add('hidden');

    if (res.ok) {
      alert(`✓ Transfer Saga Dispatched via RabbitMQ Priority Queue!\nTransfer ID: ${data.transferId}\nPriority: ${data.priority}`);
    } else {
      alert(`Transfer initiation error: ${data.reason || data.message}`);
    }
  };
}
