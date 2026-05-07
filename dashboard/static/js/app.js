/* ══════ GempaRadar — Main Application JS ══════ */

// ── Init Lucide Icons ──
lucide.createIcons();

// ── Tab System ──
const tabBtns = document.querySelectorAll('.tab-btn');
const tabPages = document.querySelectorAll('.tab-page');
tabBtns.forEach(btn => {
  btn.addEventListener('click', () => {
    tabBtns.forEach(b => b.classList.remove('active'));
    tabPages.forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('page-' + btn.dataset.tab).classList.add('active');
    if (btn.dataset.tab === 'peta2d' && leafMap) setTimeout(() => leafMap.invalidateSize(), 100);
  });
});

// ── Theme Toggle ──
const themeToggle = document.getElementById('theme-toggle');
themeToggle.addEventListener('click', () => {
  document.body.classList.toggle('light-mode');
  const isLight = document.body.classList.contains('light-mode');
  themeToggle.innerHTML = `<i data-lucide="${isLight ? 'sun' : 'moon'}" id="theme-icon" size="16"></i>`;
  lucide.createIcons();
  updateMapTiles(leafMap, isLight);
  if (mapGlobe && mapGlobe.isStyleLoaded()) {
    mapGlobe.setConfigProperty('basemap', 'lightPreset', isLight ? 'day' : 'night');
  }
  if (map3d && map3d.isStyleLoaded()) {
    map3d.setConfigProperty('basemap', 'lightPreset', isLight ? 'day' : 'night');
  }
  
  // Update Charts if they exist
  if (magChart) {
    // magChart is always in the dark hero — keep dark palette
    magChart.update('none');
  }
  if (depthDoughnutChart) {
    depthDoughnutChart.update('none');
  }
  // Force redraw some elements
  lucide.createIcons();
});

function updateMapTiles(map, isLight) {
  if (!map) return;
  // We will keep the colors "alive" by using OSM/Esri by default
  const url = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
  
  // Find the current base layer and replace it only if it's one of the Carto ones
  map.eachLayer(l => { 
    if (l._url && (l._url.includes('cartocdn.com') || l._url.includes('basemaps'))) {
      map.removeLayer(l);
    }
  });
  L.tileLayer(url, { attribution: '&copy; OpenStreetMap contributors' }).addTo(map);
}

// ── WIB Clock ──
const DAYS = ['Minggu','Senin','Selasa','Rabu','Kamis','Jumat','Sabtu'];
const MONTHS = ['Jan','Feb','Mar','Apr','Mei','Jun','Jul','Agt','Sep','Okt','Nov','Des'];
function tickClock() {
  const now = new Date();
  const wib = new Date(now.getTime() + 7*3600000);
  document.getElementById('wib-time').textContent =
    String(wib.getUTCHours()).padStart(2,'0') + ':' +
    String(wib.getUTCMinutes()).padStart(2,'0') + ':' +
    String(wib.getUTCSeconds()).padStart(2,'0');
  document.getElementById('wib-date').textContent =
    DAYS[wib.getUTCDay()] + ', ' + wib.getUTCDate() + ' ' + MONTHS[wib.getUTCMonth()] + ' ' + wib.getUTCFullYear();
}
tickClock(); setInterval(tickClock, 1000);

// ── Time formatting ──
function timeAgo(isoStr) {
  if (!isoStr) return 'Baru saja';
  try {
    const diff = Math.floor((Date.now() - new Date(isoStr).getTime()) / 1000);
    if (diff < 60) return diff + ' detik lalu';
    if (diff < 3600) return Math.floor(diff / 60) + ' menit lalu';
    if (diff < 86400) return Math.floor(diff / 3600) + ' jam lalu';
    return Math.floor(diff / 86400) + ' hari lalu';
  } catch(e) { return 'Baru saja'; }
}

// ── Charts ──
let magChart = null, depthChart = null, depthDoughnutChart = null;

// ── Leaflet & Mapbox Maps ──
let leafMap = null, markerLayer = null;
let mapGlobe = null;
let map3d = null;
let map3dInitialized = false;
let allGempa = [];
let activeTab = 'terkini';
let lastDataIds = new Set();

// Audio context for alert
let audioCtx = null;
function playAlertSound() {
  try {
    if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = audioCtx.createOscillator();
    const gain = audioCtx.createGain();
    osc.connect(gain); gain.connect(audioCtx.destination);
    osc.type = 'sine'; osc.frequency.value = 880;
    gain.gain.setValueAtTime(0.3, audioCtx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + 1.5);
    osc.start(); osc.stop(audioCtx.currentTime + 1.5);
  } catch(e) {}
}

function initMap(id) {
  const isLight = document.body.classList.contains('light-mode');
  const map = L.map(id, { center: [-2, 118], zoom: 5, minZoom: 3, maxZoom: 13, zoomControl: false });
  L.control.zoom({ position: 'bottomright' }).addTo(map);

  // Street — OpenStreetMap standard (default, always visible like USGS)
  const baseStreet = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors', maxZoom: 19
  });
  const baseDark = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OSM &copy; CARTO', subdomains: 'abcd', maxZoom: 20
  });
  const baseSat = L.layerGroup([
    L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
      attribution: 'Tiles &copy; Esri'
    }),
    L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Transportation/MapServer/tile/{z}/{y}/{x}', {
      attribution: 'Tiles &copy; Esri', opacity: 0.7
    }),
    L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}', {
      attribution: 'Tiles &copy; Esri'
    })
  ]);

  // Default: Street in light, Dark in dark mode
  (isLight ? baseStreet : baseDark).addTo(map);
  L.control.layers({ "Street": baseStreet, "Gelap": baseDark, "Satelit": baseSat }, null, { position: 'topright' }).addTo(map);

  const legend = L.control({ position: 'bottomleft' });
  legend.onAdd = () => {
    const d = L.DomUtil.create('div', 'map-legend');
    d.innerHTML = `<h4>Magnitudo</h4>
      <div class="lgrow"><div class="lgdot" style="background:#ef4444"></div>Kuat (&gt;5.0)</div>
      <div class="lgrow"><div class="lgdot" style="background:#f97316"></div>Sedang (4–5)</div>
      <div class="lgrow"><div class="lgdot" style="background:#3b82f6"></div>Minor (3–4)</div>
      <div class="lgrow"><div class="lgdot" style="background:#94a3b8"></div>Mikro (&lt;3)</div>`;
    return d;
  };
  legend.addTo(map);
  return map;
}

// Init Peta 2D
leafMap = initMap('map');
markerLayer = L.layerGroup().addTo(leafMap);
setTimeout(() => leafMap.invalidateSize(), 300);

// Set Mapbox token globally
const _mbt = 'pk.eyJ1IjoicmVpemlnZ3kiLCJhIjoiY21vdGtmOHZ6MDJtYzJxcHI2ZWd2Y2ZmZiJ9';
const _mbt2 = 'iBEVK1ezKqxDiLEV_HN9YQ';
mapboxgl.accessToken = `${_mbt}.${_mbt2}`;

// Init Globe Map in Dashboard
function initGlobeMap() {
  try {
    const container = document.getElementById('map-globe');
    if (!container) { console.warn('map-globe container not found'); return; }
    const isLight = document.body.classList.contains('light-mode');
    mapGlobe = new mapboxgl.Map({
      container: 'map-globe',
      style: 'mapbox://styles/mapbox/standard',
      center: [118, -2],
      zoom: 1.8,
      projection: 'globe'
    });
    
    mapGlobe.on('style.load', () => {
      // Style config — separated so errors don't block marker rendering
      try {
        mapGlobe.setConfigProperty('basemap', 'lightPreset', isLight ? 'day' : 'night');
        mapGlobe.setConfigProperty('basemap', 'showPointOfInterestLabels', false);
        mapGlobe.setConfigProperty('basemap', 'showTransitLabels', false);
      } catch(e) { console.warn('Globe style config error:', e); }
      // Render markers (allGempa may already be populated by fetchAndRender)
      if (allGempa.length > 0) {
        try { renderMapboxMarkers(mapGlobe, allGempa, 'globe-count'); } catch(e) { console.warn('Globe markers error:', e); }
      }
    });
  } catch(e) {
    console.warn('initGlobeMap error:', e);
    mapGlobe = null;
  }
}
setTimeout(initGlobeMap, 100);

// Init Peta 3D Kota (Mapbox GL JS)
function initMap3D() {
  if (map3dInitialized) return;
  try {
    const container = document.getElementById('map3d');
    if (!container) { console.warn('map3d container not found'); return; }
    const isLight = document.body.classList.contains('light-mode');
    map3d = new mapboxgl.Map({
      container: 'map3d',
      style: 'mapbox://styles/mapbox/standard',
      center: [106.8229, -6.1944],
      zoom: 16,
      pitch: 65,
      bearing: -17.6
    });
    
    map3d.on('style.load', () => {
      try {
        map3d.setConfigProperty('basemap', 'lightPreset', isLight ? 'day' : 'night');
        if (allGempa.length > 0) renderMapboxMarkers(map3d, allGempa, 'map3d-count');
      } catch(e) { console.warn('Map3D style.load error:', e); }
    });
    map3dInitialized = true;
  } catch(e) {
    console.warn('initMap3D error:', e);
    map3d = null;
  }
}

// Ensure 3D map is resized when its tab is opened
tabBtns.forEach(btn => {
  if (btn.dataset.tab === 'peta3d') {
    btn.addEventListener('click', () => {
      initMap3D();
      if (map3d) setTimeout(() => map3d.resize(), 100);
    });
  }
});

// ── Tab wiring for map tabs ──
document.querySelectorAll('.map-tab').forEach(tab => {
  tab.addEventListener('click', () => {
    const parent = tab.closest('.panel');
    parent.querySelectorAll('.map-tab').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    activeTab = tab.dataset.tab;
    closeDetailCard();
    renderMap(filterByTab(allGempa, activeTab), markerLayer, leafMap);
  });
});

function filterByTab(list, tab) {
  switch (tab) {
    case 'terkini': return [...list].sort((a,b) => (b.event_time||'').localeCompare(a.event_time||'')).slice(0, 1);
    case 'm5': return list.filter(g => (g.magnitude||0) >= 5.0);
    case 'dirasakan': return list.filter(g => (g.felt||0) > 0 || (g.sig||0) >= 400 || ((g.magnitude||0) >= 4.5 && (g.depth_km||999) <= 60));
    case 'tsunami': return list.filter(g => g.tsunami === 1 || ((g.magnitude||0) >= 6.0 && (g.depth_km||999) < 100));
    case 'realtime': return list;
    default: return list;
  }
}

// ── Helpers ──
function magColor(m) { return m>=5?'#ef4444':m>=4?'#f97316':m>=3?'#3b82f6':'#94a3b8'; }
function magR(m) { return Math.max(5, m * 3.8); }
function magCls(m) { return m<3?'m-mikro':m<4?'m-minor':m<5?'m-sedang':'m-kuat'; }
function dCls(d) { return d<70?'d-dang':d<300?'d-men':'d-dlm'; }
function dLbl(d) { return d<70?'Dangkal':d<300?'Menengah':'Dalam'; }
function fmt(iso) {
  if (!iso) return '—';
  try { return new Date(iso).toLocaleString('id-ID',{timeZone:'Asia/Jakarta',hour12:false,day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'}); }
  catch { return iso; }
}
function fmtWIB(iso) {
  if (!iso) return '—';
  try {
    const d = new Date(iso);
    return d.toLocaleDateString('id-ID',{timeZone:'Asia/Jakarta',day:'numeric',month:'long',year:'numeric'})
      + ' • ' + d.toLocaleTimeString('id-ID',{timeZone:'Asia/Jakarta',hour:'2-digit',minute:'2-digit',hour12:false}) + ' WIB';
  } catch { return iso; }
}

// ── Detail Card ──
function showDetailCard(g, mapId) {
  const m = g.magnitude||0, d = g.depth_km||0;
  const felt = g.felt||0;
  const tsun = g.tsunami===1||(m>=6.0&&d<100)||(m>=5.5&&d<50);
  let statusCls='gs-confirmed', statusTxt='Terkonfirmasi';
  if (felt>0) { statusCls='gs-felt'; statusTxt='Gempa Dirasakan'; }
  if (tsun) { statusCls='gs-warning'; statusTxt='Berpotensi Tsunami'; }
  
  // Move card to active map
  const card = document.getElementById('gdetail-card');
  const targetMap = document.getElementById(mapId);
  if (targetMap && targetMap.parentElement) {
    targetMap.parentElement.appendChild(card);
  }

  document.getElementById('gd-status').className = 'gdetail-status ' + statusCls;
  document.getElementById('gd-status').textContent = statusTxt;
  document.getElementById('gd-title').textContent = g.place||'—';
  document.getElementById('gd-time').textContent = fmtWIB(g.event_time);
  const magEl = document.getElementById('gd-mag');
  magEl.textContent = m.toFixed(1)+' '+(g.magnitude_type||'').toUpperCase();
  magEl.style.color = magColor(m);
  document.getElementById('gd-depth').textContent = d.toFixed(1)+' km ('+dLbl(d)+')';
  const lat=(g.latitude||0).toFixed(2), lon=(g.longitude||0).toFixed(2);
  document.getElementById('gd-coord').textContent = Math.abs(lat)+' '+(g.latitude>=0?'LU':'LS')+'–'+Math.abs(lon)+' '+(g.longitude>=0?'BT':'BB');
  const tsEl = document.getElementById('gd-tsunami');
  tsEl.textContent = tsun?'Berpotensi Tsunami':'Tidak Berpotensi';
  tsEl.className = 'rv '+(tsun?'gdetail-tsunami-warn':'gdetail-tsunami-safe');
  document.getElementById('gd-link').href = g.url||'#';
  card.classList.add('visible');
}
function closeDetailCard() { document.getElementById('gdetail-card').classList.remove('visible'); }

// ── Render Map (shared for 2D) ──
function renderMap(list, layer, map) {
  if (!layer) return;
  layer.clearLayers();
  const countEl = document.getElementById('map-count');
  if (countEl) countEl.textContent = list.length + ' titik';
  const now = Date.now(), H24 = 86400000;
  
  list.forEach(g => {
    if (!g.latitude || !g.longitude) return;
    const m=g.magnitude||0, cl=magColor(m), r=magR(m);
    let html;
    
    // If we are in realtime tab and the earthquake is recent, use pulsating effect
    if (activeTab === 'realtime') {
      const eventMs = g.event_time ? new Date(g.event_time).getTime() : 0;
      const isRecent = eventMs > 0 && (now - eventMs) < H24;
      
      if (isRecent) {
        const d=r*2, total=d*5, half=d/2;
        const rs = `position:absolute;top:50%;left:50%;margin-top:-${half}px;margin-left:-${half}px;width:${d}px;height:${d}px;border-radius:50%;border:2px solid ${cl};pointer-events:none;`;
        html = `<div style="position:relative;width:${total}px;height:${total}px;overflow:visible">
          <div style="${rs}animation:gempa-ripple 2.5s 0s infinite ease-out"></div>
          <div style="${rs}animation:gempa-ripple 2.5s 0.9s infinite ease-out"></div>
          <div style="position:absolute;top:50%;left:50%;margin-top:-${half}px;margin-left:-${half}px;width:${d}px;height:${d}px;border-radius:50%;background:${cl};opacity:.9;border:1.5px solid rgba(255,255,255,.4);cursor:pointer"></div>
          <div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:#fff;font-size:${Math.max(8,r*.9)}px;font-weight:700;pointer-events:none;text-shadow:0 1px 3px rgba(0,0,0,.8)">${m.toFixed(1)}</div>
        </div>`;
      } else {
        html = `<div style="width:${r*2}px;height:${r*2}px;border-radius:50%;background:${cl};opacity:.7;border:1.5px solid rgba(255,255,255,.2);display:flex;align-items:center;justify-content:center;cursor:pointer">
          <span style="color:#fff;font-size:${Math.max(7,r*.85)}px;font-weight:700;text-shadow:0 1px 3px rgba(0,0,0,.8)">${m.toFixed(1)}</span></div>`;
      }
      
      const marker = L.marker([g.latitude, g.longitude], {
        icon: L.divIcon({ html, className:'', iconSize: isRecent ? [r*10,r*10] : [r*2,r*2], iconAnchor: isRecent ? [r*5,r*5] : [r,r] })
      });
      marker.on('click', () => {
        showDetailCard(g, map._container.id);
        map.setView([g.latitude, g.longitude], 8, { animate: true });
      });
      layer.addLayer(marker);
      
    } else {
      // Standard static rendering for other tabs
      html = `<div style="width:${r*2}px;height:${r*2}px;border-radius:50%;background:${cl};opacity:.85;border:1.5px solid rgba(255,255,255,.3);display:flex;align-items:center;justify-content:center;cursor:pointer">
        <span style="color:#fff;font-size:${Math.max(7,r*.85)}px;font-weight:700;text-shadow:0 1px 3px rgba(0,0,0,.8)">${m.toFixed(1)}</span></div>`;
      const marker = L.marker([g.latitude, g.longitude], {
        icon: L.divIcon({ html, className:'', iconSize:[r*2,r*2], iconAnchor:[r,r] })
      });
      marker.on('click', () => {
        showDetailCard(g, map._container.id);
        map.setView([g.latitude, g.longitude], 8, { animate: true });
      });
      layer.addLayer(marker);
    }
  });
  // Also refresh the sidebar
  renderMapSidebar(list);
}

// ── Map Sidebar (USGS-style left list) ──
let activeSidebarId = null;
let sidebarSortMode = 'time';

function fmtShort(iso) {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString('id-ID', {
      timeZone: 'Asia/Jakarta', day: '2-digit', month: 'short',
      hour: '2-digit', minute: '2-digit', hour12: false
    });
  } catch { return iso; }
}

function renderMapSidebar(list) {
  const el = document.getElementById('msb-list');
  const countEl = document.getElementById('msb-count');
  if (!el) return;

  let sorted = [...list];
  if (sidebarSortMode === 'mag') {
    sorted.sort((a, b) => (b.magnitude || 0) - (a.magnitude || 0));
  } else {
    sorted.sort((a, b) => (b.event_time || '').localeCompare(a.event_time || ''));
  }

  if (countEl) countEl.textContent = sorted.length + ' gempa';

  if (!sorted.length) {
    el.innerHTML = '<div style="padding:24px 16px;text-align:center;color:var(--text-muted);font-size:0.8rem">Tidak ada data</div>';
    return;
  }

  el.innerHTML = sorted.map(g => {
    const m = g.magnitude || 0;
    const depth = g.depth_km || 0;
    const id = g.id || (g.place + g.event_time);
    const isActive = id === activeSidebarId;
    const tsun = g.tsunami === 1 || (m >= 6.0 && depth < 100);
    const safeG = JSON.stringify(g).replace(/"/g, '&quot;');
    return `<div class="msb-item${isActive ? ' active' : ''}" data-msb-id="${id}"
      onclick="sidebarSelect(${safeG}, '${id}')">
      <div class="msb-mag" style="background:${magColor(m)}">${m.toFixed(1)}</div>
      <div class="msb-info">
        <div class="msb-place">${g.place || '—'}</div>
        <div class="msb-meta">
          <span>${fmtShort(g.event_time)}</span>
          <span class="msb-badge">${depth.toFixed(0)} km · ${dLbl(depth)}</span>
          ${tsun ? '<span class="msb-badge msb-badge-warn">Tsunami</span>' : ''}
        </div>
      </div>
    </div>`;
  }).join('');
}

function sidebarSelect(g, id) {
  activeSidebarId = id;
  document.querySelectorAll('.msb-item').forEach(el => el.classList.remove('active'));
  const target = document.querySelector(`.msb-item[data-msb-id="${id}"]`);
  if (target) {
    target.classList.add('active');
    target.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }
  if (leafMap && g.latitude && g.longitude) {
    leafMap.setView([g.latitude, g.longitude], 8, { animate: true });
  }
  showDetailCard(g, 'map');
}

// Sort handler
document.getElementById('msb-sort')?.addEventListener('change', e => {
  sidebarSortMode = e.target.value;
  const list = filterByTab(allGempa, activeTab);
  renderMapSidebar(list);
  if (map3dInitialized) renderMap3DSidebar(list);
});

// ── 3D Map Sidebar Rendering ──
function renderMap3DSidebar(list) {
  const el = document.getElementById('map3d-list');
  const countEl = document.getElementById('map3d-count-sidebar');
  if (!el) return;

  let sorted = [...list].sort((a, b) => (b.event_time || '').localeCompare(a.event_time || ''));
  if (countEl) countEl.textContent = sorted.length + ' gempa';

  if (!sorted.length) {
    el.innerHTML = '<div style="padding:24px 16px;text-align:center;color:var(--text-muted);font-size:0.8rem">Tidak ada data real-time</div>';
    return;
  }

  el.innerHTML = sorted.map(g => {
    const m = g.magnitude || 0;
    const depth = g.depth_km || 0;
    const id = (g.id || (g.place + g.event_time)) + '-3d';
    const isActive = id === activeSidebarId;
    const safeG = JSON.stringify(g).replace(/"/g, '&quot;');
    return `<div class="msb-item${isActive ? ' active' : ''}" data-msb-id="${id}"
      onclick="sidebarSelect3D(${safeG}, '${id}')">
      <div class="msb-mag" style="background:${magColor(m)}">${m.toFixed(1)}</div>
      <div class="msb-info">
        <div class="msb-place">${g.place || '—'}</div>
        <div class="msb-meta">
          <span>${fmtShort(g.event_time)}</span>
          <span class="msb-badge">${depth.toFixed(0)} km</span>
        </div>
      </div>
    </div>`;
  }).join('');
}

function sidebarSelect3D(g, id) {
  activeSidebarId = id;
  document.querySelectorAll('#map3d-list .msb-item').forEach(el => el.classList.remove('active'));
  const target = document.querySelector(`#map3d-list .msb-item[data-msb-id="${id}"]`);
  if (target) {
    target.classList.add('active');
    target.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }
  if (map3d && g.latitude && g.longitude) {
    map3d.flyTo({
      center: [g.longitude, g.latitude],
      zoom: 12,
      pitch: 60,
      bearing: 0,
      speed: 1.5,
      curve: 1,
      essential: true
    });
  }
}

// ── Render Mapbox (Globe & 3D) ──
function renderMapboxMarkers(mapInstance, list, countId) {
  if (!mapInstance) return;
  if (!mapInstance.isStyleLoaded()) {
    // Style not yet ready — queue this render for when it finishes loading
    mapInstance.once('style.load', () => renderMapboxMarkers(mapInstance, list, countId));
    return;
  }
  const countEl = document.getElementById(countId);
  if (countEl) countEl.textContent = list.length + ' titik';
  
  // Remove existing markers specific to this map
  const existingMarkers = mapInstance.getContainer().querySelectorAll('.mapbox-marker');
  existingMarkers.forEach(el => el.remove());

  const isGlobe = mapInstance.getContainer().id === 'map-globe';

  const now = Date.now(), H24 = 86400000;

  list.forEach(g => {
    if (!g.latitude || !g.longitude) return;
    const m=g.magnitude||0, cl=magColor(m), r=magR(m);
    const eventMs = g.event_time ? new Date(g.event_time).getTime() : 0;
    const isRecent = eventMs > 0 && (now - eventMs) < H24;

    // Create a DOM element for each marker
    const el = document.createElement('div');
    el.className = 'mapbox-marker' + (isRecent ? ' mapbox-marker-pulse' : '');
    el.style.width = (r*2) + 'px';
    el.style.height = (r*2) + 'px';
    el.style.borderRadius = '50%';
    el.style.background = cl;
    el.style.opacity = isRecent ? '1' : '0.8';
    el.style.border = '1.5px solid rgba(255,255,255,0.4)';
    el.style.display = 'flex';
    el.style.alignItems = 'center';
    el.style.justifyContent = 'center';
    el.style.cursor = 'pointer';
    el.style.zIndex = isRecent ? '10' : '1';
    el.innerHTML = `<span style="color:#fff;font-size:${Math.max(7,r*0.85)}px;font-weight:700;text-shadow:0 1px 3px rgba(0,0,0,.8)">${m.toFixed(1)}</span>`;
    
    if (isRecent) {
      const ring = document.createElement('div');
      ring.className = 'mapbox-marker-pulse-ring';
      ring.style.setProperty('--pulse-color', cl);
      el.appendChild(ring);
    }

    // Fly to location on click
    el.addEventListener('click', (e) => {
      e.stopPropagation();
      const z = mapInstance.getContainer().id === 'map3d' ? 14 : (isGlobe ? 6 : 8);
      mapInstance.flyTo({
        center: [g.longitude, g.latitude],
        zoom: z,
        pitch: mapInstance.getContainer().id === 'map3d' ? 65 : 0,
        speed: 1.2,
        essential: true
      });
      if (isGlobe) showGlobeDetailCard(g);
      if (mapInstance.getContainer().id === 'map3d') sidebarSelect3D(g, (g.id || (g.place + g.event_time)) + '-3d');
    });

    // Add marker to map
    new mapboxgl.Marker(el).setLngLat([g.longitude, g.latitude]).addTo(mapInstance);
  });
}

// ── Globe Detail Card ──
function showGlobeDetailCard(g) {
  const card = document.getElementById('globe-detail-card');
  if (!card) return;
  const m = g.magnitude || 0;
  const depth = g.depth_km ?? '—';
  const dc = g.depth_category || (depth < 70 ? 'Dangkal' : depth < 300 ? 'Menengah' : 'Dalam');
  const hasTsunami = g.tsunami === 1 || (m >= 6 && depth < 100);

  // Status badge
  const statusEl = document.getElementById('gdc-status');
  if (statusEl) {
    if (hasTsunami) { statusEl.className = 'gdetail-status gs-warning'; statusEl.textContent = 'POTENSI TSUNAMI'; }
    else if (g.felt > 0 || g.sig >= 400) { statusEl.className = 'gdetail-status gs-felt'; statusEl.textContent = 'Dirasakan'; }
    else { statusEl.className = 'gdetail-status gs-confirmed'; statusEl.textContent = 'Terkonfirmasi'; }
  }

  const setEl = (id, val) => { const e = document.getElementById(id); if (e) e.textContent = val; };
  const setHtml = (id, val) => { const e = document.getElementById(id); if (e) e.innerHTML = val; };

  setEl('gdc-title', g.place || '—');
  setEl('gdc-time', g.event_time ? fmt(g.event_time) + ' WIB' : '—');

  const magEl = document.getElementById('gdc-mag');
  if (magEl) {
    magEl.textContent = m.toFixed(1) + ' ' + (g.magnitude_type || 'M');
    magEl.style.color = magColor(m);
  }

  setEl('gdc-depth', depth + ' km — ' + dc);
  setEl('gdc-coord', (g.latitude != null ? g.latitude.toFixed(4) : '—') + '°, ' + (g.longitude != null ? g.longitude.toFixed(4) : '—') + '°');
  setHtml('gdc-tsunami', hasTsunami
    ? '<span style="color:var(--red);font-weight:600">⚠ Berpotensi</span>'
    : '<span style="color:var(--green)">Tidak berpotensi</span>');

  const linkEl = document.getElementById('gdc-link');
  if (linkEl) linkEl.href = g.url || '#';

  card.classList.add('visible');
  lucide.createIcons();
}

function closeGlobeDetailCard() {
  const card = document.getElementById('globe-detail-card');
  if (card) card.classList.remove('visible');
}

// ── Render Gempa Table with Grouping ──
function renderGempa(list) {
  const tb = document.getElementById('gempa-tbody');
  document.getElementById('gempa-count').textContent = list.length + ' event';
  if (!list.length) { tb.innerHTML = '<tr><td colspan="5" class="loading">Belum ada data gempa</td></tr>'; return; }
  const grouped = [];
  list.forEach(g => {
    const prev = grouped[grouped.length - 1];
    if (prev && prev.place === g.place && Math.abs(new Date(prev.event_time) - new Date(g.event_time)) < 60000) {
      prev._count = (prev._count || 1) + 1;
    } else { grouped.push({ ...g, _count: 1 }); }
  });
  const search = (document.getElementById('t-search')?.value || '').toLowerCase();
  tb.innerHTML = grouped.filter(g =>
    (g.place||'').toLowerCase().includes(search)
  ).map(g => {
    const m=g.magnitude||0;
    const id = g.id || (g.place + g.event_time);
    const isNew = lastDataIds.size > 0 && !lastDataIds.has(id);
    const pt = (g.place||'').length > 35 ? g.place.slice(0,35)+'…' : (g.place||'—');
    const gl = g._count > 1 ? `<div style="font-size:.55rem;color:var(--orange);font-weight:700;">${g._count} events</div>` : '';
    if (isNew) {
      const title = m >= 5.0 ? `⚠️ Gempa KUAT Baru: M${m.toFixed(1)}` : `Gempa Baru: M${m.toFixed(1)}`;
      showToast(title, `${g.place}`, m >= 5.0);
      if (m >= 4.0) playAlertSound();
    }
    return `<tr class="${isNew?'new-row':''}" onclick="leafMap.setView([${g.latitude},${g.longitude}], 8); showDetailCard(${JSON.stringify(g).replace(/"/g, '&quot;')}, 'map');" style="cursor:pointer">
      <td><span class="mbadge ${magCls(m)}" style="min-width:38px; font-size:0.8rem;">${m.toFixed(1)}</span></td>
      <td class="place-cell" style="font-size:0.75rem;">${pt}${gl}</td>
      <td class="time-cell" style="font-size:0.65rem;">${fmt(g.event_time).split(',')[0]}</td>
    </tr>`;
  }).join('');
  lastDataIds = new Set(list.map(g => g.id || (g.place + g.event_time)));
}

// Filter listeners
document.getElementById('t-search')?.addEventListener('input', () => renderGempa(allGempa.slice(0,20)));
document.getElementById('t-mag-filter')?.addEventListener('change', () => renderGempa(allGempa.slice(0,20)));

// ── Toast ──
function showToast(title, body, critical=false) {
  const c = document.getElementById('toast-container'); if (!c) return;
  const t = document.createElement('div');
  t.className = `toast ${critical?'t-red':''}`;
  t.innerHTML = `<div class="toast-hdr"><i data-lucide="${critical?'alert-triangle':'info'}" size="16"></i> ${title}</div><div class="toast-body">${body}</div>`;
  c.appendChild(t); lucide.createIcons();
  setTimeout(() => t.classList.add('visible'), 10);
  setTimeout(() => { t.classList.remove('visible'); setTimeout(() => t.remove(), 300); }, 5000);
}

function setConnectivity(ok) {
  const b = document.getElementById('error-banner');
  if (ok) b.classList.remove('visible'); else b.classList.add('visible');
}

// ── Render Wilayah ──
function renderWilayah(list) {
  const el = document.getElementById('wilayah-list');
  if (!list||!list.length) { el.innerHTML='<div class="loading">Belum ada data</div>'; return; }
  const mx = list[0].count || 1;
  el.innerHTML = list.slice(0,10).map((w,i) => `<div class="witem">
    <div class="wrank">${i+1}</div>
    <div class="wbar-wrap"><div class="wname" title="${w.wilayah}">${w.wilayah||'—'}</div><div class="wbar" style="width:${Math.round(w.count/mx*100)}%"></div></div>
    <div class="wcnt">${w.count}×</div></div>`).join('');
}

// ── Render Berita ──
const GRAD = [['#0d1b2a','#1b4a7a'],['#1a0808','#5a1515'],['#0a1a0e','#1a5a28'],['#1a1400','#5a4000'],['#150a1a','#3a186a'],['#0a0e16','#1a3060']];
function renderBerita(list) {
  const el = document.getElementById('news-grid');
  document.getElementById('berita-count').textContent = list.length + ' artikel';
  if (!list.length) { el.innerHTML='<div class="loading">Belum ada berita</div>'; return; }
  el.innerHTML = list.map((n,i) => {
    const src = (n.source||'News Source').replace(/Google News \((.+?)\)/,'$1');
    const time = n.published_time ? timeAgo(n.published_time) : 'Baru saja';
    return `<div class="nitem nitem-compact">
      <div class="ncontent">
        <a href="${n.link||n.url||'#'}" target="_blank" rel="noopener">${n.title}</a>
        <div class="nmeta">
          <span><i data-lucide="clock" size="12" style="vertical-align:middle;margin-right:4px"></i>${time}</span>
          <span>${src}</span>
        </div>
      </div>
    </div>`;
  }).join('');
  lucide.createIcons();
}

// ── Charts ──
function getChartColors() {
  const isLight = document.body.classList.contains('light-mode');
  return {
    gridColor: isLight ? '#e4e4e7' : '#262626',
    tickColor: isLight ? '#71717a' : '#a1a1aa',
  };
}

function renderMagChart(dist) {
  const canvas = document.getElementById('magChart');
  if (!canvas || typeof Chart === 'undefined') return;
  // magChart lives inside the always-dark hero section — hard-code dark palette
  const gridColor = 'rgba(255,255,255,0.08)';
  const tickColor = 'rgba(255,255,255,0.35)';
  const ORDER = ['Mikro (<3)','Minor (3-4)','Sedang (4-5)','Kuat (>5)'];
  const CMAP = {'Mikro (<3)':'#94a3b8','Minor (3-4)':'#3b82f6','Sedang (4-5)':'#f97316','Kuat (>5)':'#ef4444'};
  const vals = ORDER.map(k => dist[k]??0), colors = ORDER.map(k => CMAP[k]);
  if (magChart) { magChart.destroy(); magChart = null; }
  magChart = new Chart(canvas, {
    type:'bar', data:{labels:ORDER,datasets:[{data:vals,backgroundColor:colors,borderRadius:4,barThickness:12}]},
    options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false}},
      scales:{
        x:{grid:{color:gridColor},ticks:{color:tickColor,font:{size:10}}},
        y:{grid:{display:false},ticks:{color:tickColor,font:{size:10},precision:0}}
      }
    }
  });
}
function renderDepthChart(dist) {
  const canvas = document.getElementById('depthChart');
  if (!canvas || typeof Chart === 'undefined') return;
  const { gridColor, tickColor } = getChartColors();
  const labels=['Dangkal','Menengah','Dalam'];
  const vals=[dist['Dangkal (<70 km)']||0,dist['Menengah (70-300 km)']||0,dist['Dalam (>300 km)']||0];
  if (depthChart) depthChart.destroy();
  depthChart = new Chart(canvas, {
    type:'bar', data:{labels,datasets:[{data:vals,backgroundColor:['#22c55e','#f97316','#ef4444'],borderRadius:4}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},
      scales:{
        x:{grid:{display:false},ticks:{color:tickColor}},
        y:{grid:{color:gridColor},ticks:{color:tickColor}}
      }
    }
  });
}

function renderDepthDoughnut(dist) {
  const canvas = document.getElementById('depthDoughnut');
  if (!canvas || typeof Chart === 'undefined') return;
  const vals = [dist['Dangkal (<70 km)']||0, dist['Menengah (70-300 km)']||0, dist['Dalam (>300 km)']||0];
  const total = vals.reduce((a,b) => a+b, 0);
  if (depthDoughnutChart) { depthDoughnutChart.destroy(); depthDoughnutChart = null; }
  depthDoughnutChart = new Chart(canvas, {
    type: 'doughnut',
    data: {
      labels: ['Dangkal', 'Menengah', 'Dalam'],
      datasets: [{
        data: vals,
        backgroundColor: ['#22c55e', '#f97316', '#ef4444'],
        borderColor: 'rgba(3,7,18,0.6)',
        borderWidth: 2,
        hoverOffset: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '64%',
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const pct = total > 0 ? ((ctx.parsed / total) * 100).toFixed(1) : 0;
              return ` ${ctx.label}: ${ctx.parsed} (${pct}%)`;
            }
          }
        }
      }
    }
  });
}

// ── Summary Cards ──
function updateSummary(s) {
  document.getElementById('s-total').textContent = s.total_gempa ?? '—';
  document.getElementById('s-maxmag').textContent = s.max_magnitude!=null ? s.max_magnitude.toFixed(1) : '—';
  document.getElementById('s-depth').textContent = s.rata_rata_kedalaman!=null ? s.rata_rata_kedalaman.toFixed(1) : '—';
  document.getElementById('s-wilayah').textContent = s.wilayah_teraktif || '—';
  // Globe side panel stats
  const setEl = (id, val) => { const e = document.getElementById(id); if (e) e.textContent = val; };
  setEl('gs-total', s.total_gempa ?? '—');
  setEl('gs-maxmag', s.max_magnitude!=null ? s.max_magnitude.toFixed(1) : '—');
  setEl('gs-region', s.wilayah_teraktif || '—');
  // Compute avg magnitude from distribusi
  if (s.distribusi_magnitudo) {
    const dist = s.distribusi_magnitudo;
    const totalN = (dist['Mikro (<3)']||0)+(dist['Minor (3-4)']||0)+(dist['Sedang (4-5)']||0)+(dist['Kuat (>5)']||0);
    const avgMag = totalN > 0
      ? ((dist['Mikro (<3)']||0)*2 + (dist['Minor (3-4)']||0)*3.5 + (dist['Sedang (4-5)']||0)*4.5 + (dist['Kuat (>5)']||0)*5.5) / totalN
      : null;
    setEl('gs-avgmag', avgMag != null ? avgMag.toFixed(1) : '—');
  } else if (s.rata_rata_magnitudo != null) {
    setEl('gs-avgmag', s.rata_rata_magnitudo.toFixed(1));
  }
}

function updateMagDist(dist) {
  if (!dist) return;
  const setEl = (id, val) => { const e = document.getElementById(id); if (e) e.textContent = val; };
  setEl('gd-mikro', dist['Mikro (<3)'] ?? '—');
  setEl('gd-minor', dist['Minor (3-4)'] ?? '—');
  setEl('gd-sedang', dist['Sedang (4-5)'] ?? '—');
  setEl('gd-kuat', dist['Kuat (>5)'] ?? '—');
}

function updateDepth(dist) {
  if (!dist) return;
  document.getElementById('d-dang').textContent = dist['Dangkal (<70 km)'] ?? '—';
  document.getElementById('d-men').textContent = dist['Menengah (70-300 km)'] ?? '—';
  document.getElementById('d-dlm').textContent = dist['Dalam (>300 km)'] ?? '—';
}

// ── Main Fetch ──
async function fetchAndRender() {
  try {
    const res = await fetch('/api/data');
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    setConnectivity(true);
    allGempa = data.gempa_all || data.gempa_terbaru || [];
    const gempa = data.gempa_terbaru || [];
    const berita = data.berita_terbaru || [];
    const spark = data.spark_results || {};

    // Render core UI (table, cards, charts)
    try { renderGempa(gempa); } catch(e) { console.warn('renderGempa error:', e); }
    try { renderMap(filterByTab(allGempa, activeTab), markerLayer, leafMap); } catch(e) { console.warn('renderMap error:', e); }
    try { renderBerita(berita); } catch(e) { console.warn('renderBerita error:', e); }
    try { updateSummary(spark); } catch(e) { console.warn('updateSummary error:', e); }
    try { updateDepth(spark.distribusi_kedalaman); } catch(e) { console.warn('updateDepth error:', e); }
    try { if (spark.distribusi_magnitudo) { renderMagChart(spark.distribusi_magnitudo); updateMagDist(spark.distribusi_magnitudo); } } catch(e) { console.warn('renderMagChart error:', e); }
    try { if (spark.distribusi_kedalaman) { renderDepthDoughnut(spark.distribusi_kedalaman); } } catch(e) { console.warn('renderDepthDoughnut error:', e); }
    try { if (spark.top_wilayah) renderWilayah(spark.top_wilayah); } catch(e) { console.warn('renderWilayah error:', e); }

    // Render Mapbox maps (non-critical — don't break connectivity status)
    try { if (mapGlobe) renderMapboxMarkers(mapGlobe, allGempa, 'globe-count'); } catch(e) { console.warn('Globe render error:', e); }
    try { 
      if (map3dInitialized && map3d) {
        renderMapboxMarkers(map3d, allGempa, 'map3d-count');
        renderMap3DSidebar(allGempa);
      }
    } catch(e) { console.warn('Map3D render error:', e); }

    const lastUpEl = document.getElementById('last-update');
    if (lastUpEl) lastUpEl.textContent = 'Sync: ' + new Date().toLocaleTimeString('id-ID');
  } catch(e) {
    console.error('Fetch error:', e);
    setConnectivity(false);
  } finally {
    setTimeout(fetchAndRender, 30000);
  }
}

fetchAndRender();
