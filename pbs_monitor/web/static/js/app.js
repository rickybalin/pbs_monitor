/**
 * PBS Monitor Dashboard — Vue 3 frontend
 *
 * Renders a live node map (Canvas), running jobs table,
 * queue status bars, and system utilization header.
 *
 * API contract (all GET):
 *   /api/system   → {system_name, total_nodes, topology, node_index, last_collection}
 *   /api/snapshot  → {timestamp, system, state_string, state_counts, jobs:{running:[]}, queues:[]}
 */

// ── constants ────────────────────────────────────────────────────────────────

const STATE_COLORS = {
    A: '#4ade80', // free — green
    B: '#6b7280', // offline — gray
    C: '#ef4444', // down — red
    D: '#f59e0b', // busy — amber
    E: '#3b82f6', // job-exclusive — blue
    F: '#06b6d4', // job-sharing — cyan
    G: '#8b5cf6', // reserve — violet
    H: '#a855f7', // resv-exclusive — purple
    I: '#4b5563', // down,offline — dark gray
    J: '#7f1d1d', // state-unknown,down — dark red
    K: '#581c87', // state-unknown,down,offline — dark purple
    L: '#2563eb', // job-exclusive,resv-exclusive — darker blue
    M: '#4c1d95', // offline,resv-exclusive — dark violet
    N: '#374151', // unknown — slate
};
const FALLBACK_COLOR = '#1f2937';

// 24 distinct job colours so neighbouring jobs don't collide visually
const JOB_PALETTE = [
    '#3b82f6','#2563eb','#1d4ed8','#1e40af','#1e3a8a',
    '#60a5fa','#0ea5e9','#0284c7','#0369a1','#075985',
    '#6366f1','#4f46e5','#4338ca','#3730a3','#312e81',
    '#8b5cf6','#7c3aed','#6d28d9','#5b21b6','#4c1d95',
    '#818cf8','#a78bfa','#c084fc','#38bdf8',
];

const QUEUE_COLORS = [
    '#3b82f6','#f59e0b','#ef4444','#8b5cf6','#06b6d4',
    '#10b981','#f43f5e','#eab308','#14b8a6','#ec4899',
];

// ── helpers ──────────────────────────────────────────────────────────────────

function hashStr(s) {
    let h = 0;
    for (let i = 0; i < s.length; i++) {
        h = ((h << 5) - h) + s.charCodeAt(i);
        h |= 0;
    }
    return Math.abs(h);
}

function jobColor(jobId) {
    return JOB_PALETTE[hashStr(String(jobId)) % JOB_PALETTE.length];
}

function queueColor(name) {
    return QUEUE_COLORS[hashStr(String(name)) % QUEUE_COLORS.length];
}

function fmtDuration(totalSec) {
    if (totalSec == null) return '--';
    const s = Math.max(0, Math.floor(totalSec));
    const d = Math.floor(s / 86400);
    const h = Math.floor((s % 86400) / 3600);
    const m = Math.floor((s % 3600) / 60);
    if (d > 0) return `${d}d ${h}h ${m}m`;
    if (h > 0) return `${h}h ${m}m`;
    return `${m}m`;
}

function timeSince(isoStr) {
    if (!isoStr) return '';
    const diff = Math.floor((Date.now() - new Date(isoStr).getTime()) / 1000);
    if (diff < 60) return `${diff}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    return `${Math.floor(diff / 3600)}h ${Math.floor((diff % 3600) / 60)}m ago`;
}

// ── Vue app ──────────────────────────────────────────────────────────────────

const { createApp, ref, reactive, computed, onMounted, onUnmounted, watch, nextTick } = Vue;

createApp({
    setup() {
        // ---- state ----
        const systemInfo = ref(null);
        const snapshot   = ref(null);
        const loading    = ref(true);
        const error      = ref(null);
        const lastFetch  = ref(null);          // Date when we last got data

        const sortKey    = ref('nodes');
        const sortDesc   = ref(true);
        const selectedJobId = ref(null);
        const hoveredJobId  = ref(null);

        const nodeCanvas   = ref(null);        // template ref
        const mapContainer = ref(null);        // template ref
        const tooltip = reactive({ visible: false, x: 0, y: 0, nodeName: '', state: '', jobId: '', owner: '', project: '', queue: '' });

        // Precomputed layout (survives across redraws until resize / new data)
        let layout = [];        // [{x, y, idx}]
        let cellSize = 10;
        let mapCols = 0;
        let mapRows = 0;

        // Index look-ups rebuilt when snapshot arrives
        let nodeToJobMap = new Map();   // snapshot_index → job short_id
        let jobToIndices = new Map();   // job short_id → Set<snapshot_index>

        // ---- derived ----
        const systemName = computed(() => systemInfo.value?.system_name || 'PBS Monitor');
        const utilization = computed(() => snapshot.value?.system?.utilization_percent ?? 0);
        const jobs = computed(() => {
            const s = snapshot.value;
            if (!s) return { running: 0, queued: 0, held: 0 };
            return {
                running: s.jobs?.running?.length ?? s.system?.running_jobs ?? 0,
                queued:  s.system?.queued_jobs ?? 0,
                held:    s.system?.held_jobs ?? 0,
            };
        });

        const freshnessClass = computed(() => {
            if (!snapshot.value?.timestamp) return '';
            const age = (Date.now() - new Date(snapshot.value.timestamp).getTime()) / 1000;
            if (age < 300)  return 'green';
            if (age < 900)  return 'yellow';
            return 'red';
        });
        const timeSinceLastUpdate = computed(() => timeSince(snapshot.value?.timestamp));

        const sortedJobs = computed(() => {
            const list = [...(snapshot.value?.jobs?.running || [])];
            const key = sortKey.value;
            return list.sort((a, b) => {
                let va = a[key] ?? '';
                let vb = b[key] ?? '';
                if (typeof va === 'string') va = va.toLowerCase();
                if (typeof vb === 'string') vb = vb.toLowerCase();
                if (va < vb) return sortDesc.value ? 1 : -1;
                if (va > vb) return sortDesc.value ? -1 : 1;
                return 0;
            });
        });

        const sortedQueues = computed(() =>
            [...(snapshot.value?.queues || [])]
                .filter(q => q.total > 0)
                .sort((a, b) => b.total - a.total)
        );

        // ---- data fetching ----
        async function fetchSystem() {
            const res = await fetch('/api/system');
            if (!res.ok) throw new Error(`System API ${res.status}`);
            systemInfo.value = await res.json();
        }

        async function fetchSnapshot() {
            const res = await fetch('/api/snapshot');
            if (!res.ok) throw new Error(`Snapshot API ${res.status}`);
            snapshot.value = await res.json();
            lastFetch.value = new Date();
            rebuildIndexes();
        }

        async function fetchData() {
            try {
                if (!systemInfo.value) await fetchSystem();
                await fetchSnapshot();
                loading.value = false;
                error.value = null;
            } catch (e) {
                console.error(e);
                error.value = e.message;
                if (!snapshot.value) loading.value = false; // show error overlay
            }
        }

        function rebuildIndexes() {
            nodeToJobMap = new Map();
            jobToIndices = new Map();
            const running = snapshot.value?.jobs?.running || [];
            for (const job of running) {
                const indices = job.node_indices || [];
                jobToIndices.set(job.job_id, new Set(indices));
                for (const idx of indices) {
                    nodeToJobMap.set(idx, job.job_id);
                }
            }
        }

        // ---- canvas layout ----
        function computeLayout() {
            const canvas = nodeCanvas.value;
            const container = mapContainer.value;
            if (!canvas || !container || !systemInfo.value) return;

            const totalNodes = (systemInfo.value.node_index || []).length;
            if (totalNodes === 0) return;

            const containerW = container.clientWidth - 32;   // 16px padding each side
            const topo = systemInfo.value.topology;
            const rackNames = topo?.rack_names || [];
            const nodesPerRack = topo?.nodes_per_rack || [];

            if (rackNames.length > 0) {
                // Rack-aware layout: racks as columns, nodes within each rack as rows
                const nRacks = rackNames.length;
                const maxNodesInRack = Math.max(...nodesPerRack);

                // How many racks per row?  Try to keep aspect ratio reasonable.
                const racksPerRow = Math.min(nRacks, Math.max(10, Math.floor(containerW / 60)));
                const rackRows = Math.ceil(nRacks / racksPerRow);
                const gap = 2;

                // Cell size: fit racksPerRow racks, each maxNodesInRack cells tall
                const availW = containerW - (racksPerRow - 1) * gap;
                cellSize = Math.max(3, Math.min(12, Math.floor(availW / racksPerRow)));

                layout = [];
                let globalIdx = 0;  // running index into snapshot_data

                for (let ri = 0; ri < nRacks; ri++) {
                    const col = ri % racksPerRow;
                    const row = Math.floor(ri / racksPerRow);
                    const xOff = col * (cellSize + gap);
                    const yOff = row * (maxNodesInRack * (cellSize + 1) + gap * 4);

                    const count = nodesPerRack[ri];
                    for (let ni = 0; ni < count; ni++) {
                        layout.push({ x: xOff, y: yOff + ni * (cellSize + 1), idx: globalIdx });
                        globalIdx++;
                    }
                }

                // Canvas dimensions
                const maxX = Math.max(...layout.map(l => l.x)) + cellSize;
                const maxY = Math.max(...layout.map(l => l.y)) + cellSize;
                canvas.width = maxX;
                canvas.height = maxY;
                mapCols = racksPerRow;
                mapRows = 0; // not used in rack mode
            } else {
                // Flat grid fallback
                const cols = Math.ceil(Math.sqrt(totalNodes * 1.5));
                cellSize = Math.max(3, Math.min(12, Math.floor(containerW / cols)));
                const rows = Math.ceil(totalNodes / cols);

                layout = [];
                for (let i = 0; i < totalNodes; i++) {
                    layout.push({
                        x: (i % cols) * (cellSize + 1),
                        y: Math.floor(i / cols) * (cellSize + 1),
                        idx: i,
                    });
                }
                canvas.width = cols * (cellSize + 1);
                canvas.height = rows * (cellSize + 1);
                mapCols = cols;
                mapRows = rows;
            }
        }

        // ---- canvas draw ----
        function drawMap() {
            const canvas = nodeCanvas.value;
            if (!canvas || layout.length === 0) return;
            const ctx = canvas.getContext('2d');
            const stateStr = snapshot.value?.state_string || '';

            ctx.clearRect(0, 0, canvas.width, canvas.height);

            const highlightSet = hoveredJobId.value
                ? jobToIndices.get(hoveredJobId.value)
                : selectedJobId.value
                    ? jobToIndices.get(selectedJobId.value)
                    : null;

            for (const cell of layout) {
                const ch = stateStr[cell.idx] || '0';
                let color = STATE_COLORS[ch] || FALLBACK_COLOR;

                // Per-job colouring for occupied nodes
                if ('EFL'.includes(ch)) {
                    const jid = nodeToJobMap.get(cell.idx);
                    if (jid) color = jobColor(jid);
                }

                ctx.fillStyle = color;
                ctx.fillRect(cell.x, cell.y, cellSize, cellSize);

                // Highlight border
                if (highlightSet && highlightSet.has(cell.idx)) {
                    ctx.strokeStyle = '#ffffff';
                    ctx.lineWidth = 1.5;
                    ctx.strokeRect(cell.x - 0.5, cell.y - 0.5, cellSize + 1, cellSize + 1);
                }
            }
        }

        // ---- canvas interaction ----
        function cellAtMouse(e) {
            const canvas = nodeCanvas.value;
            if (!canvas) return null;
            const rect = canvas.getBoundingClientRect();
            const scaleX = canvas.width / rect.width;
            const scaleY = canvas.height / rect.height;
            const mx = (e.clientX - rect.left) * scaleX;
            const my = (e.clientY - rect.top) * scaleY;

            // Linear scan is fine for ≤10K cells
            for (const cell of layout) {
                if (mx >= cell.x && mx < cell.x + cellSize &&
                    my >= cell.y && my < cell.y + cellSize) {
                    return cell;
                }
            }
            return null;
        }

        function onCanvasMove(e) {
            const cell = cellAtMouse(e);
            if (!cell) { tooltip.visible = false; return; }

            const nodeIndex = systemInfo.value?.node_index || [];
            const stateStr  = snapshot.value?.state_string || '';
            const name = nodeIndex[cell.idx] || `node-${cell.idx}`;
            const ch   = stateStr[cell.idx] || '?';

            tooltip.nodeName = name;
            tooltip.state    = STATE_CHAR_LABELS[ch] || ch;
            tooltip.jobId    = '';
            tooltip.owner    = '';
            tooltip.project  = '';
            tooltip.queue    = '';

            const jid = nodeToJobMap.get(cell.idx);
            if (jid) {
                const job = (snapshot.value?.jobs?.running || []).find(j => j.job_id === jid);
                if (job) {
                    tooltip.jobId   = job.job_id;
                    tooltip.owner   = job.owner;
                    tooltip.project = job.project;
                    tooltip.queue   = job.queue;
                }
            }

            // Position tooltip near mouse, keep on-screen
            const container = mapContainer.value;
            const rect = container.getBoundingClientRect();
            tooltip.x = e.clientX - rect.left + 12;
            tooltip.y = e.clientY - rect.top + 12;
            tooltip.visible = true;
        }

        function onCanvasLeave() { tooltip.visible = false; }

        function onCanvasClick(e) {
            const cell = cellAtMouse(e);
            if (!cell) return;
            const jid = nodeToJobMap.get(cell.idx);
            if (jid) {
                selectedJobId.value = (selectedJobId.value === jid) ? null : jid;
                requestAnimationFrame(drawMap);
                // Scroll table row into view
                nextTick(() => {
                    const row = document.getElementById(`job-row-${jid}`);
                    if (row) row.scrollIntoView({ behavior: 'smooth', block: 'center' });
                });
            }
        }

        // ---- jobs table ----
        function sortJobs(key) {
            if (sortKey.value === key) sortDesc.value = !sortDesc.value;
            else { sortKey.value = key; sortDesc.value = true; }
        }

        function highlightJob(jid) { hoveredJobId.value = jid; requestAnimationFrame(drawMap); }
        function clearHighlight()   { hoveredJobId.value = null; requestAnimationFrame(drawMap); }
        function selectJob(jid)     { selectedJobId.value = (selectedJobId.value === jid) ? null : jid; requestAnimationFrame(drawMap); }
        function isOverdue(job)     { return job.remaining_seconds <= 0 && job.elapsed_seconds > 0; }

        // ---- tooltipStyle ----
        const tooltipStyle = computed(() => ({
            left: tooltip.x + 'px',
            top:  tooltip.y + 'px',
        }));

        // ---- lifecycle ----
        let pollTimer = null;

        function onResize() {
            computeLayout();
            requestAnimationFrame(drawMap);
        }

        // Watch for new data → recompute + redraw
        watch(snapshot, () => {
            nextTick(() => {
                computeLayout();
                requestAnimationFrame(drawMap);
            });
        });

        onMounted(async () => {
            await fetchData();
            pollTimer = setInterval(fetchData, 30000);
            window.addEventListener('resize', onResize);
        });

        onUnmounted(() => {
            if (pollTimer) clearInterval(pollTimer);
            window.removeEventListener('resize', onResize);
        });

        return {
            // state
            systemInfo, snapshot, loading, error, lastFetch,
            sortKey, sortDesc, selectedJobId, hoveredJobId,
            nodeCanvas, mapContainer, tooltip, tooltipStyle,
            // computed
            systemName, utilization, jobs, freshnessClass, timeSinceLastUpdate,
            sortedJobs, sortedQueues,
            // methods
            fetchData, sortJobs, selectJob, highlightJob, clearHighlight, isOverdue,
            onCanvasMove, onCanvasLeave, onCanvasClick,
            fmtDuration, queueColor,
        };
    }
}).mount('#app');
