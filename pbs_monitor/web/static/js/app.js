/**
 * PBS Monitor Dashboard — Vue 3 frontend
 */

// ── constants ────────────────────────────────────────────────────────────────

const STATE_COLORS = {
    A: '#4ade80', B: '#6b7280', C: '#ef4444', D: '#f59e0b',
    E: '#3b82f6', F: '#06b6d4', G: '#8b5cf6', H: '#a855f7',
    I: '#4b5563', J: '#7f1d1d', K: '#581c87', L: '#2563eb',
    M: '#4c1d95', N: '#374151',
};
const STATE_CHAR_LABELS = {
    A:'free', B:'offline', C:'down', D:'busy', E:'job-exclusive',
    F:'job-sharing', G:'reserve', H:'resv-exclusive', I:'down,offline',
    J:'state-unknown,down', K:'state-unknown,down,offline',
    L:'job-exclusive,resv-exclusive', M:'offline,resv-exclusive', N:'unknown',
};
const FALLBACK_COLOR = '#1f2937';

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
    for (let i = 0; i < s.length; i++) { h = ((h << 5) - h) + s.charCodeAt(i); h |= 0; }
    return Math.abs(h);
}
function jobColor(id) { return JOB_PALETTE[hashStr(String(id)) % JOB_PALETTE.length]; }
function queueColor(name) { return QUEUE_COLORS[hashStr(String(name)) % QUEUE_COLORS.length]; }

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
    if (diff < 0) return 'just now';
    if (diff < 60) return `${diff}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    return `${Math.floor(diff / 3600)}h ${Math.floor((diff % 3600) / 60)}m ago`;
}

// ── Vue app ──────────────────────────────────────────────────────────────────

const { createApp, ref, reactive, computed, onMounted, onUnmounted, watch, nextTick } = Vue;

createApp({
    setup() {
        const systemInfo   = ref(null);
        const snapshot     = ref(null);
        const loading      = ref(true);
        const error        = ref(null);

        const activeTab    = ref('running');
        const sortKey      = ref('nodes');
        const sortDesc     = ref(true);
        const queuedSortKey  = ref('score');
        const queuedSortDesc = ref(true);
        const selectedJobId = ref(null);
        const hoveredJobId  = ref(null);

        const nodeCanvas   = ref(null);
        const mapContainer = ref(null);
        const tooltip = reactive({ visible: false, x: 0, y: 0, nodeName: '', state: '', jobId: '', owner: '', project: '', queue: '' });

        let layout = [];
        let cellSize = 10;
        let nodeToJobMap = new Map();
        let jobToIndices = new Map();

        // ── derived ──

        const systemName = computed(() => systemInfo.value?.system_name || 'PBS Monitor');
        const busyNodes = computed(() => {
            const counts = snapshot.value?.state_counts || {};
            return (counts['job-exclusive'] || 0) + (counts['job-sharing'] || 0) + (counts['job-exclusive,resv-exclusive'] || 0);
        });
        const utilization = computed(() => {
            const total = totalComputeNodes.value;
            if (!total) return 0;
            return Math.round(busyNodes.value / total * 100);
        });
        const totalComputeNodes = computed(() => (systemInfo.value?.node_index || []).length);
        const stateCounts = computed(() => snapshot.value?.state_counts || {});

        const jobCounts = computed(() => {
            const s = snapshot.value;
            if (!s) return { running: 0, queued: 0, held: 0 };
            return {
                running: s.jobs?.running?.length ?? s.system?.running_jobs ?? 0,
                queued:  s.jobs?.queued?.length ?? s.system?.queued_jobs ?? 0,
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

        function sortedList(list) {
            const key = sortKey.value;
            return [...list].sort((a, b) => {
                let va = a[key] ?? '';
                let vb = b[key] ?? '';
                if (typeof va === 'string') va = va.toLowerCase();
                if (typeof vb === 'string') vb = vb.toLowerCase();
                if (va < vb) return sortDesc.value ? 1 : -1;
                if (va > vb) return sortDesc.value ? -1 : 1;
                return 0;
            });
        }

        const sortedRunningJobs = computed(() => sortedList(snapshot.value?.jobs?.running || []));
        const sortedQueuedJobs  = computed(() => {
            const list = snapshot.value?.jobs?.queued || [];
            const key = queuedSortKey.value;
            return [...list].sort((a, b) => {
                let va = a[key] ?? '';
                let vb = b[key] ?? '';
                // nulls last
                if (va == null && vb == null) return 0;
                if (va == null) return 1;
                if (vb == null) return -1;
                if (typeof va === 'string') va = va.toLowerCase();
                if (typeof vb === 'string') vb = vb.toLowerCase();
                if (va < vb) return queuedSortDesc.value ? 1 : -1;
                if (va > vb) return queuedSortDesc.value ? -1 : 1;
                return 0;
            });
        });

        const sortedQueues = computed(() => {
            const qs = [...(snapshot.value?.queues || [])].filter(q => q.total > 0).sort((a, b) => b.total - a.total);
            const maxTotal = qs.length > 0 ? Math.max(...qs.map(q => q.total)) : 1;
            return qs.map(q => ({
                ...q,
                runningPctScaled: (q.running / maxTotal * 100).toFixed(1),
                queuedPctScaled:  (q.queued  / maxTotal * 100).toFixed(1),
                heldPctScaled:    (q.held    / maxTotal * 100).toFixed(1),
            }));
        });

        // ── data fetching ──

        async function fetchSystem() {
            const res = await fetch('/api/system');
            if (!res.ok) throw new Error(`System API ${res.status}`);
            systemInfo.value = await res.json();
        }

        async function fetchSnapshot() {
            const res = await fetch('/api/snapshot');
            if (!res.ok) throw new Error(`Snapshot API ${res.status}`);
            snapshot.value = await res.json();
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
                if (!snapshot.value) loading.value = false;
            }
        }

        function rebuildIndexes() {
            nodeToJobMap = new Map();
            jobToIndices = new Map();
            for (const job of (snapshot.value?.jobs?.running || [])) {
                const indices = job.node_indices || [];
                // Store as Set of positions in our layout (0-based into node_index)
                const layoutPositions = new Set();
                const snapIndices = systemInfo.value?.snapshot_indices || [];
                for (const snapIdx of indices) {
                    // Find which layout position has this snapshot_index
                    const pos = snapIndices.indexOf(snapIdx);
                    if (pos !== -1) {
                        layoutPositions.add(pos);
                        nodeToJobMap.set(pos, job.job_id);
                    }
                }
                jobToIndices.set(job.job_id, layoutPositions);
            }
        }

        // ── canvas layout ──

        function computeLayout() {
            const canvas = nodeCanvas.value;
            const container = mapContainer.value;
            if (!canvas || !container || !systemInfo.value) return;

            const totalNodes = (systemInfo.value.node_index || []).length;
            if (totalNodes === 0) return;

            const containerW = container.clientWidth - 24; // padding
            const topo = systemInfo.value.topology;
            const rackNames = topo?.rack_names || [];
            const nodesPerRack = topo?.nodes_per_rack || [];

            if (rackNames.length > 0) {
                const nRacks = rackNames.length;
                const maxInRack = Math.max(...nodesPerRack);
                const gap = 2;

                // Try fitting all racks in one row first, then wrap
                const racksPerRow = Math.min(nRacks, Math.max(10, Math.floor(containerW / 20)));
                const rackRows = Math.ceil(nRacks / racksPerRow);

                // Each rack is 1 cell wide, maxInRack cells tall
                cellSize = Math.max(3, Math.min(14, Math.floor((containerW - (racksPerRow - 1) * gap) / racksPerRow)));

                layout = [];
                let globalIdx = 0;

                for (let ri = 0; ri < nRacks; ri++) {
                    const col = ri % racksPerRow;
                    const row = Math.floor(ri / racksPerRow);
                    const xOff = col * (cellSize + gap);
                    const yOff = row * (maxInRack * (cellSize + 1) + gap * 6);

                    for (let ni = 0; ni < nodesPerRack[ri]; ni++) {
                        layout.push({ x: xOff, y: yOff + ni * (cellSize + 1), idx: globalIdx });
                        globalIdx++;
                    }
                }

                const maxX = Math.max(...layout.map(l => l.x)) + cellSize;
                const maxY = Math.max(...layout.map(l => l.y)) + cellSize;
                canvas.width = maxX;
                canvas.height = maxY;
                canvas.style.width = maxX + 'px';
                canvas.style.height = maxY + 'px';
            } else {
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
                const w = cols * (cellSize + 1);
                const h = rows * (cellSize + 1);
                canvas.width = w;
                canvas.height = h;
                canvas.style.width = w + 'px';
                canvas.style.height = h + 'px';
            }
        }

        // ── canvas draw ──

        function drawMap() {
            const canvas = nodeCanvas.value;
            if (!canvas || layout.length === 0) return;
            const ctx = canvas.getContext('2d');
            const stateStr = snapshot.value?.state_string || '';
            const snapIndices = systemInfo.value?.snapshot_indices || [];

            ctx.clearRect(0, 0, canvas.width, canvas.height);

            const highlightSet = hoveredJobId.value
                ? jobToIndices.get(hoveredJobId.value)
                : selectedJobId.value
                    ? jobToIndices.get(selectedJobId.value)
                    : null;

            for (const cell of layout) {
                // Map layout position → snapshot_data index
                const snapIdx = snapIndices[cell.idx];
                const ch = (snapIdx != null) ? (stateStr[snapIdx] || '0') : '0';
                let color = STATE_COLORS[ch] || FALLBACK_COLOR;

                if ('EFL'.includes(ch)) {
                    const jid = nodeToJobMap.get(cell.idx);
                    if (jid) color = jobColor(jid);
                }

                ctx.fillStyle = color;
                ctx.fillRect(cell.x, cell.y, cellSize, cellSize);

                if (highlightSet && highlightSet.has(cell.idx)) {
                    ctx.strokeStyle = '#ffffff';
                    ctx.lineWidth = 1.5;
                    ctx.strokeRect(cell.x - 0.5, cell.y - 0.5, cellSize + 1, cellSize + 1);
                }
            }
        }

        // ── canvas interaction ──

        function cellAtMouse(e) {
            const canvas = nodeCanvas.value;
            if (!canvas) return null;
            const rect = canvas.getBoundingClientRect();
            const scaleX = canvas.width / rect.width;
            const scaleY = canvas.height / rect.height;
            const mx = (e.clientX - rect.left) * scaleX;
            const my = (e.clientY - rect.top) * scaleY;
            for (const cell of layout) {
                if (mx >= cell.x && mx < cell.x + cellSize &&
                    my >= cell.y && my < cell.y + cellSize) return cell;
            }
            return null;
        }

        function onCanvasMove(e) {
            const cell = cellAtMouse(e);
            if (!cell) { tooltip.visible = false; return; }

            const nodeIndex = systemInfo.value?.node_index || [];
            const snapIndices = systemInfo.value?.snapshot_indices || [];
            const stateStr  = snapshot.value?.state_string || '';
            const snapIdx = snapIndices[cell.idx];
            const ch = (snapIdx != null) ? (stateStr[snapIdx] || '?') : '?';

            tooltip.nodeName = nodeIndex[cell.idx] || `node-${cell.idx}`;
            tooltip.state    = STATE_CHAR_LABELS[ch] || ch;
            tooltip.jobId = ''; tooltip.owner = ''; tooltip.project = ''; tooltip.queue = '';

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
                activeTab.value = 'running';
                requestAnimationFrame(drawMap);
                nextTick(() => {
                    const row = document.getElementById(`job-row-${jid}`);
                    if (row) row.scrollIntoView({ behavior: 'smooth', block: 'center' });
                });
            }
        }

        // ── jobs table ──

        function sortJobs(key) {
            if (sortKey.value === key) sortDesc.value = !sortDesc.value;
            else { sortKey.value = key; sortDesc.value = true; }
        }
        function sortQueuedJobs(key) {
            if (queuedSortKey.value === key) queuedSortDesc.value = !queuedSortDesc.value;
            else { queuedSortKey.value = key; queuedSortDesc.value = true; }
        }
        function fmtScore(score) {
            if (score == null) return '--';
            // Score is eligible_time in seconds; show as hours
            const h = score / 3600;
            if (h >= 24) return (h / 24).toFixed(1) + 'd';
            return h.toFixed(1) + 'h';
        }
        function highlightJob(jid) { hoveredJobId.value = jid; requestAnimationFrame(drawMap); }
        function clearHighlight()   { hoveredJobId.value = null; requestAnimationFrame(drawMap); }
        function selectJob(jid)     { selectedJobId.value = (selectedJobId.value === jid) ? null : jid; requestAnimationFrame(drawMap); }
        function isOverdue(job)     { return job.remaining_seconds <= 0 && job.elapsed_seconds > 0; }

        const tooltipStyle = computed(() => ({ left: tooltip.x + 'px', top: tooltip.y + 'px' }));

        // ── lifecycle ──

        let pollTimer = null;

        function onResize() { computeLayout(); requestAnimationFrame(drawMap); }

        watch(snapshot, () => {
            nextTick(() => { computeLayout(); requestAnimationFrame(drawMap); });
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
            systemInfo, snapshot, loading, error,
            activeTab, sortKey, sortDesc, queuedSortKey, queuedSortDesc, selectedJobId, hoveredJobId,
            nodeCanvas, mapContainer, tooltip, tooltipStyle,
            systemName, utilization, busyNodes, totalComputeNodes, stateCounts, jobCounts, freshnessClass, timeSinceLastUpdate,
            sortedRunningJobs, sortedQueuedJobs, sortedQueues,
            fetchData, sortJobs, sortQueuedJobs, selectJob, highlightJob, clearHighlight, isOverdue,
            onCanvasMove, onCanvasLeave, onCanvasClick,
            fmtDuration, fmtScore, queueColor,
        };
    }
}).mount('#app');
