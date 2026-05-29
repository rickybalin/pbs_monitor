/**
 * PBS Monitor — Analytics page (Vue 3 + Chart.js)
 */

const ANALYTICS_PALETTE = [
    '#3b82f6','#f59e0b','#ef4444','#8b5cf6','#06b6d4',
    '#10b981','#f43f5e','#eab308','#14b8a6','#ec4899',
    '#a78bfa','#fb923c','#34d399','#f472b6','#60a5fa',
    '#fbbf24','#22d3ee','#a3e635','#fda4af','#c084fc',
];

function colorFor(groupName, sortedGroups) {
    const idx = sortedGroups.indexOf(groupName);
    return ANALYTICS_PALETTE[((idx >= 0 ? idx : 0)) % ANALYTICS_PALETTE.length];
}

function fmtBin(iso, freq) {
    // 'h' → 'MM-DD HH:00'  'd' → 'YYYY-MM-DD'  'w' → 'YYYY-MM-DD' (week of)
    const d = new Date(iso);
    if (isNaN(d)) return iso;
    const pad = n => String(n).padStart(2, '0');
    if (freq === 'h') return `${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:00`;
    return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
}

// Collapse reservation-like groups (R##### or M#####) into a single 'Reservations' key
function collapseResvGroups(series, groups) {
    const resvRe = /^[RMrm]\d+$/;
    const collapsed = {};
    const newGroups = [];
    let hasResv = false;
    for (const g of groups) {
        if (resvRe.test(g)) {
            hasResv = true;
            if (!collapsed['Reservations']) collapsed['Reservations'] = null;
        } else {
            collapsed[g] = series[g];
            newGroups.push(g);
        }
    }
    if (hasResv) {
        // Sum all reservation series element-wise
        const resvGroups = groups.filter(g => resvRe.test(g));
        const len = (series[resvGroups[0]] || []).length;
        const summed = new Array(len).fill(0);
        for (const g of resvGroups) {
            const vals = series[g] || [];
            for (let i = 0; i < len; i++) summed[i] += (vals[i] || 0);
        }
        collapsed['Reservations'] = summed.map(v => Math.round(v * 100) / 100);
        newGroups.push('Reservations');
    }
    return { series: collapsed, groups: newGroups.sort() };
}

// Filter out groups whose total node-hours across all bins is below a threshold
const DEPTH_MIN_NODE_HOURS = 100;   // hide queues with < 100 total node-hours

const { createApp, ref, reactive, computed, onMounted } = Vue;

createApp({
    setup() {
        // ── state ──
        const systemName     = ref('PBS Monitor');
        const days           = ref(30);
        const freqOverride   = ref('auto');     // 'auto' | 'h' | 'd' | 'w'
        const groupBy        = ref('queue');    // 'queue' | 'allocation_type'
        const xAxis          = ref('queue_time');
        const loading        = ref(false);
        const error          = ref(null);
        const scatterNote    = ref(null);
        const lastRefresh    = ref(null);

        const utilMeta       = ref('');
        const depthMeta      = ref('');

        const freqChoices = [
            { k: 'auto', l: 'Auto' },
            { k: 'h',    l: 'Hour' },
            { k: 'd',    l: 'Day' },
            { k: 'w',    l: 'Week' },
        ];

        const effectiveFreq = computed(() => {
            if (freqOverride.value !== 'auto') return freqOverride.value;
            if (days.value <= 7) return 'h';
            if (days.value < 90) return 'd';
            return 'w';
        });
        const effectiveFreqLabel = computed(() => ({ h:'Hour', d:'Day', w:'Week' })[effectiveFreq.value]);

        // Filter state
        const filterDims = [
            { key: 'queue',           label: 'Queue' },
            { key: 'owner',           label: 'Owner' },
            { key: 'project',         label: 'Project' },
            { key: 'allocation_type', label: 'Alloc Type' },
        ];
        const filterOptions   = reactive({ queue: [], owner: [], project: [], allocation_type: [] });
        const filterSelections = reactive({ queue: [], owner: [], project: [], allocation_type: [] });
        const filterMode      = reactive({ queue: 'include', owner: 'include', project: 'include', allocation_type: 'include' });
        const filterSearch    = reactive({ queue: '', owner: '', project: '', allocation_type: '' });
        const openFilterPanel = ref(null);

        function toggleFilterPanel(key) {
            openFilterPanel.value = (openFilterPanel.value === key) ? null : key;
        }
        function activeFilterCount(key) { return filterSelections[key].length; }
        function filteredOptions(key) {
            const s = (filterSearch[key] || '').toLowerCase();
            const opts = filterOptions[key] || [];
            if (!s) return opts;
            return opts.filter(v => v.toLowerCase().includes(s));
        }
        function clearFilter(key) {
            filterSelections[key] = [];
            filterMode[key] = 'include';
        }
        function applyFilters() {
            openFilterPanel.value = null;
            reload();
            reloadScatter();
        }

        // Close dropdown when clicking outside
        function _outsideClick(e) {
            if (!e.target.closest('.filter-dropdown')) openFilterPanel.value = null;
        }

        // ── chart canvases & instances ──
        const utilCanvas    = ref(null);
        const depthCanvas   = ref(null);
        const scatterCanvas = ref(null);
        let _utilChart      = null;
        let _depthChart     = null;
        let _scatterChart   = null;

        // ── query string building ──
        function buildParams(extra = {}) {
            const p = new URLSearchParams();
            p.set('days', days.value);
            if (freqOverride.value !== 'auto') p.set('freq', freqOverride.value);
            p.set('group_by', groupBy.value);
            for (const dim of filterDims) {
                const sel = filterSelections[dim.key];
                if (!sel || sel.length === 0) continue;
                const paramKey = filterMode[dim.key] === 'exclude'
                    ? `${dim.key}_exclude`
                    : dim.key;
                for (const v of sel) p.append(paramKey, v);
            }
            for (const [k, v] of Object.entries(extra)) p.set(k, v);
            return p.toString();
        }

        // ── fetchers ──
        async function fetchFilters() {
            try {
                const r = await fetch(`/api/analytics/filters?days=${days.value}`);
                if (!r.ok) return;
                const data = await r.json();
                filterOptions.queue           = data.queues || [];
                filterOptions.owner           = data.owners || [];
                filterOptions.project         = data.projects || [];
                filterOptions.allocation_type = data.allocation_types || [];
            } catch (e) { console.warn('filters fetch:', e); }
        }

        async function fetchSystemName() {
            try {
                const r = await fetch('/api/system');
                if (r.ok) {
                    const d = await r.json();
                    systemName.value = d.system_name || 'PBS Monitor';
                }
            } catch {}
        }

        const loadingUtil  = ref(false);
        const loadingDepth = ref(false);
        const loadingScatter = ref(false);
        const loadingAny = computed(() => loadingUtil.value || loadingDepth.value || loadingScatter.value);

        async function reload() {
            loadingUtil.value  = true;
            loadingDepth.value = true;
            loading.value = true;
            error.value = null;
            try {
                const qs = buildParams();
                const [uRes, dRes] = await Promise.all([
                    fetch(`/api/analytics/utilization?${qs}`),
                    fetch(`/api/analytics/queue-depth?${qs}`),
                ]);
                if (!uRes.ok || !dRes.ok) throw new Error('API error');
                const uData = await uRes.json();
                const dData = await dRes.json();

                // Collapse reservation groups
                const uCollapsed = collapseResvGroups(uData.series, uData.groups);
                uData.series = uCollapsed.series; uData.groups = uCollapsed.groups;
                const dCollapsed = collapseResvGroups(dData.series, dData.groups);
                dData.series = dCollapsed.series; dData.groups = dCollapsed.groups;

                // Filter depth: drop queues with negligible total
                const dFiltered = filterSmallGroups(dData.series, dData.groups, DEPTH_MIN_NODE_HOURS);
                dData.series = dFiltered.series; dData.groups = dFiltered.groups;

                renderLineChart('util',  uData, '%', '/ capacity', true);
                renderLineChart('depth', dData, 'system-hours', 'queued backlog', true);
                utilMeta.value  = `${uData.groups.length} group(s) · ${uData.bins.length} bins · ${uData.total_nodes} compute nodes`;
                depthMeta.value = `${dData.groups.length} group(s) · ${dData.bins.length} bins · normalized to ${dData.total_nodes} nodes (< ${DEPTH_MIN_NODE_HOURS} system-hours hidden)`;
                lastRefresh.value = new Date().toLocaleTimeString();
            } catch (e) {
                console.error(e);
                error.value = `Failed to load analytics: ${e.message}`;
            } finally {
                loading.value = false;
                loadingUtil.value  = false;
                loadingDepth.value = false;
            }
        }

        async function reloadScatter() {
            loadingScatter.value = true;
            try {
                const qs = buildParams({ x_axis: xAxis.value });
                const r = await fetch(`/api/analytics/wait-vs-score?${qs}`);
                if (!r.ok) throw new Error('API error');
                const data = await r.json();
                scatterNote.value = data.note || null;
                renderScatter(data);
            } catch (e) {
                console.error('scatter:', e);
                scatterNote.value = `Failed: ${e.message}`;
            } finally {
                loadingScatter.value = false;
            }
        }

        // ── chart renderers ──
        function filterSmallGroups(series, groups, minTotal) {
            const kept = groups.filter(g => {
                const vals = series[g] || [];
                return vals.reduce((a, b) => a + b, 0) >= minTotal;
            });
            const filtered = {};
            for (const g of kept) filtered[g] = series[g];
            return { series: filtered, groups: kept };
        }

        function _commonLineOpts(yLabel, stacked) {
            return {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { labels: { color: '#e0e0e0', boxWidth: 12 }, position: 'bottom' },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        itemSort: (a, b) => b.parsed.y - a.parsed.y,
                        callbacks: {
                            beforeBody: () => [],
                            afterBody: (items) => {
                                // items are already sorted largest-first by itemSort
                                const total = items.reduce((s, it) => s + (it.parsed.y || 0), 0);
                                const hidden = Math.max(0, items.length - 5);
                                return [
                                    `Total: ${total.toFixed(2)}`,
                                    ...(hidden > 0 ? [`(+${hidden} more not shown)`] : []),
                                ];
                            },
                            label: (item) => {
                                // Only render top 5 items
                                const sorted = item.chart.tooltip.dataPoints
                                    .slice()
                                    .sort((a, b) => b.parsed.y - a.parsed.y);
                                const rank = sorted.findIndex(d => d.datasetIndex === item.datasetIndex);
                                if (rank >= 5) return null;
                                return ` ${item.dataset.label}: ${item.parsed.y.toFixed(2)}`;
                            },
                        },
                    },
                },
                scales: {
                    x: { ticks: { color: '#94a3b8', maxRotation: 45, autoSkip: true, maxTicksLimit: 20 },
                         grid: { color: '#2d3748' },
                         stacked: stacked || false },
                    y: { ticks: { color: '#94a3b8' }, grid: { color: '#2d3748' },
                         title: { display: true, text: yLabel, color: '#94a3b8' },
                         stacked: stacked || false },
                },
            };
        }

        function renderLineChart(which, data, yUnit, subtitle, stacked) {
            const freq = data.freq;
            const labels = (data.bins || []).map(b => fmtBin(b, freq));
            const sorted = [...(data.groups || [])].sort();
            const datasets = sorted.map(grp => ({
                label: grp,
                data: data.series[grp] || [],
                borderColor: colorFor(grp, sorted),
                backgroundColor: colorFor(grp, sorted) + '99',
                borderWidth: 1.5,
                pointRadius: 0,
                pointHoverRadius: 4,
                tension: 0.2,
                fill: stacked ? 'origin' : false,
            }));

            const canvas = which === 'util' ? utilCanvas.value : depthCanvas.value;
            if (!canvas) return;
            const existing = which === 'util' ? _utilChart : _depthChart;
            if (existing) existing.destroy();

            const chart = new Chart(canvas.getContext('2d'), {
                type: 'line',
                data: { labels, datasets },
                options: _commonLineOpts(yUnit, stacked),
            });
            if (which === 'util')  _utilChart  = chart;
            else                   _depthChart = chart;
        }

        function renderScatter(data) {
            if (!scatterCanvas.value) return;
            if (_scatterChart) { _scatterChart.destroy(); _scatterChart = null; }
            const points = data.points || [];
            if (points.length === 0) return;

            // Group by queue
            const byQueue = {};
            for (const p of points) {
                if (!byQueue[p.queue]) byQueue[p.queue] = [];
                byQueue[p.queue].push({ x: p.x, y: p.y, job_id: p.job_id, owner: p.owner });
            }
            const sortedQ = Object.keys(byQueue).sort();
            const datasets = sortedQ.map(q => ({
                label: q,
                data: byQueue[q],
                backgroundColor: colorFor(q, sortedQ),
                borderColor: colorFor(q, sortedQ),
                pointRadius: 3,
                pointHoverRadius: 5,
            }));

            const xLabel = data.x_axis === 'elapsed_time' ? 'Elapsed time (hours)' : 'Queue time (hours)';
            _scatterChart = new Chart(scatterCanvas.value.getContext('2d'), {
                type: 'scatter',
                data: { datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { labels: { color: '#e0e0e0', boxWidth: 12 }, position: 'bottom' },
                        tooltip: {
                            callbacks: {
                                label: (ctx) => {
                                    const d = ctx.raw;
                                    return `${ctx.dataset.label}: x=${d.x}h y=${d.y} (${d.job_id} ${d.owner})`;
                                },
                            },
                        },
                    },
                    scales: {
                        x: { type: 'linear', ticks: { color: '#94a3b8' }, grid: { color: '#2d3748' },
                             title: { display: true, text: xLabel, color: '#94a3b8' } },
                        y: { type: 'logarithmic',
                             ticks: { color: '#94a3b8',
                                      callback: (v) => Number.isInteger(Math.log10(v)) ? v.toLocaleString() : '' },
                             grid: { color: '#2d3748' },
                             title: { display: true, text: 'Score at run start (log scale)', color: '#94a3b8' } },
                    },
                },
            });
        }

        function setDays(d) {
            days.value = d;
            // refresh filter options for new window, then reload
            fetchFilters().then(() => {
                reload();
                reloadScatter();
            });
        }

        onMounted(async () => {
            document.addEventListener('click', _outsideClick);
            await fetchSystemName();
            await fetchFilters();
            await reload();
            await reloadScatter();
        });

        return {
            // state
            systemName, days, freqOverride, groupBy, xAxis,
            loading, loadingAny, error, scatterNote, lastRefresh,
            utilMeta, depthMeta,
            freqChoices, effectiveFreq, effectiveFreqLabel,
            // filters
            filterDims, filterOptions, filterSelections, filterMode, filterSearch,
            openFilterPanel,
            toggleFilterPanel, activeFilterCount, filteredOptions, clearFilter, applyFilters,
            // canvases
            utilCanvas, depthCanvas, scatterCanvas,
            // actions
            setDays, reload, reloadScatter,
        };
    }
}).mount('#app');
