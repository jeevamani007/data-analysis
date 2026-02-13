/**
 * Banking Data Analysis Assistant - Frontend Application
 * Connects to backend APIs for comprehensive banking data analysis
 */

// API Configuration
const API_BASE_URL = 'http://localhost:8000/api';

// Format 24h "HH:MM:SS" or "HH:MM" to readable "10:30 AM" / "10:30:45 AM"
function formatTimeReadable(str) {
    if (!str || typeof str !== 'string') return str;
    var parts = str.trim().split(':');
    var h = parseInt(parts[0], 10);
    var m = parts[1] ? parseInt(parts[1], 10) : 0;
    var s = parts[2] ? parseInt(parts[2], 10) : 0;
    if (isNaN(h)) return str;
    var ampm = h >= 12 ? 'PM' : 'AM';
    h = h % 12;
    if (h === 0) h = 12;
    var mStr = m < 10 ? '0' + m : String(m);
    if (s > 0) {
        var sStr = s < 10 ? '0' + s : String(s);
        return h + ':' + mStr + ':' + sStr + ' ' + ampm;
    }
    return h + ':' + mStr + ' ' + ampm;
}

// Format "YYYY-MM-DD HH:MM:SS" or ISO to readable date-time (DB timestamp)
// full: "Jan 15, 2025 10:30 AM" | compact: "15 Jan 10:30 AM" for diagram
function formatDateTimeReadable(str, compact) {
    if (!str || typeof str !== 'string') return str;
    str = str.trim();
    var d = new Date(str.replace(' ', 'T'));
    if (isNaN(d.getTime())) return str;
    var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    var datePart = compact
        ? (d.getDate() + ' ' + months[d.getMonth()])
        : (months[d.getMonth()] + ' ' + d.getDate() + ', ' + d.getFullYear());
    var h = d.getHours(), m = d.getMinutes(), s = d.getSeconds();
    var ampm = h >= 12 ? 'PM' : 'AM';
    h = h % 12;
    if (h === 0) h = 12;
    var timePart = h + ':' + (m < 10 ? '0' + m : m) + (s > 0 ? ':' + (s < 10 ? '0' + s : s) : '') + ' ' + ampm;
    return datePart + ' ' + timePart;
}

// State Management
let uploadedFiles = [];
let sessionId = null;
let analysisResults = null;
let dateColumnInfo = null;

// Retail Event Explanations (column-observed from user files)
const RETAIL_EVENT_EXPLANATIONS = {
    'Customer Visit': 'Customer visited the store or site',
    'Product View': 'Customer viewed a product',
    'Product Search': 'Customer searched for products',
    'Add To Cart': 'Customer added product to cart',
    'Remove From Cart': 'Customer removed product from cart',
    'Apply Coupon': 'Customer applied a coupon or discount',
    'Checkout Started': 'Customer started checkout',
    'Address Entered': 'Customer entered delivery address',
    'Payment Selected': 'Customer selected payment method',
    'Payment Success': 'Payment succeeded',
    'Payment Failed': 'Payment failed',
    'Order Placed': 'Customer placed order',
    'Order Confirmed': 'Order was confirmed',
    'Invoice Generated': 'Invoice was generated for the order',
    'Order Packed': 'Shop packed the product',
    'Order Shipped': 'Order handed over to courier',
    'Out For Delivery': 'Courier on the way to customer',
    'Order Delivered': 'Product reached customer',
    'Order Cancelled': 'Order was cancelled',
    'Return Initiated': 'Customer requested a return',
    'Return Received': 'Returned product was received',
    'Refund Processed': 'Refund was processed',
    'User Signed Up': 'Customer created account',
    'User Logged In': 'Customer logged in',
    'User Logged Out': 'Customer signed out',
    // Legacy
    'Product Viewed': 'Customer viewed a product',
    'Added to Cart': 'Customer added product to cart',
    'Removed from Cart': 'Customer removed product from cart',
    'Order Created': 'Customer placed order',
    'Payment Initiated': 'Customer started payment',
    'Payment Completed': 'Payment succeeded',
    'Out for Delivery': 'Courier on the way to customer',
    'Return Requested': 'Return requested',
    'Product Returned': 'Product was returned',
    'Refund Initiated': 'Refund started',
    'Refund Completed': 'Refund completed'
};

// Insurance Event Explanations (column-observed from user files, 40 events)
const INSURANCE_EVENT_EXPLANATIONS = {
    'Customer Registered': 'Customer registered for insurance',
    'KYC Completed': 'KYC verification completed',
    'Policy Quoted': 'Policy quote generated',
    'Policy Purchased': 'Policy was purchased',
    'Policy Activated': 'Policy became active',
    'Premium Due': 'Premium payment due',
    'Premium Paid': 'Premium payment received',
    'Payment Failed': 'Payment failed',
    'Policy Renewed': 'Policy was renewed',
    'Policy Expired': 'Policy expired',
    'Claim Requested': 'Claim was requested',
    'Claim Registered': 'Claim registered in system',
    'Claim Verified': 'Claim verified',
    'Claim Assessed': 'Claim assessed',
    'Claim Approved': 'Claim approved',
    'Claim Rejected': 'Claim rejected',
    'Claim Paid': 'Claim payment disbursed',
    'Nominee Updated': 'Nominee details updated',
    'Policy Cancelled': 'Policy cancelled',
    'Policy Closed': 'Policy closed',
    'Document Submitted': 'Document submitted for verification',
    'Document Verified': 'Document verification completed',
    'Medical Test Scheduled': 'Medical test scheduled',
    'Medical Test Completed': 'Medical test completed',
    'Risk Assessed': 'Risk assessment completed',
    'Underwriting Started': 'Underwriting process started',
    'Underwriting Completed': 'Underwriting completed',
    'Premium Calculated': 'Premium amount calculated',
    'Auto Debit Enabled': 'Auto debit enabled for premium',
    'Auto Debit Disabled': 'Auto debit disabled',
    'Reminder Sent': 'Premium reminder sent',
    'Grace Period Started': 'Grace period started',
    'Grace Period Ended': 'Grace period ended',
    'Policy Suspended': 'Policy suspended',
    'Reinstatement Requested': 'Reinstatement requested',
    'Policy Reinstated': 'Policy reinstated',
    'Payout Initiated': 'Payout initiated',
    'Payout Completed': 'Payout completed',
    'Fraud Check Started': 'Fraud check started',
    'Fraud Check Cleared': 'Fraud check cleared'
};

// Finance Event Explanations (40 events – observed from columns + row data across DB)
const FINANCE_EVENT_EXPLANATIONS = {
    'Customer Registered': 'Customer registered',
    'KYC Completed': 'KYC verification completed',
    'Account Opened': 'Account opened',
    'Account Closed': 'Account closed',
    'Login': 'User logged in',
    'Logout': 'User logged out',
    'Deposit': 'Deposit recorded',
    'Withdrawal': 'Withdrawal recorded',
    'Transfer Initiated': 'Transfer initiated',
    'Transfer Completed': 'Transfer completed',
    'Payment Initiated': 'Payment initiated',
    'Payment Success': 'Payment successful',
    'Payment Failed': 'Payment failed',
    'Loan Applied': 'Loan application submitted',
    'Loan Approved': 'Loan approved',
    'Loan Disbursed': 'Loan disbursed',
    'Policy Purchased': 'Policy purchased',
    'Premium Paid': 'Premium paid',
    'Claim Requested': 'Claim requested',
    'Claim Paid': 'Claim paid',
    'Application Submitted': 'Application submitted',
    'Application Reviewed': 'Application reviewed',
    'Proposal Generated': 'Proposal generated',
    'Proposal Accepted': 'Proposal accepted',
    'Identity Verified': 'Identity verified',
    'Address Verified': 'Address verified',
    'Income Verified': 'Income verified',
    'Beneficiary Added': 'Beneficiary added',
    'Beneficiary Updated': 'Beneficiary updated',
    'Coverage Activated': 'Coverage activated',
    'Coverage Changed': 'Coverage changed',
    'Installment Generated': 'Installment generated',
    'Installment Paid': 'Installment paid',
    'Penalty Applied': 'Penalty applied',
    'Discount Applied': 'Discount applied',
    'Case Escalated': 'Case escalated',
    'Case Resolved': 'Case resolved',
    'Support Ticket Created': 'Support ticket created',
    'Support Ticket Closed': 'Support ticket closed',
    'Account Frozen': 'Account frozen'
};


// Diagram State for Interactivity
window.diagramState = {
    positions: {},      // { 'Login': {x, y}, ... }
    paths: [],          // [{ case_id: 1, sequence: [...] }, ...]
    timings: {},        // { pathIdx_stepIdx: timingObj }
    segmentOffsets: {}, // { [pathIdx]: { [segmentIdx]: {dx, dy} } } – manual arrow shifts
    same_time_groups: [], // [{ event, timestamp_str, case_ids }] – same timestamp across cases
    boxWidth: 160,
    boxHeight: 70,
    // Controls whether per-segment time/date boxes are rendered on the diagram paths.
    // Default: true (other diagrams can still use timing labels).
    showTimeLabels: true,
    // Optional filter: when set to a Set of case_id strings, only these
    // pipelines are rendered on the main Sankey diagram. null/empty = all.
    visibleCaseIds: null
};

// Arrow drag state (for per-connector manual adjustment)
window.arrowDrag = {
    active: false,
    pathIdx: null,
    segmentIdx: null,
    startX: 0,
    startY: 0,
    origDx: 0,
    origDy: 0
};

// Drag Handlers
window.draggedEvent = null;
window.dragOffsetX = 0;
window.dragOffsetY = 0;

window.startDrag = function (e, eventName) {
    if (e.button !== 0) return; // Left click only
    // console.log('Start Drag:', eventName);
    window.draggedEvent = eventName;
    const el = document.getElementById('box-' + eventName.replace(/\s+/g, '-'));
    if (!el) return;

    const rect = el.getBoundingClientRect(); // Get absolute position
    // Handle case where offsetParent might be null or different
    window.dragOffsetX = e.clientX - rect.left;
    window.dragOffsetY = e.clientY - rect.top;

    // Add global listeners
    document.addEventListener('mousemove', window.onDrag);
    document.addEventListener('mouseup', window.endDrag);
    e.preventDefault();
};

window.onDrag = function (e) {
    if (!window.draggedEvent) return;

    const eventName = window.draggedEvent;
    const el = document.getElementById('box-' + eventName.replace(/\s+/g, '-'));
    if (!el) return;

    const container = el.offsetParent; // Should be the relative container
    if (!container) return;

    const containerRect = container.getBoundingClientRect();

    // Calculate new relative coordinates
    let newX = e.clientX - containerRect.left - window.dragOffsetX;
    let newY = e.clientY - containerRect.top - window.dragOffsetY;

    // Snap to grid (5px)
    newX = Math.round(newX / 5) * 5;
    newY = Math.round(newY / 5) * 5;

    // Boundary checks (optional, but good to keep inside container)
    newX = Math.max(0, newX);
    newY = Math.max(0, newY);

    // Update DOM
    el.style.left = newX + 'px';
    el.style.top = newY + 'px';

    // Update State
    if (window.diagramState && window.diagramState.positions) {
        if (!window.diagramState.positions[eventName]) {
            window.diagramState.positions[eventName] = {};
        }
        window.diagramState.positions[eventName].x = newX;
        window.diagramState.positions[eventName].y = newY;

        // Redraw paths on next frame so lines follow the moved box
        requestAnimationFrame(function () { window.renderDiagramPaths(); });
    }
};

window.endDrag = function () {
    window.draggedEvent = null;
    document.removeEventListener('mousemove', window.onDrag);
    document.removeEventListener('mouseup', window.endDrag);
};

// Diagram pan (drag-to-pan) state and handlers for Unified Case Flow Diagram
window.diagramPan = { active: false, startX: 0, startY: 0, translateX: 0, translateY: 0 };

window.startDiagramPan = function (e) {
    if (e.button !== 0) return;
    var wrapper = document.getElementById('diagram-pan-wrapper');
    if (!wrapper) return;

    // Do NOT start panning if the user clicked on:
    // - an event box (div id starts with "box-")
    // - any arrow / label element that is draggable
    if (e.target && e.target.closest) {
        if (e.target.closest('[id^="box-"]') || e.target.closest('[data-arrow-draggable="1"]')) {
            return;
        }
    }

    window.diagramPan.active = true;
    window.diagramPan.startX = e.clientX;
    window.diagramPan.startY = e.clientY;
    window.diagramPan.translateX = window.diagramPan.translateX || 0;
    window.diagramPan.translateY = window.diagramPan.translateY || 0;
    wrapper.style.cursor = 'grabbing';
    document.addEventListener('mousemove', window.onDiagramPan);
    document.addEventListener('mouseup', window.endDiagramPan);
    e.preventDefault();
};

window.onDiagramPan = function (e) {
    if (!window.diagramPan.active) return;
    var dx = e.clientX - window.diagramPan.startX;
    var dy = e.clientY - window.diagramPan.startY;
    window.diagramPan.translateX += dx;
    window.diagramPan.translateY += dy;
    window.diagramPan.startX = e.clientX;
    window.diagramPan.startY = e.clientY;
    var tx = window.diagramPan.translateX;
    var ty = window.diagramPan.translateY;
    var content = document.getElementById('diagram-pan-content');
    if (content) content.style.transform = 'translate3d(' + tx + 'px, ' + ty + 'px, 0)';
    var pathsGroup = document.getElementById('diagram-paths-container');
    if (pathsGroup) pathsGroup.setAttribute('transform', 'translate(' + tx + ',' + ty + ')');
};

window.endDiagramPan = function () {
    window.diagramPan.active = false;
    var wrapper = document.getElementById('diagram-pan-wrapper');
    if (wrapper) wrapper.style.cursor = 'grab';
    document.removeEventListener('mousemove', window.onDiagramPan);
    document.removeEventListener('mouseup', window.endDiagramPan);
};

window.renderDiagramPaths = function () {
    const svg = document.getElementById('diagram-svg-layer');
    if (!svg) return;

    const container = document.getElementById('diagram-paths-container');
    if (!container) return; // Safety check

    const state = window.diagramState;
    if (!state || !state.paths) return;

    var contentEl = document.getElementById('diagram-pan-content');
    if (contentEl) {
        var contentRect = contentEl.getBoundingClientRect();
        Object.keys(state.positions || {}).forEach(function (eventName) {
            var boxId = 'box-' + eventName.replace(/\s+/g, '-');
            var boxEl = document.getElementById(boxId);
            if (boxEl) {
                var boxRect = boxEl.getBoundingClientRect();
                if (!state.positions[eventName]) state.positions[eventName] = {};
                state.positions[eventName].x = Math.round(boxRect.left - contentRect.left);
                state.positions[eventName].y = Math.round(boxRect.top - contentRect.top);
            }
        });
    }

    let pathsHTML = '';
    const boxWidth = state.boxWidth;
    const boxHeight = state.boxHeight;

    // Determine which case paths are currently visible (for Sankey-style overlap control)
    const visibleSet = (state.visibleCaseIds && typeof state.visibleCaseIds.size === 'number' && state.visibleCaseIds.size > 0)
        ? state.visibleCaseIds
        : null;
    const visibleIndexMap = {};
    const visibleIdxs = [];
    state.paths.forEach(function (p, idx) {
        const idStr = String(p.case_id);
        if (!visibleSet || visibleSet.has(idStr)) {
            visibleIdxs.push(idx);
            visibleIndexMap[idx] = visibleIdxs.length - 1;
        }
    });
    const totalInDiagram = visibleIdxs.length || state.paths.length;
    // Dynamic spread factor so connectors separate more clearly when many Case IDs
    // are drawn in the same unified diagram. Keeps 2–3 paths fairly tight, but
    // fans out more aggressively when we have many pipelines.
    const spreadFactor = 1 + Math.min(2.5, Math.max(0, (totalInDiagram - 1) / 3));
    // Dynamic stroke width so paths are visibly thicker on the main diagram,
    // while not becoming too heavy when there are many pipelines.
    const baseStrokeWidth = totalInDiagram <= 3 ? 4.5 : (totalInDiagram <= 7 ? 3.8 : 3.2);
    /** Where each path touches each event (segment end point). Used to link same-time case paths. */
    const touchPointsByEvent = {};

    try {
        function resolvePos(ev, positions) {
            if (!positions) return null;
            if (positions[ev]) return positions[ev];
            if (positions[ev.replace(/\s+/g, '')]) return positions[ev.replace(/\s+/g, '')];
            if (positions[ev.replace(/([a-z])([A-Z])/g, '$1 $2')]) return positions[ev.replace(/([a-z])([A-Z])/g, '$1 $2')];
            return null;
        }
        state.paths.forEach((path, pathIdx) => {
            // Skip hidden pipelines (when user has toggled Case IDs)
            const visOrdinal = Object.prototype.hasOwnProperty.call(visibleIndexMap, pathIdx)
                ? visibleIndexMap[pathIdx]
                : null;
            if (visOrdinal === null) {
                return;
            }
            const sequence = path.sequence;
            const color = path.color;
            const timings = state.timings[pathIdx] || [];

            for (let i = 0; i < sequence.length - 1; i++) {
                const fromEvent = sequence[i];
                const toEvent = sequence[i + 1];

                const fromPos = resolvePos(fromEvent, state.positions);
                const toPos = resolvePos(toEvent, state.positions);

                if (!fromPos || !toPos) continue;

                // Base coordinates from node centers
                let x1 = fromPos.x + boxWidth / 2;
                let y1 = fromPos.y + boxHeight / 2;
                let x2 = toPos.x + boxWidth / 2;
                let y2 = toPos.y + boxHeight / 2;

                let deltaX = x2 - x1;
                let deltaY = y2 - y1;

                // Compute perpendicular / tangent vectors once
                let nx = -deltaY;
                let ny = deltaX;
                const len = Math.sqrt(nx * nx + ny * ny) || 1;

                // Small static root offset per case so connectors don’t sit
                // exactly on top of each other where they touch the event boxes.
                const idxFromCenterRoot = (visOrdinal - (totalInDiagram - 1) / 2);
                const rootPerpStep = 8 * spreadFactor; // px spacing at node (scaled by path count, more separation)
                const rootPerpOffset = idxFromCenterRoot * rootPerpStep;
                if (len > 0 && rootPerpOffset !== 0) {
                    const nxUnitRoot = nx / len;
                    const nyUnitRoot = ny / len;
                    x1 += nxUnitRoot * rootPerpOffset;
                    y1 += nyUnitRoot * rootPerpOffset;
                    x2 += nxUnitRoot * rootPerpOffset;
                    y2 += nyUnitRoot * rootPerpOffset;
                    // recompute deltas from adjusted endpoints
                    deltaX = x2 - x1;
                    deltaY = y2 - y1;
                    nx = -deltaY;
                    ny = deltaX;
                }

                // Offset Logic (per-case curve separation) – scaled by spreadFactor
                const offsetStep = 10 * spreadFactor;
                const offset = (visOrdinal - (totalInDiagram - 1) / 2) * offsetStep;

                // Curve Logic
                const mx = (x1 + x2) / 2;
                const my = (y1 + y2) / 2;
                const curveAmt = 50 + Math.abs(offset * 2);

                const cpx = mx + (nx / len) * curveAmt * (i % 2 === 0 ? 1 : -1);
                const cpy = my + (ny / len) * curveAmt * (i % 2 === 0 ? 1 : -1);

                // Apply any manual drag offset for this specific connector segment.
                // IMPORTANT: we keep start/end points anchored on the events (x1,y1,x2,y2)
                // and only move the control point and labels. This keeps the arrow
                // visually connected to the event boxes while allowing separation.
                const segOffsetsForPath = state.segmentOffsets && state.segmentOffsets[pathIdx];
                const segOffset = segOffsetsForPath && segOffsetsForPath[i] ? segOffsetsForPath[i] : { dx: 0, dy: 0 };
                const offX = segOffset.dx || 0;
                const offY = segOffset.dy || 0;

                const x1p = x1;
                const y1p = y1;
                const x2p = x2;
                const y2p = y2;
                const cpxp = cpx + offX;
                const cpyp = cpy + offY;

                // Collect where this path touches the "to" event (for same-time connector)
                if (toEvent && toEvent !== 'Process' && toEvent !== 'End') {
                    if (!touchPointsByEvent[toEvent]) touchPointsByEvent[toEvent] = [];
                    touchPointsByEvent[toEvent].push({ pathIdx: pathIdx, case_id: path.case_id, x: x2p, y: y2p });
                }

                // Path (draggable connector – control point moves, endpoints stay attached)
                pathsHTML += `
                <path d="M ${x1p},${y1p} Q ${cpxp},${cpyp} ${x2p},${y2p}" 
                      stroke="${color}" 
                      stroke-width="${baseStrokeWidth.toFixed(1)}" 
                      fill="none" 
                      marker-end="url(#arrow-${path.case_id})"
                      opacity="0.9"
                      style="cursor: move;"
                      data-arrow-draggable="1"
                      data-path-idx="${pathIdx}"
                      data-seg-idx="${i}" />
            `;

                // Time Label (t=0.7): duration (event-to-event) + DB date-time per segment.
                // Can be globally disabled per-diagram via window.diagramState.showTimeLabels.
                const timing = timings[i];
                const showTimeLabels = state.showTimeLabels !== false;
                if (showTimeLabels && timing) {
                    const tTime = 0.7;
                    const lxTime = (1 - tTime) * (1 - tTime) * x1p + 2 * (1 - tTime) * tTime * cpxp + tTime * tTime * x2p;
                    const lyTime = (1 - tTime) * (1 - tTime) * y1p + 2 * (1 - tTime) * tTime * cpyp + tTime * tTime * y2p;
                    var line1 = timing.label || '';
                    var dbDateTime = timing.end_datetime || timing.start_datetime || '';
                    var line2 = dbDateTime ? formatDateTimeReadable(dbDateTime, true) : formatTimeReadable(timing.start_time || timing.end_time || '');
                    if (timing.label === 'Start') {
                        line1 = 'Start';
                        if (timing.end_datetime) line2 = formatDateTimeReadable(timing.end_datetime, true);
                        else if (timing.end_time) line2 = formatTimeReadable(timing.end_time);
                    } else if (timing.label === 'End') {
                        line1 = 'End';
                        if (timing.end_datetime) line2 = formatDateTimeReadable(timing.end_datetime, true);
                        else line2 = formatTimeReadable(timing.end_time || timing.start_time || '');
                    } else if (timing.label === '00:00' || timing.label === '0s' || timing.label === '0 sec') {
                        line1 = '0 sec';
                    }
                    if (timing.date_only && line2) {
                        line2 = line2.replace(/\s+\d{1,2}:\d{2}(:\d{2})?\s*[AP]M$/i, '').trim() + ' (Date only)';
                    }
                    if (line1 || line2) {
                        var boxH = line2 ? 38 : 18;
                        var boxW = 102;
                        pathsHTML += `
                    <g>
                        <rect x="${lxTime - boxW / 2}" y="${lyTime - 16}" width="${boxW}" height="${boxH}" rx="6" fill="white" stroke="${color}" stroke-width="1" opacity="0.95" style="filter: drop-shadow(0px 1px 1px rgba(0,0,0,0.1)); cursor: move;" data-arrow-draggable="1" data-path-idx="${pathIdx}" data-seg-idx="${i}" />
                        <text x="${lxTime}" y="${lyTime - 3}" text-anchor="middle" font-size="10" font-weight="700" fill="${color}" style="cursor: move;" data-arrow-draggable="1" data-path-idx="${pathIdx}" data-seg-idx="${i}">${line1}</text>
                        ${line2 ? `<text x="${lxTime}" y="${lyTime + 10}" text-anchor="middle" font-size="7.5" font-weight="500" fill="#64748b" style="cursor: move;" data-arrow-draggable="1" data-path-idx="${pathIdx}" data-seg-idx="${i}">${line2}</text>` : ''}
                    </g>
                `;
                    }
                }

                // Case ID Label (closer to start of connector for clearer attachment, drawn last so it stays on top)
                const tCase = 0.18;
                let lxCase = (1 - tCase) * (1 - tCase) * x1p + 2 * (1 - tCase) * tCase * cpxp + tCase * tCase * x2p;
                let lyCase = (1 - tCase) * (1 - tCase) * y1p + 2 * (1 - tCase) * tCase * cpyp + tCase * tCase * y2p;

                // Extra offset per visible case path so labels don't overlap:
                // - perpendicular to the line to spread them "above/below"
                // - slightly along the line so pills don't sit exactly on top
                const idxFromCenter = (visOrdinal - (totalInDiagram - 1) / 2);
                const labelPerpStep = 20 * spreadFactor;   // distance between stacked labels
                const labelAlongStep = 10 * spreadFactor;  // small along-the-line separation
                const labelPerpOffset = idxFromCenter * labelPerpStep;
                const labelAlongOffset = idxFromCenter * labelAlongStep;

                if (len > 0) {
                    const nxUnit = nx / len;
                    const nyUnit = ny / len;
                    const txUnit = deltaX / len;
                    const tyUnit = deltaY / len;
                    lxCase += nxUnit * labelPerpOffset + txUnit * labelAlongOffset;
                    lyCase += nyUnit * labelPerpOffset + tyUnit * labelAlongOffset;
                }
                const caseLabelText = 'Case ' + String(path.case_id).padStart(3, '0');
                const caseBoxWidth = Math.max(70, caseLabelText.length * 6.5);

                pathsHTML += `
                <rect x="${lxCase - caseBoxWidth / 2}" y="${lyCase - 10}" width="${caseBoxWidth}" height="20" rx="10"
                      fill="white"
                      stroke="${color}"
                      stroke-width="1.5"
                      opacity="0.98"
                      style="cursor: move;"
                      data-arrow-draggable="1"
                      data-path-idx="${pathIdx}"
                      data-seg-idx="${i}" />
                <text x="${lxCase}" y="${lyCase + 4}" 
                      text-anchor="middle" 
                      font-size="10" 
                      font-weight="800"
                      fill="${color}"
                      style="text-shadow: 0px 0px 2px rgba(0,0,0,0.2); cursor: move;"
                      data-arrow-draggable="1"
                      data-path-idx="${pathIdx}"
                      data-seg-idx="${i}">
                    ${caseLabelText}
                </text>
            `;
            }
        }); // End foreach
    } catch (e) {
        console.error("Path Render Error:", e);
    }

    // Same-time connectors: one arrow that links all case paths that hit this event at the same time
    const sameTimeGroups = state.same_time_groups || [];
    sameTimeGroups.forEach(function (grp) {
        const eventName = grp.event;
        const caseIds = grp.case_ids || [];
        const allTouch = touchPointsByEvent[eventName];
        if (!allTouch || caseIds.length < 2) return;
        const points = allTouch.filter(function (pt) { return caseIds.indexOf(pt.case_id) !== -1; });
        if (points.length < 2) return;
        points.sort(function (a, b) { return a.x - b.x; });
        const d = 'M ' + points.map(function (p) { return p.x + ',' + p.y; }).join(' L ');
        const midX = (points[0].x + points[points.length - 1].x) / 2;
        const midY = (points[0].y + points[points.length - 1].y) / 2;
        pathsHTML += '<path d="' + d + '" stroke="#475569" stroke-width="2.5" fill="none" marker-end="url(#same-time-arrow)" opacity="0.9" />';
        pathsHTML += '<text x="' + midX + '" y="' + (midY + 14) + '" text-anchor="middle" font-size="9" fill="#475569" font-weight="600">same time</text>';
    });

    if (container) {
        container.innerHTML = pathsHTML;

        // Attach drag handlers to any arrow / label elements marked as draggable
        try {
            const dragEls = container.querySelectorAll('[data-arrow-draggable="1"]');
            dragEls.forEach(function (el) {
                el.addEventListener('mousedown', function (ev) {
                    if (ev.button !== 0) return; // left button only
                    const pIdx = parseInt(this.getAttribute('data-path-idx'), 10);
                    const sIdx = parseInt(this.getAttribute('data-seg-idx'), 10);
                    if (isNaN(pIdx) || isNaN(sIdx)) return;

                    const offsets = (window.diagramState.segmentOffsets &&
                        window.diagramState.segmentOffsets[pIdx] &&
                        window.diagramState.segmentOffsets[pIdx][sIdx]) || { dx: 0, dy: 0 };

                    window.arrowDrag.active = true;
                    window.arrowDrag.pathIdx = pIdx;
                    window.arrowDrag.segmentIdx = sIdx;
                    window.arrowDrag.startX = ev.clientX;
                    window.arrowDrag.startY = ev.clientY;
                    window.arrowDrag.origDx = offsets.dx || 0;
                    window.arrowDrag.origDy = offsets.dy || 0;

                    // Global listeners so drag continues even if cursor leaves the path
                    document.addEventListener('mousemove', window.onArrowDragMove);
                    document.addEventListener('mouseup', window.endArrowDrag);

                    ev.preventDefault();
                    ev.stopPropagation();
                });
            });
        } catch (err) {
            console.error('Failed to wire drag handlers for arrows:', err);
        }

        if (window.diagramPan && (window.diagramPan.translateX || window.diagramPan.translateY)) {
            var tx = window.diagramPan.translateX || 0;
            var ty = window.diagramPan.translateY || 0;
            container.setAttribute('transform', 'translate(' + tx + ',' + ty + ')');
        }
    }
};

// Toggle which Case IDs are drawn on the main Sankey-style diagram.
// - First click on a case chip: show only that pipeline.
// - Click more chips: add/remove pipelines.
// - When all chips are turned off, we reset to "show all".
window.toggleUnifiedCaseVisibility = function (caseId) {
    try {
        var state = window.diagramState;
        if (!state || !state.paths) return;
        var idStr = String(caseId);

        // Initialize visibility set if needed
        if (!state.visibleCaseIds || typeof state.visibleCaseIds.size !== 'number') {
            state.visibleCaseIds = new Set();
        }
        var set = state.visibleCaseIds;

        if (set.has(idStr)) {
            // Turn OFF this pipeline
            set.delete(idStr);
            // If none left selected, reset filter to "all visible"
            if (set.size === 0) {
                state.visibleCaseIds = null;
            }
        } else {
            // If currently no filter, start a new filter with just this case
            if (!set || state.visibleCaseIds === null) {
                state.visibleCaseIds = new Set([idStr]);
            } else {
                set.add(idStr);
            }
        }

        // Re-render with updated visibility
        window.renderDiagramPaths();
    } catch (e) {
        console.error('toggleUnifiedCaseVisibility error', e);
    }
};

// Global arrow-drag handlers
window.onArrowDragMove = function (e) {
    if (!window.arrowDrag || !window.arrowDrag.active) return;

    const state = window.diagramState;
    if (!state || !state.segmentOffsets) return;

    const pIdx = window.arrowDrag.pathIdx;
    const sIdx = window.arrowDrag.segmentIdx;
    if (pIdx == null || sIdx == null) return;

    const dx = (e.clientX - window.arrowDrag.startX) + window.arrowDrag.origDx;
    const dy = (e.clientY - window.arrowDrag.startY) + window.arrowDrag.origDy;

    if (!state.segmentOffsets[pIdx]) state.segmentOffsets[pIdx] = {};
    state.segmentOffsets[pIdx][sIdx] = { dx, dy };

    // Re-render paths so only this arrow (and its labels) move visually
    window.renderDiagramPaths();
};

window.endArrowDrag = function () {
    if (!window.arrowDrag) return;
    window.arrowDrag.active = false;
    window.arrowDrag.pathIdx = null;
    window.arrowDrag.segmentIdx = null;
    document.removeEventListener('mousemove', window.onArrowDragMove);
    document.removeEventListener('mouseup', window.endArrowDrag);
};

// Build Sankey nodes and links from transition counts (pattern from user data, no hardcoding)
function buildSankeyFromFlowData(flowData) {
    let links = flowData.transition_counts || [];
    const casePaths = flowData.case_paths || [];
    if (links.length === 0 && casePaths.length > 0) {
        const countMap = {};
        casePaths.forEach(function (p) {
            const seq = p.path_sequence || [];
            for (let i = 0; i < seq.length - 1; i++) {
                const f = seq[i];
                const t = seq[i + 1];
                if (f && t) {
                    const key = f + '\u2192' + t;
                    countMap[key] = (countMap[key] || 0) + 1;
                }
            }
        });
        links = Object.keys(countMap).map(function (k) {
            const [from, to] = k.split('\u2192');
            return { from: from, to: to, count: countMap[k] };
        });
    }
    if (links.length === 0) return { nodes: [], links: [], nodeOrder: [] };

    const nodeValues = {};
    links.forEach(function (l) {
        const to = l.to;
        const c = l.count || 0;
        nodeValues[to] = (nodeValues[to] || 0) + c;
    });
    let processOut = 0;
    links.forEach(function (l) {
        if (l.from === 'Process') processOut += (l.count || 0);
    });
    if (processOut > 0) nodeValues['Process'] = processOut;
    const nodeSet = new Set(Object.keys(nodeValues));
    let nodeOrder = flowData.all_event_types || [];
    if (nodeOrder.length === 0) {
        nodeOrder = ['Process'];
        nodeSet.forEach(function (n) {
            if (n !== 'Process' && n !== 'End') nodeOrder.push(n);
        });
        nodeOrder.push('End');
    }
    nodeOrder = nodeOrder.filter(function (n) { return nodeSet.has(n); });
    const nodes = nodeOrder.map(function (name) {
        return { name: name, value: nodeValues[name] || 0 };
    });
    return { nodes: nodes, links: links, nodeOrder: nodeOrder };
}

// Render Sankey diagram SVG - TREE STRUCTURE: Process (top) → Events (middle rows) → End (bottom)
// Each path gets one color that flows through entire path (Process → Event → Event → End)
function renderSankeyDiagramSVG(flowData) {
    const sankey = buildSankeyFromFlowData(flowData);
    if (sankey.nodes.length === 0 || sankey.links.length === 0) {
        return '<div style="padding:2rem;text-align:center;color:#64748b;">No flow patterns to display. Run analysis with event sequences.</div>';
    }

    const nodeOrder = sankey.nodeOrder;
    const nodes = sankey.nodes;
    const links = sankey.links;
    
    // Get viewport dimensions for full-screen
    let viewportWidth = 1200;
    let viewportHeight = 800;
    try {
        if (typeof window !== 'undefined') {
            viewportWidth = window.innerWidth || 1200;
            viewportHeight = window.innerHeight || 800;
        }
    } catch (e) {}
    
    // Calculate total flow to determine scale (ensure fits screen)
    const totalFlow = nodes.reduce(function (sum, n) { return sum + (n.value || 0); }, 0);
    const maxNodeVal = Math.max(1, Math.max.apply(null, nodes.map(function (n) { return n.value || 0; })));
    
    // HORIZONTAL FLOW STRUCTURE: Process (left) → Events (middle columns) → End (right)
    // Nodes are VERTICAL bars arranged in COLUMNS, pipelines flow HORIZONTALLY
    
    const availableHeight = Math.floor(viewportHeight * 0.95);
    const availableWidth = Math.floor(viewportWidth * 0.98);
    const nodeBarWidth = 80; // Width of vertical node bars (increased for better visibility)
    const padding = { top: 120, left: 140, right: 140, bottom: 120 };
    
    // Calculate scale so total flow height fits available vertical space
    const maxFlowHeight = availableHeight - padding.top - padding.bottom;
    const flowScale = Math.min(maxFlowHeight / Math.max(totalFlow, maxNodeVal), 18);
    
    const nodeIndex = {};
    nodeOrder.forEach(function (n, i) { nodeIndex[n] = i; });
    
    // HORIZONTAL STRUCTURE: Nodes in columns (left to right)
    const nodePositions = {};
    const nodeSizes = {};
    const linkFlowPos = {}; // key: "from->to", value: { startY, endY } for vertical stacking
    
    // Group links
    const linksByFrom = {};
    const linksByTo = {};
    links.forEach(function (l) {
        if (!linksByFrom[l.from]) linksByFrom[l.from] = [];
        linksByFrom[l.from].push(l);
        if (!linksByTo[l.to]) linksByTo[l.to] = [];
        linksByTo[l.to].push(l);
    });
    
    // Determine column for each event (Process=col 0, End=last col, middle events by order)
    const eventCol = {};
    eventCol['Process'] = 0;
    const middleNodes = nodes.filter(function (n) { return n.name !== 'Process' && n.name !== 'End'; });
    middleNodes.forEach(function (n, idx) {
        eventCol[n.name] = idx + 1; // Column 1, 2, 3, ...
    });
    eventCol['End'] = middleNodes.length + 1;
    const totalCols = middleNodes.length + 2;
    
    // Calculate horizontal spacing between columns
    const colGap = Math.max(320, (availableWidth - padding.left - padding.right - nodeBarWidth * totalCols) / Math.max(totalCols - 1, 1));
    const centerY = padding.top + maxFlowHeight / 2;
    
    // Process node: left column, vertical bar
    const processNode = nodes.find(function (n) { return n.name === 'Process'; });
    if (processNode) {
        const h = Math.max(90, (processNode.value || 0) * flowScale);
        const x = padding.left;
        const y = centerY - h / 2;
        nodePositions['Process'] = { x: x, y: y, width: nodeBarWidth, height: h };
        nodeSizes['Process'] = { width: nodeBarWidth, height: h };
    }
    
    // Calculate flow positions: Process outgoing flows (stack vertically)
    if (processNode && linksByFrom['Process']) {
        const processOutgoing = linksByFrom['Process'].slice().sort(function (a, b) {
            return (nodeIndex[a.to] || 999) - (nodeIndex[b.to] || 999);
        });
        let processY = centerY - (processNode.value || 0) * flowScale / 2;
        processOutgoing.forEach(function (l) {
            const key = 'Process->' + l.to;
            linkFlowPos[key] = { startY: processY, endY: processY + (l.count || 0) * flowScale };
            processY += (l.count || 0) * flowScale;
        });
    }
    
    // Middle nodes: position in their column, vertically based on incoming flows
    middleNodes.forEach(function (n) {
        const incoming = linksByTo[n.name] || [];
        if (incoming.length === 0) return;
        
        // Calculate vertical position from incoming flows
        let minInY = Infinity;
        let maxInY = -Infinity;
        incoming.forEach(function (l) {
            const key = l.from + '->' + l.to;
            const pos = linkFlowPos[key];
            if (pos) {
                minInY = Math.min(minInY, pos.startY);
                maxInY = Math.max(maxInY, pos.endY);
            }
        });
        
        if (minInY === Infinity) {
            minInY = centerY - (n.value || 0) * flowScale / 2;
            maxInY = centerY + (n.value || 0) * flowScale / 2;
        }
        
        const nodeCenterY = (minInY + maxInY) / 2;
        const nodeH = Math.max(90, maxInY - minInY, (n.value || 0) * flowScale);
        const col = eventCol[n.name];
        const x = padding.left + col * (colGap + nodeBarWidth);
        
        nodePositions[n.name] = { x: x, y: nodeCenterY - nodeH / 2, width: nodeBarWidth, height: nodeH };
        nodeSizes[n.name] = { width: nodeBarWidth, height: nodeH };
        
        // Calculate outgoing flows from this node (vertical stacking)
        const outgoing = linksByFrom[n.name] || [];
        if (outgoing.length > 0) {
            outgoing.sort(function (a, b) {
                return (nodeIndex[a.to] || 999) - (nodeIndex[b.to] || 999);
            });
            const totalOut = outgoing.reduce(function (sum, l) { return sum + (l.count || 0); }, 0);
            let outY = nodeCenterY - (totalOut * flowScale) / 2;
            outgoing.forEach(function (l) {
                const key = n.name + '->' + l.to;
                linkFlowPos[key] = { startY: outY, endY: outY + (l.count || 0) * flowScale };
                outY += (l.count || 0) * flowScale;
            });
        }
    });
    
    // End node: right column, vertical bar
    const endNode = nodes.find(function (n) { return n.name === 'End'; });
    if (endNode) {
        const incoming = linksByTo['End'] || [];
        let minEndY = Infinity;
        let maxEndY = -Infinity;
        incoming.forEach(function (l) {
            const key = l.from + '->End';
            const pos = linkFlowPos[key];
            if (pos) {
                minEndY = Math.min(minEndY, pos.startY);
                maxEndY = Math.max(maxEndY, pos.endY);
            }
        });
        
        if (minEndY === Infinity) {
            minEndY = centerY - (endNode.value || 0) * flowScale / 2;
            maxEndY = centerY + (endNode.value || 0) * flowScale / 2;
        }
        
        const endCenterY = (minEndY + maxEndY) / 2;
        const endH = Math.max(90, (endNode.value || 0) * flowScale);
        const endX = padding.left + (totalCols - 1) * (colGap + nodeBarWidth);
        nodePositions['End'] = { x: endX, y: endCenterY - endH / 2, width: nodeBarWidth, height: endH };
        nodeSizes['End'] = { width: nodeBarWidth, height: endH };
    }
    
    const svgWidth = padding.left + (totalCols - 1) * (colGap + nodeBarWidth) + nodeBarWidth + padding.right;
    const svgHeight = padding.top + maxFlowHeight + padding.bottom;
    
    // Color system: Each path from Process gets one color that flows through entire path
    // Trace paths: Process → first event → ... → End, assign color per unique first-event path
    const pathColorPalette = [
        '#ec4899', // Pink
        '#3b82f6', // Blue
        '#10b981', // Green
        '#f59e0b', // Orange
        '#8b5cf6', // Purple
        '#ef4444', // Red
        '#06b6d4', // Cyan
        '#84cc16', // Lime
        '#f97316', // Orange-red
        '#6366f1', // Indigo
        '#14b8a6', // Teal
        '#f43f5e', // Rose
        '#a855f7', // Violet
        '#22c55e', // Emerald
        '#0ea5e9'  // Sky blue
    ];
    
    // Find all unique first events (directly from Process)
    const firstEvents = new Set();
    links.forEach(function (l) {
        if (l.from === 'Process') {
            firstEvents.add(l.to);
        }
    });
    
    // Assign color to each first-event path
    const pathColors = {}; // key: first event name, value: color
    const firstEventArray = Array.from(firstEvents).sort();
    firstEventArray.forEach(function (eventName, idx) {
        pathColors[eventName] = pathColorPalette[idx % pathColorPalette.length];
    });
    
    // For each link, determine which path it belongs to (trace back to Process)
    const linkPathColor = {}; // key: "from->to", value: color
    const processedLinks = new Set();
    
    // Start with Process → first event links
    links.forEach(function (l) {
        if (l.from === 'Process') {
            const key = l.from + '->' + l.to;
            linkPathColor[key] = pathColors[l.to] || '#64748b';
            processedLinks.add(key);
        }
    });
    
    // Propagate colors through paths: if link from A→B has color, then all B→C links get same color
    let changed = true;
    while (changed) {
        changed = false;
        links.forEach(function (l) {
            const key = l.from + '->' + l.to;
            if (processedLinks.has(key)) return;
            
            // Find incoming link to this link's source
            links.forEach(function (incoming) {
                if (incoming.to === l.from && processedLinks.has(incoming.from + '->' + incoming.to)) {
                    const incomingKey = incoming.from + '->' + incoming.to;
                    linkPathColor[key] = linkPathColor[incomingKey];
                    processedLinks.add(key);
                    changed = true;
                }
            });
        });
    }
    
    // Assign remaining links a default color
    links.forEach(function (l) {
        const key = l.from + '->' + l.to;
        if (!linkPathColor[key]) {
            linkPathColor[key] = '#64748b';
        }
    });
    
    // Node colors: use color of incoming path (or outgoing if no incoming)
    const nodeColors = {};
    nodeOrder.forEach(function (nodeName) {
        if (nodeName === 'Process') {
            nodeColors['Process'] = '#ec4899'; // Pink
        } else if (nodeName === 'End') {
            nodeColors['End'] = '#1e40af'; // Dark blue
        } else {
            // Find color from incoming link
            let nodeColor = '#64748b';
            links.forEach(function (l) {
                if (l.to === nodeName && linkPathColor[l.from + '->' + l.to]) {
                    nodeColor = linkPathColor[l.from + '->' + l.to];
                }
            });
            nodeColors[nodeName] = nodeColor;
        }
    });
    
    let pathsHTML = '';
    links.forEach(function (l) {
        const fromPos = nodePositions[l.from];
        const toPos = nodePositions[l.to];
        if (!fromPos || !toPos) return;
        
        const count = l.count || 0;
        const flowKey = l.from + '->' + l.to;
        const flowPos = linkFlowPos[flowKey];
        if (!flowPos) return;
        
        // HORIZONTAL FLOW: Pipelines flow left to right between vertical node bars
        const flowCenterY = (flowPos.startY + flowPos.endY) / 2;
        const flowThickness = flowPos.endY - flowPos.startY;
        
        // Connection points: right edge of source node, left edge of target node
        const x1 = fromPos.x + nodeSizes[l.from].width;
        const y1 = flowCenterY;
        const x2 = toPos.x;
        const y2 = flowCenterY;
        
        // Horizontal curved path (left to right)
        const dx = x2 - x1;
        const controlOffset = Math.min(dx * 0.4, 100); // Horizontal curve control
        const halfThick = Math.max(flowThickness / 2, 8); // Ensure minimum thickness for visibility
        
        // Draw filled horizontal band (Sankey pipeline flowing rightward)
        const pathBand = 'M ' + x1 + ' ' + (y1 - halfThick) + 
                        ' C ' + (x1 + controlOffset) + ' ' + (y1 - halfThick) + 
                        ', ' + (x2 - controlOffset) + ' ' + (y2 - halfThick) + 
                        ', ' + x2 + ' ' + (y2 - halfThick) +
                        ' L ' + x2 + ' ' + (y2 + halfThick) +
                        ' C ' + (x2 - controlOffset) + ' ' + (y2 + halfThick) + 
                        ', ' + (x1 + controlOffset) + ' ' + (y1 + halfThick) + 
                        ', ' + x1 + ' ' + (y1 + halfThick) + ' Z';
        
        // Pipeline color: use path color (flows through entire path from Process)
        const color = linkPathColor[flowKey] || '#64748b';
        // Increased opacity, stroke and visibility for pipeline structure
        pathsHTML += '<path d="' + pathBand + '" fill="' + color + '" opacity="0.95" stroke="' + color + '" stroke-width="4" />';
        
        // Add count label on pipeline (compulsory - show count on each flow)
        const labelX = (x1 + x2) / 2;
        const labelY = flowCenterY;
        const countStr = String(count);
        const labelWidth = Math.max(36, countStr.length * 11 + 10);
        const labelHeight = 24;
        // Background for label readability (white box with colored border matching pipeline)
        pathsHTML += '<rect x="' + (labelX - labelWidth / 2) + '" y="' + (labelY - labelHeight / 2) + '" width="' + labelWidth + '" height="' + labelHeight + '" rx="6" fill="white" opacity="0.96" stroke="' + color + '" stroke-width="3" />';
        pathsHTML += '<text x="' + labelX + '" y="' + (labelY + 7) + '" text-anchor="middle" font-size="15" font-weight="700" fill="' + color + '">' + count + '</text>';
    });
    
    // Render nodes (HORIZONTAL FLOW: all nodes are vertical bars)
    let nodeHTML = '';
    nodes.forEach(function (n) {
        const pos = nodePositions[n.name];
        if (!pos) return;
        
        // Use assigned color for each event (based on path color)
        const nodeColor = nodeColors[n.name] || '#64748b';
        
        // Vertical bar: nodes are vertical rectangles (thicker stroke for visibility)
        nodeHTML += '<rect x="' + pos.x + '" y="' + pos.y + '" width="' + pos.width + '" height="' + pos.height + '" rx="8" fill="' + nodeColor + '" stroke="#fff" stroke-width="4" opacity="0.95" />';
        
        // Label: name above node, value below node (vertical bar layout)
        nodeHTML += '<text x="' + (pos.x + pos.width / 2) + '" y="' + (pos.y - 14) + '" text-anchor="middle" font-size="17" font-weight="600" fill="#0f172a">' + n.name + '</text>';
        nodeHTML += '<text x="' + (pos.x + pos.width / 2) + '" y="' + (pos.y + pos.height + 28) + '" text-anchor="middle" font-size="16" font-weight="700" fill="#0f172a">' + n.value + '</text>';
    });
    
    // Full-screen responsive SVG - fills viewport, with full screen toggle
    return '<div id="sankey-diagram-container" style="position:relative;width:100%;height:92vh;min-height:750px;max-height:96vh;overflow:auto;background:#fff;border-radius:8px;box-shadow:0 2px 12px rgba(0,0,0,0.15);margin:0;">' +
           '<button type="button" onclick="toggleSankeyFullscreen()" title="Toggle full screen" style="position:absolute;top:12px;right:12px;z-index:10;padding:8px 14px;border-radius:8px;border:1px solid #cbd5e1;background:#fff;color:#334155;font-size:14px;font-weight:600;cursor:pointer;box-shadow:0 2px 6px rgba(0,0,0,0.1);display:flex;align-items:center;gap:6px;">' +
           '<span style="font-size:18px;">⛶</span> Full Screen</button>' +
           '<svg width="100%" height="100%" viewBox="0 0 ' + svgWidth + ' ' + svgHeight + '" preserveAspectRatio="xMidYMid meet" style="display:block;" xmlns="http://www.w3.org/2000/svg">' +
           '<g>' + pathsHTML + '</g>' +
           '<g>' + nodeHTML + '</g>' +
           '</svg></div>';
}

// Render Unified Case Flow Diagram - TRUE Sankey (flow counts from patterns, no hardcoded structure)
// (Global so both Banking and Healthcare screens can use it)
function renderUnifiedCaseFlowDiagram(flowData) {
    if (!flowData || !flowData.case_paths || flowData.case_paths.length === 0) {
        return '';
    }

    const casePaths = flowData.case_paths || [];
    const CASE_COLOR_PALETTE = [
        '#ef4444', '#3b82f6', '#10b981', '#f97316', '#8b5cf6',
        '#ec4899', '#22c55e', '#0ea5e9', '#eab308', '#6366f1',
        '#14b8a6', '#f43f5e', '#a855f7', '#84cc16', '#06b6d4'
    ];
    casePaths.forEach((p, idx) => {
        if (!p.color) {
            p.color = CASE_COLOR_PALETTE[idx % CASE_COLOR_PALETTE.length];
        }
    });
    let eventTypes = flowData.all_event_types || [];
    if (eventTypes.length === 0 && casePaths.length > 0) {
        const seen = new Set();
        eventTypes = ['Process'];
        casePaths.forEach(function (p) {
            (p.path_sequence || []).forEach(function (s) {
                if (s && s !== 'Process' && s !== 'End' && !seen.has(s)) {
                    seen.add(s);
                    eventTypes.push(s);
                }
            });
        });
        eventTypes.push('End');
    }
    const totalCases = flowData.total_cases || 0;

    let legendHTML = '<div style="display: flex; flex-direction: column; gap: 0.75rem; margin-bottom: 1.25rem;">';
    legendHTML += '<div style="display: flex; flex-wrap: wrap; gap: 1rem;">';
    casePaths.forEach((path, idx) => {
        const caseIdLabel = `Case ${String(path.case_id).padStart(3, '0')}`;
        legendHTML += `
                <button type="button"
                        data-single-case-id="${String(path.case_id)}"
                        data-case-toggle="1"
                        style="display: inline-flex; align-items: center; gap: 0.4rem; padding: 0.35rem 0.65rem; border-radius: 999px; border: 1px solid rgba(148,163,184,0.7); background: #ffffff; cursor: pointer; font-size: 0.8rem; color: #0f172a; box-shadow: 0 1px 2px rgba(15,23,42,0.08);">
                    <span style="width: 18px; height: 3px; background: ${path.color}; border-radius: 2px;"></span>
                    <span style="font-weight: 600;">${caseIdLabel}</span>
                </button>
            `;
    });
    legendHTML += '</div>';
    legendHTML += '<div style="font-size: 0.8rem; color: #6b7280;">Click any Case ID chip to see the step-by-step flow for that case. Main diagram shows flow counts (e.g. Process [4] Register).</div>';
    legendHTML += '</div>';

    const diagramHTML = renderSankeyDiagramSVG(flowData);

    return `
            <section style="margin-bottom: 1.5rem;">
                <h2 style="font-size: 1.6rem; margin-bottom: 0.4rem; color: var(--text-primary); text-align: center;">
                    🔄 Unified Case Flow Diagram (Sankey)
                </h2>
                <p style="color: var(--text-muted); margin-bottom: 0.75rem; font-size: 0.9rem; text-align: center;">
                    Flow counts from your data: how many cases go from one event to the next (e.g. Process [4] Register, Register [2] Login). <strong>Counts shown on each pipeline.</strong> Node numbers = flow through that step.
                </p>
                
                <div style="background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 12px; padding: 0.75rem;">
                    <div style="margin-bottom: 0.5rem;">
                        <div style="font-size: 0.85rem; font-weight: 600; color: var(--text-primary); margin-bottom: 0.4rem;">Legend & Case Filter</div>
                        ${legendHTML}
                    </div>
                    
                    <div style="width:100%;margin:0;padding:0;">${diagramHTML}</div>

                    <div id="single-case-flow-container" style="margin-top: 1.75rem; border-top: 1px dashed #e5e7eb; padding-top: 1.25rem;">
                        <div style="font-size: 0.9rem; font-weight: 600; color: var(--text-primary); margin-bottom: 0.5rem;">
                            Single Case Flow (Filtered)
                        </div>
                        <div id="single-case-flow-content" style="font-size: 0.85rem; color: #6b7280;">
                            Click a Case ID chip above to see the step-by-step flow for that single case only.
                        </div>
                    </div>
                </div>
            </section>
        `;
}

// Make it accessible via window for inline handlers if needed
window.renderUnifiedCaseFlowDiagram = renderUnifiedCaseFlowDiagram;

// Full screen toggle for Sankey diagram
window.toggleSankeyFullscreen = function () {
    const el = document.getElementById('sankey-diagram-container');
    if (!el) return;
    if (!document.fullscreenElement) {
        el.requestFullscreen().catch(function () {});
    } else {
        document.exitFullscreen();
    }
};

// ---------------------------------------------------------------------------
// Single Case Flow Diagram (filtered by Case ID)
// ---------------------------------------------------------------------------

window.showSingleCaseFlow = function (caseIdStr) {
    try {
        const container = document.getElementById('single-case-flow-content');
        if (!container) return;
        const body = document.getElementById('single-case-flow-body');

        // When switching to a new Case ID, stop any existing step auto-play loop
        // so we do not keep advancing steps for an old diagram.
        if (window.stopMergedStepAutoPlay) {
            window.stopMergedStepAutoPlay();
        }

        const activeUnifiedData =
            window.currentBankingUnifiedFlowData ||
            window.currentRetailUnifiedFlowData ||
            window.currentInsuranceUnifiedFlowData ||
            window.currentFinanceUnifiedFlowData ||
            window.currentHealthcareUnifiedFlowData ||
            null;
        if (!activeUnifiedData || !activeUnifiedData.case_paths || !activeUnifiedData.case_paths.length) {
            container.innerHTML = '<div style="color: #b91c1c;">No unified flow data is loaded. Please run analysis first.</div>';
            return;
        }

        const caseIdNum = isNaN(Number(caseIdStr)) ? caseIdStr : Number(caseIdStr);
        const match = activeUnifiedData.case_paths.find(function (p) {
            return String(p.case_id) === String(caseIdStr) || p.case_id === caseIdNum;
        });
        if (!match) {
            container.innerHTML = '<div style="color: #b91c1c;">Selected Case ID not found in current results.</div>';
            return;
        }

        // Build event list only from this one case so unrelated events/boxes are not shown
        let singleEvents = [];
        if (match.path_sequence && Array.isArray(match.path_sequence)) {
            const seen = {};
            singleEvents.push('Process');
            match.path_sequence.forEach(function (ev) {
                if (!ev || ev === 'Process' || ev === 'End') return;
                if (!seen[ev]) {
                    seen[ev] = true;
                    singleEvents.push(ev);
                }
            });
            singleEvents.push('End');
        }

        const singleFlowData = {
            all_event_types: singleEvents.length ? singleEvents : (activeUnifiedData.all_event_types || []),
            case_paths: [match],
            total_cases: 1,
            same_time_groups: [],
            filterLabel: 'Case ID ' + caseIdStr,  // So diagram title shows "Case ID X - Step-by-Step"
            isSingleCaseFilter: true,
        };

        const html = renderMergedCaseFlowDiagram(singleFlowData);
        container.innerHTML = html || '<div style="color: #6b7280;">No steps available for this Case ID.</div>';
        if (window.initMergedStepDiagram) {
            setTimeout(function () {
                // Initialize step navigation state for this single-case diagram
                window.initMergedStepDiagram();
                // Automatically advance to the next step every 4 seconds
                if (window.startMergedStepAutoPlay) {
                    window.startMergedStepAutoPlay(4000);
                }
            }, 0);
        }

        // Reveal accordion body and scroll into view for better UX
        if (body) {
            body.style.display = 'block';
        }
        const containerWrapper = document.getElementById('single-case-flow-container');
        if (containerWrapper && containerWrapper.scrollIntoView) {
            containerWrapper.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
    } catch (e) {
        console.error('showSingleCaseFlow error', e);
    }
};

// ---------------------------------------------------------------------------
// User-merged Flow Diagram (all Case IDs for one user)
// ---------------------------------------------------------------------------

window.showUserMergedFlow = function (userId) {
    console.log('=== showUserMergedFlow CALLED ===');
    console.log('User ID parameter:', userId);
    console.log('Type:', typeof userId);
    
    // Ensure userId is a string for comparison
    if (userId == null || userId === '') {
        console.error('Invalid userId:', userId);
        alert('Error: Invalid user ID. Please try again.');
        return;
    }
    
    try {
        const container = document.getElementById('single-case-flow-content');
        if (!container) {
            console.error('Container not found!');
            alert('Error: Filter container not found. Please refresh the page.');
            return;
        }
        const body = document.getElementById('single-case-flow-body');
        if (!body) {
            console.error('Body not found!');
        }

        // When switching to a new user-merged flow, stop any existing step auto-play loop
        // so we do not keep advancing steps for an old diagram.
        if (window.stopMergedStepAutoPlay) {
            window.stopMergedStepAutoPlay();
        }

        const activeUnifiedData =
            window.currentBankingUnifiedFlowData ||
            window.currentRetailUnifiedFlowData ||
            window.currentInsuranceUnifiedFlowData ||
            window.currentFinanceUnifiedFlowData ||
            window.currentHealthcareUnifiedFlowData ||
            null;
        
        console.log('Active unified data:', activeUnifiedData ? 'Found' : 'Not found');
        if (!activeUnifiedData || !activeUnifiedData.case_paths || !activeUnifiedData.case_paths.length) {
            container.innerHTML = '<div style="color: #b91c1c;">No unified flow data is loaded. Please run analysis first.</div>';
            return;
        }

        console.log('Total case paths available:', activeUnifiedData.case_paths.length);

        // Filter to show ALL case paths for this user
        // Handle both string and number comparisons for user_id
        const userIdStr = String(userId).trim();
        console.log('Filtering for user ID:', userIdStr);
        
        const filteredPaths = (activeUnifiedData.case_paths || []).filter(function (p) {
            const pUserId = p.user_id != null ? String(p.user_id).trim() : '';
            const matches = pUserId === userIdStr;
            if (matches) {
                console.log('Found matching path - Case ID:', p.case_id, 'User ID:', p.user_id, 'Sequence:', p.path_sequence);
            }
            return matches;
        });
        
        console.log('Filtered paths count:', filteredPaths.length);
        
        if (!filteredPaths.length) {
            // Debug: show what user_ids we have
            const allUserIds = [...new Set((activeUnifiedData.case_paths || []).map(p => String(p.user_id || 'null')))];
            console.log('Available user IDs:', allUserIds);
            container.innerHTML = '<div style="color: #b91c1c;">No Case IDs found for user <strong>' + userId + '</strong> in current results.<br><small>Available users: ' + allUserIds.slice(0, 10).join(', ') + (allUserIds.length > 10 ? '...' : '') + '</small></div>';
            return;
        }

        // Sort by case_id (already ascending by start time in back-end)
        filteredPaths.sort(function (a, b) {
            const aId = Number(a.case_id) || 0;
            const bId = Number(b.case_id) || 0;
            return aId - bId;
        });

        // Get list of case IDs for display
        const caseIds = filteredPaths.map(function (p) { return p.case_id; }).join(', ');

        // MERGE all case paths into one continuous path for this user
        // Combine sequences: Process → [Case1 steps] → [Case2 steps] → ... → End
        const mergedPathSequence = ['Process'];
        const mergedTimings = [];
        let prevEvent = 'Process';

        filteredPaths.forEach(function (p, pathIdx) {
            const seq = Array.isArray(p.path_sequence) ? p.path_sequence : [];
            const timings = Array.isArray(p.timings) ? p.timings : [];
            
            if (seq.length === 0) return; // Skip empty paths
            
            // For first path: start from index 1 (skip "Process")
            // For subsequent paths: start from index 1 (skip "Process"), but also skip "End" from previous path
            const startIdx = 1; // Always skip "Process"
            
            // If this is not the first path and previous path ended with "End", 
            // we need to remove that "End" from merged sequence before adding new events
            if (pathIdx > 0 && mergedPathSequence.length > 0 && mergedPathSequence[mergedPathSequence.length - 1] === 'End') {
                mergedPathSequence.pop(); // Remove the "End" from previous case
                if (mergedTimings.length > 0) {
                    mergedTimings.pop(); // Remove the timing entry for that "End"
                }
                // Update prevEvent to be the last real event before "End"
                if (mergedPathSequence.length > 0) {
                    prevEvent = mergedPathSequence[mergedPathSequence.length - 1];
                }
            }
            
            // Add all events from this case path (skip "Process", include everything else including "End")
            for (let i = startIdx; i < seq.length; i++) {
                const ev = seq[i];
                if (!ev) continue;
                
                // Skip "Process" if it somehow appears in the middle
                if (ev === 'Process') continue;
                
                // Add event to merged sequence
                mergedPathSequence.push(ev);
                
                // Get timing info if available
                const timingIdx = i - 1; // timings array is one shorter than path_sequence
                const timing = (timingIdx >= 0 && timingIdx < timings.length) ? timings[timingIdx] : {};
                const duration = timing.duration_seconds || 0;
                const timeLabel = timing.label || '0 sec';
                
                mergedTimings.push({
                    from: prevEvent,
                    to: ev,
                    duration_seconds: duration,
                    label: timeLabel,
                    start_time: timing.start_time || '',
                    end_time: timing.end_time || '',
                    start_datetime: timing.start_datetime || '',
                    end_datetime: timing.end_datetime || '',
                });
                
                prevEvent = ev;
            }
        });

        // Ensure End is at the very end (only one "End" for the entire merged flow)
        if (mergedPathSequence.length === 0 || mergedPathSequence[mergedPathSequence.length - 1] !== 'End') {
            // Remove any existing "End" entries first
            while (mergedPathSequence.length > 0 && mergedPathSequence[mergedPathSequence.length - 1] === 'End') {
                mergedPathSequence.pop();
                if (mergedTimings.length > 0) {
                    mergedTimings.pop();
                }
            }
            // Update prevEvent
            if (mergedPathSequence.length > 0) {
                prevEvent = mergedPathSequence[mergedPathSequence.length - 1];
            }
            // Add final "End"
            mergedPathSequence.push('End');
            mergedTimings.push({
                from: prevEvent,
                to: 'End',
                duration_seconds: 0,
                label: 'End',
                start_time: '',
                end_time: '',
                start_datetime: '',
                end_datetime: '',
            });
        }

        // Build event list from merged sequence - include ALL unique events that appear (in order of first appearance)
        // This ensures the diagram shows all event boxes needed for the merged flow
        // NOTE: We include unique events for boxes, but the path_sequence has ALL events (including duplicates)
        // so edges will be created correctly from consecutive events in path_sequence
        const seen = {};
        const allEvents = ['Process'];
        mergedPathSequence.forEach(function (ev) {
            if (!ev || ev === 'Process' || ev === 'End') return;
            if (!seen[ev]) {
                seen[ev] = true;
                allEvents.push(ev);
            }
        });
        allEvents.push('End');
        
        // Also ensure we include ALL events from original paths (in case merge missed some)
        filteredPaths.forEach(function (p) {
            const seq = Array.isArray(p.path_sequence) ? p.path_sequence : [];
            seq.forEach(function (ev) {
                if (!ev || ev === 'Process' || ev === 'End') return;
                if (!seen[ev]) {
                    seen[ev] = true;
                    allEvents.splice(allEvents.length - 1, 0, ev); // Insert before 'End'
                }
            });
        });

        // Verify we have a valid merged sequence
        if (mergedPathSequence.length < 3) { // At least Process, one event, End
            container.innerHTML = '<div style="color: #b91c1c;">Error: Merged sequence is too short. Found ' + filteredPaths.length + ' case(s) but could not merge them properly.<br><small>Merged sequence: ' + JSON.stringify(mergedPathSequence) + '</small></div>';
            return;
        }

        // Create merged case path with new synthetic Case ID
        const maxCaseId = filteredPaths.reduce(function (m, p) {
            const v = Number(p.case_id) || 0;
            return v > m ? v : m;
        }, 0);
        const baseColor = filteredPaths[0].color || '#0f172a';

        const mergedPath = {
            case_id: maxCaseId + 1,
            user_id: userId,
            color: baseColor,
            path_sequence: mergedPathSequence,
            timings: mergedTimings,
            total_duration: mergedTimings.reduce(function (sum, t) { return sum + (t.duration_seconds || 0); }, 0),
        };

        // Create flow data with single merged path
        // Use the merged sequence events, but ensure we include ALL events that appear in the merged path_sequence
        // This is critical: all_event_types determines which event boxes are drawn
        const finalEventTypes = allEvents.length > 2 ? allEvents : (activeUnifiedData.all_event_types || []);
        
        // Double-check: ensure every event in mergedPathSequence is in finalEventTypes (except Process/End which are always there)
        const eventTypesSet = new Set(finalEventTypes);
        mergedPathSequence.forEach(function(ev) {
            if (ev && ev !== 'Process' && ev !== 'End' && !eventTypesSet.has(ev)) {
                console.warn('Warning: Event "' + ev + '" in merged sequence but not in all_event_types. Adding it.');
                finalEventTypes.splice(finalEventTypes.length - 1, 0, ev); // Insert before 'End'
                eventTypesSet.add(ev);
            }
        });
        
        const mergedFlowData = {
            all_event_types: finalEventTypes,
            case_paths: [mergedPath],  // Single merged path with ALL case IDs combined
            total_cases: 1,
            same_time_groups: [],
            filterLabel: 'User ' + userId,  // So diagram title shows "User X – Step-by-Step Flow"
        };

        // Debug: log merged sequence
        console.log('=== USER MERGE DEBUG ===');
        console.log('User ID:', userId);
        console.log('Filtered paths count:', filteredPaths.length);
        console.log('Original case IDs:', caseIds);
        console.log('Original paths:');
        filteredPaths.forEach(function(p, idx) {
            console.log('  Path ' + idx + ' (Case ' + p.case_id + '):', p.path_sequence);
        });
        console.log('Merged path sequence:', mergedPathSequence);
        console.log('Merged path sequence length:', mergedPathSequence.length);
        console.log('Merged timings length:', mergedTimings.length);
        console.log('Merged timings:', mergedTimings);
        console.log('All event types (unique):', allEvents);
        console.log('Merged path object:', mergedPath);
        console.log('Merged flow data:', mergedFlowData);
        console.log('========================');

        // Verify merged path is valid before rendering
        if (mergedPathSequence.length < 3) {
            container.innerHTML = '<div style="color: #b91c1c; padding: 1rem; background: #fef2f2; border-radius: 6px;">' +
                '⚠️ Error: Merged sequence is too short (' + mergedPathSequence.length + ' events). ' +
                'Expected at least Process → [events] → End.<br>' +
                '<small>Merged sequence: ' + JSON.stringify(mergedPathSequence) + '</small><br>' +
                '<small>Original paths: ' + filteredPaths.length + ' case(s)</small><br>' +
                '<small>Filtered paths details: ' + JSON.stringify(filteredPaths.map(p => ({ case_id: p.case_id, seq_length: (p.path_sequence || []).length }))) + '</small>' +
                '</div>';
            return;
        }
        
        // Validate: merged sequence should start with Process and end with End
        if (mergedPathSequence[0] !== 'Process') {
            console.warn('Warning: Merged sequence does not start with Process. Prepending Process.');
            mergedPathSequence.unshift('Process');
            // Add a timing entry for Process if needed
            if (mergedTimings.length === 0 || mergedTimings[0].from !== 'Process') {
                mergedTimings.unshift({
                    from: 'Process',
                    to: mergedPathSequence[1] || 'End',
                    duration_seconds: 0,
                    label: 'Start',
                });
            }
        }
        if (mergedPathSequence[mergedPathSequence.length - 1] !== 'End') {
            console.warn('Warning: Merged sequence does not end with End. Appending End.');
            mergedPathSequence.push('End');
            const lastEvent = mergedPathSequence[mergedPathSequence.length - 2] || 'Process';
            mergedTimings.push({
                from: lastEvent,
                to: 'End',
                duration_seconds: 0,
                label: 'End',
            });
        }
        
        // Update mergedPath with validated sequence
        mergedPath.path_sequence = mergedPathSequence;
        mergedPath.timings = mergedTimings;

        // Render the merged diagram
        console.log('Calling renderMergedCaseFlowDiagram with:', mergedFlowData);
        const html = renderMergedCaseFlowDiagram(mergedFlowData);
        console.log('renderMergedCaseFlowDiagram returned HTML length:', html ? html.length : 0);
        
        // Show full merged sequence for verification
        const fullSequence = mergedPathSequence.join(' → ');
        
        const explanation = '<div style="background: #f0f9ff; border-left: 4px solid #0ea5e9; padding: 1rem; margin-bottom: 1rem; border-radius: 4px;">' +
            '<strong style="color: #0ea5e9;">✅ Merged View:</strong> Combined all <strong>' + filteredPaths.length + ' Case ID(s)</strong> for user <strong>' + userId + '</strong> (Case IDs: <strong>' + caseIds + '</strong>) into one continuous flow.<br>' +
            '<div style="background: white; padding: 0.75rem; border-radius: 6px; margin-top: 0.75rem; font-family: monospace; font-size: 0.85rem; color: #1e40af; border: 1px solid #bfdbfe;">' +
            '<strong>Full Merged Sequence (' + mergedPathSequence.length + ' steps):</strong><br>' +
            '<div style="margin-top: 0.5rem; word-break: break-all; line-height: 1.6;">' + fullSequence + '</div>' +
            '</div>' +
            '</div>';
        
        if (!html || html.trim() === '') {
            container.innerHTML = explanation + '<div style="color: #b91c1c; margin-top: 1rem; padding: 1rem; background: #fef2f2; border-radius: 6px;">⚠️ Diagram rendering failed. Merged path has ' + mergedPathSequence.length + ' events. Check console for details.</div>';
            console.error('renderMergedCaseFlowDiagram returned empty HTML');
            console.error('Merged flow data:', JSON.stringify(mergedFlowData, null, 2));
        } else {
            container.innerHTML = explanation + html;
            console.log('✅ Successfully rendered merged diagram');
            if (window.initMergedStepDiagram) {
                setTimeout(function () {
                    // Initialize step navigation for this user-merged flow
                    window.initMergedStepDiagram();
                    // Automatically advance to the next step every 4 seconds
                    if (window.startMergedStepAutoPlay) {
                        window.startMergedStepAutoPlay(4000);
                    }
                }, 0);
            }
        }

        if (body) {
            body.style.display = 'block';
        }
        const containerWrapper = document.getElementById('single-case-flow-container');
        if (containerWrapper && containerWrapper.scrollIntoView) {
            containerWrapper.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
    } catch (e) {
        console.error('showUserMergedFlow error', e);
        const container = document.getElementById('single-case-flow-content');
        if (container) {
            container.innerHTML = '<div style="color: #b91c1c;">Error filtering user flow: ' + String(e.message || e) + '</div>';
        }
    }
};

window.toggleSingleCaseFlow = function (evt) {
    try {
        var body = null;
        if (evt && evt.target) {
            var btn = evt.target.closest ? evt.target.closest('button') : null;
            if (btn && btn.nextElementSibling) {
                body = btn.nextElementSibling;
            }
        }
        if (!body) {
            body = document.getElementById('single-case-flow-body');
        }
        if (!body) return;
        var isHidden = body.style.display === 'none' || body.style.display === '';
        body.style.display = isHidden ? 'block' : 'none';
    } catch (e) {
        console.error('toggleSingleCaseFlow error', e);
    }
};
// Delegate clicks from any Case ID chip with data-single-case-id
if (typeof document !== 'undefined' && document.addEventListener) {
    document.addEventListener('click', function (evt) {
        var target = evt.target;
        if (!target || !(target.closest)) return;
        var chip = target.closest('[data-single-case-id]');
        if (chip) {
            var cid = chip.getAttribute('data-single-case-id');
            if (cid) {
                // Always update the Single Case Flow panel
                window.showSingleCaseFlow(cid);
                // Additionally, toggle visibility of this Case ID in the main Sankey diagram
                if (window.toggleUnifiedCaseVisibility) {
                    window.toggleUnifiedCaseVisibility(cid);
                }
            }
            return;
        }
        // User merge button: data-user-merge-id (avoids broken onclick quotes)
        var userBtn = target.closest('[data-user-merge-id]');
        if (userBtn) {
            var uid = userBtn.getAttribute('data-user-merge-id');
            if (uid != null) {
                window.showUserMergedFlow(uid);
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Merged Event-to-Event Diagram (aggregated across all Case IDs)
// ---------------------------------------------------------------------------
function renderMergedCaseFlowDiagram(flowData) {
    if (!flowData || !flowData.case_paths || flowData.case_paths.length === 0) {
        return '';
    }

    const casePaths = flowData.case_paths || [];
    const firstPath = casePaths[0];
    const isMergedSinglePath = casePaths.length === 1 && firstPath && Array.isArray(firstPath.path_sequence) && firstPath.path_sequence.length > 1;

    // When showing all cases (main diagram on domain pages), use the Sankey flow-count diagram
    if (casePaths.length > 1 && !flowData.isSingleCaseFilter) {
        const diagramContent = typeof renderSankeyDiagramSVG === 'function' ? renderSankeyDiagramSVG(flowData) : '';
        const totalCases = flowData.total_cases || casePaths.length;
        return `
        <section style="margin-bottom: 2.5rem;">
            <h2 style="font-size: 1.6rem; margin-bottom: 0.5rem; color: var(--text-primary); text-align: center;">
                Unified Case Flow Diagram (Sankey)
            </h2>
            <p style="color: var(--text-muted); margin-bottom: 1rem; font-size: 0.9rem; text-align: center;">
                Flow counts from your data: how many cases go from one event to the next (e.g. Process [4] Register). Node numbers = flow through that step.
            </p>
            <div style="background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 16px; padding: 1.5rem;">
                ${diagramContent}
            </div>
        </section>
        `;
    }

    // ---------------------------------------------------------------------
    // 1) Single merged path (per-user or per-case) – tree layout, no duplicate boxes
    // ---------------------------------------------------------------------
    if (isMergedSinglePath) {
        const mergedPath = firstPath;
        const seq = mergedPath.path_sequence || [];

        if (!seq || seq.length < 2) {
            return '';
        }

        // Count how many times each from→to transition appears across the sequence
        const fromToCounts = {};
        const pairOccurrenceIndex = {};
        for (let i = 0; i < seq.length - 1; i++) {
            const from = seq[i];
            const to = seq[i + 1];
            if (!from || !to) continue;
            const key = from + '→' + to;
            fromToCounts[key] = (fromToCounts[key] || 0) + 1;
        }

        // Build unique event list from this sequence (no duplicate boxes)
        const seenEvents = {};
        const eventTypesSingle = ['Process'];
        seq.forEach(function (ev) {
            if (!ev || ev === 'Process' || ev === 'End') return;
            if (!seenEvents[ev]) {
                seenEvents[ev] = true;
                eventTypesSingle.push(ev);
            }
        });
        eventTypesSingle.push('End');

        // Fixed positions to create a domain-aware tree layout (no duplicate nodes)
        const fixedPositionsSingle = {
            'Process': { x: 50, y: 250 },
            'Created Account': { x: 400, y: 150 },
            'Account Open': { x: 400, y: 150 },
            'Login': { x: 750, y: 150 },
            'Login / Logout': { x: 750, y: 150 },
            'Check Balance': { x: 1100, y: 150 },
            'Balance Inquiry': { x: 1100, y: 150 },
            'Withdrawal Transaction': { x: 1450, y: 150 },
            'Credit': { x: 400, y: 450 },
            'Deposit': { x: 400, y: 450 },
            'Logout': { x: 750, y: 450 },
            'End': { x: 1250, y: 250 },
            // Healthcare events
            'Register': { x: 100, y: 250 },
            'Visit': { x: 400, y: 250 },
            'Appointment': { x: 400, y: 250 },
            'Procedure': { x: 550, y: 250 },
            'Treatment': { x: 550, y: 250 },
            'Pharmacy': { x: 700, y: 250 },
            'LabTest': { x: 850, y: 250 },
            'Lab Test': { x: 850, y: 250 },
            'Billing': { x: 1000, y: 250 },
            'Admission': { x: 450, y: 400 },
            'Discharge': { x: 700, y: 400 },
            'Doctor': { x: 950, y: 400 },
            // Retail events
            'Customer Visit': { x: 80, y: 250 }, 'Product View': { x: 200, y: 250 }, 'Product Search': { x: 320, y: 250 },
            'Add To Cart': { x: 440, y: 250 }, 'Remove From Cart': { x: 560, y: 250 }, 'Apply Coupon': { x: 680, y: 250 },
            'Checkout Started': { x: 800, y: 250 }, 'Address Entered': { x: 920, y: 250 }, 'Payment Selected': { x: 1040, y: 250 },
            'Payment Success': { x: 1160, y: 250 }, 'Payment Failed': { x: 1280, y: 250 }, 'Order Placed': { x: 200, y: 400 },
            'Order Confirmed': { x: 320, y: 400 }, 'Invoice Generated': { x: 440, y: 400 }, 'Order Packed': { x: 560, y: 400 },
            'Order Shipped': { x: 680, y: 400 }, 'Out For Delivery': { x: 800, y: 400 }, 'Order Delivered': { x: 920, y: 400 },
            'Order Cancelled': { x: 1040, y: 400 }, 'Return Initiated': { x: 200, y: 550 }, 'Return Received': { x: 320, y: 550 },
            'Refund Processed': { x: 440, y: 550 }, 'User Signed Up': { x: 80, y: 400 }, 'User Logged In': { x: 80, y: 550 },
            'User Logged Out': { x: 200, y: 150 },
            // Insurance events
            'Customer Registered': { x: 80, y: 250 }, 'KYC Completed': { x: 200, y: 250 }, 'Policy Quoted': { x: 320, y: 250 },
            'Policy Purchased': { x: 440, y: 250 }, 'Policy Activated': { x: 560, y: 250 }, 'Premium Due': { x: 680, y: 250 },
            'Premium Paid': { x: 800, y: 250 }, 'Policy Renewed': { x: 80, y: 400 },
            'Policy Expired': { x: 200, y: 400 }, 'Claim Requested': { x: 320, y: 400 }, 'Claim Registered': { x: 440, y: 400 },
            'Claim Verified': { x: 560, y: 400 }, 'Claim Assessed': { x: 680, y: 400 }, 'Claim Approved': { x: 800, y: 400 },
            'Claim Rejected': { x: 920, y: 400 }, 'Claim Paid': { x: 80, y: 550 }, 'Nominee Updated': { x: 200, y: 550 },
            'Policy Cancelled': { x: 320, y: 550 }, 'Policy Closed': { x: 440, y: 550 },
            // Finance events
            'Account Opened': { x: 320, y: 250 }, 'Account Closed': { x: 440, y: 250 },
            'Invest': { x: 560, y: 250 }, 'Value Update': { x: 680, y: 250 }, 'Redeem': { x: 800, y: 250 },
            'Switch': { x: 920, y: 250 }, 'Dividend': { x: 80, y: 400 },
            'Transfer Initiated': { x: 560, y: 400 }, 'Transfer Completed': { x: 680, y: 400 },
            'Payment Initiated': { x: 1040, y: 250 }, 'Loan Applied': { x: 80, y: 550 },
            'Loan Approved': { x: 200, y: 550 }, 'Loan Disbursed': { x: 320, y: 550 },
            'Application Submitted': { x: 80, y: 700 }, 'Application Reviewed': { x: 200, y: 700 },
            'Proposal Generated': { x: 320, y: 700 }, 'Proposal Accepted': { x: 440, y: 700 },
            'Identity Verified': { x: 560, y: 700 }, 'Address Verified': { x: 680, y: 700 },
            'Income Verified': { x: 800, y: 700 }, 'Beneficiary Added': { x: 920, y: 700 },
            'Beneficiary Updated': { x: 80, y: 850 }, 'Coverage Activated': { x: 200, y: 850 },
            'Coverage Changed': { x: 320, y: 850 }, 'Installment Generated': { x: 440, y: 850 },
            'Installment Paid': { x: 560, y: 850 }, 'Penalty Applied': { x: 680, y: 850 },
            'Discount Applied': { x: 800, y: 850 }, 'Case Escalated': { x: 920, y: 850 },
            'Case Resolved': { x: 80, y: 1000 }, 'Support Ticket Created': { x: 200, y: 1000 },
            'Support Ticket Closed': { x: 320, y: 1000 }, 'Account Frozen': { x: 440, y: 1000 }
        };

        const boxWidth = 160;
        const boxHeight = 70;

        const eventPositionsSingle = {};
        let dynamicX = 50;
        const takenPositionsSingle = Object.values(fixedPositionsSingle).map(function (p) { return p.x + ',' + p.y; });
        function resolveEventPos(ev, posMap) {
            if (posMap[ev]) return posMap[ev];
            if (posMap[ev.replace(/\s+/g, '')]) return posMap[ev.replace(/\s+/g, '')];
            if (posMap[ev.replace(/([a-z])([A-Z])/g, '$1 $2')]) return posMap[ev.replace(/([a-z])([A-Z])/g, '$1 $2')];
            return null;
        }
        eventTypesSingle.forEach(function (event) {
            const pos = fixedPositionsSingle[event] || fixedPositionsSingle[event.replace('Transaction', '').trim()] || resolveEventPos(event, fixedPositionsSingle);
            if (pos) {
                eventPositionsSingle[event] = pos;
            } else {
                let placed = false;
                for (let r = 0; r < 3; r++) {
                    for (let c = 0; c < 5; c++) {
                        const tx = 50 + c * 350;
                        const ty = 150 + r * 300;
                        const key = tx + ',' + ty;
                        if (takenPositionsSingle.indexOf(key) === -1 && !placed) {
                            eventPositionsSingle[event] = { x: tx, y: ty };
                            takenPositionsSingle.push(key);
                            placed = true;
                        }
                    }
                }
                if (!placed) {
                    eventPositionsSingle[event] = { x: dynamicX, y: 750 };
                    dynamicX += 350;
                }
            }
        });

        const maxX = Math.max.apply(null, Object.values(eventPositionsSingle).map(function (p) { return p.x; })) + 250;
        const maxY = Math.max.apply(null, Object.values(eventPositionsSingle).map(function (p) { return p.y; })) + 150;
        const svgWidth = Math.max(900, maxX);
        const svgHeight = Math.max(400, maxY);

        // Auto-scale diagram so it fits nicely on screen
        let viewportWidth = 1200;
        let viewportHeight = 800;
        try {
            if (typeof window !== 'undefined') {
                if (window.innerWidth) viewportWidth = window.innerWidth;
                if (window.innerHeight) viewportHeight = window.innerHeight;
            }
        } catch (e) {
            // ignore, use defaults
        }

        const availableWidth = Math.max(viewportWidth - 80, 600);
        const availableHeight = Math.max(viewportHeight - 180, 400);

        const scaleX = availableWidth / svgWidth;
        const scaleY = availableHeight / svgHeight;
        const autoScale = Math.min(scaleX, scaleY, 1.4);

        const scaledWidth = svgWidth * autoScale;
        const scaledHeight = svgHeight * autoScale;
        const scaleStyle = autoScale !== 1 ? `transform: scale(${autoScale}); transform-origin: 0 0;` : '';
        const diagramMinHeight = Math.max(availableHeight, 420);

        // Build edges: every consecutive pair in the sequence becomes one arrow
        const edges = [];
        for (let j = 0; j < seq.length - 1; j++) {
            const fromEv = seq[j];
            const toEv = seq[j + 1];
            if (!fromEv || !toEv) continue;

            const edgeKey = fromEv + '→' + toEv;
            const repeatCount = fromToCounts[edgeKey] || 1;
            const stepNumber = j + 1;
            let stepLabel = 'Step ' + stepNumber;
            if (repeatCount > 1) {
                stepLabel += ' (' + repeatCount + 'x)';
            }

            pairOccurrenceIndex[edgeKey] = (pairOccurrenceIndex[edgeKey] || 0) + 1;
            const occurrenceIndex = pairOccurrenceIndex[edgeKey];

            edges.push({
                from: fromEv,
                to: toEv,
                stepNumber: stepNumber,
                stepLabel: stepLabel,
                repeatCount: repeatCount,
                occurrenceIndex: occurrenceIndex,
                totalPairCount: repeatCount
            });
        }

        console.log('=== MERGED SINGLE-PATH FLOW (Tree) ===');
        console.log('Total steps:', edges.length);
        edges.forEach(function (e) {
            console.log('  ' + e.stepLabel + ': ' + e.from + ' → ' + e.to);
        });
        console.log('========================================');

        let svgHTML = `<svg width="${svgWidth}" height="${svgHeight}" style="position: absolute; top: 0; left: 0; z-index: 3; transform-origin: 0 0;">
        <defs>
            <marker id="merged-arrow" markerWidth="9" markerHeight="6" refX="8" refY="3" orient="auto">
                <polygon points="0 0, 9 3, 0 6" fill="#475569" />
            </marker>
        </defs>
        `;

        // Draw arrows for each step between unique event boxes.
        // - If from === to, draw a self-loop arc around the box.
        // - If the same from→to pair repeats, fan the arrows using curved paths.
        edges.forEach(function (e) {
            const fromPos = eventPositionsSingle[e.from];
            const toPos = eventPositionsSingle[e.to];
            if (!fromPos || !toPos) return;

            const x1 = fromPos.x + boxWidth / 2;
            const y1 = fromPos.y + boxHeight / 2;
            const rawX2 = toPos.x + boxWidth / 2;
            const rawY2 = toPos.y + boxHeight / 2;

            let labelX;
            let labelY;
            let pathElement = '';

            const dxFull = rawX2 - x1;
            const dyFull = rawY2 - y1;

            // Self-loop: from event to same event – always show as a clear curved arrow
            if (e.from === e.to) {
                const radius = 55;
                const startAngle = -Math.PI / 3; // angle for loop start
                const endAngle = -2 * Math.PI / 3; // angle for loop end

                const sx = x1 + radius * Math.cos(startAngle);
                const sy = y1 + radius * Math.sin(startAngle);
                const ex = x1 + radius * Math.cos(endAngle);
                const ey = y1 + radius * Math.sin(endAngle);
                const cx = x1 + radius * 1.2; // control point a bit outside
                const cy = y1 - radius * 1.4;

                labelX = x1 + radius * 1.1;
                labelY = y1 - radius * 1.8;

                pathElement = `<path d="M ${sx} ${sy} Q ${cx} ${cy} ${ex} ${ey}"
                      stroke="#475569"
                      stroke-width="2.8"
                      fill="none"
                      marker-end="url(#merged-arrow)"
                      opacity="0.98" />`;
            } else {
                const lenFull = Math.sqrt(dxFull * dxFull + dyFull * dyFull) || 1;
                const nx = -dyFull / lenFull;
                const ny = dxFull / lenFull;

                // Shorten arrow so head is just before the target box edge
                const margin = 26;
                const ux = dxFull / lenFull;
                const uy = dyFull / lenFull;
                const x2 = rawX2 - ux * margin;
                const y2 = rawY2 - uy * margin;

                const dx = x2 - x1;
                const dy = y2 - y1;

                if (e.totalPairCount > 1) {
                    // Fan multiple arrows for same from→to pair
                    const baseOffset = 40;
                    const groupIndex = e.occurrenceIndex - (e.totalPairCount + 1) / 2;
                    const offset = groupIndex * baseOffset;

                    const mx = (x1 + x2) / 2;
                    const my = (y1 + y2) / 2;
                    const cx = mx + nx * offset;
                    const cy = my + ny * offset;

                    // Move label slightly off the connector so arrow is fully visible
                    labelX = cx + nx * 22;
                    labelY = cy + ny * 22;

                    pathElement = `<path d="M ${x1} ${y1} Q ${cx} ${cy} ${x2} ${y2}"
                      stroke="#475569"
                      stroke-width="2.6"
                      fill="none"
                      marker-end="url(#merged-arrow)"
                      opacity="0.95" />`;
                } else {
                    // Single straight arrow
                    const mx = (x1 + x2) / 2;
                    const my = (y1 + y2) / 2;

                    // Move label slightly off the connector so arrow is fully visible
                    labelX = mx + nx * 22;
                    labelY = my + ny * 22;

                    pathElement = `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}"
                      stroke="#475569"
                      stroke-width="2.6"
                      marker-end="url(#merged-arrow)"
                      opacity="0.95" />`;
                }
            }

            const labelWidth = 120;
            const labelOffset = labelWidth / 2;

            svgHTML += `
            <g class="merged-step-edge" data-merged-step-edge="1" data-step-number="${e.stepNumber}">
                ${pathElement}
                <rect x="${labelX - labelOffset}" y="${labelY - 16}" width="${labelWidth}" height="22" rx="11"
                      fill="white" stroke="#475569" stroke-width="0.9" opacity="0.96" />
                <text x="${labelX}" y="${labelY - 1}" text-anchor="middle"
                      font-size="10" font-weight="600" fill="#111827">
                    ${e.stepLabel}
                </text>
            </g>
            `;
        });

        svgHTML += '</svg>';

        // Draw one box per unique event (tree layout – no duplicates)
        let boxesHTML = '';
        eventTypesSingle.forEach(function (event, idx) {
            const pos = eventPositionsSingle[event];
            if (!pos) return;

            const ev = event;
            const isFirst = idx === 0;
            const isLast = idx === eventTypesSingle.length - 1;
            const isEnd = ev === 'End' || isLast;

            let boxBg = '#64748b';
            if (ev === 'Process' || isFirst) boxBg = '#1e40af';
            else if (isEnd) boxBg = '#ffffff';

            const borderStyle = isEnd
                ? 'border: 3px solid #1e40af; background: white; color: #1e40af;'
                : `background: ${boxBg}; color: white; box-shadow: 0 3px 6px rgba(15,23,42,0.25);`;

            boxesHTML += `
            <div style="
                position: absolute;
                left: ${pos.x}px;
                top: ${pos.y}px;
                width: ${boxWidth}px;
                height: ${boxHeight}px;
                ${borderStyle}
                border-radius: ${isEnd ? '50px' : '12px'};
                display: flex;
                align-items: center;
                justify-content: center;
                text-align: center;
                font-weight: 700;
                font-size: 0.95rem;
                z-index: 1;
                padding: 0.5rem;
                line-height: 1.2;
            ">
                ${ev}
            </div>
            `;
        });

        const filterLabel = (flowData && flowData.filterLabel) ? flowData.filterLabel : '';
        const diagramTitle = filterLabel
            ? filterLabel + ' – Step-by-Step Tree Flow'
            : 'Step-by-Step Flow (Tree Structure)';
        const diagramDescription = 'Each event appears once as a box. Every step draws an arrow from the previous event to the next, and repeated transitions fan out as multiple arrows without duplicating event boxes.';

        // Expose total steps for global controls
        if (typeof window !== 'undefined') {
            window._mergedStepTotal = edges.length;
        }

        const stepButtonsHTML = edges.map(function (e) {
            return `<button type="button"
                        class="merged-step-chip"
                        data-step-chip="${e.stepNumber}"
                        onclick="window.jumpMergedStep && window.jumpMergedStep(${e.stepNumber});"
                        style="padding: 0.2rem 0.65rem; font-size: 0.8rem; border-radius: 999px; border: 1px solid #cbd5e1; background: white; color: #0f172a; cursor: pointer;">
                        ${e.stepNumber}
                    </button>`;
        }).join('');

        return `
        <section id="merged-step-flow-section" style="margin-bottom: 2.5rem;">
            <h2 style="font-size: 1.6rem; margin-bottom: 0.5rem; color: var(--text-primary); text-align: center;">
                ${diagramTitle}
            </h2>
            <p style="color: var(--text-muted); margin-bottom: 1rem; font-size: 0.9rem; text-align: center;">
                ${diagramDescription}
            </p>
            <div style="display:flex; justify-content:center; align-items:center; gap:0.75rem; margin-bottom:0.75rem;">
                <button type="button"
                        onclick="window.prevMergedStep && window.prevMergedStep();"
                        style="padding:0.3rem 0.75rem; font-size:0.8rem; border-radius:999px; border:1px solid #cbd5e1; background:white; cursor:pointer; color:#0f172a;">
                    ◀ Prev
                </button>
                <div id="merged-step-indicator" style="font-size:0.85rem; color:#0f172a; font-weight:600;">
                    Step 1 of ${edges.length}
                </div>
                <button type="button"
                        onclick="window.nextMergedStep && window.nextMergedStep();"
                        style="padding:0.3rem 0.75rem; font-size:0.8rem; border-radius:999px; border:1px solid #cbd5e1; background:white; cursor:pointer; color:#0f172a;">
                    Next ▶
                </button>
            </div>
            <div style="display:flex; flex-wrap:wrap; justify-content:center; gap:0.35rem; margin-bottom:0.75rem;">
                ${stepButtonsHTML}
            </div>
            <div style="background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 16px; padding: 1.5rem;">
                <div style="position: relative; min-height: ${diagramMinHeight}px; height: ${diagramMinHeight}px; overflow: auto;">
                    <div style="position: relative; width: ${scaledWidth}px; height: ${scaledHeight}px; margin: 0 auto;">
                        <div style="position: absolute; left: 0; top: 0; width: ${svgWidth}px; height: ${svgHeight}px; ${scaleStyle}">
                            ${svgHTML}
                            ${boxesHTML}
                        </div>
                    </div>
                </div>
            </div>
        </section>
        `;
    }

    // ---------------------------------------------------------------------
    // 2) Multi-path aggregated flow – keep existing "merged counts" diagram
    // ---------------------------------------------------------------------

    let eventTypes = flowData.all_event_types || [];
    if (!eventTypes || eventTypes.length === 0) {
        const seen = new Set();
        eventTypes = ['Process'];
        casePaths.forEach(function (p) {
            (p.path_sequence || []).forEach(function (s) {
                if (s && s !== 'Process' && s !== 'End' && !seen.has(s)) {
                    seen.add(s);
                    eventTypes.push(s);
                }
            });
        });
        eventTypes.push('End');
    }

    // Build unique user patterns and CRT (how many users follow each pattern),
    // then create one edge per (pattern, from, to) with width ∝ CRT.
    const patternSequences = {};   // patternKey -> sequence array
    const patternUserSets = {};    // patternKey -> Set of user ids

    casePaths.forEach(function (p) {
        const seq = p.path_sequence || [];
        if (!seq || seq.length < 2) return;

        const patternKey = seq.join('→');
        if (!patternSequences[patternKey]) {
            patternSequences[patternKey] = seq;
        }

        const userKey = p.user_id != null ? String(p.user_id) : String(p.case_id);
        if (!patternUserSets[patternKey]) {
            patternUserSets[patternKey] = new Set();
        }
        patternUserSets[patternKey].add(userKey);
    });

    const patternCRT = {}; // patternKey -> number of users
    Object.keys(patternSequences).forEach(function (pk) {
        const s = patternUserSets[pk];
        patternCRT[pk] = s ? s.size : 0;
    });

    const edgeMapByPattern = {}; // key: patternKey|from→to
    Object.keys(patternSequences).forEach(function (patternKey) {
        const seq = patternSequences[patternKey] || [];
        const crt = patternCRT[patternKey] || 0;
        if (crt <= 0 || seq.length < 2) return;

        for (let i = 0; i < seq.length - 1; i++) {
            const from = seq[i];
            const to = seq[i + 1];
            if (!from || !to) continue;
            const k = patternKey + '|' + from + '→' + to;
            if (!edgeMapByPattern[k]) {
                edgeMapByPattern[k] = {
                    from: from,
                    to: to,
                    segmentCount: 0,
                    caseCount: crt,   // treat CRT as caseCount for width
                    patternKey: patternKey
                };
            }
            edgeMapByPattern[k].segmentCount += 1;
        }
    });

    let edges = Object.values(edgeMapByPattern).filter(function (e) {
        return e.caseCount > 0;
    });

    if (edges.length === 0) {
        return '';
    }

    // Sankey-style layout: columns by earliest step index, rows for events in that step.
    const boxWidth = 160;
    const boxHeight = 70;

    // Determine earliest index (from Process) where each event appears in any pattern.
    const stepIndexByEvent = {};
    Object.values(patternSequences).forEach(function (seq) {
        seq.forEach(function (ev, idx) {
            if (!ev || ev === 'Process' || ev === 'End') return;
            if (stepIndexByEvent[ev] == null || idx < stepIndexByEvent[ev]) {
                stepIndexByEvent[ev] = idx;
            }
        });
    });
    let maxStepIndex = 0;
    Object.keys(stepIndexByEvent).forEach(function (ev) {
        if (stepIndexByEvent[ev] > maxStepIndex) {
            maxStepIndex = stepIndexByEvent[ev];
        }
    });

    // Assign each event to a "level" (column)
    const levelByEvent = {};
    eventTypes.forEach(function (ev) {
        if (ev === 'Process') {
            levelByEvent[ev] = 0;
        } else if (ev === 'End') {
            levelByEvent[ev] = maxStepIndex + 1;
        } else if (stepIndexByEvent[ev] != null) {
            levelByEvent[ev] = stepIndexByEvent[ev] + 1; // shift right from Process
        } else {
            levelByEvent[ev] = 1;
        }
    });

    // Group events by level to stack them vertically
    const eventsByLevel = {};
    Object.keys(levelByEvent).forEach(function (ev) {
        const lvl = levelByEvent[ev];
        if (!eventsByLevel[lvl]) eventsByLevel[lvl] = [];
        eventsByLevel[lvl].push(ev);
    });

    const eventPositions = {};
    const colGap = 260;
    const rowGap = 110;
    const baseX = 60;
    const baseY = 220;

    Object.keys(eventsByLevel).map(Number).sort(function (a, b) { return a - b; }).forEach(function (lvl) {
        const nodes = eventsByLevel[lvl];
        if (!nodes || !nodes.length) return;
        nodes.sort();
        const colX = baseX + lvl * colGap;
        const totalHeight = (nodes.length - 1) * rowGap;
        const colTop = baseY - totalHeight / 2;
        nodes.forEach(function (ev, idx) {
            const y = colTop + idx * rowGap;
            eventPositions[ev] = { x: colX, y: y };
        });
    });

    const maxX = Math.max.apply(null, Object.values(eventPositions).map(function (p) { return p.x; })) + 250;
    const maxY = Math.max.apply(null, Object.values(eventPositions).map(function (p) { return p.y; })) + 150;
    const svgWidth = Math.max(900, maxX);
    const svgHeight = Math.max(400, maxY);

    // Auto-scale diagram so it fits nicely inside the visible screen for ALL domains
    let viewportWidth = 1200;
    let viewportHeight = 800;
    try {
        if (typeof window !== 'undefined') {
            if (window.innerWidth) viewportWidth = window.innerWidth;
            if (window.innerHeight) viewportHeight = window.innerHeight;
        }
    } catch (e) {
        // ignore, use defaults
    }

    const availableWidth = Math.max(viewportWidth - 80, 600);
    const availableHeight = Math.max(viewportHeight - 180, 400);

    const scaleX = availableWidth / svgWidth;
    const scaleY = availableHeight / svgHeight;
    const autoScale = Math.min(scaleX, scaleY, 1.4);

    const scaledWidth = svgWidth * autoScale;
    const scaledHeight = svgHeight * autoScale;
    const scaleStyle = autoScale !== 1 ? `transform: scale(${autoScale}); transform-origin: 0 0;` : '';
    const diagramMinHeight = Math.max(availableHeight, 420);

    // Stroke width scale based on case count (CRT). We exaggerate differences
    // so patterns followed by many users are clearly thicker than rare ones.
    const maxCaseCount = edges.reduce(function (m, e) { return Math.max(m, e.caseCount || 1); }, 0) || 1;
    function edgeStrokeWidth(e) {
        const c = Math.max(1, e.caseCount || 1);
        const ratio = c / maxCaseCount;          // 0..1
        const boost = Math.sqrt(ratio);          // non-linear, emphasizes big flows
        const minW = 2.0;
        const maxW = 10.0;
        return minW + (maxW - minW) * boost;     // between 2 and 10 px
    }

    let svgHTML = `<svg width="${svgWidth}" height="${svgHeight}" style="position: absolute; top: 0; left: 0; z-index: 3; transform-origin: 0 0;">
        <defs>
            <marker id="merged-arrow" markerWidth="9" markerHeight="6" refX="8" refY="3" orient="auto">
                <polygon points="0 0, 9 3, 0 6" fill="#475569" />
            </marker>
        </defs>
    `;

    // Identify bidirectional edges (return paths) to curve them
    const bidirectionalEdges = new Set();
    edges.forEach(function (e1) {
        edges.forEach(function (e2) {
            if (e1.from === e2.to && e1.to === e2.from) {
                bidirectionalEdges.add(e1.from + '→' + e1.to);
                bidirectionalEdges.add(e2.from + '→' + e2.to);
            }
        });
    });

    // Assign a distinct color per edge pattern (from→to) so each flow stands out.
    const edgeColorMap = {};
    // Palette: one distinct color per patternKey (path pattern).
    const edgePalette = [
        '#2563eb', '#0f766e', '#f97316', '#ec4899', '#a855f7',
        '#22c55e', '#facc15', '#f43f5e', '#06b6d4', '#4b5563',
        '#15803d', '#1d4ed8', '#ea580c', '#be185d', '#7c2d12',
        '#4c1d95', '#047857', '#b45309', '#4338ca', '#7f1d1d',
        '#0e7490', '#16a34a', '#eab308', '#9d174d', '#1e293b',
        '#10b981', '#3b82f6', '#f59e0b', '#6366f1', '#ef4444',
        '#14b8a6', '#8b5cf6', '#fbbf24', '#fb7185', '#475569'
    ];
    let colorIdx = 0;
    edges.forEach(function (e) {
        const pk = e.patternKey || '';
        if (!edgeColorMap[pk]) {
            edgeColorMap[pk] = edgePalette[colorIdx % edgePalette.length];
            colorIdx += 1;
        }
    });

    console.log('=== MERGED DIAGRAM EDGES (Aggregated) ===');
    console.log('Total edges:', edges.length);
    console.log('Bidirectional edges (return paths):', Array.from(bidirectionalEdges));
    edges.forEach(function (e) {
        const pk = e.patternKey || '';
        console.log('  ' + e.from + ' → ' + e.to + ': ' + e.segmentCount + ' seg(s), CRT=' + (e.caseCount || 0) + ', patternKey=' + pk + ', color=' + edgeColorMap[pk]);
    });
    console.log('==========================================');

    edges.forEach(function (e) {
        const fromPos = eventPositions[e.from];
        const toPos = eventPositions[e.to];
        if (!fromPos || !toPos) return;
        const x1 = fromPos.x + boxWidth / 2;
        const y1 = fromPos.y + boxHeight / 2;
        const rawX2 = toPos.x + boxWidth / 2;
        const rawY2 = toPos.y + boxHeight / 2;
        const sw = edgeStrokeWidth(e);

        const label = (e.caseCount || 0) + ' user' + ((e.caseCount || 0) === 1 ? '' : 's');
        const edgeKey = e.from + '→' + e.to;
        const isBidirectional = bidirectionalEdges.has(edgeKey);
        const strokeColor = edgeColorMap[e.patternKey || ''] || '#475569';

        let pathElement = '';
        let labelX;
        let labelY;

        const dxFull = rawX2 - x1;
        const dyFull = rawY2 - y1;
        const lenFull = Math.sqrt(dxFull * dxFull + dyFull * dyFull) || 1;
        const ux = dxFull / lenFull;
        const uy = dyFull / lenFull;
        const margin = 26;
        const x2 = rawX2 - ux * margin;
        const y2 = rawY2 - uy * margin;

        if (isBidirectional) {
            const isReturnPath = x2 < x1 || (e.from !== 'Process' && e.to !== 'End' && Math.abs(x2 - x1) < 100);
            const curveDirection = isReturnPath ? -1 : 1;
            const curveOffset = Math.max(80, Math.abs(x2 - x1) * 0.3) * curveDirection;
            const controlX1 = x1 + (x2 - x1) * 0.25;
            const controlY1 = y1 + curveOffset;
            const controlX2 = x1 + (x2 - x1) * 0.75;
            const controlY2 = y2 + curveOffset;
            labelX = (controlX1 + controlX2) / 2;
            labelY = (controlY1 + controlY2) / 2 - 6;
            pathElement = `<path d="M ${x1} ${y1} C ${controlX1} ${controlY1}, ${controlX2} ${controlY2}, ${x2} ${y2}"
                      stroke="${strokeColor}"
                      stroke-width="${sw.toFixed(1)}"
                      fill="none"
                      marker-end="url(#merged-arrow)"
                      opacity="0.9"
                      style="stroke-dasharray: ${isReturnPath ? '5,3' : 'none'};" />`;
        } else {
            const mx = (x1 + x2) / 2;
            const my = (y1 + y2) / 2;
            // Move label slightly off the connector so arrows are not covered
            const nx = -dyFull / lenFull;
            const ny = dxFull / lenFull;
            labelX = mx + nx * 20;
            labelY = my + ny * 20;

            pathElement = `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}"
                      stroke="${strokeColor}"
                      stroke-width="${sw.toFixed(1)}"
                      marker-end="url(#merged-arrow)"
                      opacity="0.9" />`;
        }

        const labelWidth = 80;
        const labelOffset = labelWidth / 2;

        svgHTML += `
            <g>
                ${pathElement}
                <rect x="${labelX - labelOffset}" y="${labelY - 16}" width="${labelWidth}" height="20" rx="10"
                      fill="white" stroke="${strokeColor}" stroke-width="0.8" opacity="0.96" />
                <text x="${labelX}" y="${labelY - 2}" text-anchor="middle"
                      font-size="10" font-weight="600" fill="#0f172a">
                    ${label}
                </text>
            </g>
        `;
    });

    svgHTML += '</svg>';

    let boxesHTML = '';
    eventTypes.forEach(function (event) {
        const pos = eventPositions[event];
        if (!pos) return;
        const isEnd = event === 'End';
        let boxBg = '#0f172a';
        if (event === 'Process') boxBg = '#1e40af';
        else if (isEnd) boxBg = '#ffffff';
        else boxBg = '#64748b';
        const borderStyle = isEnd
            ? 'border: 3px solid #1e40af; background: white; color: #1e40af;'
            : `background: ${boxBg}; color: white; box-shadow: 0 3px 6px rgba(15,23,42,0.25);`;
        boxesHTML += `
            <div style="
                position: absolute;
                left: ${pos.x}px;
                top: ${pos.y}px;
                width: ${boxWidth}px;
                height: ${boxHeight}px;
                ${borderStyle}
                border-radius: ${isEnd ? '50px' : '12px'};
                display: flex;
                align-items: center;
                justify-content: center;
                text-align: center;
                font-weight: 700;
                font-size: 0.95rem;
                z-index: 2;
                padding: 0.5rem;
                line-height: 1.2;
            ">
                ${event}
            </div>
        `;
    });

    const filterLabel = (flowData && flowData.filterLabel) ? flowData.filterLabel : '';
    const diagramTitle = 'Event-to-Event Counts (Merged Across Case IDs)';
    const diagramDescription = 'Each arrow shows how many Case IDs move from one event to the next across all paths (no per-case splitting).';

    return `
        <section style="margin-bottom: 2.5rem;">
            <h2 style="font-size: 1.6rem; margin-bottom: 0.5rem; color: var(--text-primary); text-align: center;">
                ${diagramTitle}
            </h2>
            <p style="color: var(--text-muted); margin-bottom: 1rem; font-size: 0.9rem; text-align: center;">
                ${diagramDescription}
            </p>
            <div style="background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 16px; padding: 1.5rem;">
                <div style="position: relative; min-height: ${diagramMinHeight}px; height: ${diagramMinHeight}px; overflow: auto;">
                    <div style="position: relative; width: ${scaledWidth}px; height: ${scaledHeight}px; margin: 0 auto;">
                        <div style="position: absolute; left: 0; top: 0; width: ${svgWidth}px; height: ${svgHeight}px; ${scaleStyle}">
                            ${svgHTML}
                            ${boxesHTML}
                        </div>
                    </div>
                </div>
            </div>
        </section>
    `;
}

window.renderMergedCaseFlowDiagram = renderMergedCaseFlowDiagram;

// ---------------------------------------------------------------------------
// Step navigation helpers for merged single-path diagrams
// ---------------------------------------------------------------------------

if (typeof window !== 'undefined') {
    window._mergedStepCurrent = 1;
    window._mergedStepTotal = window._mergedStepTotal || 0;
    // Timer bookkeeping for automatic step progression (auto-play)
    window._mergedStepAutoTimer = window._mergedStepAutoTimer || null;
    window._mergedStepAutoIntervalMs = window._mergedStepAutoIntervalMs || 4000;
}

window.updateMergedStepIndicator = function () {
    try {
        const indicator = document.getElementById('merged-step-indicator');
        if (!indicator || !window._mergedStepTotal) return;
        const cur = window._mergedStepCurrent || 1;
        indicator.textContent = 'Step ' + cur + ' of ' + window._mergedStepTotal;
    } catch (e) {
        console.error('updateMergedStepIndicator error', e);
    }
};

window.highlightMergedStep = function (stepNumber) {
    try {
        if (!stepNumber || stepNumber < 1) return;
        window._mergedStepCurrent = stepNumber;

        const groups = document.querySelectorAll('[data-merged-step-edge="1"]');
        if (!groups || !groups.length) return;

        groups.forEach(function (g) {
            const sn = Number(g.getAttribute('data-step-number') || '0');
            const isActive = sn === stepNumber;

            const path = g.querySelector('path, line');
            const rect = g.querySelector('rect');
            const text = g.querySelector('text');

            if (path) {
                path.setAttribute('stroke-width', isActive ? '4.2' : '2.4');
                path.setAttribute('stroke', isActive ? '#f97316' : '#475569');
                path.setAttribute('opacity', isActive ? '1.0' : '0.85');
            }
            if (rect) {
                rect.setAttribute('stroke', isActive ? '#f97316' : '#475569');
                rect.setAttribute('stroke-width', isActive ? '1.2' : '0.8');
                rect.setAttribute('fill', isActive ? '#fff7ed' : 'white');
            }
            if (text) {
                text.setAttribute('fill', isActive ? '#7c2d12' : '#111827');
                text.setAttribute('font-size', isActive ? '11' : '10');
            }

            if (isActive && g.parentNode) {
                g.parentNode.appendChild(g); // bring active edge to front
            }
        });

        const chips = document.querySelectorAll('.merged-step-chip');
        chips.forEach(function (chip) {
            const sn = Number(chip.getAttribute('data-step-chip') || '0');
            const isActive = sn === stepNumber;
            chip.style.background = isActive ? '#0f172a' : 'white';
            chip.style.color = isActive ? '#f9fafb' : '#0f172a';
            chip.style.borderColor = isActive ? '#0f172a' : '#cbd5e1';
            chip.style.transform = isActive ? 'scale(1.05)' : 'scale(1.0)';
        });

        window.updateMergedStepIndicator();

        // Auto-scroll selected edge into view (centered) if possible
        const section = document.getElementById('merged-step-flow-section');
        if (section) {
            const active = document.querySelector('[data-merged-step-edge="1"][data-step-number="' + stepNumber + '"]');
            if (active && active.getBoundingClientRect) {
                const box = active.getBoundingClientRect();
                const host = section.querySelector('div[style*="overflow: auto"]');
                if (host && host.getBoundingClientRect) {
                    const hBox = host.getBoundingClientRect();
                    const offsetY = (box.top + box.bottom) / 2 - (hBox.top + hBox.bottom) / 2;
                    host.scrollTop += offsetY;
                }
            }
        }
    } catch (e) {
        console.error('highlightMergedStep error', e);
    }
};

window.initMergedStepDiagram = function () {
    try {
        if (!window._mergedStepTotal) {
            const groups = document.querySelectorAll('[data-merged-step-edge="1"]');
            window._mergedStepTotal = groups ? groups.length : 0;
        }
        if (!window._mergedStepTotal) return;
        window._mergedStepCurrent = 1;
        window.highlightMergedStep(1);
    } catch (e) {
        console.error('initMergedStepDiagram error', e);
    }
};

window.nextMergedStep = function () {
    try {
        if (!window._mergedStepTotal) return;
        const cur = window._mergedStepCurrent || 1;
        const next = cur >= window._mergedStepTotal ? 1 : cur + 1;
        window.highlightMergedStep(next);

        // If user manually clicks "Next ▶" and no auto-play timer is running yet,
        // start automatic progression so steps continue every few seconds.
        if (typeof window !== 'undefined' &&
            window.startMergedStepAutoPlay &&
            !window._mergedStepAutoTimer &&
            (window._mergedStepTotal || 0) > 1) {
            // Use existing interval if set, otherwise default to 4000ms
            const ms = window._mergedStepAutoIntervalMs || 4000;
            window.startMergedStepAutoPlay(ms);
        }
    } catch (e) {
        console.error('nextMergedStep error', e);
    }
};

window.prevMergedStep = function () {
    try {
        if (!window._mergedStepTotal) return;
        const cur = window._mergedStepCurrent || 1;
        const prev = cur <= 1 ? window._mergedStepTotal : cur - 1;
        window.highlightMergedStep(prev);
    } catch (e) {
        console.error('prevMergedStep error', e);
    }
};

window.jumpMergedStep = function (stepNumber) {
    try {
        const step = Number(stepNumber);
        if (!step || step < 1) return;
        window.highlightMergedStep(step);
    } catch (e) {
        console.error('jumpMergedStep error', e);
    }
};

// ---------------------------------------------------------------------------
// Auto-play helpers – automatically advance steps every N milliseconds
// ---------------------------------------------------------------------------

window.startMergedStepAutoPlay = function (intervalMs) {
    try {
        const total = window._mergedStepTotal || 0;
        if (!total || total < 2) {
            // Nothing to auto-play if there are fewer than 2 steps
            return;
        }

        const ms = (typeof intervalMs === 'number' && intervalMs > 0)
            ? intervalMs
            : (window._mergedStepAutoIntervalMs || 4000);

        // Clear any existing timer before starting a new one
        if (window._mergedStepAutoTimer) {
            clearInterval(window._mergedStepAutoTimer);
            window._mergedStepAutoTimer = null;
        }

        window._mergedStepAutoIntervalMs = ms;
        console.log('[AutoPlay] Starting merged-step auto-play. Total steps =', total, 'Interval (ms) =', ms);
        window._mergedStepAutoTimer = setInterval(function () {
            try {
                if (!window._mergedStepTotal || window._mergedStepTotal < 2) {
                    // If diagram changed and no longer has steps, stop auto-play
                    if (window._mergedStepAutoTimer) {
                        clearInterval(window._mergedStepAutoTimer);
                        window._mergedStepAutoTimer = null;
                    }
                    return;
                }
                if (window.nextMergedStep) {
                    window.nextMergedStep();
                }
            } catch (timerErr) {
                console.error('startMergedStepAutoPlay timer error', timerErr);
            }
        }, ms);
    } catch (e) {
        console.error('startMergedStepAutoPlay error', e);
    }
};

window.stopMergedStepAutoPlay = function () {
    try {
        if (window._mergedStepAutoTimer) {
            clearInterval(window._mergedStepAutoTimer);
            window._mergedStepAutoTimer = null;
            console.log('[AutoPlay] Stopped merged-step auto-play timer.');
        }
    } catch (e) {
        console.error('stopMergedStepAutoPlay error', e);
    }
};

// DOM Elements
const dropZone = document.getElementById('dropZone');
const fileInput = document.getElementById('fileInput');
const fileList = document.getElementById('fileList');
const analyzeBtn = document.getElementById('analyzeBtn');
const uploadSection = document.getElementById('uploadSection');
const loadingSection = document.getElementById('loadingSection');
const resultsSection = document.getElementById('resultsSection');

// Toggle Open Date Day explanation: click point â†’ show box; click same point again â†’ hide
window.showOpenDateExplanation = function (index, nodeEl) {
    const daily = window.openDateDailyData;
    if (!daily || !daily.length) return;
    const placeholder = document.getElementById('open-date-explanation-placeholder');
    const content = document.getElementById('open-date-explanation-content');
    if (!placeholder || !content) return;
    const prev = window.openDateSelectedIndex;
    if (prev === index || index === null) {
        placeholder.style.display = 'block';
        content.style.display = 'none';
        window.openDateSelectedIndex = null;
        document.querySelectorAll('.open-date-node-selected').forEach(n => n.classList.remove('open-date-node-selected'));
        return;
    }
    window.openDateSelectedIndex = index;
    document.querySelectorAll('.open-date-day-node, .open-date-timeline-diagram .timeline-node-start, .open-date-timeline-diagram .timeline-node-end').forEach(n => n.classList.remove('open-date-node-selected'));
    if (nodeEl) nodeEl.classList.add('open-date-node-selected');
    const entry = index === -1 ? daily[0] : index === -2 ? daily[daily.length - 1] : daily[index];
    if (!entry) return;
    const creations = entry.creations || [];
    const multiCusts = entry.multi_create_customers || [];
    let html = '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;"><div style="font-size: 1.15rem; font-weight: 700; color: #0f172a;">… Date: ' + entry.date + '</div><button type="button" onclick="showOpenDateExplanation(null, null); event.stopPropagation();" style="padding: 0.35rem 0.7rem; font-size: 0.8rem; background: #f1f5f9; border: 1px solid #e2e8f0; border-radius: 6px; cursor: pointer; color: #475569;">â† Back</button></div>';
    html += '<div style="font-size: 0.95rem; color: #475569; margin-bottom: 1rem;">' + entry.count + ' account(s) created on this date.</div>';
    if (entry.multi_create_same_day && multiCusts.length) {
        html += '<div style="padding: 0.6rem 1rem; background: rgba(236,72,153,0.15); border-radius: 8px; margin-bottom: 1rem; font-size: 1rem; font-weight: 600; color: #be185d;">âš ï¸ One customer created 2+ accounts this day: ' + multiCusts.join(', ') + '.</div>';
    }
    html += '<div style="display: flex; flex-direction: column; gap: 0.75rem;">';
    creations.forEach(function (cr) {
        const cid = cr.customer_id || '?';
        const time = cr.time_str || '';
        const tod = cr.time_of_day || '';
        const suffix = multiCusts.indexOf(cid) >= 0 ? ' <span style="color: #ec4899; font-size: 0.85rem;">(2+ accounts this day)</span>' : '';
        html += '<div style="padding: 0.85rem 1rem; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; font-size: 1rem; color: #1e293b; line-height: 1.6;"><strong style="color: #0f172a;">Customer ' + cid + '</strong> created account at <strong style="color: #3b82f6;">' + (time || 'â€”') + '</strong> (' + tod + ').' + suffix + '</div>';
    });
    html += '</div>';
    placeholder.style.display = 'none';
    content.style.display = 'block';
    content.innerHTML = html;
};

// Toggle Login Day explanation: click point â†’ show box; click same point again â†’ hide
window.showLoginDayExplanation = function (index, nodeEl) {
    const daily = window.loginDailyData;
    if (!daily || !daily.length) return;
    const placeholder = document.getElementById('login-explanation-placeholder');
    const content = document.getElementById('login-explanation-content');
    if (!placeholder || !content) return;
    const prev = window.loginSelectedIndex;
    if (prev === index || index === null) {
        placeholder.style.display = 'block';
        content.style.display = 'none';
        window.loginSelectedIndex = null;
        document.querySelectorAll('.login-node-selected').forEach(n => n.classList.remove('login-node-selected'));
        return;
    }
    window.loginSelectedIndex = index;
    document.querySelectorAll('.login-day-node, .login-users-diagram .timeline-node-start, .login-users-diagram .timeline-node-end').forEach(n => n.classList.remove('login-node-selected'));
    if (nodeEl) nodeEl.classList.add('login-node-selected');
    const entry = index === -1 ? daily[0] : index === -2 ? daily[daily.length - 1] : daily[index];
    if (!entry) return;
    const logins = entry.logins || [];
    let html = '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;"><div style="font-size: 1.15rem; font-weight: 700; color: #0f172a;">… Date: ' + entry.date + '</div><button type="button" onclick="showLoginDayExplanation(null, null); event.stopPropagation();" style="padding: 0.35rem 0.7rem; font-size: 0.8rem; background: #f1f5f9; border: 1px solid #e2e8f0; border-radius: 6px; cursor: pointer; color: #475569;">â† Back</button></div>';
    html += '<div style="font-size: 0.95rem; color: #475569; margin-bottom: 1rem;">' + entry.login_count + ' login(s). New: ' + entry.new_account_logins + ', Old: ' + entry.old_account_logins + '.</div>';
    if (entry.multi_login_same_day && (entry.multi_login_accounts || []).length) {
        html += '<div style="padding: 0.6rem 1rem; background: rgba(236,72,153,0.15); border-radius: 8px; margin-bottom: 1rem; font-size: 1rem; font-weight: 600; color: #be185d;">âš ï¸ One user logged in 2+ times on this day: ' + entry.multi_login_accounts.join(', ') + '.</div>';
    }
    html += '<div style="display: flex; flex-direction: column; gap: 0.75rem;">';
    const multiAccs = entry.multi_login_accounts || [];
    logins.forEach(function (lg) {
        const acc = lg.account_id || '?';
        const time = lg.time_str || lg.login_at || 'â€”';
        const tod = lg.time_of_day || '';
        const created = lg.created_at || '';
        const isMulti = multiAccs.indexOf(acc) >= 0;
        const suffix = isMulti ? ' <span style="color: #ec4899; font-size: 0.85rem;">(2+ logins this day)</span>' : '';
        html += '<div style="padding: 0.85rem 1rem; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; font-size: 1rem; color: #1e293b; line-height: 1.6;"><strong style="color: #0f172a;">Account ' + acc + '</strong> logged in at <strong style="color: #3b82f6;">' + time + '</strong> (' + tod + '). Account created ' + created + '.' + suffix + '</div>';
    });
    html += '</div>';
    placeholder.style.display = 'none';
    content.style.display = 'block';
    content.innerHTML = html;
};

// Toggle Transaction Day explanation: click point â†’ show box; click same point again â†’ hide, return to placeholder
window.showTxnDayExplanation = function (index, nodeEl) {
    const daily = window.txnDailyData;
    if (!daily || !daily.length) return;
    const placeholder = document.getElementById('txn-explanation-placeholder');
    const content = document.getElementById('txn-explanation-content');
    if (!placeholder || !content) return;
    const prev = window.txnSelectedIndex;
    if (prev === index || index === null) {
        placeholder.style.display = 'block';
        content.style.display = 'none';
        window.txnSelectedIndex = null;
        document.querySelectorAll('.txn-node-selected').forEach(n => n.classList.remove('txn-node-selected'));
        return;
    }
    window.txnSelectedIndex = index;
    document.querySelectorAll('.txn-day-node, .transaction-details-diagram .timeline-node-start, .transaction-details-diagram .timeline-node-end').forEach(n => n.classList.remove('txn-node-selected'));
    if (nodeEl) nodeEl.classList.add('txn-node-selected');
    const entry = index === -1 ? daily[0] : index === -2 ? daily[daily.length - 1] : daily[index];
    if (!entry) return;
    const txns = entry.transactions || [];
    const multiAccs = entry.multi_accounts || [];
    let html = '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;"><div style="font-size: 1.15rem; font-weight: 700; color: #0f172a;">… Date: ' + entry.date + '</div><button type="button" onclick="showTxnDayExplanation(null, null); event.stopPropagation();" style="padding: 0.35rem 0.7rem; font-size: 0.8rem; background: #f1f5f9; border: 1px solid #e2e8f0; border-radius: 6px; cursor: pointer; color: #475569;">â† Back</button></div>';
    html += '<div style="font-size: 0.95rem; color: #475569; margin-bottom: 1rem;">' + entry.transaction_count + ' transaction(s). Credits: ' + entry.credits + ', Debits: ' + entry.debits + ', Refunds: ' + entry.refunds + ', Blocked: ' + entry.declined + '. <strong>PASS: ' + (entry.pass_count || 0) + '</strong> Â· <strong style="color: #dc2626;">FAIL: ' + (entry.fail_count || 0) + '</strong></div>';
    if (entry.multi_user_same_day && multiAccs.length) html += '<div style="padding: 0.6rem 1rem; background: rgba(236,72,153,0.15); border-radius: 8px; margin-bottom: 1rem; font-size: 1rem; font-weight: 600; color: #be185d;">âš ï¸ ' + multiAccs.join(', ') + ' performed 2+ transactions on this day.</div>';
    html += '<div style="display: flex; flex-direction: column; gap: 0.75rem;">';
    txns.forEach(function (t) {
        const status = t.status || 'PASS';
        const statusClr = status === 'FAIL' ? '#dc2626' : '#059669';
        html += '<div style="padding: 0.85rem 1rem; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; font-size: 1rem; color: #1e293b; line-height: 1.6;"><strong style="color: #0f172a;">Account ' + (t.account || '?') + '</strong> at <strong style="color: #f59e0b;">' + (t.time || 'â€”') + '</strong> Â· ' + (t.type || '') + ' ' + (t.amount || '') + ' Â· Balance ' + (t.balance_before || '0') + ' â†’ ' + (t.balance_after || '0') + '.<br><span style="display: inline-block; margin-top: 0.35rem; padding: 0.2rem 0.5rem; border-radius: 6px; font-weight: 600; font-size: 0.9rem; background: ' + (status === 'FAIL' ? 'rgba(220,38,38,0.12)' : 'rgba(5,150,105,0.12)') + '; color: ' + statusClr + ';">Status: ' + status + '</span> â€” ' + (t.status_explanation || (status === 'FAIL' ? 'Transaction declined or blocked' : 'Transaction completed successfully')) + '<br><span style="color: #475569;">' + (t.meaning || '') + '</span></div>';
    });
    html += '</div>';
    placeholder.style.display = 'none';
    content.style.display = 'block';
    content.innerHTML = html;
};

// Toggle explanation (click only) - defined early so column-detect and results pages can use it
window.toggleTimestampExplanation = function (contentId, btnEl) {
    const el = document.getElementById(contentId);
    if (!el || !btnEl) return;
    el.classList.toggle('expanded');
    btnEl.textContent = el.classList.contains('expanded') ? 'Hide explanation' : 'Show explanation';
};

// Toggle timeline entry 6-feature box (for Open Date Timeline diagram)
window.toggleTimelineFeatures = function (contentId) {
    const el = document.getElementById(contentId);
    const btn = document.getElementById(contentId + '-btn');
    if (!el || !btn) return;
    const isHidden = el.style.display === 'none';
    el.style.display = isHidden ? 'block' : 'none';
    btn.textContent = isHidden ? 'Hide 6 features' : 'Show 6 features';
};

// Initialize Event Listeners
document.addEventListener('DOMContentLoaded', () => {
    initializeEventListeners();
});

function initializeEventListeners() {
    fileInput.addEventListener('change', handleFileSelect);
    dropZone.addEventListener('click', () => fileInput.click());
    dropZone.addEventListener('dragover', handleDragOver);
    dropZone.addEventListener('dragleave', handleDragLeave);
    dropZone.addEventListener('drop', handleDrop);
    analyzeBtn.addEventListener('click', analyzeDatabase);
}

// File Selection Handlers
function handleFileSelect(event) {
    const files = Array.from(event.target.files);
    addFiles(files);
}

function handleDragOver(event) {
    event.preventDefault();
    dropZone.classList.add('drag-over');
}

function handleDragLeave(event) {
    event.preventDefault();
    dropZone.classList.remove('drag-over');
}

function handleDrop(event) {
    event.preventDefault();
    dropZone.classList.remove('drag-over');
    const files = Array.from(event.dataTransfer.files);
    const csvFiles = files.filter(file => file.name.endsWith('.csv'));

    if (csvFiles.length !== files.length) {
        alert('Please upload only CSV files');
        return;
    }

    addFiles(csvFiles);
}

function addFiles(files) {
    uploadedFiles = [...uploadedFiles, ...files];
    displayFileList();

    if (uploadedFiles.length > 0) {
        analyzeBtn.style.display = 'flex';
    }
}

function displayFileList() {
    fileList.innerHTML = '';

    uploadedFiles.forEach((file, index) => {
        const fileItem = document.createElement('div');
        fileItem.className = 'file-item';
        fileItem.innerHTML = `
            <div class="file-icon">„</div>
            <div class="file-info">
                <div class="file-name">${file.name}</div>
                <div class="file-size">${formatFileSize(file.size)}</div>
            </div>
            <button onclick="removeFile(${index})" style="background: rgba(255,0,0,0.2); color: #ff6b6b; border: none; padding: 0.5rem 1rem; border-radius: 5px; cursor: pointer;">
                Remove
            </button>
        `;
        fileList.appendChild(fileItem);
    });
}

window.removeFile = function (index) {
    uploadedFiles.splice(index, 1);
    displayFileList();

    if (uploadedFiles.length === 0) {
        analyzeBtn.style.display = 'none';
    }
};

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

// Upload and Analysis
async function analyzeDatabase() {
    if (uploadedFiles.length === 0) {
        alert('Please upload at least one CSV file');
        return;
    }

    try {
        showLoadingSection();
        animateLoadingSteps();

        // Step 1: Upload files
        const formData = new FormData();
        uploadedFiles.forEach(file => {
            formData.append('files', file);
        });

        const uploadResponse = await fetch(`${API_BASE_URL}/upload`, {
            method: 'POST',
            body: formData
        });

        if (!uploadResponse.ok) {
            throw new Error(`Upload failed: ${uploadResponse.status}`);
        }

        const uploadData = await uploadResponse.json();
        sessionId = uploadData.message.split('SESSION_ID:')[1];

        // Step 2: Analyze database
        const analyzeResponse = await fetch(`${API_BASE_URL}/analyze/${sessionId}`, {
            method: 'POST'
        });

        if (!analyzeResponse.ok) {
            throw new Error(`Analysis failed: ${analyzeResponse.status}`);
        }

        const analyzeData = await analyzeResponse.json();

        if (!analyzeData.success) {
            throw new Error(analyzeData.error || 'Analysis failed');
        }

        analysisResults = analyzeData.profiles;

        // Step 3: Show domain split visualization
        showDomainSplitView();

    } catch (error) {
        console.error('Error:', error);
        alert(`Analysis Error: ${error.message}`);
        resetToUpload();
    }
}

// Loading Animation
function showLoadingSection() {
    uploadSection.style.display = 'none';
    loadingSection.style.display = 'flex';
    resultsSection.style.display = 'none';
    updateAppUrl('#loading');
}

function showResultsSection() {
    uploadSection.style.display = 'none';
    loadingSection.style.display = 'none';
    resultsSection.style.display = 'block';
    updateAppUrl('#results');
}

function resetToUpload() {
    uploadSection.style.display = 'block';
    loadingSection.style.display = 'none';
    resultsSection.style.display = 'none';
    updateAppUrl('#upload');
}

// Update URL when page/section changes (no full reload) - URL updates automatically
function updateAppUrl(hash) {
    try {
        const base = window.location.pathname || '/';
        const url = hash ? base + hash : base;
        if (window.location.hash !== (hash || '')) {
            history.pushState({ view: hash || 'upload' }, '', url);
        }
    } catch (e) { }
}

function animateLoadingSteps() {
    const steps = ['step1', 'step2', 'step3', 'step4', 'step5'];
    let currentStep = 0;

    const interval = setInterval(() => {
        if (currentStep < steps.length) {
            document.getElementById(steps[currentStep]).classList.add('active');
            if (currentStep > 0) {
                document.getElementById(steps[currentStep - 1]).classList.remove('active');
                document.getElementById(steps[currentStep - 1]).classList.add('complete');
                document.getElementById(steps[currentStep - 1]).querySelector('.step-icon').textContent = 'âœ“';
            }
            currentStep++;
        } else {
            clearInterval(interval);
        }
    }, 800);
}

// Domain Split Visualization (Multi-DB Support)
function showDomainSplitView() {
    const mainContent = document.getElementById('mainContent');

    if (!analysisResults || analysisResults.length === 0) {
        mainContent.innerHTML = `
            <div style="height: 100%; display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 2rem; text-align: center;">
                <div style="font-size: 4rem; margin-bottom: 1rem;"></div>
                <h2 style="font-size: 1.8rem; margin-bottom: 1rem; color: var(--text-primary);">No Databases Detected</h2>
                <p style="color: var(--text-secondary); margin-bottom: 2rem; max-width: 500px;">
                    We could not detect distinct data groups. Try uploading CSV files with clear table structure.
                </p>
                <button class="btn-secondary" onclick="location.reload()" style="padding: 0.75rem 1.5rem;">â† Try Different Files</button>
            </div>
        `;
        showResultsSection();
        return;
    }

    let htmlContent = `
        <div style="height: 100%; display: flex; flex-direction: column; align-items: center; padding: 2rem; overflow-y: auto;">
            <h2 style="font-size: 2.5rem; margin-bottom: 2rem; color: var(--text-primary); text-align: center; font-weight: 700;">¦ Detected Databases</h2>
            <p style="color: var(--text-secondary); margin-bottom: 3rem; max-width: 700px; text-align: center; font-size: 1.1rem;">
                We analyzed your files and detected <strong>${analysisResults.length}</strong> distinct data group(s).
            </p>
            
            <div style="width: 100%; max-width: 1200px; display: flex; flex-direction: column; gap: 4rem;">
    `;

    // Iterate over each profile
    analysisResults.forEach((profile, index) => {
        const domainData = profile.domain_analysis;
        const profileId = `profile-${index}`;
        const bankingPct = domainData?.percentages?.Banking ?? 0;
        const financePct = domainData?.percentages?.Finance ?? 0;
        const insurancePct = domainData?.percentages?.Insurance ?? 0;
        const healthcarePct = domainData?.percentages?.Healthcare ?? 0;
        const retailPct = domainData?.percentages?.Retail ?? 0;
        const otherPct = domainData?.percentages?.Other ?? 0;

        // Determine primary domain
        const primaryDomain = domainData?.primary_domain || 'Other';
        const isBanking = primaryDomain === 'Banking';
        const isFinance = primaryDomain === 'Finance';
        const isInsurance = primaryDomain === 'Insurance';
        const isHealthcare = primaryDomain === 'Healthcare';
        const isRetail = primaryDomain === 'Retail';

        // Set card colors based on primary domain
        const cardColor = isBanking
            ? '#0F766E'
            : (isFinance
                ? '#4F46E5'
                : (isInsurance
                    ? '#7C3AED'
                    : (isHealthcare
                        ? '#14B8A6'
                        : (isRetail ? '#F59E0B' : '#64748B'))));
        const cardLabel = isBanking
            ? ` ${profile.database_name}`
            : (isFinance
                ? ` Finance Database ${index + 1}`
                : (isInsurance
                    ? ` Insurance Database ${index + 1}`
                    : (isHealthcare
                        ? ` Healthcare Database ${index + 1}`
                        : (isRetail
                            ? ` Retail Database ${index + 1}`
                            : ` Database ${index + 1}: General / Mixed`))));

        if (!domainData || !domainData.chart_data) return;

        htmlContent += `
            <div style="background: var(--bg-card); border: 1px solid var(--border); border-radius: 20px; padding: 2rem; position: relative; box-shadow: var(--shadow-sm);">
                <div style="position: absolute; top: -15px; left: 20px; background: ${cardColor}; color: white; padding: 5px 15px; border-radius: 12px; font-weight: 600; font-size: 0.9rem;">
                    ${cardLabel}
                </div>
                
                <p style="color: var(--text-secondary); margin-top: 1rem; margin-bottom: 2rem; text-align: center;">
                    ${domainData.explanation}
                </p>

                <div style="display: grid; grid-template-columns: 300px 1fr; gap: 3rem; align-items: center;">
                    <!-- Pie Chart -->
                    <div style="position: relative; height: 300px; width: 300px; margin: 0 auto;">
                        <canvas id="chart-${index}" width="300" height="300"></canvas>
                    </div>

                    <!-- Details -->
                    <div>
                        <h3 style="margin-bottom: 1.5rem; color: var(--text-muted); font-size: 1.1rem;">Domain Breakdown</h3>
                        <div style="display: flex; flex-direction: column; gap: 1rem;">
                            <!-- Banking -->
                            <div style="display: flex; align-items: center; gap: 1rem; padding: 1rem; background: ${isBanking ? 'var(--accent-primary-light)' : 'var(--bg-page)'}; border-radius: 12px; border: 1px solid ${isBanking ? 'rgba(15, 118, 110, 0.2)' : 'var(--border)'};">
                                <div style="width: 24px; height: 24px; background: #0F766E; border-radius: 6px;"></div>
                                <div style="flex: 1;">
                                    <div style="font-weight: 600; font-size: 1rem; color: var(--text-primary);">Banking Domain</div>
                                    <div style="color: var(--text-muted); font-size: 0.85rem;">Customer accounts, transactions</div>
                                </div>
                                <div style="font-size: 1.5rem; font-weight: 700; color: ${isBanking ? 'var(--accent-primary-dark)' : 'var(--text-muted)'};">
                                    ${bankingPct}%
                                </div>
                            </div>

                            <!-- Finance -->
                            <div style="display: flex; align-items: center; gap: 1rem; padding: 1rem; background: ${isFinance ? 'rgba(79, 70, 229, 0.1)' : 'var(--bg-page)'}; border-radius: 12px; border: 1px solid ${isFinance ? 'rgba(79, 70, 229, 0.3)' : 'var(--border)'};">
                                <div style="width: 24px; height: 24px; background: #4F46E5; border-radius: 6px;"></div>
                                <div style="flex: 1;">
                                    <div style="font-weight: 600; font-size: 1rem; color: var(--text-primary);">Finance Domain</div>
                                    <div style="color: var(--text-muted); font-size: 0.85rem;">Invoices, GST, payroll, expenses</div>
                                </div>
                                <div style="font-size: 1.5rem; font-weight: 700; color: ${isFinance ? '#4F46E5' : 'var(--text-muted)'};">
                                    ${financePct}%
                                </div>
                            </div>

                            <!-- Insurance -->
                            <div style="display: flex; align-items: center; gap: 1rem; padding: 1rem; background: ${isInsurance ? 'rgba(124, 58, 237, 0.1)' : 'var(--bg-page)'}; border-radius: 12px; border: 1px solid ${isInsurance ? 'rgba(124, 58, 237, 0.3)' : 'var(--border)'};">
                                <div style="width: 24px; height: 24px; background: #7C3AED; border-radius: 6px;"></div>
                                <div style="flex: 1;">
                                    <div style="font-weight: 600; font-size: 1rem; color: var(--text-primary);">Insurance Domain</div>
                                    <div style="color: var(--text-muted); font-size: 0.85rem;">Policies, claims, premiums</div>
                                </div>
                                <div style="font-size: 1.5rem; font-weight: 700; color: ${isInsurance ? '#7C3AED' : 'var(--text-muted)'};">
                                    ${insurancePct}%
                                </div>
                            </div>

                            <!-- Healthcare -->
                            <div style="display: flex; align-items: center; gap: 1rem; padding: 1rem; background: ${isHealthcare ? 'rgba(20, 184, 166, 0.1)' : 'var(--bg-page)'}; border-radius: 12px; border: 1px solid ${isHealthcare ? 'rgba(20, 184, 166, 0.3)' : 'var(--border)'};">
                                <div style="width: 24px; height: 24px; background: #14B8A6; border-radius: 6px;"></div>
                                <div style="flex: 1;">
                                    <div style="font-weight: 600; font-size: 1rem; color: var(--text-primary);">Healthcare Domain</div>
                                    <div style="color: var(--text-muted); font-size: 0.85rem;">Patient records, treatments</div>
                                </div>
                                <div style="font-size: 1.5rem; font-weight: 700; color: ${isHealthcare ? '#14B8A6' : 'var(--text-muted)'};">
                                    ${healthcarePct}%
                                </div>
                            </div>

                            <!-- Retail -->
                            <div style="display: flex; align-items: center; gap: 1rem; padding: 1rem; background: ${isRetail ? 'rgba(245, 158, 11, 0.1)' : 'var(--bg-page)'}; border-radius: 12px; border: 1px solid ${isRetail ? 'rgba(245, 158, 11, 0.3)' : 'var(--border)'};">
                                <div style="width: 24px; height: 24px; background: #F59E0B; border-radius: 6px;"></div>
                                <div style="flex: 1;">
                                    <div style="font-weight: 600; font-size: 1rem; color: var(--text-primary);">Retail Domain</div>
                                    <div style="color: var(--text-muted); font-size: 0.85rem;">Sales / POS / invoice data</div>
                                </div>
                                <div style="font-size: 1.5rem; font-weight: 700; color: ${isRetail ? '#F59E0B' : 'var(--text-muted)'};">
                                    ${retailPct}%
                                </div>
                            </div>

                            <!-- Other -->
                            <div style="display: flex; align-items: center; gap: 1rem; padding: 1rem; background: var(--bg-page); border-radius: 12px; border: 1px solid var(--border);">
                                <div style="width: 24px; height: 24px; background: var(--text-muted); border-radius: 6px;"></div>
                                <div style="flex: 1;">
                                    <div style="font-weight: 600; font-size: 1rem; color: var(--text-primary);">Other Domain</div>
                                    <div style="color: var(--text-muted); font-size: 0.85rem;">Unclassified data</div>
                                </div>
                                <div style="font-size: 1.5rem; font-weight: 700; color: var(--text-muted);">
                                    ${otherPct}%
                                </div>
                            </div>
                        </div>

                         <div style="margin-top: 1.5rem;">
                            <h4 style="font-size: 0.9rem; color: var(--text-muted); margin-bottom: 0.5rem;">Structure &amp; Connections</h4>
                            <button type="button" class="explanation-trigger" onclick="toggleExplanation('expl-${index}')" id="btn-expl-${index}">
                                Show explanation
                            </button>
                            <div class="explanation-content" id="expl-${index}" style="max-height: 200px; overflow-y: auto;">
                                ${profile.database_explanation || 'No explanation available.'}
                            </div>
                        </div>

                        <!-- Analyze Button (Per Database) -->
                        <div style="margin-top: 2rem;">
                            <button 
                                type="button" 
                                class="btn-primary" 
                                id="analyze-btn-${index}"
                                onclick="startDatabaseAnalysis(${index})"
                                style="width: 100%; padding: 1rem 2rem; font-size: 1.1rem; position: relative;">
                                ${isBanking
                ? '🏦 Analyze Banking Data'
                : (isFinance
                    ? '💰 Analyze Finance Data'
                    : (isInsurance
                        ? '🛡️ Analyze Insurance Data'
                        : (isHealthcare
                            ? '🏥 Analyze Healthcare Data'
                            : (isRetail
                                ? '🛒 Analyze Retail Data'
                                : '📊 Analyze Data'))))
            } →
                            </button>
                            <div id="analyze-status-${index}" style="margin-top: 0.5rem; text-align: center; color: var(--text-muted); font-size: 0.9rem; display: none;">
                                Processing...
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    });

    htmlContent += `
            </div>
        </div>
    `;

    mainContent.innerHTML = htmlContent;
    showResultsSection();
    updateAppUrl('#databases');

    window.toggleExplanation = function (id) {
        const el = document.getElementById(id);
        const btn = document.getElementById('btn-' + id);
        if (!el || !btn) return;
        el.classList.toggle('expanded');
        btn.textContent = el.classList.contains('expanded') ? 'Hide explanation' : 'Show explanation';
    };

    // Render charts
    setTimeout(() => {
        analysisResults.forEach((profile, index) => {
            const domainData = profile.domain_analysis;
            if (!domainData || !domainData.chart_data) return;

            const ctx = document.getElementById(`chart-${index}`).getContext('2d');
            new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: domainData.chart_data.labels,
                    datasets: [{
                        data: domainData.chart_data.values,
                        backgroundColor: domainData.chart_data.colors,
                        borderWidth: 2,
                        borderColor: '#1e293b'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label: function (context) {
                                    return context.label + ': ' + context.parsed + '%';
                                }
                            }
                        }
                    }
                }
            });
        });
    }, 100);
}

// New function to start analysis for a specific database
window.startDatabaseAnalysis = async function (profileIndex) {
    const profile = analysisResults[profileIndex];
    const btn = document.getElementById(`analyze-btn-${profileIndex}`);
    const statusDiv = document.getElementById(`analyze-status-${profileIndex}`);

    if (!profile) {
        alert('Database profile not found');
        return;
    }

    // Disable button and show processing state
    btn.disabled = true;
    btn.style.opacity = '0.6';
    btn.style.cursor = 'not-allowed';
    statusDiv.style.display = 'block';
    statusDiv.innerHTML = '<div class="loading-spinner" style="width: 20px; height: 20px; margin: 0 auto;"></div> Processing analysis...';

    // Determine domain type
    const domainData = profile.domain_analysis;
    const primaryDomain = domainData?.primary_domain || 'Other';

    try {
        if (primaryDomain === 'Finance') {
            statusDiv.style.display = 'none';
            statusDiv.innerHTML = '';
            showFinanceAnalysisResults(profile);
            return;
        }
        if (primaryDomain === 'Banking') {
            showBankingAnalysisResults(profile);
        } else if (primaryDomain === 'Retail') {
            showRetailAnalysisResults(profile);
        } else if (primaryDomain === 'Healthcare') {
            // Show healthcare analysis with new diagram format
            showHealthcareAnalysisResults(profile);
        } else if (primaryDomain === 'Insurance') {
            showInsuranceAnalysisResults(profile);
        } else {
            statusDiv.style.display = 'block';
            statusDiv.style.color = 'var(--text-muted)';
            statusDiv.innerHTML = 'Domain is General/Other. For case timelines, use Banking, Finance, Insurance, Healthcare, or Retail data.';
            btn.disabled = false;
            btn.style.opacity = '1';
            btn.style.cursor = 'pointer';
        }
    } catch (error) {
        console.error('Analysis error:', error);
        statusDiv.style.display = 'block';
        statusDiv.style.color = '#ef4444';
        statusDiv.textContent = `Error: ${error.message}`;
        btn.disabled = false;
        btn.style.opacity = '1';
        btn.style.cursor = 'pointer';
    }
};

// Banking Analysis Results - Case ID per User (Sessions)
function showBankingAnalysisResults(profile) {
    const mainContent = document.getElementById('mainContent');
    const bkData = profile.banking_analysis;

    if (!bkData || !bkData.success) {
        mainContent.innerHTML = `
            <div style="max-width: 700px; margin: 0 auto; padding: 3rem; text-align: center;">
                <div style="font-size: 4rem; margin-bottom: 1.5rem;">¦</div>
                <h2 style="font-size: 2rem; margin-bottom: 1rem; color: #0F766E;">Banking Database Detected</h2>
                <p style="color: var(--text-secondary); margin-bottom: 2rem;">
                    ${bkData?.error || 'No activities found. We look for login time, logout time, created time, or open time in your files.'}
                </p>
                <button class="btn-secondary" onclick="showDomainSplitView()">â† Back to Database List</button>
            </div>
        `;
        return;
    }

    // --- Column-level Event Blueprint (uses backend event_columns from banking_analyzer) ---
    function buildEventBlueprintFromBackend(eventColumns, caseDetails) {
        const eventOrder = ['account_open', 'login', 'deposit', 'withdraw', 'refund', 'failed', 'logout', 'check_balance'];
        const eventLabels = {
            account_open: 'Created Account',
            login: 'Login',
            logout: 'Logout',
            deposit: 'Deposit / Credit',
            withdraw: 'Withdrawal Transaction',
            refund: 'Refund',
            failed: 'Failed / Declined',
            check_balance: 'Check Balance'
        };

        const eventMap = eventColumns || {};

        // If we see real logout steps in the Case ID data but the backend
        // did not report any explicit logout columns (rare edge case),
        // synthesize a simple placeholder so the Logout box still appears
        // in the diagram. This keeps the diagram 100% driven by dynamic
        // events, but never hides Logout when it is present in sessions.
        const hasLogoutEvents = Array.isArray(caseDetails)
            ? caseDetails.some(c => (c.event_sequence || []).includes('logout'))
            : false;
        if (hasLogoutEvents && (!eventMap.logout || eventMap.logout.length === 0)) {
            eventMap.logout = eventMap.logout || [];
            if (!eventMap.logout.includes('Sessions.logout_time')) {
                eventMap.logout.push('Sessions.logout_time');
            }
        }
        const hasAnyEvent = Object.keys(eventMap).some((k) => (eventMap[k] || []).length > 0);
        if (!hasAnyEvent) {
            // Count total detected events to show in message
            const totalEventTypes = Object.keys(eventMap).length;
            return `
                <section style="margin-bottom: 2rem;">
                    <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);">
                        📋 Event Columns Blueprint
                    </h2>
                    <div style="background: rgba(251,191,36,0.1); border-left: 4px solid #f59e0b; border-radius: 12px; padding: 1.5rem; margin-bottom: 1rem;">
                        <div style="font-size: 1.1rem; font-weight: 600; color: #d97706; margin-bottom: 0.75rem;">
                            ⚠️ No Banking Event Columns Detected
                        </div>
                        <p style="color: var(--text-primary); margin-bottom: 1rem; font-size: 0.95rem; line-height: 1.6;">
                            We could not find columns that look like banking events (login, logout, deposit, withdraw, refund, failed, or account opened) from your uploaded tables.
                        </p>
                        <p style="color: var(--text-muted); margin-bottom: 1rem; font-size: 0.9rem; line-height: 1.6;">
                            The diagram will appear when your files have columns with these patterns:
                        </p>
                        <div style="background: white; border-radius: 8px; padding: 1rem; margin-bottom: 1rem;">
                            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 0.75rem; font-size: 0.85rem;">
                                <div>
                                    <strong style="color: #059669;">✓ Login:</strong> 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">login_time</code>, 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">login_date</code>
                                </div>
                                <div>
                                    <strong style="color: #dc2626;">✓ Logout:</strong> 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">logout_time</code>, 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">logout_date</code>
                                </div>
                                <div>
                                    <strong style="color: #0ea5e9;">✓ Account Open:</strong> 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">open_date</code>, 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">created_at</code>
                                </div>
                                <div>
                                    <strong style="color: #22c55e;">✓ Deposit/Credit:</strong> 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">deposit_date</code>, 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">credit_time</code>, 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">credit_amount</code>
                                </div>
                                <div>
                                    <strong style="color: #f59e0b;">✓ Withdraw/Debit:</strong> 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">withdraw_date</code>, 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">debit_time</code>, 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">withdrawal_amount</code>
                                </div>
                                <div>
                                    <strong style="color: #8b5cf6;">✓ Refund:</strong> 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">refund_date</code>, 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">refund_timestamp</code>
                                </div>
                                <div>
                                    <strong style="color: #ef4444;">✓ Failed/Status:</strong> 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">status</code>, 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">failed</code>, 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">declined</code>
                                </div>
                                <div>
                                    <strong style="color: #6366f1;">✓ Event Type:</strong> 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">event</code>, 
                                    <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">transaction_type</code>
                                </div>
                            </div>
                        </div>
                        <p style="color: var(--text-muted); font-size: 0.85rem; line-height: 1.5;">
                            <strong>💡 Tip:</strong> Column names are flexible! We look for keywords like <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">login</code>, <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">deposit</code>, <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">withdraw</code> combined with <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">date</code>, <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">time</code>, or <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">timestamp</code>. Amount columns (like <code style="background: #f1f5f9; padding: 0.15rem 0.35rem; border-radius: 4px;">credit_amount</code>) are also detected!
                        </p>
                    </div>
                </section>
            `;
        }

        function renderColumns(evType) {
            const cols = eventMap[evType] || [];
            if (!cols.length) return '<div style="font-size: 0.78rem; color: #e5e7eb; opacity: 0.7;">Not detected</div>';

            // Extract just the column name (remove "Table." prefix if present)
            const columnNames = cols.map(c => {
                const parts = c.split('.');
                return parts.length > 1 ? parts[1] : c; // If format is "Table.Column", take Column only
            });

            // Show up to 3 columns, then "+X more"
            const first = columnNames.slice(0, 3);
            const extra = columnNames.length - first.length;

            let inner = first
                .map((c) => `<div style="font-size: 0.78rem; color: #e5e7eb; text-overflow: ellipsis; overflow: hidden; white-space: nowrap;" title="${c}">${c}</div>`)
                .join('');
            if (extra > 0) {
                inner += `<div style="font-size: 0.72rem; color: #cbd5f5; margin-top: 0.15rem;">+${extra} more</div>`;
            }
            return inner;
        }

        function has(evType) {
            return (eventMap[evType] || []).length > 0;
        }

        // Count detected event types for summary
        const detectedCount = Object.keys(eventMap).filter(k => (eventMap[k] || []).length > 0).length;
        const totalEventTypes = 8; // Total possible event types

        // Layout similar to the provided blueprint image
        let html = `
            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.8rem; margin-bottom: 1rem; color: var(--text-primary); text-align: center;">
                    📊 Banking Event Blueprint
                </h2>
                
                <!-- Detection Summary -->
                <div style="background: linear-gradient(135deg, rgba(15,118,110,0.08), rgba(13,92,84,0.06)); border: 1px solid rgba(15,118,110,0.3); border-radius: 12px; padding: 1rem; margin-bottom: 1.5rem; text-align: center;">
                    <div style="font-size: 1rem; color: var(--text-primary); margin-bottom: 0.5rem;">
                        <strong style="font-size: 1.4rem; color: #0F766E;">${detectedCount}</strong> out of <strong>${totalEventTypes}</strong> event types detected
                    </div>
                    <div style="font-size: 0.85rem; color: var(--text-muted);">
                        ${detectedCount === totalEventTypes ? '✅ All event types found! Your banking data is fully mapped.' :
                detectedCount >= 4 ? '✓ Good coverage! Some event types are missing but the main flow is detected.' :
                    detectedCount >= 1 ? '⚠️ Partial detection. Consider adding more event-related columns for complete analysis.' :
                        '❌ No events detected. See the guide above for expected column names.'}
                    </div>
                </div>

                <p style="color: var(--text-muted); margin-bottom: 1.5rem; font-size: 0.98rem; text-align: center;">
                    This blueprint is a visual demo built only from column names. Each box is one event type. Inside each box we list the matching table columns (Table.Column) that look like that event.
                </p>
                <div style="background: #f9fafb; border-radius: 20px; padding: 1.75rem; border: 1px solid #e5e7eb;">
                    <div style="display: flex; flex-direction: column; align-items: center; gap: 1.8rem;">

                        <!-- Top row: Start / Created Account -->
                        <div style="display: flex; justify-content: center; gap: 6rem; width: 100%;">
                            <div style="display: flex; flex-direction: column; align-items: center; gap: 0.35rem;">
                                <div style="font-size: 0.85rem; color: #6b7280;">Start</div>
                                <div style="min-width: 120px; padding: 0.6rem 0.9rem; border-radius: 999px; background: #1d4ed8; color: white; text-align: center; font-weight: 600;">
                                    Process
                                </div>
                            </div>
                            ${has('account_open') ? `
                            <div style="display: flex; flex-direction: column; align-items: center; gap: 0.35rem;">
                                <div style="font-size: 0.85rem; color: #6b7280;">Account</div>
                                <div style="min-width: 150px; padding: 0.6rem 0.9rem; border-radius: 999px; background: #1d4ed8; color: white; text-align: center; font-weight: 600;">
                                    ${eventLabels.account_open}
                                </div>
                                <div style="margin-top: 0.35rem; padding: 0.4rem 0.6rem; border-radius: 10px; background: #eff6ff; border: 1px solid #dbeafe; max-width: 260px;">
                                    ${renderColumns('account_open')}
                                </div>
                            </div>` : ''}
                        </div>

                        <!-- Center login -->
                        ${has('login') ? `
                        <div style="display: flex; flex-direction: column; align-items: center; gap: 0.35rem;">
                            <div style="min-width: 130px; padding: 0.6rem 0.9rem; border-radius: 16px; background: #1d4ed8; color: white; text-align: center; font-weight: 600;">
                                ${eventLabels.login}
                            </div>
                            <div style="margin-top: 0.35rem; padding: 0.4rem 0.6rem; border-radius: 10px; background: #eff6ff; border: 1px solid #dbeafe; max-width: 260px;">
                                ${renderColumns('login')}
                            </div>
                        </div>` : ''}

                        <!-- Middle row: left Credit/Deposit, center transaction column, right Logout -->
                        <div style="display: flex; justify-content: center; gap: 4rem; width: 100%; align-items: flex-start;">

                            <!-- Left: Credit / Deposit -->
                            <div style="display: flex; flex-direction: column; gap: 0.75rem; align-items: center;">
                                ${has('deposit') ? `
                                <div style="min-width: 130px; padding: 0.6rem 0.9rem; border-radius: 16px; background: #1d4ed8; color: white; text-align: center; font-weight: 600;">
                                    Credit / Deposit
                                </div>
                                <div style="margin-top: 0.15rem; padding: 0.4rem 0.6rem; border-radius: 10px; background: #eff6ff; border: 1px solid #dbeafe; max-width: 260px;">
                                    ${renderColumns('deposit')}
                                </div>` : ''}
                            </div>

                            <!-- Center: Withdrawal / Credit / Refund / Check Balance -->
                            <div style="display: flex; flex-direction: column; gap: 0.75rem; align-items: center;">
                                ${has('withdraw') ? `
                                <div style="min-width: 180px; padding: 0.55rem 1rem; border-radius: 18px; background: #0f766e; color: white; text-align: center; font-weight: 600;">
                                    ${eventLabels.withdraw}
                                </div>
                                <div style="margin-top: 0.15rem; padding: 0.4rem 0.6rem; border-radius: 10px; background: #ecfdf3; border: 1px solid #bbf7d0; max-width: 280px;">
                                    ${renderColumns('withdraw')}
                                </div>` : ''}

                                ${has('deposit') ? `
                                <div style="min-width: 140px; padding: 0.55rem 1rem; border-radius: 18px; background: #0f766e; color: white; text-align: center; font-weight: 600;">
                                    Credit
                                </div>
                                <div style="margin-top: 0.15rem; padding: 0.4rem 0.6rem; border-radius: 10px; background: #ecfdf3; border: 1px solid #bbf7d0; max-width: 280px;">
                                    ${renderColumns('deposit')}
                                </div>` : ''}

                                ${has('refund') ? `
                                <div style="min-width: 140px; padding: 0.55rem 1rem; border-radius: 18px; background: #0f766e; color: white; text-align: center; font-weight: 600;">
                                    ${eventLabels.refund}
                                </div>
                                <div style="margin-top: 0.15rem; padding: 0.4rem 0.6rem; border-radius: 10px; background: #ecfdf3; border: 1px solid #bbf7d0; max-width: 280px;">
                                    ${renderColumns('refund')}
                                </div>` : ''}

                                ${has('check_balance') ? `
                                <div style="min-width: 160px; padding: 0.55rem 1rem; border-radius: 18px; background: #0f766e; color: white; text-align: center; font-weight: 600;">
                                    ${eventLabels.check_balance}
                                </div>
                                <div style="margin-top: 0.15rem; padding: 0.4rem 0.6rem; border-radius: 10px; background: #ecfdf3; border: 1px solid #bbf7d0; max-width: 280px;">
                                    ${renderColumns('check_balance')}
                                </div>` : ''}

                                ${has('failed') ? `
                                <div style="min-width: 160px; padding: 0.55rem 1rem; border-radius: 18px; background: #b91c1c; color: white; text-align: center; font-weight: 600;">
                                    ${eventLabels.failed}
                                </div>
                                <div style="margin-top: 0.15rem; padding: 0.4rem 0.6rem; border-radius: 10px; background: #fef2f2; border: 1px solid #fecaca; max-width: 280px;">
                                    ${renderColumns('failed')}
                                </div>` : ''}
                            </div>

                            <!-- Right: Logout and End -->
                            <div style="display: flex; flex-direction: column; gap: 1rem; align-items: center;">
                                ${has('logout') ? `
                                <div style="min-width: 120px; padding: 0.6rem 0.9rem; border-radius: 16px; background: #1d4ed8; color: white; text-align: center; font-weight: 600;">
                                    ${eventLabels.logout}
                                </div>
                                <div style="margin-top: 0.15rem; padding: 0.4rem 0.6rem; border-radius: 10px; background: #eff6ff; border: 1px solid #dbeafe; max-width: 260px;">
                                    ${renderColumns('logout')}
                                </div>` : ''}

                                <div style="margin-top: 1.2rem; min-width: 110px; padding: 0.55rem 0.9rem; border-radius: 999px; border: 2px solid #1d4ed8; color: #1d4ed8; text-align: center; font-weight: 600;">
                                    End
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </section>
        `;

        return html;
    }

    // Use global Sankey diagram (delegate to window.renderUnifiedCaseFlowDiagram)
    function renderUnifiedCaseFlowDiagram(flowData) {
        return window.renderUnifiedCaseFlowDiagram ? window.renderUnifiedCaseFlowDiagram(flowData) : '';
    }
    function _renderUnifiedCaseFlowDiagramOld(flowData) {
        if (!flowData || !flowData.case_paths || flowData.case_paths.length === 0) {
            return '';
        }

        const eventTypes = flowData.all_event_types || [];
        const casePaths = flowData.case_paths || [];
        // Ensure each Case ID has a distinct color (backend may or may not send colors)
        const CASE_COLOR_PALETTE = [
            '#ef4444', '#3b82f6', '#10b981', '#f97316', '#8b5cf6',
            '#ec4899', '#22c55e', '#0ea5e9', '#eab308', '#6366f1',
            '#14b8a6', '#f43f5e', '#a855f7', '#84cc16', '#06b6d4'
        ];
        casePaths.forEach((p, idx) => {
            if (!p.color) {
                p.color = CASE_COLOR_PALETTE[idx % CASE_COLOR_PALETTE.length];
            }
        });
        const totalCases = flowData.total_cases || 0;

        // Build legend
        let legendHTML = '<div style="display: flex; flex-wrap: wrap; gap: 1rem; margin-bottom: 1rem;">';
        casePaths.forEach((path, idx) => {
            legendHTML += `
                <div style="display: flex; align-items: center; gap: 0.4rem;">
                    <div style="width: 24px; height: 3px; background: ${path.color}; border-radius: 2px;"></div>
                    <span style="font-size: 0.85rem; color: var(--text-primary); font-weight: 600;">Case ${String(path.case_id).padStart(3, '0')}</span>
                </div>
            `;
        });
        legendHTML += '</div>';

        // Create event boxes layout (pan wrapper added after svgWidth/svgHeight are computed)
        let diagramHTML = '';

        // Standard Banking Flow Layout (Fixed positions for "Neat" look)
        // Coordinates: x, y
        const fixedPositions = {
            'Process': { x: 50, y: 150 },
            'Created Account': { x: 400, y: 150 },
            'Account Open': { x: 400, y: 150 },
            'Login': { x: 750, y: 150 },
            'Check Balance': { x: 1100, y: 150 },          // New event
            'Balance Inquiry': { x: 1100, y: 150 },        // Alias
            'Withdrawal Transaction': { x: 1450, y: 150 }, // Shifted right
            'Credit': { x: 400, y: 450 },                  // Bottom row start
            'Deposit': { x: 400, y: 450 },
            'Logout': { x: 750, y: 450 },
            'End': { x: 1450, y: 450 }                     // Shifted right
        };

        const boxWidth = 160;
        const boxHeight = 70; // Slightly shorter boxes

        // Calculate positions
        const eventPositions = {};

        // Dynamic placement for events not in fixedPositions
        let dynamicX = 50;
        let dynamicY = 350; // Start looking in bottom row
        const takenPositions = Object.values(fixedPositions).map(p => `${p.x},${p.y}`);

        eventTypes.forEach(event => {
            if (fixedPositions[event]) {
                eventPositions[event] = fixedPositions[event];
            } else if (fixedPositions[event.replace('Transaction', '').trim()]) {
                // Fuzzy match for 'Withdrawal' vs 'Withdrawal Transaction' if needed
                eventPositions[event] = fixedPositions[event.replace('Transaction', '').trim()];
            } else {
                // Find a spot
                let placed = false;
                // Try to place in empty spots on a simple grid
                for (let r = 0; r < 3; r++) {
                    for (let c = 0; c < 5; c++) {
                        const tx = 50 + c * 350; // Wider spacing
                        const ty = 150 + r * 300; // Taller spacing
                        const key = `${tx},${ty}`;
                        if (!takenPositions.includes(key) && !placed) {
                            eventPositions[event] = { x: tx, y: ty };
                            takenPositions.push(key);
                            placed = true;
                        }
                    }
                }
                // Fallback if grid full
                if (!placed) {
                    eventPositions[event] = { x: dynamicX, y: 750 };
                    dynamicX += 350;
                }
            }
        });

        // SVG for paths
        // Calculate bounding box
        const maxX = Math.max(...Object.values(eventPositions).map(p => p.x)) + 250;
        const maxY = Math.max(...Object.values(eventPositions).map(p => p.y)) + 150;
        const svgWidth = Math.max(900, maxX);
        const svgHeight = Math.max(500, maxY);

        // Outer container (scroll) + pan wrapper (hit area) + pan content (SVG + boxes move together)
        diagramHTML += '<div id="diagram-outer-container" style="position: relative; min-height: 500px; max-height: 75vh; padding: 1rem; overflow: auto;">';
        diagramHTML += '<div id="diagram-pan-wrapper" style="position: absolute; left: 0; top: 0; width: ' + svgWidth + 'px; height: ' + svgHeight + 'px; cursor: grab; z-index: 0; user-select: none;" onmousedown="startDiagramPan(event)">';
        diagramHTML += '<div id="diagram-pan-content" style="position: absolute; left: 0; top: 0; width: ' + svgWidth + 'px; height: ' + svgHeight + 'px; will-change: transform;">';

        diagramHTML += `<svg id="diagram-svg-layer" width="${svgWidth}" height="${svgHeight}" style="position: absolute; top: 0; left: 0; z-index: 1; transform-origin: 0 0;">
            <defs>
                <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
                <polygon points="0 0, 10 3.5, 0 7" fill="#64748b" opacity="0.8" />
                </marker>
                <marker id="same-time-arrow" markerWidth="8" markerHeight="5" refX="7" refY="2.5" orient="auto">
                <polygon points="0 0, 8 2.5, 0 5" fill="#64748b" opacity="0.9" />
                </marker>
            </defs>
            <g id="diagram-paths-container"></g>
        `;

        // Generate markers for all cases (Must be outside the disabled loop)
        casePaths.forEach(path => {
            diagramHTML += `
                <defs>
                    <marker id="arrow-${path.case_id}" markerWidth="8" markerHeight="6" 
                    refX="7" refY="3" orient="auto">
                    <polygon points="0 0, 8 3, 0 6" fill="${path.color}" />
                    </marker>
                </defs>
            `;
        });

        // Initialize Global State
        const sequences = casePaths.map(p => ({
            case_id: p.case_id,
            sequence: p.path_sequence,
            color: p.color
        }));
        const timingMap = {};
        casePaths.forEach((p, idx) => { timingMap[idx] = p.timings || {}; });

        window.diagramState.positions = eventPositions;
        window.diagramState.paths = sequences;
        window.diagramState.timings = timingMap;
        window.diagramState.boxWidth = boxWidth;
        window.diagramState.boxHeight = boxHeight;
        window.diagramState.same_time_groups = flowData.same_time_groups || [];
        // For the merged diagram, hide the per-segment time/date boxes (only show colored paths + Case IDs).
        window.diagramState.showTimeLabels = false;
        // Reset any previous case visibility filter when a new merged diagram is rendered.
        window.diagramState.visibleCaseIds = null;
        if (window.diagramPan) { window.diagramPan.translateX = 0; window.diagramPan.translateY = 0; }

        setTimeout(() => window.renderDiagramPaths(), 100);

        // Legacy loop disabled
        if (false) {
            // Draw paths for each case
            casePaths.forEach((path, pathIdx) => {
                const sequence = path.path_sequence;
                const timings = path.timings;
                const color = path.color;

                // Define specific marker for this color
                diagramHTML += `
                <defs>
                    <marker id="arrow-${path.case_id}" markerWidth="8" markerHeight="6" 
                    refX="7" refY="3" orient="auto">
                    <polygon points="0 0, 8 3, 0 6" fill="${color}" />
                    </marker>
                </defs>
            `;

                for (let i = 0; i < sequence.length - 1; i++) {
                    const fromEvent = sequence[i];
                    const toEvent = sequence[i + 1];

                    const fromPos = eventPositions[fromEvent];
                    const toPos = eventPositions[toEvent];

                    if (!fromPos || !toPos) continue;

                    // Center points
                    const x1 = fromPos.x + boxWidth / 2;
                    const y1 = fromPos.y + boxHeight / 2;
                    const x2 = toPos.x + boxWidth / 2;
                    const y2 = toPos.y + boxHeight / 2;

                    // Adjust start/end points to be on the edge of the box
                    // Simple approx: closest box edge
                    // Actually, let's just use center-to-center but mask lines behind boxes? 
                    // No, better to draw to edges.
                    // Let's stick to simple center-to-center with logic for offset.

                    // Calculate offset to separate multiple lines
                    // Spread lines by 4px
                    const totalInDiagram = casePaths.length;
                    const offsetStep = 6;
                    const offset = (pathIdx - (totalInDiagram - 1) / 2) * offsetStep;

                    // Control points for curvature
                    // New logic: Use distinct curves for overlap avoidance
                    // If y1 == y2 (same row), curve arc up or down
                    // If x1 == x2 (same col), curve out

                    const deltaX = x2 - x1;
                    const deltaY = y2 - y1;
                    const dist = Math.sqrt(deltaX * deltaX + deltaY * deltaY);

                    let cx, cy;

                    // Curvature strategy
                    if (Math.abs(deltaY) < 10) {
                        // Horizontal
                        cx = (x1 + x2) / 2;
                        cy = y1 + (pathIdx % 2 === 0 ? -40 - Math.abs(offset) : 40 + Math.abs(offset));
                    } else if (Math.abs(deltaX) < 10) {
                        // Vertical
                        cx = x1 + (pathIdx % 2 === 0 ? -40 - Math.abs(offset) : 40 + Math.abs(offset));
                        cy = (y1 + y2) / 2;
                    } else {
                        // Diagonal
                        cx = (x1 + x2) / 2 + (pathIdx % 2 === 0 ? -30 : 30);
                        cy = (y1 + y2) / 2 + (pathIdx % 2 === 0 ? 30 : -30);
                    }

                    // Make path data
                    // Shift start/end slightly by offset perpendicular to line? 
                    // Let's just shift the whole curve by offset in logic if possible, 
                    // but simpler is just varying control point `cx, cy` slightly works too.
                    // Actually simpler: just offset the control point significantly per index

                    // Refinining curvature:
                    // Use a quadratic bezier Q.
                    // Control point depends on pathIdx to separate bundles

                    const cpOffsetX = (pathIdx - (totalInDiagram) / 2) * 15;
                    const cpOffsetY = (pathIdx - (totalInDiagram) / 2) * 15;

                    // Basic control point is midpoint + some normal vector
                    const mx = (x1 + x2) / 2;
                    const my = (y1 + y2) / 2;
                    // Normal vector (-dy, dx)
                    const nx = -deltaY;
                    const ny = deltaX;
                    // Normalize
                    const len = Math.sqrt(nx * nx + ny * ny) || 1;
                    // Curvature amount
                    const curveAmt = 50 + Math.abs(offset * 2);

                    // Final Control Point
                    const cpx = mx + (nx / len) * curveAmt * (i % 2 === 0 ? 1 : -1);
                    const cpy = my + (ny / len) * curveAmt * (i % 2 === 0 ? 1 : -1);

                    // Shift final line slightly parallel?
                    // For simplicity, just draw from center to center with distinct curves

                    // Draw individual segment with arrowhead
                    diagramHTML += `
                    <path d="M ${x1},${y1} Q ${cpx},${cpy} ${x2},${y2}" 
                          stroke="${color}" 
                          stroke-width="3" 
                          fill="none" 
                          marker-end="url(#arrow-${path.case_id})"
                          opacity="0.9" />
                `;

                    // CASE ID LABEL (On EVERY segment now)
                    // Place label closer to start (t = 0.3)
                    const tCase = 0.3;
                    const lxCase = (1 - tCase) * (1 - tCase) * x1 + 2 * (1 - tCase) * tCase * cpx + tCase * tCase * x2;
                    const lyCase = (1 - tCase) * (1 - tCase) * y1 + 2 * (1 - tCase) * tCase * cpy + tCase * tCase * y2;

                    diagramHTML += `
                    <rect x="${lxCase - 25}" y="${lyCase - 8}" width="50" height="16" rx="4" fill="${color}" opacity="1"/>
                    <text x="${lxCase}" y="${lyCase + 4}" 
                          text-anchor="middle" 
                          font-size="10" 
                          font-weight="800"
                          fill="white"
                          style="text-shadow: 0px 0px 2px rgba(0,0,0,0.2);">
                        Case ${path.case_id}
                    </text>
                `;

                    // Time label
                    const timing = timings[i];
                    if (timing && timing.label !== '00:00' && timing.label !== '0s') {
                        // Position label at peak of curve (approx t=0.7)
                        const t = 0.7;
                        const lx = (1 - t) * (1 - t) * x1 + 2 * (1 - t) * t * cpx + t * t * x2;
                        const ly = (1 - t) * (1 - t) * y1 + 2 * (1 - t) * t * cpy + t * t * y2;

                        const dbTime = timing.start_time || '';

                        // Detailed Label: Duration + DB Time
                        // Increase box size to fit two lines
                        diagramHTML += `
                        <g>
                            <rect x="${lx - 35}" y="${ly - 14}" width="70" height="28" rx="6" fill="white" stroke="${color}" stroke-width="1" opacity="0.95" style="filter: drop-shadow(0px 1px 1px rgba(0,0,0,0.1));"/>
                            
                            <!-- Duration -->
                            <text x="${lx}" y="${ly - 1}" 
                                  text-anchor="middle" 
                                  font-size="10" 
                                  font-weight="700"
                                  fill="${color}">
                                ${timing.label}
                            </text>
                            
                            <!-- DB Timestamp -->
                            <text x="${lx}" y="${ly + 9}" 
                                  text-anchor="middle" 
                                  font-size="8.5" 
                                  font-weight="500"
                                  fill="#64748b">
                                ${dbTime}
                            </text>
                        </g>
                    `;
                    }
                }
            });
        } // End disabled loop

        diagramHTML += '</svg>';

        // Draw event boxes (inside pan content so they move with arrows when panning)
        eventTypes.forEach(event => {
            const pos = eventPositions[event];
            if (!pos) return;

            let boxColor = '#1e3a8a'; // Darker blue
            let boxBg = '#2563eb'; // Blue

            if (event === 'Process') { boxBg = '#1e40af'; }
            else if (event === 'End') { boxBg = '#ffffff'; boxColor = '#1e40af'; }
            else if (['Login', 'Logout'].includes(event)) { boxBg = '#2563eb'; }
            else if (['Created Account', 'Account Open'].includes(event)) { boxBg = '#2563eb'; }
            else if (['Withdrawal Transaction', 'Debit'].includes(event)) { boxBg = '#0f766e'; } // Teal
            else if (['Credit', 'Deposit'].includes(event)) { boxBg = '#0f766e'; }
            else if (['Refund'].includes(event)) { boxBg = '#059669'; } // Green
            else { boxBg = '#475569'; } // Slate

            const isEnd = event === 'End';
            const borderStyle = isEnd
                ? `border: 3px solid ${boxColor}; background: white; color: ${boxColor};`
                : `background: ${boxBg}; color: white; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);`;

            diagramHTML += `
                <div id="box-${event.replace(/\s+/g, '-')}" 
                     onmousedown="startDrag(event, '${event}')"
                     style="
                    cursor: move;
                    position: absolute;
                    left: ${pos.x}px;
                    top: ${pos.y}px;
                    width: ${boxWidth}px;
                    height: ${boxHeight}px;
                    ${borderStyle}
                    border-radius: ${isEnd ? '50px' : '12px'};
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    text-align: center;
                    font-weight: 700;
                    font-size: 0.95rem;
                    z-index: 2;
                    padding: 0.5rem;
                    line-height: 1.2;
                ">
                    ${event}
                </div>
            `;
        });

        diagramHTML += '</div></div></div>'; // close diagram-pan-content, diagram-pan-wrapper, diagram-outer-container

        return `
            <section style="margin-bottom: 2.5rem;">
                <h2 style="font-size: 1.8rem; margin-bottom: 0.5rem; color: var(--text-primary); text-align: center;">
                    🔄 Unified Case Flow Diagram
                </h2>
                <p style="color: var(--text-muted); margin-bottom: 1rem; font-size: 0.95rem; text-align: center;">
                    All ${totalCases} Case IDs shown in one unified flow. Each colored path represents one Case ID. Time labels show duration between events. When several cases have an event at the same time, a small arrow (⟶ same time) appears under that event.
                </p>
                <p style="color: var(--text-muted); margin-bottom: 0.75rem; font-size: 0.85rem; text-align: center;">
                    Drag empty area to pan • Drag event boxes to reposition
                </p>
                
                <div style="background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 16px; padding: 1.5rem;">
                    <div style="margin-bottom: 1rem;">
                        <div style="font-size: 0.9rem; font-weight: 600; color: var(--text-primary); margin-bottom: 0.5rem;">Legend</div>
                        ${legendHTML}
                    </div>
                    
                    ${diagramHTML}
                </div>
            </section>
        `;
    }

    const caseDetails = bkData.case_details || [];
    const caseIds = bkData.case_ids || [];
    const totalCases = bkData.total_cases || 0;
    const totalUsers = bkData.total_users || 0;
    const users = bkData.users || [];
    const explanations = bkData.explanations || [];
    const totalActivities = bkData.total_activities || 0;

    // Expose unified flow data globally for Single Case Flow filter
    if (bkData.unified_flow_data && bkData.unified_flow_data.case_paths && bkData.unified_flow_data.case_paths.length > 0) {
        window.currentBankingUnifiedFlowData = bkData.unified_flow_data;
    }

    if (caseDetails.length === 0) {
        mainContent.innerHTML = `
            <div style="max-width: 700px; margin: 0 auto; padding: 3rem; text-align: center;">
                <div style="font-size: 4rem; margin-bottom: 1.5rem;">¦</div>
                <h2 style="font-size: 2rem; margin-bottom: 1rem; color: #0F766E;">No Sessions Found</h2>
                <p style="color: var(--text-secondary); margin-bottom: 2rem;">
                    No sessions found. Sessions need a start (e.g. login or open) and steps (e.g. credit, debit).</p>
                <button class="btn-secondary" onclick="showDomainSplitView()"> Back to Database List</button>
            </div>
        `;
        return;
    }

    const BANK_COLOR = '#0F766E';
    const BANK_BG = 'rgba(15,118,110,0.08)';
    const BANK_BORDER = 'rgba(15,118,110,0.3)';

    let html = `
        <div style="padding: 2rem; overflow-y: auto; height: 100%;">
            <button class="btn-secondary" onclick="showDomainSplitView()" style="margin-bottom: 1rem;">â† Back</button>
            
            <h1 style="font-size: 2.5rem; margin-bottom: 0.5rem; color: ${BANK_COLOR};"> Banking Case IDs</h1>
            <p style="color: var(--text-secondary); margin-bottom: 2rem; font-size: 1.1rem;">
                ${profile.database_name} â€¢ ${totalCases} Case ID(s) â€¢ ${totalUsers} user(s) â€¢ ${totalActivities} activities
            </p>

            <section style="margin-bottom: 1.75rem;">
                <h2 style="font-size: 1.3rem; margin-bottom: 0.5rem; color: var(--text-primary);">Case Flow Filter (Single Case Diagram)</h2>
                <p style="color: var(--text-muted); margin-bottom: 0.75rem; font-size: 0.9rem;">
                    Click a Case ID chip below to see a separate flow diagram only for that Case ID (shown under the merged diagram).
                </p>
                <div style="display: flex; flex-wrap: nowrap; gap: 0.5rem; overflow-x: auto; padding-bottom: 0.25rem;">
                    ${(function () {
                        var unified = bkData.unified_flow_data || {};
                        var paths = (unified.case_paths || []);
                        var colorById = {};
                        paths.forEach(function (p) {
                            colorById[String(p.case_id)] = p.color || '${BANK_COLOR}';
                        });
                        return (caseDetails || []).map(function (c) {
                            var cid = String(c.case_id);
                            var color = colorById[cid] || '${BANK_COLOR}';
                            return '<button type="button" data-single-case-id="' + cid + '" ' +
                                   'style="cursor:pointer; border-radius:999px; border:1px solid ' + color + '; ' +
                                   'padding:0.3rem 0.7rem; font-size:0.8rem; background:#ffffff; color:' + color + '; ' +
                                   'display:inline-flex; align-items:center; gap:0.35rem; box-shadow:0 1px 2px rgba(15,23,42,0.08);">' +
                                   '<span style="width:14px; height:3px; border-radius:2px; background:' + color + ';"></span>' +
                                   '<span>Case ' + cid + '</span>' +
                                   '</button>';
                        }).join('');
                    })()}
                </div>
            </section>

            <section style="margin-bottom: 1.75rem;">
                <h2 style="font-size: 1.3rem; margin-bottom: 0.5rem; color: var(--text-primary);">User Flow Filter (Merged Case IDs)</h2>
                <p style="color: var(--text-muted); margin-bottom: 0.75rem; font-size: 0.9rem;">
                    Click a user button below to see ALL Case IDs for that user merged into one continuous flow diagram.
                </p>
                <div style="display: flex; flex-wrap: wrap; gap: 0.5rem; padding-bottom: 0.25rem;">
                    ${users.map(u => {
        const userCases = caseDetails.filter(c => c.user_id === u);
        const ids = userCases.map(c => c.case_id);
        const caseCount = ids.length;
        return '<button type="button" data-user-merge-id="' + String(u).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;') + '" ' +
               'style="cursor:pointer; border-radius:999px; border:1px solid ' + BANK_COLOR + '; ' +
               'padding:0.4rem 0.8rem; font-size:0.85rem; background:#ffffff; color:' + BANK_COLOR + '; ' +
               'display:inline-flex; align-items:center; gap:0.4rem; box-shadow:0 1px 2px rgba(15,23,42,0.08); font-weight:600;">' +
               '<span style="font-size:0.9rem;">👤</span>' +
               '<span>' + u + '</span>' +
               '<span style="background:' + BANK_COLOR + '; color:#ffffff; border-radius:999px; padding:0.1rem 0.4rem; font-size:0.75rem; font-weight:700;">' + caseCount + ' Case' + (caseCount !== 1 ? 's' : '') + '</span>' +
               '</button>';
    }).join('')}
                </div>
            </section>

            ${buildEventBlueprintFromBackend(bkData.event_columns, caseDetails)}
            
            ${renderMergedCaseFlowDiagram(bkData.unified_flow_data)}

            <section style="margin-top: 1.75rem; margin-bottom: 2rem;">
                <div id="single-case-flow-container" style="border-top: 1px dashed ${BANK_BORDER}; padding-top: 1.25rem;">
                    <button type="button"
                            onclick="window.toggleSingleCaseFlow(event)"
                            style="width: 100%; display: flex; justify-content: space-between; align-items: center; background: #ffffff; border-radius: 10px; border: 1px solid ${BANK_BORDER}; padding: 0.55rem 0.8rem; cursor: pointer; font-size: 0.9rem; color: #0f172a; margin-bottom: 0.5rem;">
                        <span style="font-weight: 600;">Single Case Flow (Filtered)</span>
                        <span style="font-size: 0.9rem; color: #6b7280;">▼</span>
                    </button>
                    <div id="single-case-flow-body" style="display: none; margin-top: 0.25rem;">
                        <div id="single-case-flow-content" style="font-size: 0.85rem; color: #6b7280;">
                            Pick a Case ID chip above to see the event-to-event flow for that single case only.
                        </div>
                    </div>
                </div>
            </section>
            
            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);">‹ Explanations</h2>
                <div style="background: ${BANK_BG}; border: 1px solid ${BANK_BORDER}; border-radius: 12px; padding: 1.25rem; margin-bottom: 2rem;">
                    <ul style="margin: 0; padding-left: 1.25rem; color: var(--text-primary); line-height: 1.8;">
                        ${explanations.map(e => '<li>' + e + '</li>').join('')}
                    </ul>
                </div>
            </section>

            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);">ðŸ‘¥ Users & Case IDs</h2>
                <p style="color: var(--text-muted); margin-bottom: 1rem; font-size: 0.95rem;">
                    Case IDs are in order of start time. Click a user card below to filter and see ALL Case IDs for that user in the diagram.
                </p>
                <div style="display: flex; flex-wrap: wrap; gap: 0.75rem; margin-bottom: 2rem;">
                    ${users.map(u => {
        const userCases = caseDetails.filter(c => c.user_id === u);
        const ids = userCases.map(c => c.case_id);
        return '<div data-user-merge-id="' + String(u).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;') + '" style="cursor:pointer; background: #f8fafc; border: 1px solid var(--border); border-radius: 10px; padding: 1rem 1.25rem;"><strong style="color: #0F766E;">' + u + '</strong><span style="color: var(--text-muted); margin-left: 0.5rem;">→ Case IDs: ' + ids.join(', ') + ' (click to filter)</span></div>';
    }).join('')}
                </div>
            </section>

            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);"> Case IDs (Ascending)</h2>
                <div style="display: flex; flex-wrap: wrap; gap: 0.75rem;">
                    ${caseDetails.map((c, i) => `
                        <div class="banking-case-node" style="flex-shrink: 0; cursor: pointer; padding: 0.6rem 1rem; border-radius: 10px; background: linear-gradient(135deg, ${BANK_COLOR}, #0D5C54); color: white; font-weight: 700; font-size: 1rem; border: 2px solid rgba(255,255,255,0.3); box-shadow: 0 2px 8px rgba(15,118,110,0.3); transition: all 0.2s;"
                            onclick="showBankingCaseDetails(${i})" role="button" tabindex="0">
                            Case #${c.case_id}
                        </div>
                    `).join('')}
                </div>
            </section>

            <div id="banking-case-details" style="display: none;">
                <div id="banking-case-details-content"></div>
            </div>
        </div>
    `;

    mainContent.innerHTML = html;
    window.bankingCaseDetails = caseDetails;
}

// Retail Analysis Results - Case ID per Customer (Order Journeys)
function showRetailAnalysisResults(profile) {
    const mainContent = document.getElementById('mainContent');
    const rtData = profile.retail_analysis;

    if (!rtData || !rtData.success) {
        mainContent.innerHTML = `
            <div style="max-width: 700px; margin: 0 auto; padding: 3rem; text-align: center;">
                <div style="font-size: 4rem; margin-bottom: 1.5rem;"></div>
                <h2 style="font-size: 2rem; margin-bottom: 1rem; color: #F59E0B;">Retail / E‑Commerce Database Detected</h2>
                <p style="color: var(--text-secondary); margin-bottom: 2rem;">
                    ${rtData?.error || 'No retail events with usable timestamps found. We look for customer/order/payment/shipping tables with date or timestamp columns.'}
                </p>
                <button class="btn-secondary" onclick="showDomainSplitView()">← Back to Database List</button>
            </div>
        `;
        return;
    }

    const caseDetails = rtData.case_details || [];
    const totalCases = rtData.total_cases || 0;
    const totalCustomers = rtData.total_customers || rtData.total_users || 0;
    const customers = rtData.customers || rtData.users || [];
    const totalActivities = rtData.total_activities || rtData.total_events || 0;
    const unifiedFlowData = rtData.unified_flow_data;
    const RETAIL_COLOR = '#F59E0B';
    const RETAIL_BG = 'rgba(245,158,11,0.08)';
    const RETAIL_BORDER = 'rgba(245,158,11,0.3)';

    if (caseDetails.length > 0 && unifiedFlowData && unifiedFlowData.case_paths && unifiedFlowData.case_paths.length > 0) {
        // Expose unified flow data globally for Single Case Flow filter
        window.currentRetailUnifiedFlowData = unifiedFlowData;
        let html = `
        <div style="padding: 2rem; overflow-y: auto; height: 100%;">
            <button class="btn-secondary" onclick="showDomainSplitView()" style="margin-bottom: 1rem;">← Back</button>
            <h1 style="font-size: 2.5rem; margin-bottom: 0.5rem; color: ${RETAIL_COLOR};"> Retail Case IDs</h1>
            <p style="color: var(--text-secondary); margin-bottom: 2rem; font-size: 1.1rem;">
                ${profile.database_name} • ${totalCases} Case ID(s) • ${totalCustomers} customer(s) • ${totalActivities} events
            </p>

            <section style="margin-bottom: 1.75rem;">
                <h2 style="font-size: 1.3rem; margin-bottom: 0.5rem; color: var(--text-primary);">Case Flow Filter (Single Case Diagram)</h2>
                <p style="color: var(--text-muted); margin-bottom: 0.75rem; font-size: 0.9rem;">
                    Click a Case ID chip to see a separate flow diagram only for that order journey (shown under the merged diagram).
                </p>
                <div style="display: flex; flex-wrap: nowrap; gap: 0.5rem; overflow-x: auto; padding-bottom: 0.25rem;">
                    ${(function () {
                        var unified = unifiedFlowData || {};
                        var paths = (unified.case_paths || []);
                        var colorById = {};
                        paths.forEach(function (p) {
                            colorById[String(p.case_id)] = p.color || '${RETAIL_COLOR}';
                        });
                        return (caseDetails || []).map(function (c) {
                            var cid = String(c.case_id);
                            var color = colorById[cid] || '${RETAIL_COLOR}';
                            return '<button type="button" data-single-case-id="' + cid + '" ' +
                                   'style="cursor:pointer; border-radius:999px; border:1px solid ' + color + '; ' +
                                   'padding:0.3rem 0.7rem; font-size:0.8rem; background:#ffffff; color:' + color + '; ' +
                                   'display:inline-flex; align-items:center; gap:0.35rem; box-shadow:0 1px 2px rgba(15,23,42,0.08);">' +
                                   '<span style="width:14px; height:3px; border-radius:2px; background:' + color + ';"></span>' +
                                   '<span>Case ' + cid + '</span>' +
                                   '</button>';
                        }).join('');
                    })()}
                </div>
            </section>

            <section style="margin-bottom: 1.75rem;">
                <h2 style="font-size: 1.3rem; margin-bottom: 0.5rem; color: var(--text-primary);">User Flow Filter (Merged Case IDs)</h2>
                <p style="color: var(--text-muted); margin-bottom: 0.75rem; font-size: 0.9rem;">
                    Click a customer button below to see ALL Case IDs for that customer merged into one continuous flow diagram.
                </p>
                <div style="display: flex; flex-wrap: wrap; gap: 0.5rem; padding-bottom: 0.25rem;">
                    ${(customers || []).map(function (u) {
                var userCases = caseDetails.filter(function (c) { return c.user_id === u; });
                var ids = userCases.map(function (c) { return c.case_id; });
                var caseCount = ids.length;
                return '<button type="button" data-user-merge-id="' + String(u).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;') + '" ' +
                       'style="cursor:pointer; border-radius:999px; border:1px solid ' + RETAIL_COLOR + '; ' +
                       'padding:0.4rem 0.8rem; font-size:0.85rem; background:#ffffff; color:' + RETAIL_COLOR + '; ' +
                       'display:inline-flex; align-items:center; gap:0.4rem; box-shadow:0 1px 2px rgba(15,23,42,0.08); font-weight:600;">' +
                       '<span style="font-size:0.9rem;">👤</span>' +
                       '<span>' + u + '</span>' +
                       '<span style="background:' + RETAIL_COLOR + '; color:#ffffff; border-radius:999px; padding:0.1rem 0.4rem; font-size:0.75rem; font-weight:700;">' + caseCount + ' Case' + (caseCount !== 1 ? 's' : '') + '</span>' +
                       '</button>';
            }).join('')}
                </div>
            </section>

            ${renderMergedCaseFlowDiagram(unifiedFlowData)}

            <section style="margin-top: 1.75rem; margin-bottom: 2rem;">
                <div id="single-case-flow-container" style="border-top: 1px dashed ${RETAIL_BORDER}; padding-top: 1.25rem;">
                    <button type="button"
                            onclick="window.toggleSingleCaseFlow(event)"
                            style="width: 100%; display: flex; justify-content: space-between; align-items: center; background: #ffffff; border-radius: 10px; border: 1px solid ${RETAIL_BORDER}; padding: 0.55rem 0.8rem; cursor: pointer; font-size: 0.9rem; color: #0f172a; margin-bottom: 0.5rem;">
                        <span style="font-weight: 600;">Single Case Flow (Filtered)</span>
                        <span style="font-size: 0.9rem; color: #6b7280;">▼</span>
                    </button>
                    <div id="single-case-flow-body" style="display: none; margin-top: 0.25rem;">
                        <div id="single-case-flow-content" style="font-size: 0.85rem; color: #6b7280;">
                            Pick a Case ID chip above to see the event-to-event flow for that single Case ID only.
                        </div>
                    </div>
                </div>
            </section>

            ${(function () {
                // Build the detected events set from all case activities
                var detectedEvents = new Set();
                caseDetails.forEach(function (c) {
                    (c.activities || []).forEach(function (a) {
                        if (a.event) detectedEvents.add(a.event);
                    });
                });

                // Define event explanations by category
                var eventExplanations = RETAIL_EVENT_EXPLANATIONS;

                // Group events by category (column-observed)
                var categories = [
                    {
                        label: 'Customer Side Events',
                        icon: '👤',
                        color: '#3B82F6',
                        events: ['Customer Visit', 'Product View', 'Product Search', 'Add To Cart', 'Remove From Cart', 'Apply Coupon', 'User Signed Up', 'User Logged In', 'User Logged Out']
                    },
                    {
                        label: 'Checkout & Payment',
                        icon: '🧾',
                        color: '#F59E0B',
                        events: ['Checkout Started', 'Address Entered', 'Payment Selected', 'Payment Success', 'Payment Failed', 'Order Placed', 'Order Confirmed', 'Invoice Generated']
                    },
                    {
                        label: 'Fulfillment Events',
                        icon: '📦',
                        color: '#22C55E',
                        events: ['Order Packed', 'Order Shipped', 'Out For Delivery', 'Order Delivered', 'Order Cancelled']
                    },
                    {
                        label: 'Returns & Refunds',
                        icon: '↩️',
                        color: '#EF4444',
                        events: ['Return Initiated', 'Return Received', 'Refund Processed']
                    }
                ];

                // Build HTML for each category that has detected events
                var html = '<section style="margin-bottom: 2.5rem;">';
                html += '<h2 style="font-size: 1.5rem; margin-bottom: 0.5rem; color: var(--text-primary);">Retail Event Steps Explained</h2>';
                html += '<p style="color: var(--text-muted); margin-bottom: 1.25rem; font-size: 0.95rem;">Each step in the workflow represents a specific action. Here are the events detected in your data:</p>';
                html += '<div style="display: flex; flex-direction: column; gap: 1.25rem;">';

                var hasAnyCategory = false;
                categories.forEach(function (cat) {
                    var foundEvents = cat.events.filter(function (ev) { return detectedEvents.has(ev); });
                    if (foundEvents.length === 0) return;
                    hasAnyCategory = true;

                    html += '<div style="background: #fefce8; border: 1px solid ' + cat.color + '33; border-radius: 12px; padding: 1rem 1.25rem;">';
                    html += '<div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.75rem;">';
                    html += '<span style="font-size: 1.1rem;">' + cat.icon + '</span>';
                    html += '<h3 style="font-size: 1.05rem; font-weight: 700; color: ' + cat.color + '; margin: 0;">' + cat.label + '</h3>';
                    html += '</div>';
                    html += '<div style="display: flex; flex-direction: column; gap: 0.5rem;">';

                    foundEvents.forEach(function (ev) {
                        var explanation = eventExplanations[ev] || ev;
                        html += '<div style="display: flex; align-items: flex-start; gap: 0.75rem; padding: 0.5rem 0.75rem; background: white; border-radius: 8px; border-left: 3px solid ' + cat.color + ';">';
                        html += '<span style="font-weight: 700; color: ' + cat.color + '; min-width: 140px; flex-shrink: 0;">' + ev + '</span>';
                        html += '<span style="color: #475569; flex: 1;">' + explanation + '</span>';
                        html += '</div>';
                    });

                    html += '</div></div>';
                });

                if (!hasAnyCategory) {
                    html += '<div style="color: var(--text-muted); font-size: 0.95rem;">No standard retail events detected in the data.</div>';
                }

                html += '</div></section>';
                return html;
            })()}

            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);">Customers & Case IDs</h2>
                <p style="color: var(--text-muted); margin-bottom: 1rem; font-size: 0.95rem;">
                    Each Case ID = one order journey. Click a Case to see events with source (table · file · row) across your data.
                </p>
                <div style="display: flex; flex-wrap: wrap; gap: 0.75rem; margin-bottom: 2rem;">
                    ${(customers || []).map(function (u) {
                var userCases = caseDetails.filter(function (c) { return c.user_id === u; });
                var ids = userCases.map(function (c) { return c.case_id; });
                return '<div data-user-merge-id="' + String(u).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;') + '" style="cursor:pointer; background: #f8fafc; border: 1px solid var(--border); border-radius: 10px; padding: 1rem 1.25rem;"><strong style="color: #F59E0B;">' + u + '</strong><span style="color: var(--text-muted); margin-left: 0.5rem;">→ Case IDs: ' + ids.join(', ') + ' (click to filter)</span></div>';
            }).join('')}
                </div>
            </section>

            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);">Case IDs (Ascending)</h2>
                <div style="display: flex; flex-wrap: wrap; gap: 0.75rem;">
                    ${caseDetails.map(function (c, i) {
                return '<div class="retail-case-node" style="flex-shrink: 0; cursor: pointer; padding: 0.6rem 1rem; border-radius: 10px; background: linear-gradient(135deg, #F59E0B, #D97706); color: white; font-weight: 700; font-size: 1rem; border: 2px solid rgba(255,255,255,0.3); box-shadow: 0 2px 8px rgba(245,158,11,0.3); transition: all 0.2s;" onclick="showRetailCaseDetails(' + i + ')" role="button" tabindex="0">Case #' + c.case_id + '</div>';
            }).join('')}
                </div>
            </section>

            <div id="retail-case-details" style="display: none;">
                <div id="retail-case-details-content"></div>
            </div>
        </div>
        `;
        mainContent.innerHTML = html;
        window.retailCaseDetails = caseDetails;
        return;
    }

    // Fallback: no case_details / unified flow (very unlikely)
    mainContent.innerHTML = `
        <div style="max-width: 700px; margin: 0 auto; padding: 3rem; text-align: center;">
            <h2 style="font-size: 2rem; margin-bottom: 1rem; color: #F59E0B;">Retail Data Detected</h2>
            <p style="color: var(--text-secondary); margin-bottom: 2rem;">
                We detected Retail / E‑commerce data, but could not build case-based timelines from the uploaded files.
            </p>
            <button class="btn-secondary" onclick="showDomainSplitView()">← Back to Database List</button>
        </div>
    `;
}

// Insurance Analysis Results - Case ID per Customer (Policy/Claim Journeys)
function showInsuranceAnalysisResults(profile) {
    const mainContent = document.getElementById('mainContent');
    const insData = profile.insurance_analysis;

    if (!insData || !insData.success) {
        mainContent.innerHTML = `
            <div style="max-width: 700px; margin: 0 auto; padding: 3rem; text-align: center;">
                <div style="font-size: 4rem; margin-bottom: 1.5rem;"></div>
                <h2 style="font-size: 2rem; margin-bottom: 1rem; color: #7C3AED;">Insurance Database Detected</h2>
                <p style="color: var(--text-secondary); margin-bottom: 2rem;">
                    ${insData?.error || 'No insurance events with usable timestamps found. We look for policy/claim tables with date or timestamp columns.'}
                </p>
                <button class="btn-secondary" onclick="showDomainSplitView()">← Back to Database List</button>
            </div>
        `;
        return;
    }

    const caseDetails = insData.case_details || [];
    const totalCases = insData.total_cases || 0;
    const totalCustomers = insData.total_customers || 0;
    const customers = insData.customers || [];
    const totalActivities = insData.total_activities || insData.total_events || 0;
    const unifiedFlowData = insData.unified_flow_data;
    const INS_COLOR = '#7C3AED';
    const INS_BG = 'rgba(124,58,237,0.08)';
    const INS_BORDER = 'rgba(124,58,237,0.3)';

    if (caseDetails.length > 0 && unifiedFlowData && unifiedFlowData.case_paths && unifiedFlowData.case_paths.length > 0) {
        // Expose unified flow data globally for Single Case Flow filter
        window.currentInsuranceUnifiedFlowData = unifiedFlowData;
        let html = `
        <div style="padding: 2rem; overflow-y: auto; height: 100%;">
            <button class="btn-secondary" onclick="showDomainSplitView()" style="margin-bottom: 1rem;">← Back</button>
            <h1 style="font-size: 2.5rem; margin-bottom: 0.5rem; color: ${INS_COLOR};"> Insurance Case IDs</h1>
            <p style="color: var(--text-secondary); margin-bottom: 2rem; font-size: 1.1rem;">
                ${profile.database_name} • ${totalCases} Case ID(s) • ${totalCustomers} customer(s) • ${totalActivities} events
            </p>

            <section style="margin-bottom: 1.75rem;">
                <h2 style="font-size: 1.3rem; margin-bottom: 0.5rem; color: var(--text-primary);">Case Flow Filter (Single Case Diagram)</h2>
                <p style="color: var(--text-muted); margin-bottom: 0.75rem; font-size: 0.9rem;">
                    Click a Case ID chip to see a separate diagram only for that insurance journey (shown under the merged diagram).
                </p>
                <div style="display: flex; flex-wrap: nowrap; gap: 0.5rem; overflow-x: auto; padding-bottom: 0.25rem;">
                    ${(function () {
                        var unified = unifiedFlowData || {};
                        var paths = (unified.case_paths || []);
                        var colorById = {};
                        paths.forEach(function (p) {
                            colorById[String(p.case_id)] = p.color || '${INS_COLOR}';
                        });
                        return (caseDetails || []).map(function (c) {
                            var cid = String(c.case_id);
                            var color = colorById[cid] || '${INS_COLOR}';
                            return '<button type="button" data-single-case-id="' + cid + '" ' +
                                   'style="cursor:pointer; border-radius:999px; border:1px solid ' + color + '; ' +
                                   'padding:0.3rem 0.7rem; font-size:0.8rem; background:#ffffff; color:' + color + '; ' +
                                   'display:inline-flex; align-items:center; gap:0.35rem; box-shadow:0 1px 2px rgba(15,23,42,0.08);">' +
                                   '<span style="width:14px; height:3px; border-radius:2px; background:' + color + ';"></span>' +
                                   '<span>Case ' + cid + '</span>' +
                                   '</button>';
                        }).join('');
                    })()}
                </div>
            </section>

            <section style="margin-bottom: 1.75rem;">
                <h2 style="font-size: 1.3rem; margin-bottom: 0.5rem; color: var(--text-primary);">User Flow Filter (Merged Case IDs)</h2>
                <p style="color: var(--text-muted); margin-bottom: 0.75rem; font-size: 0.9rem;">
                    Click a customer button below to see ALL Case IDs for that customer merged into one continuous flow diagram.
                </p>
                <div style="display: flex; flex-wrap: wrap; gap: 0.5rem; padding-bottom: 0.25rem;">
                    ${(customers || []).map(function (u) {
                        var userCases = caseDetails.filter(function (c) { return c.user_id === u; });
                        var ids = userCases.map(function (c) { return c.case_id; });
                        var caseCount = ids.length;
                        return '<button type="button" data-user-merge-id="' + String(u).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;') + '" ' +
                               'style="cursor:pointer; border-radius:999px; border:1px solid ' + INS_COLOR + '; ' +
                               'padding:0.4rem 0.8rem; font-size:0.85rem; background:#ffffff; color:' + INS_COLOR + '; ' +
                               'display:inline-flex; align-items:center; gap:0.4rem; box-shadow:0 1px 2px rgba(15,23,42,0.08); font-weight:600;">' +
                               '<span style="font-size:0.9rem;">👤</span>' +
                               '<span>' + u + '</span>' +
                               '<span style="background:' + INS_COLOR + '; color:#ffffff; border-radius:999px; padding:0.1rem 0.4rem; font-size:0.75rem; font-weight:700;">' + caseCount + ' Case' + (caseCount !== 1 ? 's' : '') + '</span>' +
                               '</button>';
                    }).join('')}
                </div>
            </section>

            ${renderMergedCaseFlowDiagram(unifiedFlowData)}

            <section style="margin-top: 1.75rem; margin-bottom: 2rem;">
                <div id="single-case-flow-container" style="border-top: 1px dashed ${INS_BORDER}; padding-top: 1.25rem;">
                    <button type="button"
                            onclick="window.toggleSingleCaseFlow(event)"
                            style="width: 100%; display: flex; justify-content: space-between; align-items: center; background: #ffffff; border-radius: 10px; border: 1px solid ${INS_BORDER}; padding: 0.55rem 0.8rem; cursor: pointer; font-size: 0.9rem; color: #0f172a; margin-bottom: 0.5rem;">
                        <span style="font-weight: 600;">Single Case Flow (Filtered)</span>
                        <span style="font-size: 0.9rem; color: #6b7280;">▼</span>
                    </button>
                    <div id="single-case-flow-body" style="display: none; margin-top: 0.25rem;">
                        <div id="single-case-flow-content" style="font-size: 0.85rem; color: #6b7280;">
                            Pick a Case ID chip above to see the event-to-event flow for that single Case ID only.
                        </div>
                    </div>
                </div>
            </section>

            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);"> How It Works</h2>
                <div style="background: ${INS_BG}; border: 1px solid ${INS_BORDER}; border-radius: 12px; padding: 1.25rem;">
                    <ul style="margin: 0; padding-left: 1.25rem; color: var(--text-primary); line-height: 1.8;">
                        ${(insData.explanations || []).map(function (e) { return '<li>' + e + '</li>'; }).join('')}
                    </ul>
                </div>
            </section>

            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);">Customers & Case IDs</h2>
                <p style="color: var(--text-muted); margin-bottom: 1rem;">Each Case ID = one insurance journey (policy lifecycle or claim). Click a Case to see events.</p>
                <div style="display: flex; flex-wrap: wrap; gap: 0.75rem; margin-bottom: 2rem;">
                    ${(customers || []).map(function (u) {
                        var userCases = caseDetails.filter(function (c) { return c.user_id === u; });
                        var ids = userCases.map(function (c) { return c.case_id; });
                        return '<div data-user-merge-id="' + String(u).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;') + '" style="cursor:pointer; background: #f8fafc; border: 1px solid var(--border); border-radius: 10px; padding: 1rem 1.25rem;"><strong style="color: #7C3AED;">' + u + '</strong><span style="color: var(--text-muted); margin-left: 0.5rem;">→ Case IDs: ' + ids.join(', ') + ' (click to filter)</span></div>';
                    }).join('')}
                </div>
            </section>

            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);">Case IDs (sorted by first event time)</h2>
                <p style="color: var(--text-muted); font-size: 0.9rem; margin-bottom: 1rem;">Each case shows its sequence of steps; each step shows <strong>Across DB</strong> source (table · file · row).</p>
                <div style="display: flex; flex-wrap: wrap; gap: 0.75rem;">
                    ${caseDetails.map(function (c, i) {
                        return '<div class="insurance-case-node" style="flex-shrink: 0; cursor: pointer; padding: 0.6rem 1rem; border-radius: 10px; background: linear-gradient(135deg, #7C3AED, #6D28D9); color: white; font-weight: 700; font-size: 1rem; border: 2px solid rgba(255,255,255,0.3); box-shadow: 0 2px 8px rgba(124,58,237,0.3); transition: all 0.2s;" onclick="showInsuranceCaseDetails(' + i + ')" role="button" tabindex="0">Case #' + c.case_id + '</div>';
                    }).join('')}
                </div>
            </section>

            <div id="insurance-case-details" style="display: none;">
                <div id="insurance-case-details-content"></div>
            </div>
        </div>
        `;
        mainContent.innerHTML = html;
        window.insuranceCaseDetails = caseDetails;
        return;
    }

    mainContent.innerHTML = `
        <div style="max-width: 700px; margin: 0 auto; padding: 3rem; text-align: center;">
            <h2 style="font-size: 2rem; margin-bottom: 1rem; color: #7C3AED;">Insurance Data Detected</h2>
            <p style="color: var(--text-secondary); margin-bottom: 2rem;">
                We detected Insurance data, but could not build case-based timelines from the uploaded files.
            </p>
            <button class="btn-secondary" onclick="showDomainSplitView()">← Back to Database List</button>
        </div>
    `;
}

// Finance Analysis Results – events across DB, Case IDs sorted by time (20 events)
function showFinanceAnalysisResults(profile) {
    const mainContent = document.getElementById('mainContent');
    const finData = profile.finance_analysis;

    if (!finData || !finData.success) {
        mainContent.innerHTML = `
            <div style="max-width: 700px; margin: 0 auto; padding: 3rem; text-align: center;">
                <div style="font-size: 4rem; margin-bottom: 1.5rem;"></div>
                <h2 style="font-size: 2rem; margin-bottom: 1rem; color: #4F46E5;">Finance Database Detected</h2>
                <p style="color: var(--text-secondary); margin-bottom: 2rem;">
                    ${finData?.error || 'No finance events with usable timestamps found across tables. We look for event/date columns and row data matching the 20 finance events.'}
                </p>
                <button class="btn-secondary" onclick="showDomainSplitView()">← Back to Database List</button>
            </div>
        `;
        return;
    }

    const caseDetails = finData.case_details || [];
    const totalCases = finData.total_cases || 0;
    const totalCustomers = finData.total_customers || 0;
    const customers = finData.customers || [];
    const totalActivities = finData.total_activities || finData.total_events || 0;
    const unifiedFlowData = finData.unified_flow_data;
    const FIN_COLOR = '#4F46E5';
    const FIN_BG = 'rgba(79,70,229,0.08)';
    const FIN_BORDER = 'rgba(79,70,229,0.3)';

    if (caseDetails.length > 0 && unifiedFlowData && unifiedFlowData.case_paths && unifiedFlowData.case_paths.length > 0) {
        // Expose unified flow data globally for Single Case Flow filter
        window.currentFinanceUnifiedFlowData = unifiedFlowData;
        let html = `
        <div style="padding: 2rem; overflow-y: auto; height: 100%;">
            <button class="btn-secondary" onclick="showDomainSplitView()" style="margin-bottom: 1rem;">← Back</button>
            <h1 style="font-size: 2.5rem; margin-bottom: 0.5rem; color: ${FIN_COLOR};"> Finance Case IDs</h1>
            <p style="color: var(--text-secondary); margin-bottom: 2rem; font-size: 1.1rem;">
                ${profile.database_name} • ${totalCases} Case(s) • ${totalCustomers} customer(s) • ${totalActivities} events
            </p>

            <section style="margin-bottom: 1.75rem;">
                <h2 style="font-size: 1.3rem; margin-bottom: 0.5rem; color: var(--text-primary);">Case Flow Filter (Single Case Diagram)</h2>
                <p style="color: var(--text-muted); margin-bottom: 0.75rem; font-size: 0.9rem;">
                    Click a Case ID chip to see a separate diagram only for that finance journey (shown under the merged diagram).
                </p>
                <div style="display: flex; flex-wrap: nowrap; gap: 0.5rem; overflow-x: auto; padding-bottom: 0.25rem;">
                    ${(function () {
                        var unified = unifiedFlowData || {};
                        var paths = (unified.case_paths || []);
                        var colorById = {};
                        paths.forEach(function (p) {
                            colorById[String(p.case_id)] = p.color || '${FIN_COLOR}';
                        });
                        return (caseDetails || []).map(function (c) {
                            var cid = String(c.case_id);
                            var color = colorById[cid] || '${FIN_COLOR}';
                            return '<button type="button" data-single-case-id="' + cid + '" ' +
                                   'style="cursor:pointer; border-radius:999px; border:1px solid ' + color + '; ' +
                                   'padding:0.3rem 0.7rem; font-size:0.8rem; background:#ffffff; color:' + color + '; ' +
                                   'display:inline-flex; align-items:center; gap:0.35rem; box-shadow:0 1px 2px rgba(15,23,42,0.08);">' +
                                   '<span style="width:14px; height:3px; border-radius:2px; background:' + color + ';"></span>' +
                                   '<span>Case ' + cid + '</span>' +
                                   '</button>';
                        }).join('');
                    })()}
                </div>
            </section>

            <section style="margin-bottom: 1.75rem;">
                <h2 style="font-size: 1.3rem; margin-bottom: 0.5rem; color: var(--text-primary);">User Flow Filter (Merged Case IDs)</h2>
                <p style="color: var(--text-muted); margin-bottom: 0.75rem; font-size: 0.9rem;">
                    Click a customer button below to see ALL Case IDs for that customer merged into one continuous flow diagram.
                </p>
                <div style="display: flex; flex-wrap: wrap; gap: 0.5rem; padding-bottom: 0.25rem;">
                    ${(customers || []).map(function (u) {
                        var userCases = caseDetails.filter(function (c) { return c.user_id === u; });
                        var ids = userCases.map(function (c) { return c.case_id; });
                        var caseCount = ids.length;
                        return '<button type="button" data-user-merge-id="' + String(u).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;') + '" ' +
                               'style="cursor:pointer; border-radius:999px; border:1px solid ' + FIN_COLOR + '; ' +
                               'padding:0.4rem 0.8rem; font-size:0.85rem; background:#ffffff; color:' + FIN_COLOR + '; ' +
                               'display:inline-flex; align-items:center; gap:0.4rem; box-shadow:0 1px 2px rgba(15,23,42,0.08); font-weight:600;">' +
                               '<span style="font-size:0.9rem;">👤</span>' +
                               '<span>' + u + '</span>' +
                               '<span style="background:' + FIN_COLOR + '; color:#ffffff; border-radius:999px; padding:0.1rem 0.4rem; font-size:0.75rem; font-weight:700;">' + caseCount + ' Case' + (caseCount !== 1 ? 's' : '') + '</span>' +
                               '</button>';
                    }).join('')}
                </div>
            </section>

            ${renderMergedCaseFlowDiagram(unifiedFlowData)}

            <section style="margin-top: 1.75rem; margin-bottom: 2rem;">
                <div id="single-case-flow-container" style="border-top: 1px dashed ${FIN_BORDER}; padding-top: 1.25rem;">
                    <button type="button"
                            onclick="window.toggleSingleCaseFlow(event)"
                            style="width: 100%; display: flex; justify-content: space-between; align-items: center; background: #ffffff; border-radius: 10px; border: 1px solid ${FIN_BORDER}; padding: 0.55rem 0.8rem; cursor: pointer; font-size: 0.9rem; color: #0f172a; margin-bottom: 0.5rem;">
                        <span style="font-weight: 600;">Single Case Flow (Filtered)</span>
                        <span style="font-size: 0.9rem; color: #6b7280;">▼</span>
                    </button>
                    <div id="single-case-flow-body" style="display: none; margin-top: 0.25rem;">
                        <div id="single-case-flow-content" style="font-size: 0.85rem; color: #6b7280;">
                            Pick a Case ID chip above to see the event-to-event flow for that single Case ID only.
                        </div>
                    </div>
                </div>
            </section>

            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);"> How It Works</h2>
                <div style="background: ${FIN_BG}; border: 1px solid ${FIN_BORDER}; border-radius: 12px; padding: 1.25rem;">
                    <ul style="margin: 0; padding-left: 1.25rem; color: var(--text-primary); line-height: 1.8;">
                        ${(finData.explanations || []).map(function (e) { return '<li>' + e + '</li>'; }).join('')}
                    </ul>
                </div>
            </section>

            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);">Customers & Case IDs</h2>
                <p style="color: var(--text-muted); margin-bottom: 1rem;">Each Case ID = one finance journey. Events found across DB (tables/files). Click a Case to see steps and source (table · file · row).</p>
                <div style="display: flex; flex-wrap: wrap; gap: 0.75rem; margin-bottom: 2rem;">
                    ${(customers || []).map(function (u) {
                        var userCases = caseDetails.filter(function (c) { return c.user_id === u; });
                        var ids = userCases.map(function (c) { return c.case_id; });
                        return '<div data-user-merge-id="' + String(u).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;') + '" style="cursor:pointer; background: #f8fafc; border: 1px solid var(--border); border-radius: 10px; padding: 1rem 1.25rem;"><strong style="color: #4F46E5;">' + u + '</strong><span style="color: var(--text-muted); margin-left: 0.5rem;">→ Case IDs: ' + ids.join(', ') + ' (click to filter)</span></div>';
                    }).join('')}
                </div>
            </section>

            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);">Case IDs (sorted by first event time)</h2>
                <p style="color: var(--text-muted); font-size: 0.9rem; margin-bottom: 1rem;">Each case shows its sequence; each step shows <strong>Across DB</strong> source (table · file · row).</p>
                <div style="display: flex; flex-wrap: wrap; gap: 0.75rem;">
                    ${caseDetails.map(function (c, i) {
                        return '<div class="finance-case-node" style="flex-shrink: 0; cursor: pointer; padding: 0.6rem 1rem; border-radius: 10px; background: linear-gradient(135deg, #4F46E5, #4338CA); color: white; font-weight: 700; font-size: 1rem; border: 2px solid rgba(255,255,255,0.3); box-shadow: 0 2px 8px rgba(79,70,229,0.3); transition: all 0.2s;" onclick="showFinanceCaseDetails(' + i + ')" role="button" tabindex="0">Case #' + c.case_id + '</div>';
                    }).join('')}
                </div>
            </section>

            <div id="finance-case-details" style="display: none;">
                <div id="finance-case-details-content"></div>
            </div>
        </div>
        `;
        mainContent.innerHTML = html;
        window.financeCaseDetails = caseDetails;
        return;
    }

    mainContent.innerHTML = `
        <div style="max-width: 700px; margin: 0 auto; padding: 3rem; text-align: center;">
            <h2 style="font-size: 2rem; margin-bottom: 1rem; color: #4F46E5;">Finance Data Detected</h2>
            <p style="color: var(--text-secondary); margin-bottom: 2rem;">
                We detected Finance data, but could not build case-based timelines from the uploaded files.
            </p>
            <button class="btn-secondary" onclick="showDomainSplitView()">← Back to Database List</button>
        </div>
    `;
}

window.showFinanceCaseDetails = function (caseIndex) {
    const container = document.getElementById('finance-case-details');
    const content = document.getElementById('finance-case-details-content');
    const cases = window.financeCaseDetails;
    if (!container || !content || !cases || caseIndex < 0 || caseIndex >= cases.length) return;

    const c = cases[caseIndex];
    const prevIdx = window.financeSelectedCaseIndex;
    if (prevIdx === caseIndex) {
        container.style.display = 'none';
        window.financeSelectedCaseIndex = null;
        return;
    }
    window.financeSelectedCaseIndex = caseIndex;

    const activities = c.activities || [];
    const seq = (c.event_sequence || []).join(' → ');
    const stepLabel = activities.length === 1 ? '1 step' : activities.length + ' steps';
    let html = `
        <div style="background: var(--bg-card); border: 2px solid #4F46E5; border-radius: 12px; padding: 1.25rem; margin-top: 1rem; box-shadow: 0 4px 20px rgba(79,70,229,0.15);">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                <h3 style="font-size: 1.15rem; color: #4F46E5; margin: 0;">Case ID ${c.case_id} · Customer ${c.user_id}</h3>
                <button class="btn-secondary" onclick="showFinanceCaseDetails(${caseIndex})" style="padding: 0.3rem 0.6rem; font-size: 0.8rem;">✕ Close</button>
            </div>
            <p style="color: #475569; font-size: 0.9rem; margin-bottom: 0.5rem;">${c.explanation || (c.first_activity_timestamp + ' → ' + c.last_activity_timestamp + ' · ' + stepLabel)}</p>
            <p style="color: #64748b; font-size: 0.8rem; margin-bottom: 1rem;"><strong>Sequence (${stepLabel}):</strong> ${seq}</p>
            <p style="color: #4F46E5; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.75rem;">Across DB — each step shows source: Table · File · Row</p>
            <div style="max-height: 420px; overflow-y: auto; display: flex; flex-direction: column; gap: 0.5rem;">
    `;

    activities.forEach(function (a, stepIdx) {
        const ev = a.event || '';
        const ts = a.timestamp_str || '';
        const tbl = a.table_name || '';
        const fname = a.file_name || '';
        const rowNum = a.source_row != null && a.source_row !== '' ? (typeof a.source_row === 'number' ? a.source_row + 1 : a.source_row) : '';
        const raw = a.raw_record || {};
        const sourceParts = [tbl, fname].filter(Boolean);
        const sourceStr = sourceParts.length ? (sourceParts.join(' · ') + (rowNum ? ' · row ' + rowNum : '')) : '';
        const rawPreview = typeof raw === 'object' && Object.keys(raw).length
            ? Object.entries(raw).slice(0, 5).map(function (kv) { return kv[0] + ': ' + (kv[1] || ''); }).join('; ')
            : '';
        let explanation = FINANCE_EVENT_EXPLANATIONS[ev];
        if (!explanation) explanation = ev;
        html += `
            <div style="padding: 0.6rem 0.8rem; background: #eef2ff; border-left: 3px solid #4F46E5; border-radius: 8px; font-size: 0.9rem;">
                <div style="display: flex; flex-direction: column; margin-bottom: 0.3rem;">
                    <span style="font-weight: 700; color: #4F46E5; font-size: 1rem;">Step ${stepIdx + 1}: ${ev}</span>
                    <span style="font-size: 0.85rem; color: #1e293b; font-weight: 600; margin-top: 0.1rem;">${explanation}</span>
                </div>
                <span style="color: var(--text-muted); font-size: 0.85rem;">${ts}</span>
                <div style="font-size: 0.8rem; color: #475569; margin-top: 0.35rem; font-weight: 600;">Across DB → ${sourceStr || '—'}</div>
                ${rawPreview ? '<div style="font-size: 0.75rem; color: #64748b; margin-top: 0.2rem; font-family: monospace;">' + rawPreview + '</div>' : ''}
            </div>
        `;
    });

    html += '</div></div>';
    content.innerHTML = html;
    container.style.display = 'block';
};

window.showInsuranceCaseDetails = function (caseIndex) {
    const container = document.getElementById('insurance-case-details');
    const content = document.getElementById('insurance-case-details-content');
    const cases = window.insuranceCaseDetails;
    if (!container || !content || !cases || caseIndex < 0 || caseIndex >= cases.length) return;

    const c = cases[caseIndex];
    const prevIdx = window.insuranceSelectedCaseIndex;
    if (prevIdx === caseIndex) {
        container.style.display = 'none';
        window.insuranceSelectedCaseIndex = null;
        return;
    }
    window.insuranceSelectedCaseIndex = caseIndex;

    const activities = c.activities || [];
    const seq = (c.event_sequence || []).join(' → ');
    const stepLabel = activities.length === 1 ? '1 step' : activities.length + ' steps';
    let html = `
        <div style="background: var(--bg-card); border: 2px solid #7C3AED; border-radius: 12px; padding: 1.25rem; margin-top: 1rem; box-shadow: 0 4px 20px rgba(124,58,237,0.15);">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                <h3 style="font-size: 1.15rem; color: #7C3AED; margin: 0;">Case ID ${c.case_id} · Customer ${c.user_id}</h3>
                <button class="btn-secondary" onclick="showInsuranceCaseDetails(${caseIndex})" style="padding: 0.3rem 0.6rem; font-size: 0.8rem;">✕ Close</button>
            </div>
            <p style="color: #475569; font-size: 0.9rem; margin-bottom: 0.5rem;">${c.explanation || (c.first_activity_timestamp + ' → ' + c.last_activity_timestamp + ' · ' + stepLabel)}</p>
            <p style="color: #64748b; font-size: 0.8rem; margin-bottom: 1rem;"><strong>Sequence (${stepLabel}):</strong> ${seq}</p>
            <p style="color: #7C3AED; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.75rem;">Across DB — each step shows source: Table · File · Row</p>
            <div style="max-height: 420px; overflow-y: auto; display: flex; flex-direction: column; gap: 0.5rem;">
    `;

    activities.forEach(function (a, stepIdx) {
        const ev = a.event || '';
        const ts = a.timestamp_str || '';
        const story = a.event_story || '';
        const policyId = a.policy_id || '';
        const tbl = a.table_name || '';
        const fname = a.file_name || '';
        const rowNum = a.source_row != null && a.source_row !== '' ? (typeof a.source_row === 'number' ? a.source_row + 1 : a.source_row) : '';
        const raw = a.raw_record || {};
        const sourceParts = [tbl, fname].filter(Boolean);
        const sourceStr = sourceParts.length ? (sourceParts.join(' · ') + (rowNum ? ' · row ' + rowNum : '')) : (story || '');
        const rawPreview = typeof raw === 'object' && Object.keys(raw).length
            ? Object.entries(raw).slice(0, 5).map(function (kv) { return kv[0] + ': ' + (kv[1] || ''); }).join('; ')
            : '';
        let explanation = INSURANCE_EVENT_EXPLANATIONS[ev];
        if (!explanation && story && typeof story === 'string') explanation = story.split('[')[0].trim();
        if (!explanation) explanation = ev;
        html += `
            <div style="padding: 0.6rem 0.8rem; background: #f5f3ff; border-left: 3px solid #7C3AED; border-radius: 8px; font-size: 0.9rem;">
                <div style="display: flex; flex-direction: column; margin-bottom: 0.3rem;">
                    <span style="font-weight: 700; color: #7C3AED; font-size: 1rem;">Step ${stepIdx + 1}: ${ev}</span>
                    <span style="font-size: 0.85rem; color: #1e293b; font-weight: 600; margin-top: 0.1rem;">${explanation}</span>
                </div>
                <span style="color: var(--text-muted); font-size: 0.85rem;">${ts}</span>
                ${policyId ? '<span style="margin-left: 0.5rem; font-size: 0.8rem; color: #94a3b8;">[Policy ' + policyId + ']</span>' : ''}
                <div style="font-size: 0.8rem; color: #475569; margin-top: 0.35rem; font-weight: 600;">Across DB → ${sourceStr || '—'}</div>
                ${rawPreview ? '<div style="font-size: 0.75rem; color: #64748b; margin-top: 0.2rem; font-family: monospace;">' + rawPreview + '</div>' : ''}
            </div>
        `;
    });

    html += '</div></div>';
    content.innerHTML = html;
    container.style.display = 'block';
};

window.showRetailCaseDetails = function (caseIndex) {
    const container = document.getElementById('retail-case-details');
    const content = document.getElementById('retail-case-details-content');
    const cases = window.retailCaseDetails;
    if (!container || !content || !cases || caseIndex < 0 || caseIndex >= cases.length) return;

    const c = cases[caseIndex];
    const prevIdx = window.retailSelectedCaseIndex;
    if (prevIdx === caseIndex) {
        container.style.display = 'none';
        window.retailSelectedCaseIndex = null;
        return;
    }
    window.retailSelectedCaseIndex = caseIndex;

    const activities = c.activities || [];
    const seq = (c.event_sequence || []).join(' → ');
    let html = `
        <div style="background: var(--bg-card); border: 2px solid #F59E0B; border-radius: 12px; padding: 1.25rem; margin-top: 1rem; box-shadow: 0 4px 20px rgba(245,158,11,0.15);">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                <h3 style="font-size: 1.15rem; color: #F59E0B; margin: 0;">Case ID ${c.case_id} · Customer ${c.user_id}</h3>
                <button class="btn-secondary" onclick="showRetailCaseDetails(${caseIndex})" style="padding: 0.3rem 0.6rem; font-size: 0.8rem;">✕ Close</button>
            </div>
            <p style="color: #475569; font-size: 0.9rem; margin-bottom: 1rem;">
                ${c.explanation || (c.first_activity_timestamp + ' → ' + c.last_activity_timestamp + ' · ' + activities.length + ' steps')}
            </p>
            <div style="max-height: 420px; overflow-y: auto; display: flex; flex-direction: column; gap: 0.5rem;">
    `;

    activities.forEach(function (a) {
        const ev = a.event || '';
        const ts = a.timestamp_str || '';
        const tbl = a.table_name || '';
        const orderId = a.order_id || '';
        const story = a.event_story || '';
        const rec = a.raw_record || {};
        const recStr = Object.keys(rec)
            .filter(function (k) { return rec[k]; })
            .map(function (k) { return k + ': ' + rec[k]; })
            .join(' · ');

        // Robust explanation: lookup -> fallback to story core -> fallback to event name
        let explanation = RETAIL_EVENT_EXPLANATIONS[ev];
        if (!explanation && story && typeof story === 'string') {
            explanation = story.split('[')[0].trim();
        }
        if (!explanation) explanation = ev;

        html += `
            <div style="padding: 0.6rem 0.8rem; background: #fffbeb; border-left: 3px solid #F59E0B; border-radius: 8px; font-size: 0.9rem;">
                <div style="display: flex; flex-direction: column; margin-bottom: 0.3rem;">
                    <span style="font-weight: 700; color: #F59E0B; font-size: 1rem;">${ev}</span>
                    <span style="font-size: 0.85rem; color: #1e293b; font-weight: 600; margin-top: 0.1rem;">${explanation}</span>
                </div>
                <span style="color: var(--text-muted); font-size: 0.85rem;">${ts}</span>
                ${orderId ? '<span style="margin-left: 0.5rem; font-size: 0.8rem; color: #94a3b8;">[Order ' + orderId + ']</span>' : ''}
                ${tbl ? '<span style="margin-left: 0.5rem; font-size: 0.8rem; color: #94a3b8;">(' + tbl + ')</span>' : ''}
                ${story ? '<div style="font-size: 0.8rem; color: #475569; margin-top: 0.3rem; border-top: 1px dashed rgba(245,158,11,0.2); padding-top: 0.3rem;">Source: ' + story + '</div>' : ''}
                ${recStr ? '<div style="font-size: 0.8rem; color: #64748b; margin-top: 0.35rem;">' + recStr + '</div>' : ''}
            </div>
        `;
    });

    html += '</div></div>';
    content.innerHTML = html;
    container.style.display = 'block';
};

window.showBankingCaseDetails = function (caseIndex) {
    const container = document.getElementById('banking-case-details');
    const content = document.getElementById('banking-case-details-content');
    const cases = window.bankingCaseDetails;
    if (!container || !content || !cases || caseIndex < 0 || caseIndex >= cases.length) return;

    const c = cases[caseIndex];
    const prevIdx = window.bankingSelectedCaseIndex;
    if (prevIdx === caseIndex) {
        container.style.display = 'none';
        window.bankingSelectedCaseIndex = null;
        return;
    }
    window.bankingSelectedCaseIndex = caseIndex;

    const activities = c.activities || [];
    const seq = (c.event_sequence || []).join(' â†’ ');
    let html = `
        <div style="background: var(--bg-card); border: 2px solid #0F766E; border-radius: 12px; padding: 1.25rem; margin-top: 1rem; box-shadow: 0 4px 20px rgba(15,118,110,0.15);">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                <h3 style="font-size: 1.15rem; color: #0F766E; margin: 0;">Case ID ${c.case_id} Â· User ${c.user_id}</h3>
                <button class="btn-secondary" onclick="showBankingCaseDetails(${caseIndex})" style="padding: 0.3rem 0.6rem; font-size: 0.8rem;">âœ• Close</button>
            </div>
            <p style="color: var(--text-muted); font-size: 0.9rem; margin-bottom: 0.5rem;">
                ${c.first_activity_timestamp} â†’ ${c.last_activity_timestamp} Â· ${activities.length} steps (in time order)
            </p>
            <p style="color: var(--text-primary); font-size: 0.95rem; margin-bottom: 1rem; font-weight: 600;">
                Steps: ${seq}
            </p>
            <p style="color: #475569; font-size: 0.88rem; margin-bottom: 1rem;">${c.explanation || ''}</p>
            <div style="max-height: 420px; overflow-y: auto; display: flex; flex-direction: column; gap: 0.5rem;">
    `;

    activities.forEach((a, idx) => {
        const ev = a.event || '';
        const ts = a.timestamp_str || '';
        const acc = a.account_id || 'â€”';
        const tbl = a.table_name || '';
        const rec = a.raw_record || {};
        const recStr = Object.entries(rec).filter(([k, v]) => v).map(([k, v]) => k + ': ' + v).join(' Â· ');
        let evColor = '#64748b';
        if (ev === 'login') evColor = '#059669';
        else if (ev === 'logout') evColor = '#dc2626';
        else if (ev === 'credit' || ev === 'deposit' || ev === 'refund') evColor = '#22c55e';
        else if (ev === 'debit' || ev === 'withdraw') evColor = '#f59e0b';
        else if (ev === 'invalid_balance' || ev === 'negative_balance') evColor = '#ef4444';
        html += `
            <div style="padding: 0.6rem 0.8rem; background: #f8fafc; border-left: 3px solid ${evColor}; border-radius: 8px; font-size: 0.9rem;">
                <span style="font-weight: 700; color: ${evColor};">${ev}</span>
                <span style="color: var(--text-muted); margin-left: 0.5rem;">${ts}</span>
                ${acc ? '<span style="margin-left: 0.5rem;">Â· Account ' + acc + '</span>' : ''}
                ${tbl ? '<span style="margin-left: 0.5rem; font-size: 0.8rem; color: #94a3b8;">(' + tbl + ')</span>' : ''}
                ${recStr ? '<div style="font-size: 0.8rem; color: #64748b; margin-top: 0.35rem;">' + recStr + '</div>' : ''}
            </div>
        `;
    });

    html += '</div></div>';
    content.innerHTML = html;
    container.style.display = 'block';
};

// Healthcare Analysis Results - Case IDs + full sequence (same logic as banking), or legacy timeline
function showHealthcareAnalysisResults(profile) {
    const mainContent = document.getElementById('mainContent');
    const hcData = profile.healthcare_analysis;

    if (!hcData || !hcData.success) {
        mainContent.innerHTML = `
            <div style="max-width: 700px; margin: 0 auto; padding: 3rem; text-align: center;">
                <div style="font-size: 4rem; margin-bottom: 1.5rem;"></div>
                <h2 style="font-size: 2rem; margin-bottom: 1rem; color: #14B8A6;">Healthcare Database Detected</h2>
                <p style="color: var(--text-secondary); margin-bottom: 2rem;">
                    Unable to perform detailed analysis. ${hcData?.error || 'No date/timestamp columns found (excluding date of birth).'}
                </p>
                <button class="btn-secondary" onclick="showDomainSplitView()">â† Back to Database List</button>
            </div>
        `;
        return;
    }

    const caseDetails = hcData.case_details || [];
    const totalCases = hcData.total_cases || 0;
    const totalUsers = hcData.total_users || 0;
    const users = hcData.users || [];
    const explanations = hcData.explanations || [];
    const totalActivities = hcData.total_activities || 0;
    const unifiedFlowData = hcData.unified_flow_data;
    const HC_COLOR = '#14B8A6';
    const HC_BG = 'rgba(20,184,166,0.08)';
    const HC_BORDER = 'rgba(20,184,166,0.3)';

    if (caseDetails.length > 0 && unifiedFlowData && unifiedFlowData.case_paths && unifiedFlowData.case_paths.length > 0) {
        // Expose unified flow data globally for Single Case Flow filter
        window.currentHealthcareUnifiedFlowData = unifiedFlowData;
        var html = `
        <div style="padding: 2rem; overflow-y: auto; height: 100%;">
            <button class="btn-secondary" onclick="showDomainSplitView()" style="margin-bottom: 1rem;">â† Back</button>
            <h1 style="font-size: 2.5rem; margin-bottom: 0.5rem; color: ${HC_COLOR};"> Healthcare Case IDs</h1>
            <p style="color: var(--text-secondary); margin-bottom: 2rem; font-size: 1.1rem;">
                ${profile.database_name} â€¢ ${totalCases} Case ID(s) â€¢ ${totalUsers} patient(s) â€¢ ${totalActivities} activities
            </p>

            <section style="margin-bottom: 1.75rem;">
                <h2 style="font-size: 1.3rem; margin-bottom: 0.5rem; color: var(--text-primary);">Case Flow Filter (Single Case Diagram)</h2>
                <p style="color: var(--text-muted); margin-bottom: 0.75rem; font-size: 0.9rem;">
                    Click a Case ID chip to see a separate diagram only for that patient journey (shown under the merged diagram).
                </p>
                <div style="display: flex; flex-wrap: nowrap; gap: 0.5rem; overflow-x: auto; padding-bottom: 0.25rem;">
                    ${(function () {
                        var unified = unifiedFlowData || {};
                        var paths = (unified.case_paths || []);
                        var colorById = {};
                        paths.forEach(function (p) {
                            colorById[String(p.case_id)] = p.color || '${HC_COLOR}';
                        });
                        return (caseDetails || []).map(function (c) {
                            var cid = String(c.case_id);
                            var color = colorById[cid] || '${HC_COLOR}';
                            return '<button type="button" data-single-case-id="' + cid + '" ' +
                                   'style="cursor:pointer; border-radius:999px; border:1px solid ' + color + '; ' +
                                   'padding:0.3rem 0.7rem; font-size:0.8rem; background:#ffffff; color:' + color + '; ' +
                                   'display:inline-flex; align-items:center; gap:0.35rem; box-shadow:0 1px 2px rgba(15,23,42,0.08);">' +
                                   '<span style="width:14px; height:3px; border-radius:2px; background:' + color + ';"></span>' +
                                   '<span>Case ' + cid + '</span>' +
                                   '</button>';
                        }).join('');
                    })()}
                </div>
            </section>

            <section style="margin-bottom: 1.75rem;">
                <h2 style="font-size: 1.3rem; margin-bottom: 0.5rem; color: var(--text-primary);">User Flow Filter (Merged Case IDs)</h2>
                <p style="color: var(--text-muted); margin-bottom: 0.75rem; font-size: 0.9rem;">
                    Click a patient button below to see ALL Case IDs for that patient merged into one continuous flow diagram.
                </p>
                <div style="display: flex; flex-wrap: wrap; gap: 0.5rem; padding-bottom: 0.25rem;">
                    ${(users || []).map(function (u) {
            var userCases = caseDetails.filter(function (c) { return c.user_id === u; });
            var ids = userCases.map(function (c) { return c.case_id; });
            var caseCount = ids.length;
            return '<button type="button" data-user-merge-id="' + String(u).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;') + '" ' +
                   'style="cursor:pointer; border-radius:999px; border:1px solid ' + HC_COLOR + '; ' +
                   'padding:0.4rem 0.8rem; font-size:0.85rem; background:#ffffff; color:' + HC_COLOR + '; ' +
                   'display:inline-flex; align-items:center; gap:0.4rem; box-shadow:0 1px 2px rgba(15,23,42,0.08); font-weight:600;">' +
                   '<span style="font-size:0.9rem;">👤</span>' +
                   '<span>' + u + '</span>' +
                   '<span style="background:' + HC_COLOR + '; color:#ffffff; border-radius:999px; padding:0.1rem 0.4rem; font-size:0.75rem; font-weight:700;">' + caseCount + ' Case' + (caseCount !== 1 ? 's' : '') + '</span>' +
                   '</button>';
        }).join('')}
                </div>
            </section>

            ${renderMergedCaseFlowDiagram(unifiedFlowData)}

            <section style="margin-top: 1.75rem; margin-bottom: 2rem;">
                <div id="single-case-flow-container" style="border-top: 1px dashed ${HC_BORDER}; padding-top: 1.25rem;">
                    <button type="button"
                            onclick="window.toggleSingleCaseFlow(event)"
                            style="width: 100%; display: flex; justify-content: space-between; align-items: center; background: #ffffff; border-radius: 10px; border: 1px solid ${HC_BORDER}; padding: 0.55rem 0.8rem; cursor: pointer; font-size: 0.9rem; color: #0f172a; margin-bottom: 0.5rem;">
                        <span style="font-weight: 600;">Single Case Flow (Filtered)</span>
                        <span style="font-size: 0.9rem; color: #6b7280;">▼</span>
                    </button>
                    <div id="single-case-flow-body" style="display: none; margin-top: 0.25rem;">
                        <div id="single-case-flow-content" style="font-size: 0.85rem; color: #6b7280;">
                            Pick a Case ID chip above to see the event-to-event flow for that single Case ID only.
                        </div>
                    </div>
                </div>
            </section>

            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);"> How It Works</h2>
                <div style="background: ${HC_BG}; border: 1px solid ${HC_BORDER}; border-radius: 12px; padding: 1.25rem; margin-bottom: 2rem;">
                    <ul style="margin: 0; padding-left: 1.25rem; color: var(--text-primary); line-height: 1.8;">
                        ${(explanations || []).map(function (e) { return '<li>' + e + '</li>'; }).join('')}
                    </ul>
                </div>
            </section>

            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);"> Case ID Explanations</h2>
                <p style="color: var(--text-muted); margin-bottom: 1rem; font-size: 0.95rem;">
                    Each case is one patient journey. Click a case to see step-by-step details.
                </p>
                <div style="display: flex; flex-direction: column; gap: 1rem; margin-bottom: 2rem;">
                    ${caseDetails.map(function (c, i) {
            var seq = (c.event_sequence || []).join(' \u2192 ');
            var expl = c.explanation || 'Case ' + c.case_id + ': Patient ' + c.user_id + '.';
            var acts = c.activities || [];
            var stepsHtml = acts.slice(0, 6).map(function (a) {
                var ex = a.explanation ? (' \u2014 ' + a.explanation) : '';
                return '<div style="font-size: 0.8rem; color: #475569; margin-top: 0.3rem;"><strong style="color: #14B8A6;">' + (a.event || '') + '</strong>' + ex + '</div>';
            }).join('');
            return '<div style="background: #f8fafc; border: 1px solid ' + HC_BORDER + '; border-radius: 12px; padding: 1rem 1.25rem; cursor: pointer;" onclick="showHealthcareCaseDetails(' + i + ')">' +
                '<div style="display: flex; align-items: center; gap: 0.75rem; margin-bottom: 0.5rem;">' +
                '<span style="background: linear-gradient(135deg, #14B8A6, #0D9488); color: white; font-weight: 700; padding: 0.3rem 0.7rem; border-radius: 8px;">Case #' + c.case_id + '</span>' +
                '<span style="font-weight: 600; color: #14B8A6;">Patient ' + c.user_id + '</span>' +
                '<span style="font-size: 0.85rem; color: var(--text-muted);">' + (c.first_activity_timestamp || '') + ' \u2192 ' + (c.last_activity_timestamp || '') + '</span>' +
                '</div>' +
                '<p style="font-size: 0.9rem; color: var(--text-primary); margin: 0 0 0.5rem 0;"><strong>Steps:</strong> ' + seq + '</p>' +
                '<div style="font-size: 0.82rem; color: #64748b; margin: 0.5rem 0 0 0;">' + stepsHtml + '</div>' +
                '<p style="font-size: 0.8rem; color: var(--text-muted); margin-top: 0.5rem;">Click for full details \u2192</p>' +
                '</div>';
        }).join('')}
                </div>
            </section>

            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);"> Patients & Case IDs</h2>
                <div style="display: flex; flex-wrap: wrap; gap: 0.75rem; margin-bottom: 2rem;">
                    ${(users || []).map(function (u) {
            var userCases = caseDetails.filter(function (c) { return c.user_id === u; });
            var ids = userCases.map(function (c) { return c.case_id; });
            return '<div data-user-merge-id="' + String(u).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;') + '" style="cursor:pointer; background: #f8fafc; border: 1px solid var(--border); border-radius: 10px; padding: 1rem 1.25rem;"><strong style="color: #14B8A6;">' + u + '</strong><span style="color: var(--text-muted); margin-left: 0.5rem;">\u2192 Case IDs: ' + ids.join(', ') + ' (click to filter)</span></div>';
        }).join('')}
                </div>
            </section>

            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);"> Quick Select</h2>
                <div style="display: flex; flex-wrap: wrap; gap: 0.75rem;">
                    ${caseDetails.map(function (c, i) {
            return '<div class="healthcare-case-node" style="flex-shrink: 0; cursor: pointer; padding: 0.6rem 1rem; border-radius: 10px; background: linear-gradient(135deg, #14B8A6, #0D9488); color: white; font-weight: 700; font-size: 1rem; border: 2px solid rgba(255,255,255,0.3); box-shadow: 0 2px 8px rgba(20,184,166,0.3); transition: all 0.2s;" onclick="showHealthcareCaseDetails(' + i + ')" role="button" tabindex="0">Case #' + c.case_id + '</div>';
        }).join('')}
                </div>
            </section>

            <div id="healthcare-case-details" style="display: none;">
                <div id="healthcare-case-details-content"></div>
            </div>
        </div>
        `;
        mainContent.innerHTML = html;
        window.healthcareCaseDetails = caseDetails;
        return;
    }

    var nodes = hcData.diagram_nodes || [];
    var firstDate = hcData.first_date || '';
    var lastDate = hcData.last_date || '';
    var totalRecords = hcData.total_records || 0;

    if (nodes.length === 0) {
        mainContent.innerHTML = `
            <div style="max-width: 700px; margin: 0 auto; padding: 3rem; text-align: center;">
                <div style="font-size: 4rem; margin-bottom: 1.5rem;"></div>
                <h2 style="font-size: 2rem; margin-bottom: 1rem; color: #14B8A6;">No Date/Time Data</h2>
                <p style="color: var(--text-secondary); margin-bottom: 2rem;">
                    No date or timestamp columns found in healthcare tables (date of birth excluded).
                </p>
                <button class="btn-secondary" onclick="showDomainSplitView()">â† Back to Database List</button>
            </div>
        `;
        return;
    }

    var html = `
        <div style="padding: 2rem; overflow-y: auto; height: 100%;">
            <button class="btn-secondary" onclick="showDomainSplitView()" style="margin-bottom: 1rem;">â† Back</button>
            <h1 style="font-size: 2.5rem; margin-bottom: 0.5rem; color: #14B8A6;"> Healthcare Data Timeline</h1>
            <p style="color: var(--text-secondary); margin-bottom: 2rem; font-size: 1.1rem;">
                ${profile.database_name} â€¢ ${totalRecords} records â€¢ Click any box to see column explanations
            </p>
            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);"> Sorted Timeline (${firstDate} → ${lastDate})</h2>
                <p style="color: var(--text-muted); margin-bottom: 1.5rem; font-size: 0.95rem;">
                    Click any box to see column explanations (admission, appointment, discharge, lab, etc.) from your uploaded files.
                </p>
                <div style="background: linear-gradient(135deg, rgba(20,184,166,0.08), rgba(13,148,136,0.06)); border: 1px solid rgba(20,184,166,0.3); border-radius: 16px; padding: 1.5rem; overflow-x: auto;">
                    <div style="display: flex; align-items: flex-start; min-width: max-content; gap: 0; flex-wrap: nowrap;">
                        <div style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 70px; padding: 0.5rem;">
                            <div style="width: 14px; height: 14px; border-radius: 50%; background: #10b981; border: 2px solid #059669; margin-bottom: 0.3rem;"></div>
                            <div style="font-size: 0.75rem; font-weight: 700; color: #059669;">START</div>
                            <div style="font-size: 0.7rem; color: var(--text-muted);">${firstDate}</div>
                        </div>
                        <div style="flex: 1; min-width: 20px; height: 24px; border-bottom: 3px solid rgba(20,184,166,0.5); align-self: flex-start; margin-top: 6px;"></div>
    `;

    nodes.forEach(function (node, i) {
        var dateLabel = node.date ? node.date.split('-').slice(1).join('/') : '';
        var timeStr = node.time ? node.time.substring(0, 5) : '';
        var label = timeStr ? dateLabel + ' ' + timeStr : dateLabel;
        html += '<div class="healthcare-node" style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 72px; cursor: pointer; padding: 0.35rem;" onclick="showHealthcareNodeDetails(' + i + ')" role="button" tabindex="0" data-node-index="' + i + '">' +
            '<div style="width: 48px; min-height: 48px; padding: 0.4rem; border-radius: 10px; background: linear-gradient(135deg, #14B8A6, #0D9488); color: white; display: flex; flex-direction: column; align-items: center; justify-content: center; font-weight: 700; font-size: 0.85rem; border: 2px solid rgba(255,255,255,0.3); box-shadow: 0 2px 8px rgba(20,184,166,0.3); transition: all 0.2s;" onmouseover="this.style.transform=\'scale(1.08)\'; this.style.boxShadow=\'0 4px 12px rgba(20,184,166,0.5)\';" onmouseout="this.style.transform=\'scale(1)\'; this.style.boxShadow=\'0 2px 8px rgba(20,184,166,0.3)\';">' +
            '<div style="font-size: 1rem;">' + node.count + '</div><div style="font-size: 0.6rem; opacity: 0.9;">records</div></div>' +
            '<div style="font-size: 0.7rem; color: var(--text-primary); font-weight: 600; margin-top: 0.35rem; text-align: center; max-width: 80px; overflow: hidden; text-overflow: ellipsis;">' + label + '</div></div>' +
            (i < nodes.length - 1 ? '<div style="flex: 1; min-width: 16px; height: 24px; border-bottom: 3px solid rgba(20,184,166,0.5); align-self: flex-start; margin-top: 6px;"></div>' : '');
    });

    html += '<div style="flex: 1; min-width: 20px; height: 24px; border-bottom: 3px solid rgba(20,184,166,0.5); align-self: flex-start; margin-top: 6px;"></div>' +
        '<div style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 70px; padding: 0.5rem;">' +
        '<div style="width: 14px; height: 14px; border-radius: 50%; background: #ef4444; border: 2px solid #dc2626; margin-bottom: 0.3rem;"></div>' +
        '<div style="font-size: 0.75rem; font-weight: 700; color: #dc2626;">END</div><div style="font-size: 0.7rem; color: var(--text-muted);">' + lastDate + '</div></div></div></div></section>' +
        '<div id="healthcare-node-details" style="display: none;"><div id="healthcare-node-details-content"></div></div></div>';
    mainContent.innerHTML = html;
    window.healthcareDiagramNodes = nodes;
    window.healthcareFullData = hcData;
}

window.showHealthcareCaseDetails = function (caseIndex) {
    var container = document.getElementById('healthcare-case-details');
    var content = document.getElementById('healthcare-case-details-content');
    var cases = window.healthcareCaseDetails;
    if (!container || !content || !cases || caseIndex < 0 || caseIndex >= cases.length) return;
    var c = cases[caseIndex];
    var prevIdx = window.healthcareSelectedCaseIndex;
    if (prevIdx === caseIndex) {
        container.style.display = 'none';
        window.healthcareSelectedCaseIndex = null;
        return;
    }
    window.healthcareSelectedCaseIndex = caseIndex;
    var activities = c.activities || [];
    var seq = (c.event_sequence || []).join(' → ');
    var html = '<div style="background: var(--bg-card); border: 2px solid #14B8A6; border-radius: 12px; padding: 1.25rem; margin-top: 1rem; box-shadow: 0 4px 20px rgba(20,184,166,0.15);">' +
        '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">' +
        '<h3 style="font-size: 1.15rem; color: #14B8A6; margin: 0;">Case ID ' + c.case_id + ' Â· Patient ' + c.user_id + '</h3>' +
        '<button class="btn-secondary" onclick="showHealthcareCaseDetails(' + caseIndex + ')" style="padding: 0.3rem 0.6rem; font-size: 0.8rem;">âœ• Close</button></div>' +
        '<p style="color: var(--text-muted); font-size: 0.9rem; margin-bottom: 0.5rem;">' + c.first_activity_timestamp + ' → ' + c.last_activity_timestamp + ' Â· ' + activities.length + ' steps (in time order)</p>' +
        '<p style="color: var(--text-primary); font-size: 0.95rem; margin-bottom: 1rem; font-weight: 600;">Steps: ' + seq + '</p>' +
        '<p style="color: #475569; font-size: 0.88rem; margin-bottom: 1rem;">' + (c.explanation || '') + '</p>' +
        '<div style="max-height: 420px; overflow-y: auto; display: flex; flex-direction: column; gap: 0.5rem;">';
    activities.forEach(function (a, idx) {
        var ev = a.event || '';
        var ts = a.timestamp_str || '';
        if (a.date_only && ts) ts = ts.replace(/\s+00:00:00$/, '') + ' (Date only)';
        var tbl = a.table_name || '';
        var expl = a.explanation || '';
        var rec = a.raw_record || {};
        var recStr = Object.keys(rec).filter(function (k) { return rec[k]; }).map(function (k) { return k + ': ' + rec[k]; }).join(' \u00B7 ');
        html += '<div style="padding: 0.6rem 0.8rem; background: #f8fafc; border-left: 3px solid #14B8A6; border-radius: 8px; font-size: 0.9rem;">' +
            '<span style="font-weight: 700; color: #14B8A6;">' + ev + '</span>' +
            '<span style="color: var(--text-muted); margin-left: 0.5rem;">' + ts + '</span>' +
            (tbl ? '<span style="margin-left: 0.5rem; font-size: 0.8rem; color: #94a3b8;">(' + tbl + ')</span>' : '') +
            (expl ? '<div style="font-size: 0.85rem; color: var(--text-primary); margin-top: 0.4rem; font-weight: 500;">' + expl + '</div>' : '') +
            (recStr && !expl ? '<div style="font-size: 0.8rem; color: #64748b; margin-top: 0.35rem;">' + recStr + '</div>' : (recStr ? '<div style="font-size: 0.75rem; color: #94a3b8; margin-top: 0.25rem;">' + recStr + '</div>' : '')) +
            '</div>';
    });
    html += '</div></div>';
    content.innerHTML = html;
    container.style.display = 'block';
};

// Inline HTML onclick safety: ensure global function name resolves
function showHealthcareNodeDetails(nodeIndex) {
    return window.showHealthcareNodeDetails(nodeIndex);
}

// Show records when clicking a timeline node
window.showHealthcareNodeDetails = function (nodeIndex) {
    const container = document.getElementById('healthcare-node-details');
    const content = document.getElementById('healthcare-node-details-content');
    const nodes = window.healthcareDiagramNodes;
    if (!container || !content || !nodes || nodeIndex < 0 || nodeIndex >= nodes.length) return;

    const node = nodes[nodeIndex];
    const prevIdx = window.healthcareSelectedNodeIndex;
    if (prevIdx === nodeIndex) {
        container.style.display = 'none';
        window.healthcareSelectedNodeIndex = null;
        return;
    }
    window.healthcareSelectedNodeIndex = nodeIndex;

    const records = node.records || [];
    const dateStr = node.date || '';
    const timeStr = node.time || '';
    const tableNames = node.table_names || [];

    // Friendly, compact, step-by-step UI
    let html = `
        <div style="background: var(--bg-card); border: 2px solid #14B8A6; border-radius: 12px; padding: 1.25rem; margin-top: 1rem; box-shadow: 0 4px 20px rgba(20,184,166,0.15);">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                <h3 style="font-size: 1.15rem; color: #14B8A6; margin: 0;">… ${dateStr} ${timeStr || ''}</h3>
                <button class="btn-secondary" onclick="showHealthcareNodeDetails(${nodeIndex})" style="padding: 0.3rem 0.6rem; font-size: 0.8rem;">âœ• Close</button>
            </div>
            <p style="color: var(--text-muted); font-size: 0.9rem; margin-bottom: 1rem;">
                <strong>${records.length}</strong> record(s) from table(s): ${tableNames.join(', ')}
            </p>
            <div style="max-height: 520px; overflow-y: auto; display: flex; flex-direction: column; gap: 0.5rem;">
    `;

    records.forEach((r, idx) => {
        const rec = r.record || {};
        const purposes = r.column_purposes || {};
        const explanations = r.value_explanations || {};
        const dataFlow = r.data_flow_explanation || '';
        const workSummary = r.work_summary || '';
        const rowEventStory = r.row_event_story || '';
        const timeLogExplanation = r.time_log_explanation || '';
        const crossTableLinks = r.cross_table_links || [];
        const tableRole = r.table_workflow_role || {};
        const stayDuration = r.stay_duration || null;
        const hospitalDelay = r.hospital_delay || null;
        const fileName = r.file_name || (r.table_name ? r.table_name + '.csv' : '');
        const keys = Object.keys(rec);
        const pairs = keys.map(k => {
            const purpose = purposes[k];
            const purposeLabel = purpose ? purpose.purpose : k;
            const classification = purpose && purpose.column_classification ? purpose.column_classification : '';
            const classShort = classification ? (classification.indexOf('Primary') >= 0 ? 'PK' : classification.indexOf('Foreign') >= 0 ? 'FK' : classification.indexOf('Date column') >= 0 ? 'Date' : classification.indexOf('Timestamp') >= 0 ? 'Time' : classification.indexOf('Status') >= 0 ? 'Status' : classification.indexOf('Amount') >= 0 ? 'Amt' : classification.indexOf('Description') >= 0 ? 'Desc' : '') : '';
            const tag = classShort ? '<span style="font-size: 0.65rem; background: #e2e8f0; color: #475569; padding: 0.1rem 0.25rem; border-radius: 3px; margin-left: 0.2rem;">' + classShort + '</span>' : '';
            const val = explanations[k] !== undefined ? explanations[k] : (rec[k] || 'Not recorded');
            const isNull = !rec[k] || String(rec[k]).trim() === '' || val === 'Not recorded' || val === 'Empty or not recorded' || val === 'Not recorded or missing';
            const valStyle = isNull ? 'color: #94a3b8; font-style: italic;' : '';
            return '<strong title="' + (classification || k) + '">' + purposeLabel + '</strong>' + tag + ': <span style="' + valStyle + '">' + val + '</span>';
        }).join(' Â· ');
        const rowNum = (r.source_row_number !== undefined && r.source_row_number !== null) ? ('<span style="font-size: 0.75rem; color: var(--text-muted);">row ' + r.source_row_number + '</span>') : '';
        const fileLabel = fileName ? '<span style="font-size: 0.75rem; color: var(--text-muted);">' + fileName + '</span>' : '';
        const roleLabel = tableRole.role ? '<span style="font-size: 0.7rem; background: rgba(20,184,166,0.2); color: #0d9488; padding: 0.15rem 0.4rem; border-radius: 4px;">' + tableRole.role + '</span>' : '';
        const roleExplDiv = tableRole.role_explanation ? '<div style="font-size: 0.78rem; color: #64748b; margin-bottom: 0.35rem;">' + tableRole.role_explanation + '</div>' : '';
        const eventLine = (timeLogExplanation && rowEventStory)
            ? ('<div style="font-size: 0.9rem; font-weight: 600; color: #0f172a; margin-bottom: 0.3rem;">' + timeLogExplanation + ' â€” ' + rowEventStory + '</div>')
            : (rowEventStory ? ('<div style="font-size: 0.9rem; font-weight: 600; color: #0f172a; margin-bottom: 0.3rem;">' + rowEventStory + '</div>') : '') +
            (timeLogExplanation ? ('<div style="font-size: 0.82rem; color: #64748b;">' + timeLogExplanation + '</div>') : '');
        let linksDiv = '';
        if (crossTableLinks.length > 0) {
            linksDiv = '<div style="margin: 0.35rem 0 0; padding: 0.4rem 0.55rem; background: #f1f5f9; border-radius: 8px; font-size: 0.8rem;"><strong>Links (FK):</strong><ul style="margin: 0.25rem 0 0 1rem; padding: 0;">' +
                crossTableLinks.map(function (link) { return '<li><strong>' + link.column + '</strong> = ' + link.value + ' â†’ ' + link.link_explanation + '</li>'; }).join('') + '</ul></div>';
        }
        const workDiv = workSummary && !rowEventStory ? '<div style="font-size: 0.86rem; font-weight: 600; color: #0f172a; margin-bottom: 0.35rem;">' + workSummary + '</div>' : '';
        let stayDiv = '';
        if (stayDuration && stayDuration.explanation) {
            stayDiv = '<div style="margin: 0.5rem 0; padding: 0.5rem 0.7rem; background: rgba(20,184,166,0.1); border-left: 3px solid #14B8A6; border-radius: 6px; font-size: 0.85rem;">' +
                '<div style="font-weight: 600; color: #0f172a; margin-bottom: 0.2rem;">Discharge: ' + (stayDuration.discharge_time || stayDuration.discharge_time_short || '') + '</div>' +
                '<div style="color: var(--text-secondary);">' + stayDuration.explanation + '</div>' +
                '</div>';
        }
        let delayDiv = '';
        if (hospitalDelay && hospitalDelay.is_hospital_delay && hospitalDelay.explanation) {
            delayDiv = '<div style="margin: 0.5rem 0; padding: 0.5rem 0.7rem; background: rgba(239,68,68,0.1); border-left: 3px solid #ef4444; border-radius: 6px; font-size: 0.85rem;">' +
                '<div style="font-weight: 700; color: #dc2626; margin-bottom: 0.2rem;">Hospital delay</div>' +
                '<div style="color: var(--text-secondary);">' + hospitalDelay.explanation + '</div>' +
                '</div>';
        }
        const flowDiv = dataFlow ? '<div style="font-size: 0.8rem; color: var(--text-muted); padding-top: 0.35rem; border-top: 1px solid rgba(148,163,184,0.25);">' + dataFlow + '</div>' : '';
        // Small card + click-to-expand (avoid huge box)
        html += '<details style="background: #f8fafc; border: 1px solid var(--border); border-radius: 10px; padding: 0.55rem 0.7rem;">' +
            '<summary style="cursor: pointer; list-style: none; display: flex; align-items: center; justify-content: space-between; gap: 0.6rem;">' +
            '<div style="display: flex; align-items: center; gap: 0.45rem; flex-wrap: wrap;">' +
            '<span style="color: #14B8A6; font-weight: 800; font-size: 0.9rem;">' + r.table_name + '</span>' +
            roleLabel +
            (fileName ? '<span style="font-size: 0.75rem; color: var(--text-muted);">' + fileName + '</span>' : '') +
            rowNum +
            '</div>' +
            '<span style="font-size: 0.78rem; color: #64748b;">Click to expand</span>' +
            '</summary>' +
            (roleExplDiv || '') +
            '<div style="padding-top: 0.35rem;">' +
            eventLine + workDiv +
            (delayDiv || '') + (stayDiv || '') +
            '<div style="margin: 0.35rem 0 0.25rem; font-size: 0.82rem; color: #0f172a;"><strong>3) Column values (with meaning):</strong></div>' +
            '<div style="font-size: 0.82rem; color: var(--text-primary); line-height: 1.45;">' + (pairs || 'â€”') + '</div>' +
            linksDiv +
            (flowDiv || '') +
            '</div>' +
            '</details>';
    });

    html += `
            </div>
        </div>
    `;

    content.innerHTML = html;
    container.style.display = 'block';
};

// Healthcare Date Details Drill-Down - SIMPLIFIED EXPLANATIONS
window.showHealthcareDateDetails = function (dateIndex) {
    const container = document.getElementById('date-details-container');
    const content = document.getElementById('date-details-content');
    if (!container || !content) return;

    const isAppointment = window.healthcareIsAppointmentMode === true;

    // Toggle behaviour: same date click â†’ hide panel
    const prevSelected = window.healthcareSelectedDateIndex;
    if (prevSelected === dateIndex) {
        container.style.display = 'none';
        window.healthcareSelectedDateIndex = null;
        return;
    }
    window.healthcareSelectedDateIndex = dateIndex;

    const dateInfo = window.healthcareTimelineData[dateIndex];
    if (!dateInfo) return;

    // When healthcare explanation is opened, hide any other explanation panels (banking timelines)
    ['open-date-explanation-panel', 'login-explanation-panel', 'txn-explanation-panel'].forEach(function (id) {
        const el = document.getElementById(id);
        if (el) el.style.display = 'none';
    });

    const slotIcons = { Morning: 'ðŸŒ…', Afternoon: 'ðŸ•‘', Evening: 'ðŸŒ†', Night: 'ðŸŒ™' };
    const slotColors = { Morning: '#F59E0B', Afternoon: '#0EA5E9', Evening: '#8B5CF6', Night: '#3B82F6' };

    let html = `
        <div style="
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            width: 420px;
            max-width: 92vw;
            max-height: 70vh;
            overflow-y: auto;
            background: var(--bg-card);
            border: 2px solid #14B8A6;
            border-radius: 12px;
            padding: 1rem 1.1rem 1.1rem;
            box-shadow: 0 16px 40px rgba(15,23,42,0.4);
            z-index: 60;
        ">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.85rem;">
                <h2 style="font-size: 1.15rem; color: #14B8A6; margin: 0;">
                    … ${dateInfo.date} ${isAppointment ? '(Appointments)' : '(Visits)'}
                </h2>
                <button 
                    onclick="showHealthcareDateDetails(window.healthcareSelectedDateIndex);"
                    class="btn-secondary"
                    style="padding: 0.25rem 0.6rem; font-size: 0.75rem;">
                    âœ• Hide
                </button>
            </div>
            
            <p style="color: var(--text-primary); margin-bottom: 0.9rem; font-size: 0.85rem;">
                <strong>${dateInfo.visit_count}</strong> ${isAppointment ? 'appointment(s) are recorded for this date.' : 'patients visited on this date.'} We group times into Morning, Afternoon, Evening and Night so it is easy to read.
            </p>
            
            <!-- TIME SLOTS -->
            <section style="margin-bottom: 1.1rem;">
                <h3 style="font-size: 0.95rem; margin-bottom: 0.5rem; color: var(--text-primary);">â° ${isAppointment ? 'Appointment Times' : 'Visit Times'} (Time Slots)</h3>
                <p style="color: var(--text-muted); margin-bottom: 0.75rem; font-size: 0.8rem;">
                    Morning = 5â€“12, Afternoon = 12â€“17, Evening = 17â€“21, Night = 21â€“5.
                </p>
                <div style="display: grid; grid-template-columns: 1fr; gap: 0.6rem;">
    `;

    const timeSlots = dateInfo.time_slots || {};
    const slotDetails = dateInfo.slot_details || {};

    ['Morning', 'Afternoon', 'Evening', 'Night'].forEach((slot) => {
        const count = timeSlots[slot] || 0;
        const icon = slotIcons[slot] || 'â°';
        const color = slotColors[slot] || '#64748B';
        const detail = slotDetails[slot] || {};
        const topDept = detail.top_department || 'N/A';
        const topReason = detail.top_diagnosis || 'N/A';
        html += `
            <div style="background: #f8fafc; border-left: 3px solid ${color}; border-radius: 8px; padding: 0.6rem 0.7rem; display: flex; flex-direction: column; gap: 0.25rem; box-shadow: 0 1px 3px rgba(15,23,42,0.06);">
                <div style="display: flex; align-items: center; justify-content: space-between;">
                    <div style="display: flex; align-items: center; gap: 0.35rem;">
                        <span style="font-size: 1.2rem;">${icon}</span>
                        <span style="font-weight: 600; font-size: 0.9rem; color: ${color};">${slot}</span>
                    </div>
                    <div style="font-size: 0.95rem; font-weight: 700; color: var(--text-primary);">
                        ${count} <span style="font-size: 0.75rem; color: #64748b;">patients</span>
                    </div>
                </div>
                <div style="font-size: 0.8rem; color: #475569;">
                    <strong>Top reason:</strong> ${topReason}
                </div>
                <div style="font-size: 0.8rem; color: #475569;">
                    <strong>Top department:</strong> ${topDept}
                </div>
            </div>
        `;
    });

    html += `
                </div>
            </section>

            <!-- STAY SUMMARY (for admitted/discharged patients) -->
    `;

    const stay = dateInfo.stay_summary || null;
    if (stay && (stay.admissions || stay.discharges || (typeof stay.average_stay_days === 'number'))) {
        const admissions = stay.admissions || 0;
        const discharges = stay.discharges || 0;
        const avgStay = typeof stay.average_stay_days === 'number' ? `${stay.average_stay_days} day(s)` : 'N/A';
        const maxStay = typeof stay.max_stay_days === 'number' ? `${stay.max_stay_days} day(s)` : null;
        const minStay = typeof stay.min_stay_days === 'number' ? `${stay.min_stay_days} day(s)` : null;
        html += `
            <section style="margin-bottom: 1.1rem;">
                <h3 style="font-size: 0.95rem; margin-bottom: 0.5rem; color: var(--text-primary);"> Stay Duration (Admit &amp; Discharge)</h3>
                <p style="color: var(--text-primary); font-size: 0.85rem; margin-bottom: 0.4rem;">
                    <strong>${admissions}</strong> patient(s) were admitted on this date and <strong>${discharges}</strong> patient(s) were discharged.
                </p>
                <p style="color: var(--text-muted); font-size: 0.8rem; margin-bottom: 0.2rem;">
                    Average stay: <strong>${avgStay}</strong>${maxStay !== null ? ` Â· Longest stay: <strong>${maxStay}</strong>` : ''}${minStay !== null ? ` Â· Shortest stay: <strong>${minStay}</strong>` : ''}.
                </p>
                <p style="color: var(--text-muted); font-size: 0.8rem;">
                    We calculate stay length from <code>admission_date_time</code> to <code>discharge_date_time</code> for each patient, then compute average and range in days so you can quickly see how long patients stayed in the hospital.
                </p>
            </section>
        `;
    }

    html += `
            <!-- GENDER BREAKDOWN -->
            <section style="margin-bottom: 1.1rem;">
                <h3 style="font-size: 0.95rem; margin-bottom: 0.6rem; color: var(--text-primary);">ðŸ‘¥ Male / Female</h3>
    `;

    const gender = dateInfo.gender_breakdown || {};
    const maleCount = gender.male ?? null;
    const femaleCount = gender.female ?? null;
    const otherCount = gender.other ?? null;

    if (maleCount === null && femaleCount === null && otherCount === null) {
        html += `
                <p style="color: var(--text-muted); font-size: 0.8rem;">No gender column detected in this data.</p>
        `;
    } else {
        html += `
                <div style="display: flex; flex-direction: column; gap: 0.4rem;">
                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.45rem 0.7rem; background: #f9fafb; border-radius: 6px;">
                        <span style="font-size: 0.85rem; color: #0f172a; font-weight: 600;">Male</span>
                        <span style="font-size: 0.95rem; color: #2563eb; font-weight: 700;">${maleCount}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.45rem 0.7rem; background: #f9fafb; border-radius: 6px;">
                        <span style="font-size: 0.85rem; color: #0f172a; font-weight: 600;">Female</span>
                        <span style="font-size: 0.95rem; color: #ec4899; font-weight: 700;">${femaleCount}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.45rem 0.7rem; background: #f9fafb; border-radius: 6px;">
                        <span style="font-size: 0.85rem; color: #0f172a; font-weight: 600;">Other / Unknown</span>
                        <span style="font-size: 0.95rem; color: #6b7280; font-weight: 700;">${otherCount}</span>
                    </div>
                </div>
        `;
    }

    html += `
            </section>
            
            <!-- VISIT / APPOINTMENT LIST WITH TIME -->
            <section style="margin-bottom: 1.1rem;">
                <h3 style="font-size: 0.95rem; margin-bottom: 0.6rem; color: var(--text-primary);">ðŸ•’ Each ${isAppointment ? 'Appointment' : 'Visit'} (Time & Slot)</h3>
    `;

    const visits = (dateInfo.visits || []).slice(0, 60);
    if (!visits.length) {
        html += `
                <p style="color: var(--text-muted); font-size: 0.8rem;">Only date information was available for this day (no separate time column detected).</p>
        `;
    } else {
        // Detect which extra columns we actually have data for (patient, admission, discharge)
        const hasPatient = visits.some(v => v.patient_id);
        const hasAdmission = visits.some(v => v.admission_time);
        const hasDischarge = visits.some(v => v.discharge_time);

        html += `
                <div style="max-height: 180px; overflow-y: auto; border-radius: 6px; border: 1px solid var(--border); background: #f9fafb;">
                    <table style="width: 100%; border-collapse: collapse; font-size: 0.8rem;">
                        <thead>
                            <tr style="background: #e5e7eb;">
                                ${hasPatient ? '<th style="padding: 0.4rem 0.5rem; text-align: left;">Patient</th>' : ''}
                                <th style="padding: 0.4rem 0.5rem; text-align: left;">Time</th>
                                <th style="padding: 0.4rem 0.5rem; text-align: left;">Slot</th>
                                <th style="padding: 0.4rem 0.5rem; text-align: left;">Department</th>
                                <th style="padding: 0.4rem 0.5rem; text-align: left;">Reason</th>
                                ${hasAdmission ? '<th style="padding: 0.4rem 0.5rem; text-align: left;">Admission</th>' : ''}
                                ${hasDischarge ? '<th style="padding: 0.4rem 0.5rem; text-align: left;">Discharge</th>' : ''}
                            </tr>
                        </thead>
                        <tbody>
        `;
        visits.forEach(v => {
            const t = v.time || 'â€”';
            const s = v.time_slot || '';
            const d = v.department || '';
            const r = v.reason || '';
            const p = v.patient_id || '';
            const adm = v.admission_time || '';
            const dis = v.discharge_time || '';
            html += `
                            <tr style="border-top: 1px solid rgba(148,163,184,0.35);">
                                ${hasPatient ? `<td style="padding: 0.35rem 0.5rem; color: #0f172a;">${p}</td>` : ''}
                                <td style="padding: 0.35rem 0.5rem; font-weight: 600; color: #0f172a;">${t}</td>
                                <td style="padding: 0.35rem 0.5rem; color: #475569;">${s}</td>
                                <td style="padding: 0.35rem 0.5rem; color: #475569;">${d}</td>
                                <td style="padding: 0.35rem 0.5rem; color: #475569;">${r}</td>
                                ${hasAdmission ? `<td style="padding: 0.35rem 0.5rem; color: #475569;">${adm}</td>` : ''}
                                ${hasDischarge ? `<td style="padding: 0.35rem 0.5rem; color: #475569;">${dis}</td>` : ''}
                            </tr>
            `;
        });
        html += `
                        </tbody>
                    </table>
                </div>
        `;
    }

    // Friendly text explanations for each appointment/visit
    const explainItems = visits.filter(v => v.explanation).slice(0, 25);
    if (explainItems.length) {
        html += `
                <div style="margin-top: 0.7rem;">
                    <div style="font-size: 0.85rem; color: var(--text-primary); font-weight: 600; margin-bottom: 0.35rem;">Simple explanations (time-wise):</div>
                    <div style="display: flex; flex-direction: column; gap: 0.25rem;">
        `;
        explainItems.forEach(v => {
            const txt = v.explanation || '';
            html += `
                        <div style="font-size: 0.8rem; color: #475569; padding: 0.3rem 0.4rem; border-radius: 4px; background: #f8fafc;">
                            ${txt}
                        </div>
            `;
        });
        html += `
                    </div>
                </div>
        `;
    }

    html += `
            </section>
            
            <!-- DEPARTMENTS -->
            <section style="margin-bottom: 1.75rem;">
                <h3 style="font-size: 1.1rem; margin-bottom: 1rem; color: var(--text-primary);"> Departments</h3>
    `;

    const depts = dateInfo.departments || {};
    const sortedDepts = Object.entries(depts).sort((a, b) => b[1] - a[1]).slice(0, 8);

    if (sortedDepts.length > 0) {
        html += `<div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 0.75rem;">`;
        sortedDepts.forEach(([dept, count]) => {
            html += `
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.75rem 1rem; background: var(--bg-page); border-radius: 8px;">
                    <span style="font-weight: 500; color: var(--text-primary);">${dept}</span>
                    <span style="font-weight: 700; font-size: 1.1rem; color: #14B8A6;">${count}</span>
                </div>
            `;
        });
        html += `</div>`;
    } else {
        html += `<p style="color: var(--text-muted); font-size: 0.9rem;">No data</p>`;
    }

    html += `
            </section>
            
            <!-- DIAGNOSES -->
            <section>
                <h3 style="font-size: 1.1rem; margin-bottom: 1rem; color: var(--text-primary);">ðŸ’Š Reasons for Visit</h3>
    `;

    const diagnoses = dateInfo.diagnoses || {};
    const sortedDiagnoses = Object.entries(diagnoses).sort((a, b) => b[1] - a[1]).slice(0, 8);

    if (sortedDiagnoses.length > 0) {
        html += `<div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 0.75rem;">`;
        sortedDiagnoses.forEach(([diag, count]) => {
            html += `
                <div style="padding: 1rem; background: var(--bg-page); border: 1px solid var(--border); border-radius: 8px; text-align: center;">
                    <div style="font-weight: 600; color: var(--text-primary); margin-bottom: 0.5rem; font-size: 0.9rem;">
                        ${diag}
                    </div>
                    <div style="font-size: 1.6rem; font-weight: 700; color: #14B8A6;">${count}</div>
                </div>
            `;
        });
        html += `</div>`;
    } else {
        html += `<p style="color: var(--text-muted); font-size: 0.9rem;">No data</p>`;
    }

    html += `
            </section>
        </div>
    `;

    content.innerHTML = html;
    container.style.display = 'block';

    // Smooth scroll to details
    setTimeout(() => {
        container.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }, 100);
};


// Show Account Analysis Results - REMOVED (use Banking Case ID from domain split)
function showAccountAnalysisResults(data, dateColumn, idColumn) {
    const mainContent = document.getElementById('mainContent');
    if (!mainContent) return;
    mainContent.innerHTML = `
        <div style="padding: 3rem; text-align: center;">
            <p style="color: var(--text-muted); margin-bottom: 1rem;">Account analysis has been replaced with Banking Case ID analysis.</p>
            <button class="btn-secondary" onclick="showDomainSplitView()">â† Back to Databases</button>
        </div>
    `;
    return;
}

// Render Age Table - stub (legacy, use Banking Case ID)
window.renderAgeTable = function (filter) {
    const container = document.getElementById('ageTableContainer');
    if (container) container.innerHTML = '<div style="padding: 2rem; color: var(--text-muted); text-align: center;">Use Banking Case ID analysis from domain split.</div>';
};

// --- REMOVED: Old account analysis UI (proceedToAccountAnalysis, showAccountAnalysisResults, renderAgeTable full) ---

