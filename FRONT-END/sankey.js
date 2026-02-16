/**
 * Sankey Diagram Visualization - Optimized for All Domains
 * Generates professional Sankey diagrams from case activity data
 * No hardcoding - derives everything from user case activities and events
 * Works with: Banking, Healthcare, Insurance, Finance, Retail
 */

/**
 * Extract event sequence from case details
 * REUSES the existing case ID sequence logic from domain analyzers
 * Works with all domains: Banking, Healthcare, Insurance, Finance, Retail
 * event_sequence is already sorted by timestamp/order by the analyzers
 */
function extractEventSequence(caseItem) {
    if (!caseItem) {
        return [];
    }
    
    // Primary: use event_sequence if available (already sorted by case ID logic)
    // This is the sequence that was already built by the domain analyzers
    // (banking_analyzer, healthcare_analyzer, retail_analyzer, etc.) respecting case ID rules
    if (caseItem.event_sequence) {
        if (Array.isArray(caseItem.event_sequence)) {
            const events = caseItem.event_sequence
                .map(e => {
                    if (e === null || e === undefined) return null;
                    const str = String(e).trim();
                    // Filter out invalid event values
                    if (!str || str.length === 0 || 
                        str.toLowerCase() === 'none' || 
                        str.toLowerCase() === 'null' || 
                        str.toLowerCase() === 'unknown' || 
                        str.toLowerCase() === 'other') {
                        return null;
                    }
                    return str;
                })
                .filter(e => e !== null && e.length > 0); // Ensure non-empty
            
            if (events.length > 0) {
                // Debug: Log if we're using event_sequence
                if (window.currentDomain === 'Retail' && events.length > 0) {
                    console.log(`[Sankey] Retail case ${caseItem.case_id}: Using event_sequence (${events.length} events)`, events.slice(0, 3));
                }
                // Return as-is - already in correct order from case ID logic
                return events;
            } else if (window.currentDomain === 'Retail') {
                console.warn(`[Sankey] Retail case ${caseItem.case_id}: event_sequence exists but all events filtered out`, {
                    originalSequence: caseItem.event_sequence,
                    filteredCount: events.length
                });
            }
        } else if (window.currentDomain === 'Retail') {
            console.warn(`[Sankey] Retail case ${caseItem.case_id}: event_sequence is not an array`, {
                type: typeof caseItem.event_sequence,
                value: caseItem.event_sequence
            });
        }
    }
    
    // Fallback 1: extract from activities array (preserve order)
    // Activities are already sorted by timestamp in the analyzers
    // Retail domain stores events in activities[].event
    if (caseItem.activities && Array.isArray(caseItem.activities)) {
        const events = [];
        // Preserve the order from activities (already sorted by case ID logic)
        caseItem.activities.forEach((activity, idx) => {
            if (!activity) return;
            
            // Handle both object and direct event value
            let eventValue = null;
            if (typeof activity === 'object') {
                // Retail uses activity.event field - check multiple possible fields
                eventValue = activity.event || activity.event_name || activity.type || activity.name || activity.step;
            } else {
                eventValue = activity;
            }
            
            // Validate and clean event value
            if (eventValue !== null && eventValue !== undefined) {
                const str = String(eventValue).trim();
                // Filter out empty, null, unknown, other
                if (str && str.length > 0 && 
                    str.toLowerCase() !== 'none' && 
                    str.toLowerCase() !== 'null' && 
                    str.toLowerCase() !== 'unknown' && 
                    str.toLowerCase() !== 'other') {
                    events.push(str);
                } else if (window.currentDomain === 'Retail') {
                    console.warn(`[Sankey] Retail case ${caseItem.case_id}: Invalid event at activity index ${idx}:`, {
                        eventValue: eventValue,
                        activity: activity
                    });
                }
            } else if (window.currentDomain === 'Retail') {
                console.warn(`[Sankey] Retail case ${caseItem.case_id}: Missing event value at activity index ${idx}`, {
                    activityKeys: activity && typeof activity === 'object' ? Object.keys(activity) : 'not an object',
                    activity: activity
                });
            }
        });
        
        if (events.length > 0) {
            // Debug: Log if we're using activities fallback
            if (window.currentDomain === 'Retail') {
                console.log(`[Sankey] Retail case ${caseItem.case_id}: Using activities fallback (${events.length} events)`, events.slice(0, 3));
            }
            return events;
        } else if (window.currentDomain === 'Retail') {
            console.warn(`[Sankey] Retail case ${caseItem.case_id}: No events extracted from activities`, {
                activitiesLength: caseItem.activities.length,
                sampleActivity: caseItem.activities[0],
                allActivityKeys: caseItem.activities[0] ? Object.keys(caseItem.activities[0]) : 'no activities'
            });
        }
    }
    
    // Fallback 2: try to find events in other possible fields
    // Some domains might store events differently
    const possibleEventFields = ['events', 'event_list', 'sequence', 'steps'];
    for (const field of possibleEventFields) {
        if (caseItem[field] && Array.isArray(caseItem[field])) {
            const events = caseItem[field]
                .map(e => {
                    if (e === null || e === undefined) return null;
                    const str = String(e).trim();
                    return str || null;
                })
                .filter(e => e !== null && e.length > 0);
            
            if (events.length > 0) {
                if (window.currentDomain === 'Retail') {
                    console.log(`[Sankey] Retail case ${caseItem.case_id}: Using ${field} fallback (${events.length} events)`);
                }
                return events;
            }
        }
    }
    
    // Debug: Log if no events found
    if (window.currentDomain === 'Retail') {
        console.error(`[Sankey] Retail case ${caseItem.case_id}: No events found!`, {
            hasEventSequence: !!caseItem.event_sequence,
            hasActivities: !!caseItem.activities,
            activitiesLength: caseItem.activities ? caseItem.activities.length : 0,
            sampleKeys: Object.keys(caseItem).slice(0, 10)
        });
    }
    
    return [];
}

/**
 * Calculate event position weights based on actual sequence positions
 * REUSES the case ID sequence logic - events are already in correct order
 * from the domain analyzers (sorted by timestamp, respecting case ID rules)
 */
function calculateEventPositions(caseDetails) {
    const eventPositions = {};
    const eventCounts = {};
    
    // Use the event_sequence that's already built by case ID logic
    // This sequence respects the rules: sorted by timestamp, case ID ascending
    caseDetails.forEach(caseItem => {
        const eventSequence = extractEventSequence(caseItem);
        // eventSequence is already in correct order from case ID logic
        
        eventSequence.forEach((event, position) => {
            if (!event) return;
            
            const eventKey = String(event).trim();
            if (!eventKey) return;
            
            if (!eventPositions[eventKey]) {
                eventPositions[eventKey] = [];
                eventCounts[eventKey] = 0;
            }
            // Store position as-is (already correct from case ID sequence)
            eventPositions[eventKey].push(position);
            eventCounts[eventKey]++;
        });
    });
    
    // Calculate average position for each event
    // This preserves the sequence order from case ID logic
    const avgPositions = {};
    Object.keys(eventPositions).forEach(event => {
        const positions = eventPositions[event];
        if (positions.length > 0) {
            const avgPos = positions.reduce((sum, p) => sum + p, 0) / positions.length;
            avgPositions[event] = {
                average: avgPos,
                count: eventCounts[event],
                min: Math.min(...positions),
                max: Math.max(...positions)
            };
        }
    });
    
    return avgPositions;
}

/**
 * Format event name for display (proper capitalization, spacing)
 * No hardcoding - works with any event name from user data
 */
function formatEventName(eventName) {
    if (!eventName) return '';
    
    let formatted = String(eventName).trim();
    if (!formatted) return '';
    
    // Replace underscores and dashes with spaces
    formatted = formatted.replace(/[_-]/g, ' ');
    
    // Title case: capitalize first letter of each word
    formatted = formatted.split(/\s+/)
        .map(word => {
            if (!word) return '';
            const trimmed = word.trim();
            if (!trimmed) return '';
            
            // Handle acronyms (keep uppercase if all caps and short)
            if (trimmed === trimmed.toUpperCase() && trimmed.length <= 5 && /^[A-Z0-9]+$/.test(trimmed)) {
                return trimmed;
            }
            
            // Capitalize first letter, lowercase rest
            return trimmed.charAt(0).toUpperCase() + trimmed.slice(1).toLowerCase();
        })
        .filter(w => w) // Remove empty strings
        .join(' ');
    
    // Handle common acronyms (case-insensitive replacement)
    const acronymMap = {
        'Kyc': 'KYC',
        'Upi': 'UPI',
        'Id': 'ID',
        'Api': 'API',
        'Dob': 'DOB',
        'Emi': 'EMI',
        'Cr': 'CR',
        'Dr': 'DR'
    };
    
    Object.keys(acronymMap).forEach(key => {
        const regex = new RegExp(`\\b${key}\\b`, 'gi');
        formatted = formatted.replace(regex, acronymMap[key]);
    });
    
    return formatted.trim();
}

/**
 * Assign events to sequential stages based on their average position
 * Groups events that occur at similar positions into the same stage
 */
function assignEventsToStages(eventPositions, allEvents) {
    if (!allEvents || allEvents.size === 0) {
        return {};
    }
    
    const eventAvgPositions = {};
    allEvents.forEach(event => {
        const posInfo = eventPositions[event] || {};
        eventAvgPositions[event] = posInfo.average || 999;
    });
    
    if (Object.keys(eventAvgPositions).length === 0) {
        const stages = {};
        allEvents.forEach(event => {
            stages[event] = 0;
        });
        return stages;
    }
    
    const positions = Object.values(eventAvgPositions);
    const minPos = Math.min(...positions);
    const maxPos = Math.max(...positions);
    const posRange = maxPos - minPos;
    
    if (posRange === 0) {
        const stages = {};
        allEvents.forEach(event => {
            stages[event] = 0;
        });
        return stages;
    }
    
    const numStages = Math.max(3, Math.min(10, Math.floor(posRange) + 1));
    const eventStages = {};
    
    allEvents.forEach(event => {
        const avgPos = eventAvgPositions[event];
        const normalizedPos = (avgPos - minPos) / posRange;
        const stage = Math.min(Math.floor(normalizedPos * numStages), numStages - 1);
        eventStages[event] = stage;
    });
    
    return eventStages;
}

/**
 * Generate Sankey diagram data from case details
 * STRICT LINEAR PIPELINE STRUCTURE: Start Process → Layer 1 → Layer 2 → ... → End Process
 * 
 * CRITICAL PIPELINE RULES (MANDATORY):
 * 1. ALL Layer 1 events MUST connect from Start Process
 * 2. ALL final layer events MUST connect to End Process
 * 3. Events in same layer NEVER connect to each other (same-layer connections forbidden)
 * 4. Connections ONLY allowed: Layer N → Layer N+1 (strictly sequential)
 * 5. NO backward connections, NO skipped layers, NO same-layer connections
 * 
 * Structure: [Start Process] → [Layer 1 Events] → [Layer 2 Events] → ... → [End Process]
 * All flows are strictly forward (left to right), creating a clean linear pipeline.
 * 
 * @param {Array} caseDetails - Array of case detail objects (from any domain)
 * @returns {Object} Sankey data with nodes and links
 */
function generateSankeyData(caseDetails) {
    if (!caseDetails || caseDetails.length === 0) {
        return {
            success: false,
            error: 'No case details provided'
        };
    }

    // Calculate event positions to order them properly
    const eventPositions = calculateEventPositions(caseDetails);
    
    // Collect all unique events from user data (no hardcoding)
    // Works with all domains: Banking, Healthcare, Insurance, Finance, Retail
    const allEvents = new Set();
    let totalEventsFound = 0;
    const firstEvents = new Set();  // Events that appear first in sequences
    const lastEvents = new Set();   // Events that appear last in sequences
    
    caseDetails.forEach((caseItem, caseIndex) => {
        const eventSequence = extractEventSequence(caseItem);
        totalEventsFound += eventSequence.length;
        
        eventSequence.forEach(event => {
            const eventKey = String(event).trim();
            if (eventKey) {
                allEvents.add(eventKey);
            }
        });
        
        // Track first and last events for Start/End connections
        if (eventSequence.length > 0) {
            if (eventSequence[0]) {
                firstEvents.add(String(eventSequence[0]).trim());
            }
            // Always track last event (even for single-event sequences)
            if (eventSequence[eventSequence.length - 1]) {
                lastEvents.add(String(eventSequence[eventSequence.length - 1]).trim());
            }
        }
    });

    if (allEvents.size === 0) {
        const sampleCase = caseDetails[0] || {};
        const availableKeys = Object.keys(sampleCase).join(', ');
        const sampleEventSequence = extractEventSequence(sampleCase);
        
        let debugInfo = `Total cases: ${caseDetails.length}, Total events found: ${totalEventsFound}`;
        debugInfo += `\nSample case keys: ${availableKeys}`;
        debugInfo += `\nSample event_sequence: ${sampleCase.event_sequence ? JSON.stringify(sampleCase.event_sequence.slice(0, 3)) : 'not found'}`;
        debugInfo += `\nSample activities: ${sampleCase.activities ? `${sampleCase.activities.length} items` : 'not found'}`;
        debugInfo += `\nExtracted sequence: ${sampleEventSequence.length > 0 ? JSON.stringify(sampleEventSequence.slice(0, 3)) : 'empty'}`;
        
        return {
            success: false,
            error: `No events found in case details.\n\n` +
                   `Available fields: ${availableKeys}\n` +
                   `Expected fields: event_sequence or activities\n` +
                   `Debug: ${debugInfo}`
        };
    }
    
    // Assign events to sequential stages (columns) for linear flow
    const eventStages = assignEventsToStages(eventPositions, allEvents);
    
    // Sort events by stage first, then by average position within stage, then by frequency
    const eventList = Array.from(allEvents).sort((a, b) => {
        const stageA = eventStages[a] !== undefined ? eventStages[a] : 999;
        const stageB = eventStages[b] !== undefined ? eventStages[b] : 999;
        
        if (stageA !== stageB) {
            return stageA - stageB;  // Primary: stage
        }
        
        const posA = eventPositions[a] || { average: 999, count: 0 };
        const posB = eventPositions[b] || { average: 999, count: 0 };
        
        if (Math.abs(posA.average - posB.average) > 0.1) {
            return posA.average - posB.average;  // Secondary: position within stage
        }
        
        if (posA.count !== posB.count) {
            return posB.count - posA.count;  // Tertiary: frequency
        }
        
        return a.localeCompare(b);  // Quaternary: alphabetical
    });

    // Add Start Process and End Process nodes
    const START_NODE_NAME = 'Start Process';
    const END_NODE_NAME = 'End Process';
    
    // Node mapping will be created after nodes are built (see below)
    const nodeMap = {};
    nodeMap[START_NODE_NAME] = 0;

    // Count transitions (from -> to) across all cases
    const transitionCounts = {};
    const caseTransitions = {};
    const transitionDetails = {};
    const startToFirst = {};  // Start → First events
    const lastToEnd = {};      // Last events → End

    caseDetails.forEach(caseItem => {
        const caseId = caseItem.case_id;
        const userId = caseItem.user_id || caseItem.customer_id || caseItem.patient_id || 'unknown';
        const eventSequence = extractEventSequence(caseItem);

        if (eventSequence.length === 0) {
            return;
        }

        // Connect Start → First event
        if (eventSequence.length > 0) {
            const firstEvent = String(eventSequence[0]).trim();
            if (firstEvent) {
                startToFirst[firstEvent] = (startToFirst[firstEvent] || 0) + 1;
                const key = `${START_NODE_NAME}→${firstEvent}`;
                if (!caseTransitions[key]) {
                    caseTransitions[key] = [];
                    transitionDetails[key] = { cases: [] };
                }
                if (!caseTransitions[key].includes(caseId)) {
                    caseTransitions[key].push(caseId);
                    transitionDetails[key].cases.push({
                        caseId: caseId,
                        userId: userId
                    });
                }
            }
        }

        // Connect Last event → End (MANDATORY: all sequences must end)
        if (eventSequence.length > 0) {
            const lastEvent = String(eventSequence[eventSequence.length - 1]).trim();
            if (lastEvent) {
                lastToEnd[lastEvent] = (lastToEnd[lastEvent] || 0) + 1;
                const key = `${lastEvent}→${END_NODE_NAME}`;
                if (!caseTransitions[key]) {
                    caseTransitions[key] = [];
                    transitionDetails[key] = { cases: [] };
                }
                if (!caseTransitions[key].includes(caseId)) {
                    caseTransitions[key].push(caseId);
                    transitionDetails[key].cases.push({
                        caseId: caseId,
                        userId: userId
                    });
                }
            }
        }

        // Track transitions between events (Event → Event) - STRICT PIPELINE ONLY
        // Only allow connections from Layer N to Layer N+1 (no same-layer connections)
        if (eventSequence.length >= 2) {
            for (let i = 0; i < eventSequence.length - 1; i++) {
                const fromEvent = String(eventSequence[i]).trim();
                const toEvent = String(eventSequence[i + 1]).trim();

                if (fromEvent && toEvent) {
                    const fromStage = eventStages[fromEvent] !== undefined ? eventStages[fromEvent] : 999;
                    const toStage = eventStages[toEvent] !== undefined ? eventStages[toEvent] : 999;
                    
                    // STRICT PIPELINE: Only allow transitions to NEXT layer (toStage = fromStage + 1)
                    // This prevents same-layer connections and creates clean sequential flow
                    if (toStage === fromStage + 1) {
                        const key = `${fromEvent}→${toEvent}`;
                        if (!transitionCounts[key]) {
                            transitionCounts[key] = 0;
                            caseTransitions[key] = [];
                            transitionDetails[key] = { cases: [] };
                        }
                        transitionCounts[key]++;
                        if (!caseTransitions[key].includes(caseId)) {
                            caseTransitions[key].push(caseId);
                            transitionDetails[key].cases.push({
                                caseId: caseId,
                                userId: userId
                            });
                        }
                    }
                }
            }
        }
    });

    // Build nodes array: Start Process + Events organized by VERTICAL LAYERS + End Process
    // Structure: Start → Layer 1 (Stage 0 events vertically) → Layer 2 (Stage 1 events vertically) → ... → End
    const nodes = [];
    const maxStage = Math.max(...Object.values(eventStages), 0);
    
    // Start Process node (big box, stage -1, left side)
    nodes.push({
        id: 0,
        name: START_NODE_NAME,
        label: 'Start Process',
        originalName: START_NODE_NAME,
        position: -1,
        frequency: caseDetails.length,
        stage: -1,
        is_start: true,
        is_end: false
    });
    
    // Organize events into VERTICAL LAYERS by stage
    // Each stage becomes a vertical column with events stacked vertically
    const eventsByStage = {};
    eventList.forEach(event => {
        const stage = eventStages[event] !== undefined ? eventStages[event] : 0;
        if (!eventsByStage[stage]) {
            eventsByStage[stage] = [];
        }
        eventsByStage[stage].push(event);
    });
    
    // Sort events within each stage by position, then add to nodes array
    // This creates vertical columns: Layer 1 (Stage 0), Layer 2 (Stage 1), etc.
    let nodeIdCounter = 1;
    const eventNodeMap = {}; // Map: event_name -> new_node_id
    
    // Process stages in order (0, 1, 2, ...)
    for (let stage = 0; stage <= maxStage; stage++) {
        if (eventsByStage[stage]) {
            // Sort events within this stage by position for vertical stacking
            const stageEvents = eventsByStage[stage].sort((a, b) => {
                const posA = eventPositions[a] || { average: 999 };
                const posB = eventPositions[b] || { average: 999 };
                return posA.average - posB.average;
            });
            
            // Add events in this stage (they'll be stacked vertically)
            stageEvents.forEach(event => {
                const posInfo = eventPositions[event] || {};
                const formattedLabel = formatEventName(event);
                const displayLabel = formattedLabel && formattedLabel.trim() ? formattedLabel.trim() : String(event).trim() || `Event ${nodeIdCounter}`;
                
                nodes.push({
                    id: nodeIdCounter,
                    name: event,
                    label: displayLabel,
                    originalName: event,
                    position: posInfo.average || nodeIdCounter,
                    frequency: posInfo.count || 0,
                    stage: stage,
                    layer: stage + 1, // Layer number (1, 2, 3, ...)
                    is_start: false,
                    is_end: false
                });
                
                eventNodeMap[event] = nodeIdCounter;
                nodeIdCounter++;
            });
        }
    }
    
    // End Process node (big box, final stage, right side)
    const endNodeId = nodeIdCounter;
    nodes.push({
        id: endNodeId,
        name: END_NODE_NAME,
        label: 'End Process',
        originalName: END_NODE_NAME,
        position: maxStage + 1,
        frequency: caseDetails.length,
        stage: maxStage + 1,
        layer: maxStage + 2,
        is_start: false,
        is_end: true
    });

    // Build links array (flows between nodes)
    const links = [];
    
    // Update nodeMap with actual node IDs from the reorganized nodes array
    // nodeMap maps event_name -> node_id (for links)
    nodes.forEach(node => {
        if (!node.is_start && !node.is_end) {
            nodeMap[node.name] = node.id;
        }
    });
    nodeMap[END_NODE_NAME] = endNodeId;
    
    // ============================================================================
    // OPTIMIZED PIPELINE CONNECTIONS
    // ============================================================================
    // Links: Start Process → ALL Layer 1 Events (MANDATORY - FIXED)
    // CRITICAL: Start MUST connect to ALL Layer 1 (stage 0) events
    // Even if Layer 1 events connect to Layer 2, they MUST still show Start → Layer 1 connection
    // This ensures Layer 1 is always visible and properly connected from Start
    const layer1Events = eventList.filter(event => {
        const eventStage = eventStages[event] !== undefined ? eventStages[event] : 999;
        return eventStage === 0 && nodeMap[event] !== undefined;
    });
    
    // Count how many times each Layer 1 event should connect from Start
    // CRITICAL: Use actual first-event counts, but ensure ALL Layer 1 events show connection
    // This ensures Layer 1 events always show Start → Layer 1, even if they also connect to Layer 2
    const layer1EventCounts = {};
    layer1Events.forEach(event => {
        // Primary: Use count from sequences where this event appears first
        let count = startToFirst[event] || 0;
        
        // If no first-event count, check if event appears in any sequence
        // This ensures standalone Layer 1 events still show Start connection
        if (count === 0) {
            caseDetails.forEach(caseItem => {
                const seq = extractEventSequence(caseItem);
                if (seq.length > 0 && String(seq[0]).trim() === event) {
                    count++;
                }
            });
        }
        
        // Ensure minimum count of 1 for visibility (all Layer 1 events must show)
        layer1EventCounts[event] = count > 0 ? count : 1;
    });
    
    // Connect ALL Layer 1 events from Start Process
    layer1Events.forEach(event => {
        const key = `${START_NODE_NAME}→${event}`;
        const detail = transitionDetails[key] || { cases: [] };
        const connectionValue = layer1EventCounts[event] || 1;
        
        links.push({
            source: 0,  // Start Process
            target: nodeMap[event],
            value: connectionValue,
            label: String(connectionValue),
            from: START_NODE_NAME,
            to: event,
            fromFormatted: 'Start Process',
            toFormatted: formatEventName(event),
            fromStage: -1,
            toStage: 0, // Always Layer 1
            caseIds: (caseTransitions[key] || []).sort((a, b) => a - b),
            cases: detail.cases || []
        });
    });
    
    // Links: Event → Event (STRICT PIPELINE: only sequential layer connections - FIXED)
    // CRITICAL RULE: Only connect Layer N → Layer N+1
    // NO same-layer connections (fromStage === toStage) - events in same layer NEVER connect
    // NO backward connections (toStage < fromStage) - flow is strictly forward
    // NO skipped layers (toStage > fromStage + 1) - must be sequential
    // CRITICAL FIX: Events in layers BEFORE final layer MUST connect to final layer
    // Example: If maxStage = 2, then Layer 1 (stage 0) → Layer 2 (stage 1) → Final Layer (stage 2)
    // Events in final layer (fromStage === maxStage) do NOT connect to other events (they connect to End)
    Object.keys(transitionCounts).forEach(key => {
        const [fromEvent, toEvent] = key.split('→');
        if (nodeMap[fromEvent] !== undefined && nodeMap[toEvent] !== undefined) {
            const fromStage = eventStages[fromEvent] !== undefined ? eventStages[fromEvent] : 999;
            const toStage = eventStages[toEvent] !== undefined ? eventStages[toEvent] : 999;
            
            // STRICT PIPELINE: Only allow connections to next sequential layer (toStage === fromStage + 1)
            // This ensures: NO same-layer connections, NO backward flow, NO skipped layers
            // CRITICAL FIX: Ensure events in layers BEFORE final layer connect to final layer
            // Flow: Layer 0 → Layer 1 → ... → Layer (maxStage-1) → Layer maxStage (final)
            // Final layer events (fromStage === maxStage) do NOT connect to other events (they connect to End)
            if (toStage === fromStage + 1 && fromStage >= 0 && toStage <= maxStage) {
                // CRITICAL: Allow connection if fromStage is before final layer (fromStage < maxStage)
                // This ensures: Layer (maxStage - 1) → Layer maxStage connections work properly
                // Examples:
                // - If maxStage = 2: Layer 0 → Layer 1 (0 < 2 ✓), Layer 1 → Layer 2 (1 < 2 ✓)
                // - Final layer (fromStage = 2): 2 < 2 ✗, so doesn't connect to other events (connects to End)
                if (fromStage < maxStage) {
                    const detail = transitionDetails[key] || { cases: [] };
                    links.push({
                        source: nodeMap[fromEvent],
                        target: nodeMap[toEvent],
                        value: transitionCounts[key],
                        label: String(transitionCounts[key]),
                        from: fromEvent,
                        to: toEvent,
                        fromFormatted: formatEventName(fromEvent),
                        toFormatted: formatEventName(toEvent),
                        fromStage: fromStage,
                        toStage: toStage,
                        caseIds: (caseTransitions[key] || []).sort((a, b) => a - b),
                        cases: detail.cases || []
                    });
                }
            }
        }
    });
    
    // Links: ALL Final Layer Events → End Process (MANDATORY - FIXED)
    // CRITICAL: Only events from the final layer (maxStage) connect to End
    // Events in layers BEFORE final layer do NOT connect to End
    // ALL events in final layer MUST connect to End Process
    // This ensures complete pipeline - every final layer event connects to End, nothing else
    
    // First, identify ALL events that are in the final layer (maxStage)
    const finalLayerEvents = [];
    eventList.forEach(event => {
        const eventStage = eventStages[event] !== undefined ? eventStages[event] : -1;
        // Only include events that are in the final layer (maxStage)
        // Events in layers before final layer (eventStage < maxStage) are excluded
        if (eventStage === maxStage && nodeMap[event] !== undefined) {
            finalLayerEvents.push(event);
        }
    });
    
    // Count how many times each final layer event should connect to End
    // CRITICAL: Use actual last-event counts, but ensure ALL final layer events show connection
    const finalLayerEventCounts = {};
    finalLayerEvents.forEach(event => {
        // Primary: Use count from sequences where this event appears last
        let count = lastToEnd[event] || 0;
        
        // If no last-event count, check if event appears as last in any sequence
        // This ensures standalone final layer events still show End connection
        if (count === 0) {
            caseDetails.forEach(caseItem => {
                const seq = extractEventSequence(caseItem);
                if (seq.length > 0 && String(seq[seq.length - 1]).trim() === event) {
                    count++;
                }
            });
        }
        
        // Also count how many times this event appears in final layer across all sequences
        // This ensures proper connection count even if event appears multiple times
        if (count === 0) {
            caseDetails.forEach(caseItem => {
                const seq = extractEventSequence(caseItem);
                seq.forEach((seqEvent, idx) => {
                    const seqEventStage = eventStages[String(seqEvent).trim()] !== undefined ? 
                                        eventStages[String(seqEvent).trim()] : -1;
                    if (String(seqEvent).trim() === event && seqEventStage === maxStage) {
                        count++;
                    }
                });
            });
        }
        
        // Ensure minimum count of 1 for visibility (all final layer events must show)
        finalLayerEventCounts[event] = count > 0 ? count : 1;
    });
    
    // Connect ALL final layer events to End Process (MANDATORY)
    // Only final layer events connect to End - no events from before layers
    finalLayerEvents.forEach(event => {
        const eventStage = eventStages[event] !== undefined ? eventStages[event] : -1;
        
        // Double-check: Only connect if event is in final layer
        if (eventStage === maxStage && nodeMap[event] !== undefined) {
            const key = `${event}→${END_NODE_NAME}`;
            const detail = transitionDetails[key] || { cases: [] };
            const connectionValue = finalLayerEventCounts[event] || 1;
            
            links.push({
                source: nodeMap[event],
                target: endNodeId,  // End Process
                value: connectionValue,
                label: String(connectionValue),
                from: event,
                to: END_NODE_NAME,
                fromFormatted: formatEventName(event),
                toFormatted: 'End Process',
                fromStage: maxStage, // Always final layer
                toStage: maxStage + 1,
                caseIds: (caseTransitions[key] || []).sort((a, b) => a - b),
                cases: detail.cases || []
            });
        }
    });

    // Calculate statistics
    const totalCases = caseDetails.length;
    const totalTransitions = Object.values(transitionCounts).reduce((sum, val) => sum + val, 0) + 
                            Object.values(startToFirst).reduce((sum, val) => sum + val, 0) + 
                            Object.values(lastToEnd).reduce((sum, val) => sum + val, 0);
    const uniquePaths = Object.keys(transitionCounts).length + Object.keys(startToFirst).length + Object.keys(lastToEnd).length;
    const uniqueUsers = new Set(
        caseDetails.map(c => c.user_id || c.customer_id || c.patient_id || 'unknown')
    ).size;
    
    const numStages = maxStage + 3;  // +3 for Start, events, End
    const eventsPerStage = {};
    eventsPerStage[-1] = 1;  // Start Process
    eventsPerStage[maxStage + 1] = 1;  // End Process
    Object.values(eventStages).forEach(stage => {
        eventsPerStage[stage] = (eventsPerStage[stage] || 0) + 1;
    });

    return {
        success: true,
        nodes: nodes,
        links: links,
        metadata: {
            totalCases: totalCases,
            totalTransitions: totalTransitions,
            uniquePaths: uniquePaths,
            uniqueEvents: eventList.length,
            uniqueUsers: uniqueUsers,
            numStages: numStages,
            eventsPerStage: eventsPerStage,
            linearFlow: true,
            hasStartEnd: true
        }
    };
}

/**
 * Get domain-specific explanation text
 * No hardcoding - generic explanations that work for all domains
 */
function getDomainExplanation(domain) {
    const domainLower = (domain || '').toLowerCase();
    
    // Generic structure - clean and simple explanations
    const baseExplanation = {
        title: `Process Pipeline`,
        description: `Linear pipeline visualization showing sequential process flow. Events organized into layers flow strictly left-to-right: Start → Layer 1 → Layer 2 → Layer 3 → ... → End. Same-layer events do not connect - only sequential layer-to-layer connections shown.`,
        examples: []
    };
    
    // Add domain-specific examples if available (but don't hardcode event names)
    const domainExamples = {
        'banking': [
            'Shows how banking transactions flow through different stages',
            'Visualizes login, transactions, and logout patterns'
        ],
        'retail': [
            'Shows process flow from browsing to delivery',
            'Visualizes order placement and fulfillment processes'
        ],
        'healthcare': [
            'Shows workflow processes through hospital systems',
            'Visualizes registration, treatment, and discharge patterns'
        ],
        'insurance': [
            'Shows insurance processes from registration to claims',
            'Visualizes policy lifecycle and claim workflows'
        ],
        'finance': [
            'Shows financial service workflows',
            'Visualizes account, loan, and transaction patterns'
        ],
        'hr': [
            'Shows HR processes from recruitment to exit',
            'Visualizes employee lifecycle: onboarding, attendance, payroll, performance, training'
        ]
    };
    
    if (domainExamples[domainLower]) {
        baseExplanation.examples = domainExamples[domainLower];
    }
    
    return baseExplanation;
}

/**
 * Render Sankey diagram using Plotly
 * @param {string} containerId - ID of the container div
 * @param {Object} sankeyData - Sankey data from generateSankeyData
 * @param {Object} options - Optional styling options
 */
function renderSankeyDiagram(containerId, sankeyData, options = {}) {
    if (!sankeyData.success) {
        document.getElementById(containerId).innerHTML = 
            `<div style="padding: 2rem; text-align: center; color: #ef4444;">
                <p style="font-size: 1.1rem; margin-bottom: 0.5rem;"><strong>Error:</strong></p>
                <p>${sankeyData.error || 'Failed to generate Sankey diagram'}</p>
            </div>`;
        return;
    }

    const { nodes, links, metadata } = sankeyData;
    const domain = options.domain || 'General';

    // Prepare data for Plotly Sankey
    // CRITICAL: Ensure labels are properly set - Plotly Sankey requires non-empty labels
    // Use formatted label, fallback to name, then originalName
    const plotlyNodeLabels = nodes.map((n, idx) => {
        // Try multiple sources for label
        let label = n.label || n.name || n.originalName;
        
        // Ensure label is a non-empty string
        if (!label || typeof label !== 'string') {
            label = String(label || '').trim();
        } else {
            label = label.trim();
        }
        
        // Final fallback if still empty
        if (!label || label.length === 0) {
            label = `Event ${idx + 1}`;
            console.warn(`[Sankey] Empty label at index ${idx}, using fallback. Node data:`, n);
        }
        
        return label;
    });
    
    // Debug: Verify all labels are set (especially important for Retail)
    console.log('[Sankey] Plotly node labels (first 10):', plotlyNodeLabels.slice(0, 10));
    console.log('[Sankey] Total labels:', plotlyNodeLabels.length);
    console.log('[Sankey] All labels valid:', plotlyNodeLabels.every(l => l && typeof l === 'string' && l.length > 0));
    const emptyLabels = plotlyNodeLabels.filter(l => !l || l.length === 0);
    if (emptyLabels.length > 0) {
        console.error(`[Sankey] ERROR: ${emptyLabels.length} empty labels found!`, emptyLabels);
        // Fix empty labels
        plotlyNodeLabels.forEach((label, idx) => {
            if (!label || label.length === 0) {
                plotlyNodeLabels[idx] = nodes[idx].name || `Event ${idx + 1}`;
            }
        });
    }
    
    // Final validation: ensure all labels are strings and non-empty
    const finalLabels = plotlyNodeLabels.map((label, idx) => {
        if (!label || typeof label !== 'string' || label.trim().length === 0) {
            const fallback = nodes[idx].name || nodes[idx].originalName || `Event ${idx + 1}`;
            console.warn(`[Sankey] Fixed invalid label at index ${idx}:`, { original: label, fallback });
            return String(fallback).trim() || `Event ${idx + 1}`;
        }
        return label;
    });
    
    console.log('[Sankey] Final validated labels (first 10):', finalLabels.slice(0, 10));
    
    // Optimized color scheme - Professional energy flow style
    // Vibrant, distinct colors for each stage with better visual hierarchy
    const getStageColor = (stage, numStages, isStart, isEnd, frequency) => {
        // Special colors for Start and End nodes (prominent and clear)
        if (isStart) return '#10B981'; // Vibrant green for Start Process
        if (isEnd) return '#EF4444';   // Vibrant red for End Process
        
        if (numStages === 0) return '#3B82F6';
        
        // Enhanced color palette - Energy flow inspired
        // Rich, saturated colors that create clear visual distinction
        const colorPalette = [
            '#3B82F6', // Blue - Layer 1
            '#8B5CF6', // Purple - Layer 2
            '#EC4899', // Pink - Layer 3
            '#F59E0B', // Orange - Layer 4
            '#10B981', // Green - Layer 5
            '#06B6D4', // Cyan - Layer 6
            '#6366F1', // Indigo - Layer 7
            '#F97316', // Orange-red - Layer 8
            '#14B8A6', // Teal - Layer 9
            '#A855F7'  // Violet - Layer 10
        ];
        
        // Use palette with cycling for many stages
        if (stage < colorPalette.length) {
            return colorPalette[stage];
        }
        
        // Fallback to HSL gradient for stages beyond palette
        const hue = 220 + (stage / Math.max(1, numStages - 1)) * 140; // Blue to Red gradient
        const saturation = 70 + (frequency > 10 ? 10 : 0); // More saturated for frequent nodes
        const lightness = 50 + (frequency > 10 ? 5 : 0); // Slightly brighter for frequent nodes
        return `hsl(${hue}, ${saturation}%, ${lightness}%)`;
    };
    
    const numStages = metadata.numStages || metadata.num_stages || 1;
    const hasStartEnd = metadata.hasStartEnd || metadata.has_start_end || false;
    const maxStage = Math.max(...nodes.map(n => n.stage !== undefined ? n.stage : 0), 0);
    
    // Calculate explicit X and Y positions for structured vertical columns
    // Group nodes by stage to create vertical columns
    const nodesByStage = {};
    nodes.forEach((n, idx) => {
        const stage = n.stage !== undefined ? n.stage : (n.is_start ? -1 : (n.is_end ? maxStage + 1 : 0));
        if (!nodesByStage[stage]) {
            nodesByStage[stage] = [];
        }
        nodesByStage[stage].push({ node: n, index: idx });
    });
    
    // Calculate X positions (horizontal columns) - evenly spaced from 0 to 1
    const stageList = Object.keys(nodesByStage).map(s => parseInt(s)).sort((a, b) => a - b);
    const xPositions = {};
    stageList.forEach((stage, idx) => {
        // Start at 0.05, end at 0.95, evenly distribute columns
        const xPos = stageList.length === 1 ? 0.5 : 0.05 + (idx / (stageList.length - 1)) * 0.9;
        xPositions[stage] = xPos;
    });
    
    // Calculate Y positions for each stage (vertical stacking within each column)
    const stageYPositions = {}; // stage -> array of y positions for nodes in that stage
    
    stageList.forEach(stage => {
        const stageNodes = nodesByStage[stage];
        
        // Sort nodes within stage by frequency (descending) for better visual order
        stageNodes.sort((a, b) => {
            const freqA = a.node.frequency || 0;
            const freqB = b.node.frequency || 0;
            if (freqA !== freqB) return freqB - freqA;
            return a.node.position - b.node.position;
        });
        
        // Calculate Y positions - optimized distribution with better spacing
        const yPositions = [];
        stageNodes.forEach((item, idx) => {
            const totalInStage = stageNodes.length;
            let yPos;
            if (totalInStage === 1) {
                yPos = 0.5; // Center if only one node
            } else {
                // Improved spacing: more padding, better distribution
                // Use 0.08 to 0.92 range for better visual balance
                const spacing = 0.84 / (totalInStage - 1);
                yPos = 0.08 + (idx * spacing);
            }
            yPositions.push({ index: item.index, yPos: yPos });
        });
        
        stageYPositions[stage] = yPositions;
    });
    
    // Build nodeX and nodeY arrays matching the nodes array order
    const nodeX = [];
    const nodeY = [];
    const nodeValues = [];
    
    nodes.forEach((n, idx) => {
        const stage = n.stage !== undefined ? n.stage : (n.is_start ? -1 : (n.is_end ? maxStage + 1 : 0));
        const xPos = xPositions[stage];
        
        // Find the Y position for this node within its stage
        const stageYPos = stageYPositions[stage];
        const nodeYEntry = stageYPos.find(entry => entry.index === idx);
        const yPos = nodeYEntry ? nodeYEntry.yPos : 0.5;
        
        nodeX.push(xPos);
        nodeY.push(yPos);
        
        // Calculate node value (size) - Optimized for energy flow style
        // Node size proportional to flow magnitude for better visual impact
        if (hasStartEnd && (n.is_start || n.is_end)) {
            // Start/End nodes: larger, more prominent
            const totalCases = metadata.totalCases || metadata.total_cases || 1;
            nodeValues.push(Math.max(totalCases, n.frequency || 0) * 2.5);
        } else {
            // Regular nodes: size based on flow volume
            const nodeId = n.id;
            const incomingFlow = links.filter(l => l.target === nodeId)
                .reduce((sum, l) => sum + (l.value || 0), 0);
            const outgoingFlow = links.filter(l => l.source === nodeId)
                .reduce((sum, l) => sum + (l.value || 0), 0);
            const totalFlow = Math.max(incomingFlow, outgoingFlow, n.frequency || 1);
            // Enhanced scaling: larger nodes for higher flow
            nodeValues.push(Math.max(totalFlow * 1.5, n.frequency || 1, 2));
        }
    });
    
    const plotlyNodes = {
        label: finalLabels, // Use validated labels - CRITICAL for displaying event names
        x: nodeX, // Explicit X positions for structured columns
        y: nodeY, // Explicit Y positions for vertical stacking
        color: nodes.map((n, idx) => {
            // Use stage-based coloring for linear flow visualization
            const stage = n.stage !== undefined ? n.stage : Math.floor(idx / Math.max(1, nodes.length / numStages));
            const isStart = n.is_start === true;
            const isEnd = n.is_end === true;
            const frequency = n.frequency || 0;
            
            // Use domain-specific colors if provided, but apply stage-based tinting
            if (options.domainColors && options.domainColors.length > 0 && !isStart && !isEnd) {
                const baseColor = options.domainColors[idx % options.domainColors.length];
                // Apply slight tinting based on stage for visual grouping
                return baseColor;
            }
            // Optimized stage-based color with frequency consideration
            return getStageColor(stage, numStages, isStart, isEnd, frequency);
        }),
        pad: 30, // Optimized padding for better visual separation
        thickness: 35, // Thicker nodes for better visibility and impact
        line: {
            color: 'rgba(0,0,0,0.15)', // Subtle border for professional look
            width: 2 // Slightly thicker border for definition
        },
        hovertemplate: '<b style="font-size: 14px; color: #0f172a;">%{label}</b><br>' +
                       '<span style="color: #64748b;">%{customdata[3]}</span><br>' +
                       '<span style="color: #475569;">Layer: <b>%{customdata[2]}</b></span><br>' +
                       '<span style="color: #475569;">Frequency: <b>%{customdata[1]}</b> cases</span>' +
                       '<extra></extra>',
        customdata: nodes.map(n => [
            n.position || 0, 
            n.frequency || 0,
            n.stage !== undefined ? (n.stage === -1 ? 'Start' : n.is_end ? 'End' : `Layer ${n.stage + 1}`) : 'Unknown',
            (n.is_start ? 'Start Process' : n.is_end ? 'End Process' : 'Event')
         ])
    };

    // Calculate link colors with optimized opacity and gradient effects
    const linkColors = links.map(l => {
        const sourceColor = plotlyNodes.color[l.source];
        const value = l.value || 1;
        const maxValue = Math.max(...links.map(link => link.value || 1));
        
        // Dynamic opacity based on flow value - larger flows more visible
        const baseOpacity = 0.65;
        const valueOpacity = Math.min(0.85, baseOpacity + (value / maxValue) * 0.2);
        
        // Convert hex to rgba with optimized opacity
        if (sourceColor && sourceColor.startsWith('#')) {
            const r = parseInt(sourceColor.slice(1, 3), 16);
            const g = parseInt(sourceColor.slice(3, 5), 16);
            const b = parseInt(sourceColor.slice(5, 7), 16);
            return `rgba(${r}, ${g}, ${b}, ${valueOpacity})`;
        }
        // Handle HSL colors
        if (sourceColor && sourceColor.startsWith('hsl')) {
            return sourceColor.replace(/\)$/, `, ${valueOpacity})`);
        }
        return `rgba(153, 153, 153, ${valueOpacity})`;
    });
    
    const plotlyLinks = {
        source: links.map(l => l.source),
        target: links.map(l => l.target),
        value: links.map(l => l.value),
        color: linkColors,
        hovertemplate: '<b style="font-size: 14px; color: #0f172a;">%{customdata[0]}</b> → <b style="font-size: 14px; color: #0f172a;">%{customdata[1]}</b><br>' +
                       '<span style="color: #475569;">Flow: <b style="color: #3b82f6;">%{value}</b> case(s)</span><br>' +
                       '<span style="color: #64748b;">From Layer <b>%{customdata[3]}</b> → To Layer <b>%{customdata[4]}</b></span><br>' +
                       (links.find(l => (l.case_ids || l.caseIds || []).length > 0) ? 
                        '<span style="color: #94a3b8; font-size: 11px;">Case IDs: %{customdata[2]}</span><br>' : '') +
                       '<extra></extra>',
        customdata: links.map(l => [
            l.from_formatted || l.fromFormatted || l.from,
            l.to_formatted || l.toFormatted || l.to,
            (l.case_ids || l.caseIds || []).slice(0, 10).join(', ') + ((l.case_ids || l.caseIds || []).length > 10 ? '...' : ''),
            (l.from_stage !== undefined ? l.from_stage + 1 : (l.fromStage !== undefined ? l.fromStage + 1 : '?')),
            (l.to_stage !== undefined ? l.to_stage + 1 : (l.toStage !== undefined ? l.toStage + 1 : '?'))
        ])
    };


    // Validate that x/y arrays match node count
    if (nodeX.length !== nodes.length || nodeY.length !== nodes.length) {
        console.error('[Sankey] Position array length mismatch:', {
            nodeX: nodeX.length,
            nodeY: nodeY.length,
            nodes: nodes.length
        });
    }
    
    const data = {
        type: 'sankey',
        orientation: 'h', // Horizontal flow (left to right)
        node: plotlyNodes,
        link: plotlyLinks,
        arrangement: 'freeform' // Freeform arrangement allows explicit x/y positioning for structured vertical columns
    };

    const domainExplanation = getDomainExplanation(domain);

    const layout = {
        title: {
            text: options.title || domainExplanation.title,
            font: {
                size: 22,
                color: options.domainColor || '#0f172a',
                family: 'Arial, sans-serif'
            },
            x: 0.5,
            xanchor: 'center',
            y: 0.98,
            yanchor: 'top'
        },
        font: {
            size: 14, // Optimized font size for better readability
            color: '#1e293b',
            family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif'
        },
        paper_bgcolor: '#ffffff',
        plot_bgcolor: '#fafafa', // Subtle background for better contrast
        margin: {
            l: 140, // Optimized left margin for longer event name labels
            r: 140, // Optimized right margin
            t: 90,  // More top space for title
            b: 120  // Increased bottom margin for better spacing
        },
        height: options.height || 800, // Increased default height for better visibility
        ...options.layout
    };

    const config = {
        displayModeBar: true,
        displaylogo: false,
        modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d'],
        responsive: true,
        toImageButtonOptions: {
            format: 'png',
            filename: `${(domain || 'case').toLowerCase()}_sankey_diagram`,
            height: layout.height,
            width: null,
            scale: 3 // Higher resolution for better quality exports
        },
        ...options.config
    };

    // Render the Sankey diagram
    // CRITICAL: Ensure labels are displayed properly - Plotly requires label array to match node count exactly
    console.log('[Sankey] Rendering diagram with', finalLabels.length, 'node labels');
    console.log('[Sankey] Data structure check:', {
        nodeCount: finalLabels.length,
        linkCount: plotlyLinks.source.length,
        labelsSample: finalLabels.slice(0, 10),
        allLabelsValid: finalLabels.every(l => l && typeof l === 'string' && l.length > 0)
    });
    
    // Verify data structure before rendering
    if (finalLabels.length === 0) {
        console.error('[Sankey] ERROR: No labels to display!');
        document.getElementById(containerId).innerHTML = 
            `<div style="padding: 2rem; text-align: center; color: #ef4444;">
                <p style="font-size: 1.1rem; margin-bottom: 0.5rem;"><strong>Error:</strong></p>
                <p>No event labels found. Cannot render Sankey diagram.</p>
            </div>`;
        return;
    }
    
    if (finalLabels.length !== nodes.length) {
        console.error('[Sankey] ERROR: Label count mismatch!', {
            labels: finalLabels.length,
            nodes: nodes.length
        });
    }
    
    Plotly.newPlot(containerId, [data], layout, config).then(() => {
        console.log('[Sankey] ✓ Diagram rendered successfully');
        console.log('[Sankey] Node labels displayed:', finalLabels.slice(0, 10));
        
        // Verify the plot was created with labels
        try {
            const plotElement = document.getElementById(containerId);
            if (plotElement && plotElement._fullData && plotElement._fullData[0]) {
                const plotData = plotElement._fullData[0];
                if (plotData.node && plotData.node.label) {
                    console.log('[Sankey] ✓ Labels verified in plot:', plotData.node.label.slice(0, 10));
                    console.log('[Sankey] Total labels in plot:', plotData.node.label.length);
                } else {
                    console.warn('[Sankey] ⚠ Labels not found in plot data structure');
                }
            }
        } catch (err) {
            console.error('[Sankey] Error verifying plot:', err);
        }
    }).catch(err => {
        console.error('[Sankey] Error rendering diagram:', err);
        document.getElementById(containerId).innerHTML = 
            `<div style="padding: 2rem; text-align: center; color: #ef4444;">
                <p style="font-size: 1.1rem; margin-bottom: 0.5rem;"><strong>Error Rendering Diagram:</strong></p>
                <p>${err.message || 'Unknown error'}</p>
                <p style="margin-top: 1rem; font-size: 0.9rem; color: #64748b;">Check browser console for details.</p>
            </div>`;
    });

    // Add metadata and explanation display
    if (metadata && options.showMetadata !== false) {
        const metadataHtml = `
            <div style="margin-top: 1.5rem; padding: 1.25rem; background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%); border-radius: 12px; border: 1px solid #e2e8f0;">
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-bottom: 1rem;">
                    <div style="text-align: center;">
                        <div style="font-size: 1.5rem; font-weight: 700; color: ${options.domainColor || '#0f172a'};">${metadata.totalCases}</div>
                        <div style="font-size: 0.85rem; color: #64748b;">Total Cases</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 1.5rem; font-weight: 700; color: ${options.domainColor || '#0f172a'};">${metadata.uniqueUsers}</div>
                        <div style="font-size: 0.85rem; color: #64748b;">Unique Processes</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 1.5rem; font-weight: 700; color: ${options.domainColor || '#0f172a'};">${metadata.uniqueEvents}</div>
                        <div style="font-size: 0.85rem; color: #64748b;">Event Types</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 1.5rem; font-weight: 700; color: ${options.domainColor || '#0f172a'};">${metadata.uniquePaths}</div>
                        <div style="font-size: 0.85rem; color: #64748b;">Unique Paths</div>
                    </div>
                </div>
            </div>
        `;
        const container = document.getElementById(containerId);
        if (container) {
            const existingMetadata = container.nextElementSibling;
            if (existingMetadata && existingMetadata.classList.contains('sankey-metadata')) {
                existingMetadata.remove();
            }
            const metadataDiv = document.createElement('div');
            metadataDiv.className = 'sankey-metadata';
            metadataDiv.innerHTML = metadataHtml;
            container.parentNode.insertBefore(metadataDiv, container.nextSibling);
        }
    }
}

/**
 * Show Sankey diagram modal/page
 * @param {Array} caseDetails - Case details array (from any domain)
 * @param {string} domain - Domain name (Banking, Retail, etc.)
 * @param {string} domainColor - Primary color for the domain
 */
function showSankeyDiagram(caseDetails, domain, domainColor) {
    if (!caseDetails || caseDetails.length === 0) {
        alert('No case details available for Sankey diagram');
        return;
    }

    // Debug: Check case details structure
    console.log(`[Sankey] Processing ${caseDetails.length} cases for domain: ${domain}`);
    if (caseDetails.length > 0) {
        const sampleCase = caseDetails[0];
        console.log('[Sankey] Sample case structure:', {
            hasEventSequence: !!sampleCase.event_sequence,
            eventSequenceLength: sampleCase.event_sequence ? sampleCase.event_sequence.length : 0,
            hasActivities: !!sampleCase.activities,
            activitiesLength: sampleCase.activities ? sampleCase.activities.length : 0,
            caseId: sampleCase.case_id,
            userId: sampleCase.user_id || sampleCase.customer_id || sampleCase.patient_id,
            keys: Object.keys(sampleCase)
        });
        
        // Show sample event sequence
        if (sampleCase.event_sequence && sampleCase.event_sequence.length > 0) {
            console.log('[Sankey] Sample event_sequence:', sampleCase.event_sequence.slice(0, 5));
        }
        if (sampleCase.activities && sampleCase.activities.length > 0) {
            const sampleEvents = sampleCase.activities.slice(0, 3).map(a => a.event);
            console.log('[Sankey] Sample activities events:', sampleEvents);
        }
    }

    // Generate Sankey data (no hardcoding - derives from case data)
    const sankeyData = generateSankeyData(caseDetails);
    
    console.log('[Sankey] Generated data:', {
        success: sankeyData.success,
        nodesCount: sankeyData.success ? sankeyData.nodes.length : 0,
        linksCount: sankeyData.success ? sankeyData.links.length : 0,
        error: sankeyData.error
    });
    
    if (!sankeyData.success) {
        const errorMsg = sankeyData.error || 'Failed to generate Sankey diagram';
        console.error('[Sankey] Error:', errorMsg);
        alert(errorMsg);
        return;
    }
    
    const domainExplanation = getDomainExplanation(domain);

    // Create container HTML with clean, modern UI/UX design
    const containerId = 'sankey-diagram-container';
    const html = `
        <div style="padding: 2rem; overflow-y: auto; height: 100%; background: #f8fafc;">
            <button class="btn-secondary" onclick="closeSankeyDiagram()" style="margin-bottom: 1.5rem; padding: 0.75rem 1.5rem; border-radius: 8px; border: 1px solid #e2e8f0; background: white; cursor: pointer; font-weight: 500;">← Back</button>
            
            <div style="background: white; border-radius: 16px; padding: 2rem; margin-bottom: 2rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                <h1 style="font-size: 2rem; margin-bottom: 0.75rem; color: ${domainColor || '#0f172a'}; font-weight: 700; letter-spacing: -0.02em;">
                    Process Pipeline Flow
                </h1>
                <p style="color: #64748b; margin-bottom: 0; font-size: 1rem; line-height: 1.6;">
                    Linear pipeline visualization showing sequential process flow from start to end
                </p>
            </div>

            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 1.5rem; border-radius: 12px; margin-bottom: 2rem; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                <div style="display: flex; align-items: center; gap: 1rem;">
                    <div style="font-size: 2rem;">➡️</div>
                    <div style="flex: 1;">
                        <p style="color: white; margin: 0; font-weight: 600; font-size: 1.1rem; margin-bottom: 0.25rem;">
                            Linear Pipeline Rule
                        </p>
                        <p style="color: rgba(255,255,255,0.9); margin: 0; font-size: 0.95rem;">
                            Flow direction: <strong>Start → Layer 1 → Layer 2 → Layer 3 → ... → End</strong><br>
                            <span style="font-size: 0.9rem;">Events in same layer do NOT connect. Only sequential layers connect.</span>
                        </p>
                    </div>
                </div>
            </div>

            <div id="${containerId}" style="background: white; border-radius: 16px; padding: 2rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 2rem;">
                <!-- Sankey diagram will be rendered here -->
            </div>

            <div style="background: white; border-radius: 16px; padding: 2rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                <h3 style="font-size: 1.5rem; margin-bottom: 1.5rem; color: #0f172a; font-weight: 700;">How to Read This Diagram</h3>
                
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 1rem; margin-bottom: 2rem;">
                    <div style="background: #f0fdf4; padding: 1.25rem; border-radius: 12px; border-left: 4px solid #10b981;">
                        <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">🟢</div>
                        <p style="color: #0f172a; font-weight: 600; margin-bottom: 0.25rem; font-size: 1rem;">Start Process</p>
                        <p style="color: #475569; margin: 0; font-size: 0.9rem; line-height: 1.5;">Green box on the left where all processes begin</p>
                    </div>
                    
                    <div style="background: #f8fafc; padding: 1.25rem; border-radius: 12px; border-left: 4px solid #64748b;">
                        <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">📦</div>
                        <p style="color: #0f172a; font-weight: 600; margin-bottom: 0.25rem; font-size: 1rem;">Vertical Layers</p>
                        <p style="color: #475569; margin: 0; font-size: 0.9rem; line-height: 1.5;">Events grouped into sequential columns (layers)</p>
                    </div>
                    
                    <div style="background: #fef3c7; padding: 1.25rem; border-radius: 12px; border-left: 4px solid #f59e0b;">
                        <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">➡️</div>
                        <p style="color: #0f172a; font-weight: 600; margin-bottom: 0.25rem; font-size: 1rem;">Sequential Flow</p>
                        <p style="color: #475569; margin: 0; font-size: 0.9rem; line-height: 1.5;">Connections only go to NEXT layer (no same-layer links)</p>
                    </div>
                    
                    <div style="background: #fef2f2; padding: 1.25rem; border-radius: 12px; border-left: 4px solid #ef4444;">
                        <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">🔴</div>
                        <p style="color: #0f172a; font-weight: 600; margin-bottom: 0.25rem; font-size: 1rem;">End Process</p>
                        <p style="color: #475569; margin: 0; font-size: 0.9rem; line-height: 1.5;">Red box on the right where all processes complete</p>
                    </div>
                </div>

                <div style="background: #f1f5f9; padding: 1.5rem; border-radius: 12px; margin-bottom: 1.5rem;">
                    <p style="color: #0f172a; font-weight: 600; margin-bottom: 0.75rem; font-size: 1.1rem;">Pipeline Flow Structure</p>
                    <div style="background: white; padding: 1rem; border-radius: 8px; margin-bottom: 1rem; text-align: center; font-family: 'Courier New', monospace;">
                        <span style="color: #10b981; font-weight: 700; font-size: 1.1rem;">Start</span>
                        <span style="color: #64748b; margin: 0 0.5rem;">→</span>
                        <span style="color: #3b82f6; font-weight: 600;">Layer 1</span>
                        <span style="color: #64748b; margin: 0 0.5rem;">→</span>
                        <span style="color: #3b82f6; font-weight: 600;">Layer 2</span>
                        <span style="color: #64748b; margin: 0 0.5rem;">→</span>
                        <span style="color: #3b82f6; font-weight: 600;">Layer 3</span>
                        <span style="color: #64748b; margin: 0 0.5rem;">→</span>
                        <span style="color: #64748b;">...</span>
                        <span style="color: #64748b; margin: 0 0.5rem;">→</span>
                        <span style="color: #ef4444; font-weight: 700; font-size: 1.1rem;">End</span>
                    </div>
                    <ul style="color: #475569; line-height: 1.8; padding-left: 1.5rem; margin: 0; font-size: 0.95rem;">
                        <li><strong>No Same-Layer Connections:</strong> Events in the same vertical layer never connect to each other</li>
                        <li><strong>Sequential Only:</strong> Connections flow strictly from Layer N to Layer N+1</li>
                        <li><strong>Ribbon Width:</strong> Thicker ribbons indicate more processes following that path</li>
                        <li><strong>Hover for Details:</strong> Hover over boxes or ribbons to see process counts and identifiers</li>
                    </ul>
                </div>

                <div style="background: #eff6ff; padding: 1.25rem; border-radius: 12px; border-left: 4px solid #3b82f6;">
                    <p style="color: #1e40af; font-weight: 600; margin-bottom: 0.5rem; font-size: 1rem;">Important Design Principle</p>
                    <p style="color: #1e3a8a; margin: 0; font-size: 0.95rem; line-height: 1.6;">
                        This is a <strong>strict linear pipeline</strong>. All events flow from left to right, one layer at a time. 
                        Events within the same layer are displayed together but remain independent - they only connect to events in the next sequential layer. 
                        This design ensures clarity and makes the process flow easy to follow.
                    </p>
                </div>
            </div>
        </div>
    `;

    // Update main content
    document.getElementById('mainContent').innerHTML = html;

    // Render diagram after a short delay to ensure container exists
    setTimeout(() => {
        // Domain-specific color palettes (optional - diagram works without them)
        const domainColors = {
            'Banking': ['#0F766E', '#14B8A6', '#5EEAD4', '#99F6E4', '#CCFBF1', '#0D9488'],
            'Retail': ['#F59E0B', '#D97706', '#FBBF24', '#FCD34D', '#FDE68A', '#F97316'],
            'Healthcare': ['#3B82F6', '#2563EB', '#60A5FA', '#93C5FD', '#DBEAFE', '#14B8A6'],
            'Insurance': ['#7C3AED', '#6D28D9', '#8B5CF6', '#A78BFA', '#C4B5FD', '#A855F7'],
            'Finance': ['#4F46E5', '#4338CA', '#6366F1', '#818CF8', '#A5B4FC', '#6366F1'],
            'HR': ['#EC4899', '#DB2777', '#F472B6', '#F9A8D4', '#FBCFE8', '#F43F5E']
        };

        renderSankeyDiagram(containerId, sankeyData, {
            title: domainExplanation.title,
            height: 850, // Increased height for better visualization
            domain: domain,
            domainColor: domainColor,
            domainColors: domainColors[domain] || null,
            showMetadata: true
        });
    }, 100);
}

/**
 * Show Sankey diagram from window variables (helper function)
 * No hardcoding - dynamically finds case details from window variables
 * @param {string} domain - Domain name
 * @param {string} domainColor - Domain color
 */
function showSankeyDiagramFromWindow(domain, domainColor) {
    let caseDetails = null;
    
    // Get case details from appropriate window variable (no hardcoding)
    const domainCaseMap = {
        'Banking': 'bankingCaseDetails',
        'Retail': 'retailCaseDetails',
        'Insurance': 'insuranceCaseDetails',
        'Finance': 'financeCaseDetails',
        'Healthcare': 'healthcareCaseDetails',
        'HR': 'hrCaseDetails'
    };
    
    const caseVarName = domainCaseMap[domain] || 'currentCaseDetails';
    
    // Try multiple sources to get case details
    // 1. Window variables (set by domain analysis functions)
    caseDetails = window[caseVarName] || window.currentCaseDetails;
    
    // 2. From current profile analysis results (if window variables not set)
    if ((!caseDetails || caseDetails.length === 0) && window.currentProfile) {
        const domainAnalysisMap = {
            'Banking': 'banking_analysis',
            'Retail': 'retail_analysis',
            'Insurance': 'insurance_analysis',
            'Finance': 'finance_analysis',
            'Healthcare': 'healthcare_analysis',
            'HR': 'hr_analysis'
        };
        
        const analysisKey = domainAnalysisMap[domain];
        if (analysisKey && window.currentProfile[analysisKey]) {
            const analysis = window.currentProfile[analysisKey];
            if (analysis && analysis.case_details && Array.isArray(analysis.case_details)) {
                caseDetails = analysis.case_details;
                console.log(`[Sankey] Found case_details in profile.${analysisKey}:`, caseDetails.length, 'cases');
            }
        }
    }
    
    // 3. Try direct access from analysis results stored in window
    if ((!caseDetails || caseDetails.length === 0) && window.currentProfile) {
        // Check if analysis results are stored directly
        const analysisKeys = ['banking_analysis', 'retail_analysis', 'insurance_analysis', 
                              'finance_analysis', 'healthcare_analysis', 'hr_analysis'];
        for (const key of analysisKeys) {
            if (window.currentProfile[key] && window.currentProfile[key].case_details) {
                const details = window.currentProfile[key].case_details;
                if (Array.isArray(details) && details.length > 0) {
                    caseDetails = details;
                    console.log(`[Sankey] Found case_details in ${key}:`, caseDetails.length, 'cases');
                    break;
                }
            }
        }
    }
    
    if (!caseDetails || caseDetails.length === 0) {
        const errorMsg = `No case details available for ${domain} domain. Please analyze your data first.`;
        console.error('[Sankey] Error:', errorMsg);
        console.error('[Sankey] Available window variables:', {
            bankingCaseDetails: window.bankingCaseDetails ? window.bankingCaseDetails.length : 'not set',
            retailCaseDetails: window.retailCaseDetails ? window.retailCaseDetails.length : 'not set',
            insuranceCaseDetails: window.insuranceCaseDetails ? window.insuranceCaseDetails.length : 'not set',
            financeCaseDetails: window.financeCaseDetails ? window.financeCaseDetails.length : 'not set',
            healthcareCaseDetails: window.healthcareCaseDetails ? window.healthcareCaseDetails.length : 'not set',
            hrCaseDetails: window.hrCaseDetails ? window.hrCaseDetails.length : 'not set',
            currentCaseDetails: window.currentCaseDetails ? window.currentCaseDetails.length : 'not set',
            currentProfile: window.currentProfile ? 'exists' : 'not set'
        });
        alert(errorMsg);
        return;
    }
    
    // Ensure caseDetails is an array
    if (!Array.isArray(caseDetails)) {
        console.error('[Sankey] caseDetails is not an array:', typeof caseDetails, caseDetails);
        alert('Invalid case details format. Expected an array.');
        return;
    }
    
    console.log(`[Sankey] Using ${caseDetails.length} cases for ${domain} domain`);
    showSankeyDiagram(caseDetails, domain, domainColor);
}

/**
 * Close Sankey diagram and return to previous view
 * No hardcoding - dynamically calls appropriate function
 */
function closeSankeyDiagram() {
    // Restore previous view based on current domain (no hardcoding)
    const domain = window.currentDomain || 'Banking';
    
    const domainFunctionMap = {
        'Banking': 'showBankingAnalysisResults',
        'Retail': 'showRetailAnalysisResults',
        'Insurance': 'showInsuranceAnalysisResults',
        'Finance': 'showFinanceAnalysisResults',
        'Healthcare': 'showHealthcareAnalysisResults',
        'HR': 'showHRAnalysisResults'
    };
    
    const functionName = domainFunctionMap[domain];
    
    if (functionName && typeof window[functionName] === 'function' && window.currentProfile) {
        window[functionName](window.currentProfile);
    } else if (typeof showDomainSplitView === 'function') {
        showDomainSplitView();
    }
}
