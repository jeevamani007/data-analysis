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
                    return str || null;
                })
                .filter(e => e !== null && e.length > 0); // Ensure non-empty
            
            if (events.length > 0) {
                // Debug: Log if we're using event_sequence
                if (window.currentDomain === 'Retail' && events.length > 0) {
                    console.log(`[Sankey] Retail case ${caseItem.case_id}: Using event_sequence (${events.length} events)`, events.slice(0, 3));
                }
                // Return as-is - already in correct order from case ID logic
                return events;
            }
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
                // Retail uses activity.event field
                eventValue = activity.event || activity.event_name || activity.type || activity.name;
            } else {
                eventValue = activity;
            }
            
            if (eventValue !== null && eventValue !== undefined) {
                const str = String(eventValue).trim();
                if (str && str.length > 0) {
                    events.push(str);
                } else if (window.currentDomain === 'Retail') {
                    console.warn(`[Sankey] Retail case ${caseItem.case_id}: Empty event at activity index ${idx}`, activity);
                }
            } else if (window.currentDomain === 'Retail') {
                console.warn(`[Sankey] Retail case ${caseItem.case_id}: Missing event value at activity index ${idx}`, activity);
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
                sampleActivity: caseItem.activities[0]
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
 * Generate Sankey diagram data from case details
 * Optimized: orders events by sequence position, not alphabetically
 * No hardcoding - derives everything from user case activities
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
    
    caseDetails.forEach((caseItem, caseIndex) => {
        const eventSequence = extractEventSequence(caseItem);
        totalEventsFound += eventSequence.length;
        
        // Debug: Log first few cases for Retail domain
        if (caseIndex < 3) {
            console.log(`[Sankey] Case ${caseIndex + 1} event extraction:`, {
                caseId: caseItem.case_id,
                hasEventSequence: !!caseItem.event_sequence,
                eventSequenceLength: caseItem.event_sequence ? caseItem.event_sequence.length : 0,
                hasActivities: !!caseItem.activities,
                activitiesLength: caseItem.activities ? caseItem.activities.length : 0,
                extractedSequence: eventSequence.slice(0, 5),
                extractedLength: eventSequence.length
            });
        }
        
        eventSequence.forEach(event => {
            const eventKey = String(event).trim();
            if (eventKey) {
                allEvents.add(eventKey);
            } else {
                console.warn(`[Sankey] Empty event found in case ${caseItem.case_id}:`, event);
            }
        });
    });

    if (allEvents.size === 0) {
        // Provide helpful error message with detailed debugging info
        const sampleCase = caseDetails[0] || {};
        const availableKeys = Object.keys(sampleCase).join(', ');
        
        // Try to extract from sample case to show what we're looking for
        const sampleEventSequence = extractEventSequence(sampleCase);
        
        let debugInfo = `Total cases: ${caseDetails.length}, Total events found: ${totalEventsFound}`;
        debugInfo += `\nSample case keys: ${availableKeys}`;
        debugInfo += `\nSample event_sequence: ${sampleCase.event_sequence ? JSON.stringify(sampleCase.event_sequence.slice(0, 3)) : 'not found'}`;
        debugInfo += `\nSample activities: ${sampleCase.activities ? `${sampleCase.activities.length} items` : 'not found'}`;
        debugInfo += `\nExtracted sequence: ${sampleEventSequence.length > 0 ? JSON.stringify(sampleEventSequence.slice(0, 3)) : 'empty'}`;
        
        console.error('[Sankey] No events found. Debug info:', debugInfo);
        
        return {
            success: false,
            error: `No events found in case details.\n\n` +
                   `Available fields: ${availableKeys}\n` +
                   `Expected fields: event_sequence or activities\n` +
                   `Debug: ${debugInfo}`
        };
    }
    
    console.log(`[Sankey] Found ${allEvents.size} unique events:`, Array.from(allEvents).slice(0, 10));
    console.log(`[Sankey] Total events found across all cases: ${totalEventsFound}`);
    
    // Debug: Show sample events with their original values
    if (allEvents.size > 0) {
        const sampleEvents = Array.from(allEvents).slice(0, 10);
        console.log('[Sankey] Sample unique events:', sampleEvents.map(e => ({
            event: e,
            length: e.length,
            isEmpty: !e || e.trim().length === 0
        })));
    }

    // Sort events by average position (REUSES case ID sequence logic)
    // Events are already ordered correctly in event_sequence by domain analyzers
    // This preserves that order in the Sankey diagram
    const eventList = Array.from(allEvents).sort((a, b) => {
        const posA = eventPositions[a] || { average: 999, count: 0 };
        const posB = eventPositions[b] || { average: 999, count: 0 };
        
        // Primary sort: by average position (preserves case ID sequence order)
        // This respects the timestamp-based ordering from case ID logic
        if (Math.abs(posA.average - posB.average) > 0.1) {
            return posA.average - posB.average;
        }
        
        // Secondary sort: by frequency (more common events first)
        if (posA.count !== posB.count) {
            return posB.count - posA.count;
        }
        
        // Tertiary sort: alphabetically
        return a.localeCompare(b);
    });

    // Create node mapping: event_name -> node_index
    const nodeMap = {};
    eventList.forEach((event, idx) => {
        nodeMap[event] = idx;
    });

    // Count transitions (from -> to) across all cases
    const transitionCounts = {};
    const caseTransitions = {};
    const transitionDetails = {}; // Store detailed info about transitions

    caseDetails.forEach(caseItem => {
        const caseId = caseItem.case_id;
        // Handle different user ID field names across domains
        const userId = caseItem.user_id || caseItem.customer_id || caseItem.patient_id || 'unknown';
        const eventSequence = extractEventSequence(caseItem);

        if (eventSequence.length < 2) {
            return;
        }

        // Track transitions in this case
        // REUSES the case ID sequence - transitions follow the exact order
        // from event_sequence which is already sorted by case ID logic
        for (let i = 0; i < eventSequence.length - 1; i++) {
            const fromEvent = String(eventSequence[i]).trim();
            const toEvent = String(eventSequence[i + 1]).trim();

            if (fromEvent && toEvent) {
                // This transition follows the case ID sequence order
                // (already sorted by timestamp, case ID ascending)
                const key = `${fromEvent}→${toEvent}`;
                if (!transitionCounts[key]) {
                    transitionCounts[key] = 0;
                    caseTransitions[key] = [];
                    transitionDetails[key] = {
                        from: fromEvent,
                        to: toEvent,
                        cases: []
                    };
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
    });

    // Build nodes array with formatted names
    // Ensure labels are always set properly for display in diagram
    const nodes = eventList.map((event, idx) => {
        const posInfo = eventPositions[event] || {};
        const formattedLabel = formatEventName(event);
        // Always ensure label is set - critical for Plotly to display event names
        // Use formatted label if available, otherwise use original event name, otherwise fallback
        let displayLabel = '';
        if (formattedLabel && formattedLabel.trim()) {
            displayLabel = formattedLabel.trim();
        } else if (event && String(event).trim()) {
            displayLabel = String(event).trim();
        } else {
            displayLabel = `Event ${idx + 1}`;
        }
        
        // Final validation - ensure label is never empty
        if (!displayLabel || displayLabel.length === 0) {
            displayLabel = `Event ${idx + 1}`;
            console.warn(`[Sankey] Empty label for event at index ${idx}, using fallback:`, event);
        }
        
        return {
            id: idx,
            name: event,
            label: displayLabel, // Always ensure label is set
            originalName: event,
            position: posInfo.average || idx,
            frequency: posInfo.count || 0
        };
    });
    
    // Debug: Verify nodes have proper labels (especially for Retail)
    console.log('[Sankey] Nodes created with labels:', nodes.slice(0, 10).map(n => ({
        id: n.id,
        name: n.name,
        label: n.label,
        hasLabel: !!n.label && n.label.length > 0,
        labelLength: n.label ? n.label.length : 0
    })));
    
    // Additional check: ensure all nodes have valid labels
    const nodesWithEmptyLabels = nodes.filter(n => !n.label || n.label.length === 0);
    if (nodesWithEmptyLabels.length > 0) {
        console.error(`[Sankey] WARNING: ${nodesWithEmptyLabels.length} nodes have empty labels!`, nodesWithEmptyLabels);
        // Fix empty labels
        nodesWithEmptyLabels.forEach((n, idx) => {
            n.label = n.name || `Event ${n.id + 1}`;
        });
    }

    // Build links array (flows between nodes)
    const links = [];
    Object.keys(transitionCounts).forEach(key => {
        const [fromEvent, toEvent] = key.split('→');
        if (nodeMap.hasOwnProperty(fromEvent) && nodeMap.hasOwnProperty(toEvent)) {
            const detail = transitionDetails[key];
            links.push({
                source: nodeMap[fromEvent],
                target: nodeMap[toEvent],
                value: transitionCounts[key],
                label: String(transitionCounts[key]),
                from: fromEvent,
                to: toEvent,
                fromFormatted: formatEventName(fromEvent),
                toFormatted: formatEventName(toEvent),
                caseIds: caseTransitions[key].sort((a, b) => a - b),
                cases: detail.cases || []
            });
        }
    });

    // Calculate statistics
    const totalCases = caseDetails.length;
    const totalTransitions = Object.values(transitionCounts).reduce((sum, val) => sum + val, 0);
    const uniquePaths = Object.keys(transitionCounts).length;
    const uniqueUsers = new Set(
        caseDetails.map(c => c.user_id || c.customer_id || c.patient_id || 'unknown')
    ).size;

    return {
        success: true,
        nodes: nodes,
        links: links,
        metadata: {
            totalCases: totalCases,
            totalTransitions: totalTransitions,
            uniquePaths: uniquePaths,
            uniqueEvents: eventList.length,
            uniqueUsers: uniqueUsers
        }
    };
}

/**
 * Get domain-specific explanation text
 * No hardcoding - generic explanations that work for all domains
 */
function getDomainExplanation(domain) {
    const domainLower = (domain || '').toLowerCase();
    
    // Generic structure that works for all domains
    const baseExplanation = {
        title: `${domain || 'Case'} Activity Flow`,
        description: `This diagram visualizes user activity patterns across all Case IDs in your ${domain || 'data'}. Each box represents an event/activity found in your data, and the ribbons show how many cases follow each path from one event to another.`,
        examples: []
    };
    
    // Add domain-specific examples if available (but don't hardcode event names)
    const domainExamples = {
        'banking': [
            'Shows how banking transactions flow through different stages',
            'Visualizes login, transactions, and logout patterns'
        ],
        'retail': [
            'Shows customer journey from browsing to delivery',
            'Visualizes order placement and fulfillment processes'
        ],
        'healthcare': [
            'Shows patient workflows through hospital processes',
            'Visualizes registration, treatment, and discharge patterns'
        ],
        'insurance': [
            'Shows insurance processes from registration to claims',
            'Visualizes policy lifecycle and claim workflows'
        ],
        'finance': [
            'Shows financial service workflows',
            'Visualizes account, loan, and transaction patterns'
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
    
    const plotlyNodes = {
        label: finalLabels, // Use validated labels - CRITICAL for displaying event names
        color: nodes.map((n, idx) => {
            // Use domain-specific colors if provided
            if (options.domainColors && options.domainColors.length > 0) {
                return options.domainColors[idx % options.domainColors.length];
            }
            // Default color palette (works for all domains)
            const colors = [
                '#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8',
                '#F7DC6F', '#BB8FCE', '#85C1E2', '#F8B739', '#52B788',
                '#E63946', '#F1FAEE', '#A8DADC', '#457B9D', '#1D3557',
                '#8B5CF6', '#EC4899', '#10B981', '#F59E0B', '#3B82F6'
            ];
            return colors[idx % colors.length];
        }),
        pad: 25, // Increased padding for better label visibility
        thickness: 30, // Increased thickness for better visibility
        line: {
            color: 'rgba(0,0,0,0.2)',
            width: 2
        },
        hovertemplate: '<b>%{label}</b><br>' +
                       'Event: %{label}<br>' +
                       'Average Position: %{customdata[0]:.1f}<br>' +
                       'Frequency: %{customdata[1]} case(s)<br>' +
                       '<extra></extra>',
        customdata: nodes.map(n => [n.position || 0, n.frequency || 0])
    };

    const plotlyLinks = {
        source: links.map(l => l.source),
        target: links.map(l => l.target),
        value: links.map(l => l.value),
        color: links.map(l => {
            // Color links based on source node with transparency
            const sourceColor = plotlyNodes.color[l.source];
            // Convert hex to rgba with 60% opacity
            if (sourceColor && sourceColor.startsWith('#')) {
                const r = parseInt(sourceColor.slice(1, 3), 16);
                const g = parseInt(sourceColor.slice(3, 5), 16);
                const b = parseInt(sourceColor.slice(5, 7), 16);
                return `rgba(${r}, ${g}, ${b}, 0.6)`;
            }
            return (sourceColor || '#999999') + '80';
        }),
        hovertemplate: '<b>%{customdata[0]}</b> → <b>%{customdata[1]}</b><br>' +
                       'Flow: <b>%{value}</b> case(s)<br>' +
                       'Case IDs: %{customdata[2]}<br>' +
                       '<extra></extra>',
        customdata: links.map(l => [
            l.fromFormatted || l.from,
            l.toFormatted || l.to,
            l.caseIds.slice(0, 10).join(', ') + (l.caseIds.length > 10 ? '...' : '')
        ])
    };

    const data = {
        type: 'sankey',
        orientation: 'h',
        node: plotlyNodes,
        link: plotlyLinks,
        arrangement: 'snap' // Better layout
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
            size: 14, // Increased font size for better readability
            color: '#334155',
            family: 'Arial, sans-serif'
        },
        paper_bgcolor: 'white',
        plot_bgcolor: 'white',
        margin: {
            l: 100, // Increased left margin for event name labels
            r: 100, // Increased right margin
            t: 100,
            b: 80   // Increased bottom margin
        },
        height: options.height || 700,
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
            scale: 2
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
                        <div style="font-size: 0.85rem; color: #64748b;">Unique Users</div>
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

    // Create container HTML
    const containerId = 'sankey-diagram-container';
    const html = `
        <div style="padding: 2rem; overflow-y: auto; height: 100%;">
            <button class="btn-secondary" onclick="closeSankeyDiagram()" style="margin-bottom: 1rem;">← Back</button>
            
            <h1 style="font-size: 2.5rem; margin-bottom: 0.5rem; color: ${domainColor || '#0f172a'};">
                Sankey Diagram - ${domain || 'Case Activity Flow'}
            </h1>
            <p style="color: var(--text-secondary); margin-bottom: 2rem; font-size: 1.1rem; line-height: 1.6;">
                ${domainExplanation.description}
            </p>

            <div id="${containerId}" style="background: white; border-radius: 12px; padding: 1.5rem; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
                <!-- Sankey diagram will be rendered here -->
            </div>

            <div style="margin-top: 2rem; padding: 1.5rem; background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%); border-radius: 12px; border: 1px solid #e2e8f0;">
                <h3 style="font-size: 1.3rem; margin-bottom: 1rem; color: #0f172a; font-weight: 700;">How to Read This Diagram</h3>
                <ul style="color: #475569; line-height: 2; padding-left: 1.5rem; margin-bottom: 1rem;">
                    <li><strong>Nodes (Boxes):</strong> Each box represents a user event/activity found in your data. Events are ordered by their typical sequence position (left to right).</li>
                    <li><strong>Flows (Ribbons):</strong> The colored ribbons show transitions from one event to another.</li>
                    <li><strong>Width:</strong> The thickness of each ribbon represents the number of cases following that path (thicker = more cases).</li>
                    <li><strong>Hover:</strong> Hover over any ribbon to see detailed information including case IDs and flow counts.</li>
                    <li><strong>Sequence:</strong> Events are arranged left-to-right based on their typical order in your case data (derived from actual sequences, not hardcoded).</li>
                </ul>
                ${domainExplanation.examples && domainExplanation.examples.length > 0 ? `
                    <div style="margin-top: 1rem; padding-top: 1rem; border-top: 1px solid #e2e8f0;">
                        <strong style="color: #0f172a;">About ${domain} Data:</strong>
                        <ul style="color: #64748b; line-height: 1.8; padding-left: 1.5rem; margin-top: 0.5rem;">
                            ${domainExplanation.examples.map(ex => `<li>${ex}</li>`).join('')}
                        </ul>
                    </div>
                ` : ''}
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
            'Finance': ['#4F46E5', '#4338CA', '#6366F1', '#818CF8', '#A5B4FC', '#6366F1']
        };

        renderSankeyDiagram(containerId, sankeyData, {
            title: domainExplanation.title,
            height: 750,
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
        'Healthcare': 'healthcareCaseDetails'
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
            'Healthcare': 'healthcare_analysis'
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
                              'finance_analysis', 'healthcare_analysis'];
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
        'Healthcare': 'showHealthcareAnalysisResults'
    };
    
    const functionName = domainFunctionMap[domain];
    
    if (functionName && typeof window[functionName] === 'function' && window.currentProfile) {
        window[functionName](window.currentProfile);
    } else if (typeof showDomainSplitView === 'function') {
        showDomainSplitView();
    }
}
