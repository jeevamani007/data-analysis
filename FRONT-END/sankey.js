/**
 * Sankey Diagram Renderer - Matches Image Format Exactly
 * Linear Pipeline Structure: Start Process → Events (in columns) → End Process
 * Uses Plotly.js for rendering Sankey diagrams matching the energy flow diagram format
 */

/**
 * Get flow data for a specific domain
 */
function getFlowDataForDomain(domain) {
    const flowDataMap = {
        'Banking': window.currentBankingUnifiedFlowData,
        'Retail': window.currentRetailUnifiedFlowData,
        'Insurance': window.currentInsuranceUnifiedFlowData,
        'Finance': window.currentFinanceUnifiedFlowData,
        'Healthcare': window.currentHealthcareUnifiedFlowData,
        'HR': window.currentHRUnifiedFlowData
    };
    return flowDataMap[domain] || null;
}

/**
 * Transform flow data to Plotly Sankey format
 * STRICT LINEAR LAYER STRUCTURE: Start Process → Layer 0 → Layer 1 → ... → Final Layer → End Process
 * Each layer only connects to the next layer (no skipping layers)
 */
function transformToPlotlySankey(flowData, domainColor) {
    if (!flowData || !flowData.case_paths || flowData.case_paths.length === 0) {
        return null;
    }

    const casePaths = flowData.case_paths || [];
    const START_NODE = 'Start Process';
    const END_NODE = 'End Process';
    
    // Step 1: Extract all unique events and determine their layer positions
    const allEvents = new Set();
    const eventLayerPositions = {}; // event -> [layer positions from each path]
    
    casePaths.forEach((path) => {
        const sequence = path.path_sequence || [];
        // Filter out 'Process' and 'End', keep only events
        const eventSequence = sequence.filter(e => {
            const eventStr = String(e).trim();
            return eventStr && eventStr !== 'Process' && eventStr !== 'End';
        });
        
        eventSequence.forEach((event, idx) => {
            const eventKey = String(event).trim();
            if (eventKey) {
                allEvents.add(eventKey);
                if (!eventLayerPositions[eventKey]) {
                    eventLayerPositions[eventKey] = [];
                }
                // Store the layer position (index in the event sequence)
                eventLayerPositions[eventKey].push(idx);
            }
        });
    });
    
    if (allEvents.size === 0) {
        return null;
    }
    
    // Step 2: Assign each event to a discrete layer based on its average index
    // IMPORTANT: Layer 0 = first step after Start Process, Layer 1 = second step, etc.
    // This keeps the pipeline strictly linear: Start → Layer 0 → Layer 1 → ... → End
    const eventLayers = {}; // event -> layer number
    const eventAvgLayerPos = {}; // event -> average index position
    let maxLayerIndex = 0;
    
    Object.keys(eventLayerPositions).forEach(event => {
        const positions = eventLayerPositions[event];
        const avgPos = positions.reduce((a, b) => a + b, 0) / positions.length;
        // Use floor of average index as the layer (0, 1, 2, ...)
        const layer = Math.max(0, Math.floor(avgPos));
        eventAvgLayerPos[event] = avgPos;
        eventLayers[event] = layer;
        if (layer > maxLayerIndex) {
            maxLayerIndex = layer;
        }
    });
    
    // Total number of layers (0..maxLayerIndex)
    const numLayers = maxLayerIndex + 1;
    
    // Step 3: Group events by layer and sort within each layer
    const eventsByLayer = {}; // layer -> [events]
    Object.keys(eventLayers).forEach(event => {
        const layer = eventLayers[event];
        if (!eventsByLayer[layer]) {
            eventsByLayer[layer] = [];
        }
        eventsByLayer[layer].push(event);
    });
    
    // Sort events within each layer by average position
    Object.keys(eventsByLayer).forEach(layer => {
        eventsByLayer[layer].sort((a, b) => {
            return (eventAvgLayerPos[a] || 0) - (eventAvgLayerPos[b] || 0);
        });
    });
    
    // Step 4: Build node list: Start Process + Layer 0 events + Layer 1 events + ... + End Process
    const nodes = [START_NODE];
    for (let layer = 0; layer < numLayers; layer++) {
        if (eventsByLayer[layer]) {
            nodes.push(...eventsByLayer[layer]);
        }
    }
    nodes.push(END_NODE);
    
    // Create node index map
    const nodeIndexMap = {};
    nodes.forEach((node, idx) => {
        nodeIndexMap[node] = idx;
    });
    
    // Step 5: Build links - STRICT LAYER-TO-LAYER CONNECTIONS ONLY
    const linkCounts = {}; // "from->to" -> count
    const linkDetails = {}; // "from->to" -> {cases: [], color: ...}
    
    casePaths.forEach((path) => {
        const sequence = path.path_sequence || [];
        const pathColor = path.color || domainColor;
        
        // Filter to get only events
        const eventSequence = sequence.filter(e => {
            const eventStr = String(e).trim();
            return eventStr && eventStr !== 'Process' && eventStr !== 'End';
        });
        
        if (eventSequence.length === 0) {
            return;
        }
        
        // Start Process → First Layer (Layer 0) events only
        const firstEvent = String(eventSequence[0]).trim();
        if (firstEvent && eventLayers[firstEvent] === 0) {
            const key = `${START_NODE}->${firstEvent}`;
            linkCounts[key] = (linkCounts[key] || 0) + 1;
            if (!linkDetails[key]) {
                linkDetails[key] = { cases: [], color: pathColor };
            }
            linkDetails[key].cases.push(path.case_id);
        }
        
        // Layer-to-Layer connections: Only connect events in consecutive layers
        for (let i = 0; i < eventSequence.length - 1; i++) {
            const fromEvent = String(eventSequence[i]).trim();
            const toEvent = String(eventSequence[i + 1]).trim();
            
            if (!fromEvent || !toEvent) {
                continue;
            }
            
            const fromLayer = eventLayers[fromEvent];
            const toLayer = eventLayers[toEvent];
            
            // ONLY allow connections between consecutive layers (toLayer = fromLayer + 1)
            // This ensures strict linear pipeline: Layer N → Layer N+1 only
            if (toLayer === fromLayer + 1) {
                const key = `${fromEvent}->${toEvent}`;
                linkCounts[key] = (linkCounts[key] || 0) + 1;
                if (!linkDetails[key]) {
                    linkDetails[key] = { cases: [], color: pathColor };
                }
                linkDetails[key].cases.push(path.case_id);
            }
        }
        
        // Final event of the path → End Process
        // IMPORTANT: Always connect the last event (whatever its layer) to End Process
        // This guarantees that every pipeline visually meets at the End node.
        const lastEvent = String(eventSequence[eventSequence.length - 1]).trim();
        if (lastEvent) {
            const key = `${lastEvent}->${END_NODE}`;
            linkCounts[key] = (linkCounts[key] || 0) + 1;
            if (!linkDetails[key]) {
                linkDetails[key] = { cases: [], color: pathColor };
            }
            linkDetails[key].cases.push(path.case_id);
        }
    });
    
    // Build Plotly format - nodes array
    const plotlyNodeLabels = nodes.map((node) => {
        if (node === START_NODE) {
            return 'Start Process';
        } else if (node === END_NODE) {
            return 'End Process';
        } else {
            // Format event name
            return String(node)
                .replace(/_/g, ' ')
                .replace(/-/g, ' ')
                .split(' ')
                .map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
                .join(' ');
        }
    });
    
    // Node colors - Start and End get domain color, events get gray
    const plotlyNodeColors = nodes.map((node) => {
        return (node === START_NODE || node === END_NODE) ? domainColor : '#94a3b8';
    });
    
    // Build links array
    const plotlyLinks = {
        source: [],
        target: [],
        value: [],
        label: [],
        color: []
    };
    
    Object.keys(linkCounts).forEach(key => {
        const [from, to] = key.split('->');
        const count = linkCounts[key];
        const detail = linkDetails[key] || {};
        
        if (nodeIndexMap[from] !== undefined && nodeIndexMap[to] !== undefined) {
            plotlyLinks.source.push(nodeIndexMap[from]);
            plotlyLinks.target.push(nodeIndexMap[to]);
            plotlyLinks.value.push(count);
            plotlyLinks.label.push(`${count}`);
            
            // Use color from path, or domain color
            const linkColor = detail.color || domainColor;
            plotlyLinks.color.push(linkColor + '80'); // Add transparency
        }
    });
    
    return {
        nodes: {
            labels: plotlyNodeLabels,
            colors: plotlyNodeColors
        },
        links: plotlyLinks,
        metadata: {
            totalCases: casePaths.length,
            totalEvents: nodes.length - 2, // Exclude Start and End
            numLayers: numLayers,
            nodeIndexMap: nodeIndexMap,
            eventLayers: eventLayers, // Pass layer information for node positioning
            eventsByLayer: eventsByLayer,
            START_NODE: START_NODE,
            END_NODE: END_NODE
        }
    };
}

/**
 * Render Sankey diagram using Plotly.js
 * Matches the image format: horizontal flow with colored bands
 */
function renderPlotlySankey(plotlyData, domain, domainColor, containerId) {
    if (!plotlyData || !plotlyData.nodes || !plotlyData.links) {
        return '<div style="padding: 2rem; text-align: center; color: #64748b;">No flow data available to display.</div>';
    }
    
    const { nodes, links, metadata } = plotlyData;
    
    // Ensure we have valid data
    if (!nodes.labels || nodes.labels.length === 0 || !links.source || links.source.length === 0) {
        return '<div style="padding: 2rem; text-align: center; color: #64748b;">Insufficient data to render diagram.</div>';
    }
    
    // Prepare data for Plotly Sankey
    // Plotly Sankey automatically arranges nodes in columns based on connections
    // We ensure strict linear layer flow: Start → Layer 0 → Layer 1 → ... → End
    const { eventLayers, numLayers, START_NODE, END_NODE } = metadata;
    
    // Calculate x positions for nodes based on layers to ensure proper layer arrangement
    const nodeXPositions = nodes.labels.map((label, idx) => {
        // Find the node name from the index
        const nodeName = Object.keys(metadata.nodeIndexMap).find(
            name => metadata.nodeIndexMap[name] === idx
        );
        
        if (nodeName === START_NODE) {
            return 0; // Start Process at left (x=0)
        } else if (nodeName === END_NODE) {
            return 1; // End Process at right (x=1)
        } else if (eventLayers && eventLayers[nodeName] !== undefined) {
            // Event nodes positioned based on their layer
            // Normalize layer to 0-1 range (excluding Start and End)
            const layer = eventLayers[nodeName];
            // Distribute layers evenly: Layer 0 at ~0.1, Layer N at ~0.9
            return 0.1 + (layer / (numLayers + 1)) * 0.8;
        }
        return undefined; // Let Plotly auto-arrange
    });
    
    const sankeyData = {
        type: 'sankey',
        orientation: 'h', // Horizontal flow (left to right)
        node: {
            pad: 25, // Padding between nodes (increased for better layer separation)
            thickness: 30, // Node thickness
            line: {
                color: '#1e293b',
                width: 1.5
            },
            label: nodes.labels,
            color: nodes.colors,
            x: nodeXPositions.filter(x => x !== undefined).length > 0 ? nodeXPositions : undefined,
            y: undefined // Auto-calculated by Plotly
        },
        link: {
            source: links.source,
            target: links.target,
            value: links.value,
            color: (links.color && links.color.length > 0) ? links.color : (domainColor + '80'),
            line: {
                color: '#1e293b',
                width: 0.5
            }
        },
        arrangement: 'snap' // Snap nodes to grid for cleaner layout
    };
    
    const layout = {
        title: {
            text: `${domain} - User Event Flow Diagram`,
            font: {
                size: 20,
                color: '#1e293b'
            },
            x: 0.5,
            xanchor: 'center'
        },
        font: {
            size: 12,
            color: '#334155'
        },
        paper_bgcolor: 'white',
        plot_bgcolor: 'white',
        width: Math.max(1200, window.innerWidth * 0.95),
        height: Math.max(600, window.innerHeight * 0.7),
        margin: {
            l: 50,
            r: 50,
            t: 80,
            b: 50
        }
    };
    
    const config = {
        displayModeBar: true,
        responsive: true,
        toImageButtonOptions: {
            format: 'png',
            filename: `${domain.toLowerCase()}_sankey_diagram`,
            height: layout.height,
            width: layout.width,
            scale: 2
        }
    };
    
    // Render the diagram
    Plotly.newPlot(containerId, [sankeyData], layout, config);
    
    return `<div id="${containerId}" style="width: 100%; height: ${layout.height}px;"></div>`;
}

/**
 * Show Sankey Diagram - Main function called from button click
 * Matches image format: linear pipeline structure
 */
window.showSankeyDiagramFromWindow = function(domain, domainColor) {
    try {
        // Get flow data for the domain
        const flowData = getFlowDataForDomain(domain);
        
        if (!flowData || !flowData.case_paths || flowData.case_paths.length === 0) {
            alert(`No flow data available for ${domain}. Please run analysis first.`);
            return;
        }
        
        // Transform to Plotly format
        const plotlyData = transformToPlotlySankey(flowData, domainColor);
        
        if (!plotlyData) {
            alert(`Could not generate Sankey diagram data for ${domain}.`);
            return;
        }
        
        // Create modal container
        const modalId = 'sankey-modal-' + Date.now();
        const containerId = 'sankey-plot-' + Date.now();
        
        const modalHTML = `
            <div id="${modalId}" style="
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background: rgba(0, 0, 0, 0.7);
                z-index: 10000;
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 2rem;
                box-sizing: border-box;
            ">
                <div style="
                    background: white;
                    border-radius: 12px;
                    width: 95%;
                    max-width: 1400px;
                    height: 90%;
                    max-height: 900px;
                    display: flex;
                    flex-direction: column;
                    box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
                ">
                    <div style="
                        padding: 1.5rem;
                        border-bottom: 2px solid #e5e7eb;
                        display: flex;
                        justify-content: space-between;
                        align-items: center;
                    ">
                        <h2 style="
                            margin: 0;
                            font-size: 1.5rem;
                            color: #1e293b;
                            font-weight: 700;
                        ">
                            📊 ${domain} - Sankey Flow Diagram
                        </h2>
                        <button onclick="document.getElementById('${modalId}').remove()" style="
                            background: #ef4444;
                            color: white;
                            border: none;
                            border-radius: 8px;
                            padding: 0.5rem 1rem;
                            font-size: 1rem;
                            cursor: pointer;
                            font-weight: 600;
                        ">✕ Close</button>
                    </div>
                    <div style="
                        flex: 1;
                        padding: 1.5rem;
                        overflow: auto;
                        display: flex;
                        flex-direction: column;
                    ">
                        <div style="
                            margin-bottom: 1rem;
                            padding: 1rem;
                            background: #f9fafb;
                            border-radius: 8px;
                            border: 1px solid #e5e7eb;
                        ">
                            <div style="font-size: 0.9rem; color: #64748b; margin-bottom: 0.5rem;">
                                <strong>Total Cases:</strong> ${plotlyData.metadata.totalCases} | 
                                <strong>Total Events:</strong> ${plotlyData.metadata.totalEvents} | 
                                <strong>Layers:</strong> ${plotlyData.metadata.numLayers}
                            </div>
                            <div style="font-size: 0.85rem; color: #64748b;">
                                Strict linear pipeline structure: Start Process → Layer 0 → Layer 1 → ... → Final Layer → End Process. 
                                Each layer only connects to the next layer. Flow width represents the number of cases moving between events.
                            </div>
                        </div>
                        <div id="${containerId}" style="
                            flex: 1;
                            width: 100%;
                            min-height: 500px;
                            background: white;
                            border-radius: 8px;
                        "></div>
                    </div>
                </div>
            </div>
        `;
        
        // Insert modal into page
        document.body.insertAdjacentHTML('beforeend', modalHTML);
        
        // Render the Plotly diagram
        setTimeout(() => {
            renderPlotlySankey(plotlyData, domain, domainColor, containerId);
        }, 100);
        
    } catch (error) {
        console.error('Error rendering Sankey diagram:', error);
        alert(`Error rendering Sankey diagram: ${error.message}`);
    }
};

