/**
 * Database Synthesis UI Module
 * Handles synthetic data generation and display
 */

// State for synthetic data
let syntheticDataResults = null;

/**
 * Generate synthetic data for a database profile
 */
async function generateSyntheticData(profileIndex, numRows = 100) {
    if (!sessionId) {
        alert('No session found. Please upload files first.');
        return;
    }

    const profile = analysisResults[profileIndex];
    if (!profile) {
        alert('Database profile not found');
        return;
    }

    // Show loading state
    const btn = document.getElementById(`synthesize-btn-${profileIndex}`);
    const statusDiv = document.getElementById(`synthesize-status-${profileIndex}`);
    
    if (btn) {
        btn.disabled = true;
        btn.style.opacity = '0.6';
        btn.style.cursor = 'not-allowed';
        btn.textContent = 'Generating...';
    }
    
    if (statusDiv) {
        statusDiv.style.display = 'block';
        statusDiv.innerHTML = '<div class="loading-spinner" style="width: 20px; height: 20px; margin: 0 auto;"></div> Analyzing schema and generating synthetic data...';
    }

    try {
        const response = await fetch(`${API_BASE_URL}/synthesize/${sessionId}?num_rows=${numRows}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });

        if (!response.ok) {
            // FastAPI returns 404 {"detail":"Session not found"} when the backend
            // has restarted (e.g., --reload) and lost in-memory session state.
            if (response.status === 404) {
                let detail = '';
                try {
                    const errJson = await response.json();
                    detail = errJson && errJson.detail ? String(errJson.detail) : '';
                } catch (e) {
                    // ignore parse errors
                }
                if (detail.toLowerCase().includes('session')) {
                    throw new Error('Session expired (backend restarted). Please re-upload your files and try again.');
                }
            }
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const result = await response.json();
        
        if (result.success) {
            syntheticDataResults = result;
            if (btn) {
                btn.disabled = false;
                btn.style.opacity = '1';
                btn.style.cursor = 'pointer';
                btn.textContent = '🔄 Generate Synthetic Data';
            }
            if (statusDiv) {
                statusDiv.style.display = 'none';
            }
            showSyntheticDataResults(profileIndex, result);
        } else {
            throw new Error(result.error || 'Synthesis failed');
        }
    } catch (error) {
        console.error('Synthesis error:', error);
        if (statusDiv) {
            statusDiv.style.display = 'block';
            statusDiv.style.color = '#ef4444';
            statusDiv.textContent = `Error: ${error.message}`;
        }
        if (btn) {
            btn.disabled = false;
            btn.style.opacity = '1';
            btn.style.cursor = 'pointer';
            btn.textContent = '🔄 Generate Synthetic Data';
        }
    }
}

/**
 * Display synthetic data results in UI - Optimized with tabs and compact layout
 */
function showSyntheticDataResults(profileIndex, result) {
    const mainContent = document.getElementById('mainContent');
    const schema = result.schema || {};
    const syntheticData = result.synthetic_data || {};
    const relationships = result.relationships || [];
    const tableNames = Object.keys(syntheticData);
    
    // Store data globally for tab switching
    window.synthesisData = {
        schema,
        syntheticData,
        relationships,
        currentTable: tableNames[0] || null
    };

    let html = `
        <div style="height: 100vh; display: flex; flex-direction: column; overflow: hidden;">
            <!-- Fixed Header -->
            <div style="background: var(--bg-card); border-bottom: 2px solid var(--border); padding: 1rem 1.5rem; flex-shrink: 0;">
                <div style="display: flex; align-items: center; justify-content: space-between;">
                    <h1 style="font-size: 1.8rem; color: var(--text-primary); font-weight: 700; margin: 0;">
                        🔄 Synthetic Data Generated
                    </h1>
                    <button class="btn-secondary" onclick="showDomainSplitView()" style="padding: 0.5rem 1rem; font-size: 0.9rem;">
                        ← Back
                    </button>
                </div>
                
                <!-- Summary Bar -->
                <div style="display: flex; gap: 2rem; margin-top: 0.75rem; font-size: 0.85rem; color: var(--text-muted);">
                    <span><strong style="color: var(--text-primary);">${tableNames.length}</strong> Tables</span>
                    <span><strong style="color: var(--text-primary);">${relationships.length}</strong> Relationships</span>
                    <span style="color: var(--text-muted);">Pattern-based generation • PK/FK preserved</span>
                </div>
            </div>

            <!-- Tab Navigation -->
            <div style="background: var(--bg-page); border-bottom: 1px solid var(--border); padding: 0.5rem 1.5rem; overflow-x: auto; flex-shrink: 0; display: flex; gap: 0.5rem;">
    `;

    // Generate tabs
    tableNames.forEach((tableName, idx) => {
        const tableData = syntheticData[tableName];
        const rowCount = tableData.row_count || 0;
        const isActive = idx === 0;
        html += `
            <button 
                onclick="switchSynthesisTable('${tableName}')" 
                id="tab-${tableName}"
                style="
                    padding: 0.6rem 1.2rem; 
                    border: none; 
                    background: ${isActive ? 'var(--bg-card)' : 'transparent'}; 
                    border-bottom: 2px solid ${isActive ? '#3b82f6' : 'transparent'};
                    color: ${isActive ? 'var(--text-primary)' : 'var(--text-muted)'}; 
                    font-weight: ${isActive ? '600' : '400'};
                    font-size: 0.9rem;
                    cursor: pointer;
                    border-radius: 8px 8px 0 0;
                    white-space: nowrap;
                    transition: all 0.2s;
                "
                onmouseover="this.style.background='${isActive ? 'var(--bg-card)' : 'rgba(59, 130, 246, 0.1)'}'"
                onmouseout="this.style.background='${isActive ? 'var(--bg-card)' : 'transparent'}'"
            >
                ${tableName} <span style="color: var(--text-muted); font-size: 0.8rem;">(${rowCount})</span>
            </button>
        `;
    });

    html += `
            </div>

            <!-- Content Area - Scrollable -->
            <div style="flex: 1; overflow-y: auto; padding: 1.5rem; background: var(--bg-page);">
    `;

    // Generate content for each table (initially hidden except first)
    tableNames.forEach((tableName, idx) => {
        const tableData = syntheticData[tableName];
        const columns = tableData.columns || [];
        const rows = tableData.rows || [];
        const rowCount = tableData.row_count || rows.length;
        const tableSchema = schema[tableName] || {};
        const tableRelationships = relationships.filter(r => 
            r.source_table === tableName || r.target_table === tableName
        );
        const primaryKey = tableSchema.primary_key;
        const columnsWithPatterns = (tableSchema.columns || []).filter(col => col.pattern_type);

        html += `
            <div id="content-${tableName}" style="display: ${idx === 0 ? 'block' : 'none'};">
                <!-- Table Header Info -->
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-bottom: 1.5rem;">
        `;

        // Compact info cards
        if (primaryKey) {
            html += `
                <div style="background: rgba(16, 185, 129, 0.1); border-left: 3px solid #10b981; border-radius: 6px; padding: 0.75rem;">
                    <div style="font-size: 0.75rem; color: #059669; font-weight: 600; margin-bottom: 0.25rem;">🔑 Primary Key</div>
                    <code style="font-size: 0.85rem; color: var(--text-primary);">${primaryKey}</code>
                </div>
            `;
        }

        if (tableRelationships.length > 0) {
            html += `
                <div style="background: rgba(139, 92, 246, 0.1); border-left: 3px solid #8b5cf6; border-radius: 6px; padding: 0.75rem;">
                    <div style="font-size: 0.75rem; color: #7c3aed; font-weight: 600; margin-bottom: 0.25rem;">🔗 Relationships</div>
                    <div style="font-size: 0.85rem; color: var(--text-primary);">${tableRelationships.length} connection(s)</div>
                </div>
            `;
        }

        if (columnsWithPatterns.length > 0) {
            html += `
                <div style="background: rgba(59, 130, 246, 0.1); border-left: 3px solid #3b82f6; border-radius: 6px; padding: 0.75rem;">
                    <div style="font-size: 0.75rem; color: #1e40af; font-weight: 600; margin-bottom: 0.25rem;">🎯 Patterns</div>
                    <div style="font-size: 0.85rem; color: var(--text-primary);">${columnsWithPatterns.length} detected</div>
                </div>
            `;
        }

        html += `
                </div>

                <!-- Collapsible Details -->
                <details style="margin-bottom: 1rem; background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 0.75rem;">
                    <summary style="cursor: pointer; font-weight: 600; color: var(--text-primary); font-size: 0.9rem; padding: 0.5rem;">
                        📋 View Details (Relationships, Patterns)
                    </summary>
                    <div style="padding: 1rem 0.5rem; border-top: 1px solid var(--border); margin-top: 0.5rem;">
        `;

        // Relationships
        if (tableRelationships.length > 0) {
            html += `
                <div style="margin-bottom: 1rem;">
                    <h4 style="font-size: 0.85rem; color: #7c3aed; margin-bottom: 0.5rem; font-weight: 600;">🔗 Relationships</h4>
                    <div style="display: flex; flex-direction: column; gap: 0.4rem; font-size: 0.8rem;">
            `;
            tableRelationships.forEach(rel => {
                const isSource = rel.source_table === tableName;
                const otherTable = isSource ? rel.target_table : rel.source_table;
                const thisCol = isSource ? rel.source_column : rel.target_column;
                const otherCol = isSource ? rel.target_column : rel.source_column;
                html += `
                    <div style="color: var(--text-primary);">
                        <strong>${thisCol}</strong> → <span style="color: #8b5cf6;">${otherTable}.${otherCol}</span>
                        ${rel.is_foreign_key ? '<span style="color: #059669; font-size: 0.7rem;">(FK)</span>' : ''}
                    </div>
                `;
            });
            html += `</div></div>`;
        }

        // Patterns
        if (columnsWithPatterns.length > 0) {
            html += `
                <div>
                    <h4 style="font-size: 0.85rem; color: #1e40af; margin-bottom: 0.5rem; font-weight: 600;">🎯 Detected Patterns</h4>
                    <div style="display: flex; flex-wrap: wrap; gap: 0.5rem;">
            `;
            columnsWithPatterns.forEach(col => {
                const patternEmoji = {
                    'email': '📧', 'account_number': '🔢', 'phone_number': '📞',
                    'amount': '💰', 'temperature': '🌡️', 'name': '👤'
                };
                const emoji = patternEmoji[col.pattern_type] || '📝';
                html += `
                    <span style="background: white; padding: 0.2rem 0.6rem; border-radius: 8px; font-size: 0.75rem; color: var(--text-primary);">
                        ${emoji} ${col.column_name} (${col.pattern_type})
                    </span>
                `;
            });
            html += `</div></div>`;
        }

        html += `
                    </div>
                </details>

                <!-- Data Table - Compact -->
                <div style="background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; overflow: hidden;">
                    <div style="overflow-x: auto; max-height: calc(100vh - 350px);">
                        <table style="width: 100%; border-collapse: collapse; font-size: 0.85rem;">
                            <thead style="position: sticky; top: 0; background: var(--bg-page); z-index: 10;">
                                <tr style="border-bottom: 2px solid var(--border);">
        `;

        columns.forEach(col => {
            html += `<th style="padding: 0.6rem 0.8rem; text-align: left; font-weight: 600; color: var(--text-primary); white-space: nowrap;">${col}</th>`;
        });

        html += `
                                </tr>
                            </thead>
                            <tbody>
        `;

        // Show first 30 rows (reduced from 50)
        const displayRows = rows.slice(0, 30);
        displayRows.forEach((row, idx) => {
            html += `<tr style="border-bottom: 1px solid var(--border); ${idx % 2 === 0 ? 'background: var(--bg-page);' : ''}">`;
            row.forEach(cell => {
                const cellValue = cell === null || cell === undefined ? 
                    '<span style="color: var(--text-muted); font-style: italic;">null</span>' : 
                    typeof cell === 'string' && cell.length > 40 ? cell.substring(0, 40) + '...' : 
                    String(cell);
                html += `<td style="padding: 0.5rem 0.8rem; color: var(--text-primary);">${cellValue}</td>`;
            });
            html += `</tr>`;
        });

        html += `
                            </tbody>
                        </table>
                    </div>
                </div>
        `;

        if (rows.length > 30) {
            html += `
                <p style="color: var(--text-muted); font-size: 0.8rem; margin-top: 0.5rem; text-align: center;">
                    Showing first 30 of ${rows.length} rows
                </p>
            `;
        }

        html += `</div>`;
    });

    html += `
            </div>
        </div>
    `;

    mainContent.innerHTML = html;
    showResultsSection();
}

/**
 * Switch between tables in synthesis view
 */
function switchSynthesisTable(tableName) {
    if (!window.synthesisData) return;
    
    // Hide all content
    const tableNames = Object.keys(window.synthesisData.syntheticData);
    tableNames.forEach(name => {
        const content = document.getElementById(`content-${name}`);
        const tab = document.getElementById(`tab-${name}`);
        if (content) content.style.display = 'none';
        if (tab) {
            tab.style.background = 'transparent';
            tab.style.borderBottomColor = 'transparent';
            tab.style.color = 'var(--text-muted)';
            tab.style.fontWeight = '400';
        }
    });
    
    // Show selected
    const content = document.getElementById(`content-${tableName}`);
    const tab = document.getElementById(`tab-${tableName}`);
    if (content) content.style.display = 'block';
    if (tab) {
        tab.style.background = 'var(--bg-card)';
        tab.style.borderBottomColor = '#3b82f6';
        tab.style.color = 'var(--text-primary)';
        tab.style.fontWeight = '600';
    }
    
    window.synthesisData.currentTable = tableName;
}

// Export functions to global scope
window.generateSyntheticData = generateSyntheticData;
window.showSyntheticDataResults = showSyntheticDataResults;
window.switchSynthesisTable = switchSynthesisTable;

