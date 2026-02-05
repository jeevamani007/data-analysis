/**
 * Banking Data Analysis Assistant - Frontend Application
 * Connects to backend APIs for comprehensive banking data analysis
 */

// API Configuration
const API_BASE_URL = 'http://localhost:8000/api';

// State Management
let uploadedFiles = [];
let sessionId = null;
let analysisResults = null;
let dateColumnInfo = null;

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
        const healthcarePct = domainData?.percentages?.Healthcare ?? 0;
        const otherPct = domainData?.percentages?.Other ?? 0;

        // Determine primary domain
        const primaryDomain = domainData?.primary_domain || 'Other';
        const isBanking = primaryDomain === 'Banking';
        const isHealthcare = primaryDomain === 'Healthcare';

        // Set card colors based on primary domain
        const cardColor = isBanking ? '#0F766E' : (isHealthcare ? '#14B8A6' : '#64748B');
        const cardLabel = isBanking ? ` ${profile.database_name}` :
            (isHealthcare ? ` Healthcare Database ${index + 1}` :
                ` Database ${index + 1}: General / Mixed`);

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
                                ${isBanking ? '¦ Analyze Banking Data' : (isHealthcare ? ' Analyze Healthcare Data' : ' Analyze Data')} â†’
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
        if (primaryDomain === 'Banking') {
            showBankingAnalysisResults(profile);
        } else if (primaryDomain === 'General/Other') {
            statusDiv.style.display = 'block';
            statusDiv.style.color = 'var(--text-muted)';
            statusDiv.innerHTML = 'Database profile loaded. Timeline analysis is available for Banking and Healthcare domains only.';
            btn.disabled = false;
            btn.style.opacity = '1';
            btn.style.cursor = 'pointer';
        } else if (primaryDomain === 'Healthcare') {
            // Show healthcare analysis with new diagram format
            showHealthcareAnalysisResults(profile);
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

    const caseDetails = bkData.case_details || [];
    const caseIds = bkData.case_ids || [];
    const totalCases = bkData.total_cases || 0;
    const totalUsers = bkData.total_users || 0;
    const users = bkData.users || [];
    const explanations = bkData.explanations || [];
    const totalActivities = bkData.total_activities || 0;

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

            ${buildEventBlueprintFromBackend(bkData.event_columns, caseDetails)}
            
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
                    Case IDs are in order of start time. Click a case to see its steps.
                </p>
                <div style="display: flex; flex-wrap: wrap; gap: 0.75rem; margin-bottom: 2rem;">
                    ${users.map(u => {
        const userCases = caseDetails.filter(c => c.user_id === u);
        const ids = userCases.map(c => c.case_id);
        return '<div style="background: #f8fafc; border: 1px solid var(--border); border-radius: 10px; padding: 1rem 1.25rem;"><strong style="color: #0F766E;">' + u + '</strong><span style="color: var(--text-muted); margin-left: 0.5rem;">â†’ Case IDs: ' + ids.join(', ') + '</span></div>';
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

// Healthcare Analysis Results - NEW: Diagram format Start ----|----|---- End
// Tables sorted by date/timestamp ascending, small boxes show date & time
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

    const nodes = hcData.diagram_nodes || [];
    const firstDate = hcData.first_date || '';
    const lastDate = hcData.last_date || '';
    const totalRecords = hcData.total_records || 0;
    const tablesSummary = hcData.tables_summary || [];

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

    let html = `
        <div style="padding: 2rem; overflow-y: auto; height: 100%;">
            <button class="btn-secondary" onclick="showDomainSplitView()" style="margin-bottom: 1rem;">â† Back</button>
            
            <h1 style="font-size: 2.5rem; margin-bottom: 0.5rem; color: #14B8A6;"> Healthcare Data Timeline</h1>
            <p style="color: var(--text-secondary); margin-bottom: 2rem; font-size: 1.1rem;">
                ${profile.database_name} â€¢ ${totalRecords} records â€¢ Click any box to see column explanations
            </p>
            
            <!-- Diagram: Start ----|----|---- End -->
            <section style="margin-bottom: 2rem;">
                <h2 style="font-size: 1.4rem; margin-bottom: 0.75rem; color: var(--text-primary);">
                    … Sorted Timeline (${firstDate} â†’ ${lastDate})
                </h2>
                <p style="color: var(--text-muted); margin-bottom: 1.5rem; font-size: 0.95rem;">
                    Click any box to see column explanations (admission, appointment, discharge, lab, etc.) from your uploaded files.
                </p>
                <div style="background: linear-gradient(135deg, rgba(20,184,166,0.08), rgba(13,148,136,0.06)); border: 1px solid rgba(20,184,166,0.3); border-radius: 16px; padding: 1.5rem; overflow-x: auto;">
                    <div style="display: flex; align-items: flex-start; min-width: max-content; gap: 0; flex-wrap: nowrap;">
                        <!-- START -->
                        <div style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 70px; padding: 0.5rem;">
                            <div style="width: 14px; height: 14px; border-radius: 50%; background: #10b981; border: 2px solid #059669; margin-bottom: 0.3rem;"></div>
                            <div style="font-size: 0.75rem; font-weight: 700; color: #059669;">START</div>
                            <div style="font-size: 0.7rem; color: var(--text-muted);">${firstDate}</div>
                        </div>
                        <div style="flex: 1; min-width: 20px; height: 24px; border-bottom: 3px solid rgba(20,184,166,0.5); align-self: flex-start; margin-top: 6px;"></div>
    `;

    nodes.forEach((node, i) => {
        const dateLabel = node.date ? node.date.split('-').slice(1).join('/') : '';
        const timeStr = node.time ? node.time.substring(0, 5) : '';
        const label = timeStr ? `${dateLabel} ${timeStr}` : dateLabel;
        html += `
                        <div class="healthcare-node" style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 72px; cursor: pointer; padding: 0.35rem;" 
                            onclick="showHealthcareNodeDetails(${i})" role="button" tabindex="0" data-node-index="${i}">
                            <div style="width: 48px; min-height: 48px; padding: 0.4rem; border-radius: 10px; background: linear-gradient(135deg, #14B8A6, #0D9488); color: white; display: flex; flex-direction: column; align-items: center; justify-content: center; font-weight: 700; font-size: 0.85rem; border: 2px solid rgba(255,255,255,0.3); box-shadow: 0 2px 8px rgba(20,184,166,0.3); transition: all 0.2s;"
                                onmouseover="this.style.transform='scale(1.08)'; this.style.boxShadow='0 4px 12px rgba(20,184,166,0.5)';"
                                onmouseout="this.style.transform='scale(1)'; this.style.boxShadow='0 2px 8px rgba(20,184,166,0.3)';">
                                <div style="font-size: 1rem;">${node.count}</div>
                                <div style="font-size: 0.6rem; opacity: 0.9;">records</div>
                            </div>
                            <div style="font-size: 0.7rem; color: var(--text-primary); font-weight: 600; margin-top: 0.35rem; text-align: center; max-width: 80px; overflow: hidden; text-overflow: ellipsis;">${label}</div>
                        </div>
                        ${i < nodes.length - 1 ? '<div style="flex: 1; min-width: 16px; height: 24px; border-bottom: 3px solid rgba(20,184,166,0.5); align-self: flex-start; margin-top: 6px;"></div>' : ''}
        `;
    });

    html += `
                        <div style="flex: 1; min-width: 20px; height: 24px; border-bottom: 3px solid rgba(20,184,166,0.5); align-self: flex-start; margin-top: 6px;"></div>
                        <!-- END -->
                        <div style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 70px; padding: 0.5rem;">
                            <div style="width: 14px; height: 14px; border-radius: 50%; background: #ef4444; border: 2px solid #dc2626; margin-bottom: 0.3rem;"></div>
                            <div style="font-size: 0.75rem; font-weight: 700; color: #dc2626;">END</div>
                            <div style="font-size: 0.7rem; color: var(--text-muted);">${lastDate}</div>
                        </div>
                    </div>
                </div>
            </section>

            <!-- Node details panel (hidden initially) -->
            <div id="healthcare-node-details" style="display: none;">
                <div id="healthcare-node-details-content"></div>
            </div>
        </div>
    `;

    mainContent.innerHTML = html;

    window.healthcareDiagramNodes = nodes;
    window.healthcareFullData = hcData;
}

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

