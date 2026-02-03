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

// Toggle Open Date Day explanation: click point → show box; click same point again → hide
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
    let html = '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;"><div style="font-size: 1.15rem; font-weight: 700; color: #0f172a;">📅 Date: ' + entry.date + '</div><button type="button" onclick="showOpenDateExplanation(null, null); event.stopPropagation();" style="padding: 0.35rem 0.7rem; font-size: 0.8rem; background: #f1f5f9; border: 1px solid #e2e8f0; border-radius: 6px; cursor: pointer; color: #475569;">← Back</button></div>';
    html += '<div style="font-size: 0.95rem; color: #475569; margin-bottom: 1rem;">' + entry.count + ' account(s) created on this date.</div>';
    if (entry.multi_create_same_day && multiCusts.length) {
        html += '<div style="padding: 0.6rem 1rem; background: rgba(236,72,153,0.15); border-radius: 8px; margin-bottom: 1rem; font-size: 1rem; font-weight: 600; color: #be185d;">⚠️ One customer created 2+ accounts this day: ' + multiCusts.join(', ') + '.</div>';
    }
    html += '<div style="display: flex; flex-direction: column; gap: 0.75rem;">';
    creations.forEach(function(cr) {
        const cid = cr.customer_id || '?';
        const time = cr.time_str || '';
        const tod = cr.time_of_day || '';
        const suffix = multiCusts.indexOf(cid) >= 0 ? ' <span style="color: #ec4899; font-size: 0.85rem;">(2+ accounts this day)</span>' : '';
        html += '<div style="padding: 0.85rem 1rem; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; font-size: 1rem; color: #1e293b; line-height: 1.6;"><strong style="color: #0f172a;">Customer ' + cid + '</strong> created account at <strong style="color: #3b82f6;">' + (time || '—') + '</strong> (' + tod + ').' + suffix + '</div>';
    });
    html += '</div>';
    placeholder.style.display = 'none';
    content.style.display = 'block';
    content.innerHTML = html;
};

// Toggle Login Day explanation: click point → show box; click same point again → hide
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
    let html = '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;"><div style="font-size: 1.15rem; font-weight: 700; color: #0f172a;">📅 Date: ' + entry.date + '</div><button type="button" onclick="showLoginDayExplanation(null, null); event.stopPropagation();" style="padding: 0.35rem 0.7rem; font-size: 0.8rem; background: #f1f5f9; border: 1px solid #e2e8f0; border-radius: 6px; cursor: pointer; color: #475569;">← Back</button></div>';
    html += '<div style="font-size: 0.95rem; color: #475569; margin-bottom: 1rem;">' + entry.login_count + ' login(s). New: ' + entry.new_account_logins + ', Old: ' + entry.old_account_logins + '.</div>';
    if (entry.multi_login_same_day && (entry.multi_login_accounts || []).length) {
        html += '<div style="padding: 0.6rem 1rem; background: rgba(236,72,153,0.15); border-radius: 8px; margin-bottom: 1rem; font-size: 1rem; font-weight: 600; color: #be185d;">⚠️ One user logged in 2+ times on this day: ' + entry.multi_login_accounts.join(', ') + '.</div>';
    }
    html += '<div style="display: flex; flex-direction: column; gap: 0.75rem;">';
    const multiAccs = entry.multi_login_accounts || [];
    logins.forEach(function(lg) {
        const acc = lg.account_id || '?';
        const time = lg.time_str || lg.login_at || '—';
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

// Toggle Transaction Day explanation: click point → show box; click same point again → hide, return to placeholder
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
    let html = '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;"><div style="font-size: 1.15rem; font-weight: 700; color: #0f172a;">📅 Date: ' + entry.date + '</div><button type="button" onclick="showTxnDayExplanation(null, null); event.stopPropagation();" style="padding: 0.35rem 0.7rem; font-size: 0.8rem; background: #f1f5f9; border: 1px solid #e2e8f0; border-radius: 6px; cursor: pointer; color: #475569;">← Back</button></div>';
    html += '<div style="font-size: 0.95rem; color: #475569; margin-bottom: 1rem;">' + entry.transaction_count + ' transaction(s). Credits: ' + entry.credits + ', Debits: ' + entry.debits + ', Refunds: ' + entry.refunds + ', Blocked: ' + entry.declined + '. <strong>PASS: ' + (entry.pass_count || 0) + '</strong> · <strong style="color: #dc2626;">FAIL: ' + (entry.fail_count || 0) + '</strong></div>';
    if (entry.multi_user_same_day && multiAccs.length) html += '<div style="padding: 0.6rem 1rem; background: rgba(236,72,153,0.15); border-radius: 8px; margin-bottom: 1rem; font-size: 1rem; font-weight: 600; color: #be185d;">⚠️ ' + multiAccs.join(', ') + ' performed 2+ transactions on this day.</div>';
    html += '<div style="display: flex; flex-direction: column; gap: 0.75rem;">';
    txns.forEach(function(t) {
        const status = t.status || 'PASS';
        const statusClr = status === 'FAIL' ? '#dc2626' : '#059669';
        html += '<div style="padding: 0.85rem 1rem; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; font-size: 1rem; color: #1e293b; line-height: 1.6;"><strong style="color: #0f172a;">Account ' + (t.account || '?') + '</strong> at <strong style="color: #f59e0b;">' + (t.time || '—') + '</strong> · ' + (t.type || '') + ' ' + (t.amount || '') + ' · Balance ' + (t.balance_before || '0') + ' → ' + (t.balance_after || '0') + '.<br><span style="display: inline-block; margin-top: 0.35rem; padding: 0.2rem 0.5rem; border-radius: 6px; font-weight: 600; font-size: 0.9rem; background: ' + (status === 'FAIL' ? 'rgba(220,38,38,0.12)' : 'rgba(5,150,105,0.12)') + '; color: ' + statusClr + ';">Status: ' + status + '</span> — ' + (t.status_explanation || (status === 'FAIL' ? 'Transaction declined or blocked' : 'Transaction completed successfully')) + '<br><span style="color: #475569;">' + (t.meaning || '') + '</span></div>';
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
            <div class="file-icon">📄</div>
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
                document.getElementById(steps[currentStep - 1]).querySelector('.step-icon').textContent = '✓';
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
        proceedToAccountAnalysis();
        return;
    }

    let htmlContent = `
        <div style="height: 100%; display: flex; flex-direction: column; align-items: center; padding: 2rem; overflow-y: auto;">
            <h2 style="font-size: 2.5rem; margin-bottom: 2rem; color: var(--text-primary); text-align: center; font-weight: 700;">🏦 Detected Databases</h2>
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
        const isBankingHigh = bankingPct >= 60;
        const dbLabel = isBankingHigh ? profile.database_name : `Database ${index + 1}: General / Mixed`;

        if (!domainData || !domainData.chart_data) return;

        htmlContent += `
            <div style="background: var(--bg-card); border: 1px solid var(--border); border-radius: 20px; padding: 2rem; position: relative; box-shadow: var(--shadow-sm);">
                <div style="position: absolute; top: -15px; left: 20px; background: ${isBankingHigh ? 'var(--accent-primary)' : 'var(--text-muted)'}; color: white; padding: 5px 15px; border-radius: 12px; font-weight: 600; font-size: 0.9rem;">
                    ${dbLabel}
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
                            <div style="display: flex; align-items: center; gap: 1rem; padding: 1rem; background: var(--accent-primary-light); border-radius: 12px; border: 1px solid rgba(15, 118, 110, 0.2);">
                                <div style="width: 24px; height: 24px; background: var(--accent-primary); border-radius: 6px;"></div>
                                <div style="flex: 1;">
                                    <div style="font-weight: 600; font-size: 1rem; color: var(--text-primary);">Banking Domain</div>
                                    <div style="color: var(--text-muted); font-size: 0.85rem;">Customer accounts, transactions</div>
                                </div>
                                <div style="font-size: 1.5rem; font-weight: 700; color: var(--accent-primary-dark);">
                                    ${domainData.percentages.Banking}%
                                </div>
                            </div>

                            <div style="display: flex; align-items: center; gap: 1rem; padding: 1rem; background: var(--bg-page); border-radius: 12px; border: 1px solid var(--border);">
                                <div style="width: 24px; height: 24px; background: var(--text-muted); border-radius: 6px;"></div>
                                <div style="flex: 1;">
                                    <div style="font-weight: 600; font-size: 1rem; color: var(--text-primary);">Other Domain</div>
                                    <div style="color: var(--text-muted); font-size: 0.85rem;">Unclassified data</div>
                                </div>
                                <div style="font-size: 1.5rem; font-weight: 700; color: var(--text-muted);">
                                    ${domainData.percentages.Other}%
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
                    </div>
                </div>
            </div>
        `;
    });

    htmlContent += `
            </div>
            <button class="btn-primary" onclick="proceedToAccountAnalysis()" style="margin-top: 3rem; padding: 1.2rem 3rem; font-size: 1.2rem;">
                Continue to Account Analysis →
            </button>
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

// Account Analysis Flow - AUTO-DETECT and ANALYZE
window.proceedToAccountAnalysis = async function () {
    const mainContent = document.getElementById('mainContent');

    mainContent.innerHTML = `
        <div style="height: 100%; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 1.5rem;">
            <div class="loading-spinner"></div>
            <h3 style="color: var(--text-primary); font-size: 1.5rem;">🔍 Auto-Detecting Columns</h3>
            <p style="color: var(--text-muted);">Using fuzzy logic to find account open date...</p>
        </div>
    `;

    try {
        // Auto-detect columns using fuzzy logic
        const detectResponse = await fetch(`${API_BASE_URL}/detect-date-columns/${sessionId}`, {
            method: 'POST'
        });

        if (!detectResponse.ok) {
            throw new Error('Column detection failed');
        }

        const detectData = await detectResponse.json();

        if (!detectData.success || !detectData.date_candidates || detectData.date_candidates.length === 0) {
            mainContent.innerHTML = `
                <div style="max-width: 600px; margin: 0 auto; padding: 3rem; text-align: center;">
                    <h2 style="font-size: 2rem; margin-bottom: 1rem; color: #ef4444;">❌ No Date Column Found</h2>
                    <p style="color: var(--text-secondary); margin-bottom: 2rem; font-size: 1.1rem;">
                        Could not automatically detect a date column for account opening.
                    </p>
                    <p style="color: var(--text-muted); margin-bottom: 2rem;">
                        We looked for columns like: open_date, created_at, signup_date, account_date, etc.
                    </p>
                    <button class="btn-secondary" onclick="location.reload()" style="padding: 1rem 2rem;">
                        ← Try Different Files
                    </button>
                </div>
            `;
            return;
        }

        const loginCandidates = detectData.login_candidates || [];
        const loginColNames = new Set((loginCandidates || []).map(c => c.column_name));
        // Prefer open_date/open_time for "open"; avoid using login_timestamp as open column
        const openCandidate = detectData.date_candidates.find(c => !loginColNames.has(c.column_name)) || detectData.date_candidates[0];
        const dateColumn = openCandidate.column_name;
        const dateConfidence = openCandidate.confidence;
        const dateTable = openCandidate.table;

        const idColumn = detectData.id_candidates.length > 0 ? detectData.id_candidates[0].column : '';
        if (!idColumn) {
            throw new Error('Could not detect customer ID column');
        }

        const loginCol = loginCandidates.length > 0 ? loginCandidates[0].column_name : null;
        const loginTable = loginCandidates.length > 0 ? loginCandidates[0].table_name : null;

        updateAppUrl('#columns');

        // Only 2 boxes: 1 = open_timestamp, 2 = login_timestamp. Click inside each to expand (explanations). Customer ID shown as small text.
        mainContent.innerHTML = `
            <div style="padding: 2rem; max-width: 700px; margin: 0 auto;">
                <div style="text-align: center; margin-bottom: 1.5rem;">
                    <div style="font-size: 3rem; margin-bottom: 0.5rem;">✅</div>
                    <h3 style="color: var(--accent-primary-dark); font-size: 1.5rem;">Columns Detected!</h3>
                    <p style="color: var(--text-muted); font-size: 0.9rem; margin-top: 0.5rem;">Customer ID for analysis: <strong>${idColumn}</strong></p>
                </div>
                <div class="timestamp-boxes" style="margin-bottom: 2rem;">
                    <div class="feature-card" style="background: linear-gradient(135deg, rgba(15,118,110,0.1), rgba(15,118,110,0.05)); border: 1px solid rgba(15,118,110,0.3); border-radius: 12px; padding: 1.25rem;">
                        <div style="font-weight: 600; color: var(--text-primary); margin-bottom: 0.5rem;">📅 Open timestamp</div>
                        <div style="font-size: 1.1rem; color: var(--accent-primary-dark); font-weight: 600;">${dateColumn}</div>
                        <div style="font-size: 0.8rem; color: var(--text-muted); margin-top: 0.35rem;">${dateConfidence}% · ${dateTable}</div>
                        <button type="button" class="explanation-trigger" onclick="toggleTimestampExplanation('col-open-expl', this)" style="margin-top: 0.75rem;">Show explanation</button>
                        <div class="explanation-content" id="col-open-expl" style="margin-top: 0.5rem;">Column for when the account was created (open_date / open_time). Used for account age.</div>
                    </div>
                </div>
                <div style="text-align: center;">
                    <button type="button" class="btn-primary" id="runAnalysisBtn" style="margin-top: 1rem; max-width: 320px;">
                        Run Analysis →
                    </button>
                </div>
            </div>
        `;

        document.getElementById('runAnalysisBtn').addEventListener('click', async function runAnalysis() {
            const btn = this;
            btn.disabled = true;
            btn.textContent = 'Analyzing...';
            try {
                const response = await fetch(`${API_BASE_URL}/analyze-accounts/${sessionId}?date_column=${encodeURIComponent(dateColumn)}&id_column=${encodeURIComponent(idColumn)}`, {
                    method: 'POST'
                });
                if (!response.ok) throw new Error('Account analysis failed');
                const data = await response.json();
                if (!data.success) throw new Error(data.error || 'Analysis failed');
                showAccountAnalysisResults(data, dateColumn, idColumn);
            } catch (error) {
                console.error('Error:', error);
                mainContent.innerHTML = `
                    <div style="max-width: 600px; margin: 0 auto; padding: 3rem; text-align: center;">
                        <h2 style="font-size: 2rem; margin-bottom: 1rem; color: #ef4444;">⚠️ Analysis Failed</h2>
                        <p style="color: var(--text-secondary); margin-bottom: 2rem;">${error.message}</p>
                        <button class="btn-secondary" onclick="location.reload()" style="padding: 1rem 2rem;">← Start Over</button>
                    </div>
                `;
            }
        });

    } catch (error) {
        console.error('Error:', error);
        mainContent.innerHTML = `
            <div style="max-width: 600px; margin: 0 auto; padding: 3rem; text-align: center;">
                <h2 style="font-size: 2rem; margin-bottom: 1rem; color: #ef4444;">❌ Detection Error</h2>
                <p style="color: var(--text-secondary); margin-bottom: 2rem;">
                    ${error.message}
                </p>
                <button class="btn-secondary" onclick="location.reload()" style="padding: 1rem 2rem;">
                    ← Try Again
                </button>
            </div>
        `;
    }
};

// Show Account Analysis Results - 5 Features with Clean UI
function showAccountAnalysisResults(data, dateColumn, idColumn) {
    updateAppUrl('#analysis');
    const mainContent = document.getElementById('mainContent');
    const ageAnalysis = data.age_analysis;
    const inactiveCustomers = data.inactive_customers;
    const multiAccount = data.multi_account_holders;
    const firstDate = ageAnalysis.first_date_str || 'N/A';
    const lastDate = ageAnalysis.last_date_str || 'N/A';
    const peakDate = ageAnalysis.peak_date_str || 'N/A';
    const peakCount = ageAnalysis.peak_count != null ? ageAnalysis.peak_count : 0;
    const totalAccounts = ageAnalysis.total_accounts != null ? ageAnalysis.total_accounts : (ageAnalysis.counts.NEW + ageAnalysis.counts.ACTIVE + ageAnalysis.counts.TRUSTED);

    mainContent.innerHTML = `
        <div style="padding: 2rem; max-width: 1400px; margin: 0 auto;">
            <div style="margin-bottom: 2rem; text-align: center;">
                <h1 style="font-size: 2.5rem; margin-bottom: 0.5rem;">📊 Account Analysis Results</h1>
                <p style="color: var(--text-secondary); font-size: 1.1rem;">
                    Based on <strong>${ageAnalysis.analyzed_table}</strong> using column: <code style="background: rgba(255,255,255,0.1); padding: 4px 8px; border-radius: 4px;">${dateColumn}</code>
                </p>
            </div>

            <!-- 5 Features Grid -->
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 1.5rem; margin-bottom: 2rem;">
                
                <!-- Feature 1: Customer Status (click to show explanation) -->
                <div class="feature-card" style="background: linear-gradient(135deg, rgba(16,185,129,0.1), rgba(16,185,129,0.05)); border: 1px solid rgba(16,185,129,0.3); border-radius: 16px; padding: 1.5rem;">
                    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 1rem;">
                        <div style="display: flex; align-items: center; gap: 0.5rem;">
                            <span style="font-size: 2rem;">👶</span>
                            <h3 style="font-size: 1.3rem; color: #10b981;">Feature 1: Customer Status</h3>
                        </div>
                        <button type="button" class="explanation-trigger" onclick="toggleTimestampExplanation('expl-customer-status', this)">Show explanation</button>
                    </div>
                    <div class="explanation-content" id="expl-customer-status" style="margin-bottom: 1rem;"><strong>Time &amp; date in your data:</strong> from ${firstDate} to ${lastDate}. <strong>Workflow:</strong> We use each open_date to compute age in days. NEW = ≤30 days (${ageAnalysis.counts.NEW}), ACTIVE = 30–365 days (${ageAnalysis.counts.ACTIVE}), TRUSTED = &gt;365 days (${ageAnalysis.counts.TRUSTED}). All ${totalAccounts} accounts are classified by when they were opened.</div>
                    <div style="display: flex; flex-direction: column; gap: 0.5rem;">
                        <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.7rem; background: rgba(16,185,129,0.1); border-radius: 8px;">
                            <span style="font-weight: 600;">NEW (≤30 days)</span>
                            <span style="font-size: 1.3rem; color: #10b981;">${ageAnalysis.counts.NEW}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.7rem; background: rgba(59,130,246,0.1); border-radius: 8px;">
                            <span style="font-weight: 600;">ACTIVE (30-365 days)</span>
                            <span style="font-size: 1.3rem; color: #3b82f6;">${ageAnalysis.counts.ACTIVE}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.7rem; background: rgba(245,158,11,0.1); border-radius: 8px;">
                            <span style="font-weight: 600;">TRUSTED (>365 days)</span>
                            <span style="font-size: 1.3rem; color: #f59e0b;">${ageAnalysis.counts.TRUSTED}</span>
                        </div>
                    </div>
                </div>

                <!-- Feature 2: Growth Trends (click to show explanation) -->
                <div class="feature-card" style="background: linear-gradient(135deg, rgba(59,130,246,0.1), rgba(59,130,246,0.05)); border: 1px solid rgba(59,130,246,0.3); border-radius: 16px; padding: 1.5rem;">
                    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 1rem;">
                        <div style="display: flex; align-items: center; gap: 0.5rem;">
                            <span style="font-size: 2rem;">📈</span>
                            <h3 style="font-size: 1.3rem; color: #3b82f6;">Feature 2: Growth Trends</h3>
                        </div>
                        <button type="button" class="explanation-trigger" onclick="toggleTimestampExplanation('expl-growth', this)">Show explanation</button>
                    </div>
                    <div class="explanation-content" id="expl-growth" style="margin-bottom: 1rem;"><strong>Time &amp; date:</strong> First account opened on <strong>${firstDate}</strong>. Most recent on <strong>${lastDate}</strong>. <strong>Workflow:</strong> We take the min and max of the open_date column. Total accounts in this period: <strong>${totalAccounts}</strong>. This is your growth window.</div>
                    <div style="padding: 1rem; background: rgba(59,130,246,0.1); border-radius: 8px;">
                        ${ageAnalysis.narrative_steps.filter(s => s.title === 'Timeline').map(s => `<div style="color: var(--text-primary);">${s.text}</div>`).join('')}
                        ${(ageAnalysis.growth_summary) ? `
                        <div style="margin-top: 0.8rem; padding-top: 0.8rem; border-top: 1px solid rgba(255,255,255,0.1); display: flex; flex-wrap: wrap; gap: 1rem;">
                            <span><strong style="color: #3b82f6;">Total Accounts:</strong> ${ageAnalysis.counts.NEW + ageAnalysis.counts.ACTIVE + ageAnalysis.counts.TRUSTED}</span>
                            <span><strong style="color: #3b82f6;">Accounts per day (avg):</strong> ${ageAnalysis.growth_summary.accounts_per_day}</span>
                            <span><strong style="color: #3b82f6;">Accounts per month (avg):</strong> ${ageAnalysis.growth_summary.accounts_per_month}</span>
                        </div>
                        ` : `
                        <div style="margin-top: 0.8rem; padding-top: 0.8rem; border-top: 1px solid rgba(255,255,255,0.1);">
                            <strong style="color: #3b82f6;">Total Accounts:</strong> ${ageAnalysis.counts.NEW + ageAnalysis.counts.ACTIVE + ageAnalysis.counts.TRUSTED}
                        </div>
                        `}
                    </div>
                </div>

                <!-- Feature 3: Peak Activity (click to show explanation) -->
                <div class="feature-card" style="background: linear-gradient(135deg, rgba(168,85,247,0.1), rgba(168,85,247,0.05)); border: 1px solid rgba(168,85,247,0.3); border-radius: 16px; padding: 1.5rem;">
                    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 1rem;">
                        <div style="display: flex; align-items: center; gap: 0.5rem;">
                            <span style="font-size: 2rem;">🚀</span>
                            <h3 style="font-size: 1.3rem; color: #a855f7;">Feature 3: Peak Activity</h3>
                        </div>
                        <button type="button" class="explanation-trigger" onclick="toggleTimestampExplanation('expl-peak', this)">Show explanation</button>
                    </div>
                    <div class="explanation-content" id="expl-peak" style="margin-bottom: 1rem;"><strong>Time &amp; date:</strong> Peak day is <strong>${peakDate}</strong> with <strong>${peakCount}</strong> account(s) opened. <strong>Workflow:</strong> We group by open_date and find the day with the highest count. That date is your peak — often linked to a campaign or promotion.</div>
                    <div style="padding: 1rem; background: rgba(168,85,247,0.1); border-radius: 8px;">
                        ${ageAnalysis.narrative_steps.filter(s => s.title === 'Peak Activity').map(s => `<div style="color: var(--text-primary); font-size: 1.05rem;">${s.text}</div>`).join('') || '<p style="color: var(--text-muted);">No significant peak detected</p>'}
                        <p style="margin-top: 0.8rem; color: var(--text-muted); font-size: 0.9rem;">💡 This likely indicates a successful marketing campaign or promotion</p>
                    </div>
                </div>

                <!-- Feature 5: Login Efficiency -->
                <!-- Feature 5: Login Efficiency -->
                ${data.login_column ? `
                <div class="feature-card" style="background: linear-gradient(135deg, rgba(139,92,246,0.1), rgba(139,92,246,0.05)); border: 1px solid rgba(139,92,246,0.3); border-radius: 16px; padding: 1.5rem;">
                    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 1rem;">
                        <div style="display: flex; align-items: center; gap: 0.5rem;">
                            <span style="font-size: 2rem;">⚡</span>
                            <h3 style="font-size: 1.3rem; color: #8b5cf6;">Feature 5: Login Efficiency</h3>
                        </div>
                        <button type="button" class="explanation-trigger" onclick="toggleTimestampExplanation('expl-login', this)">Show explanation</button>
                    </div>
                    <div class="explanation-content" id="expl-login" style="margin-bottom: 1rem;"><strong>Time &amp; date:</strong> Using login column <code>${data.login_column}</code>. <strong>Workflow:</strong> We calculate the delay between 'open_date' and 'first_login'. Short delay = high engagement.</div>
                    
                    <div style="padding: 1rem; background: rgba(139,92,246,0.1); border-radius: 8px;">
                        ${data.login_metrics && data.login_metrics.has_login_data ? `
                        <div style="font-size: 1.5rem; font-weight: 700; color: #8b5cf6; margin-bottom: 0.5rem;">
                            ${data.login_metrics.engagement_score}
                        </div>
                        <p style="color: var(--text-primary); margin-bottom: 1rem;">${data.login_metrics.engagement_story}</p>
                        
                        <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 0.5rem; text-align: center;">
                            <div style="background: rgba(255,255,255,0.1); padding: 0.5rem; border-radius: 6px;">
                                <div style="font-size: 1.1rem; font-weight: 600;">${data.login_metrics.same_day_logins}</div>
                                <div style="font-size: 0.75rem; opacity: 0.8;">Same Day</div>
                            </div>
                            <div style="background: rgba(255,255,255,0.1); padding: 0.5rem; border-radius: 6px;">
                                <div style="font-size: 1.1rem; font-weight: 600;">${data.login_metrics.delayed_logins}</div>
                                <div style="font-size: 0.75rem; opacity: 0.8;">Delayed</div>
                            </div>
                             <div style="background: rgba(255,255,255,0.1); padding: 0.5rem; border-radius: 6px;">
                                <div style="font-size: 1.1rem; font-weight: 600;">${data.login_metrics.never_logged_in}</div>
                                <div style="font-size: 0.75rem; opacity: 0.8;">No Login</div>
                            </div>
                        </div>
                        <p style="margin-top: 0.8rem; color: #c4b5fd; font-size: 0.9rem;">💡 Indicates how quickly users activate their accounts</p>
                        ` : `
                        <div style="color: var(--text-muted); text-align: center; padding: 1rem;">
                             <p>Login column detected, but no matching user IDs found in account data.</p>
                             <p style="font-size: 0.8rem; margin-top: 0.5rem;">Ensure customer IDs match between files (including case sensitivity).</p>
                        </div>
                        `}
                    </div>
                </div>
                ` : ''}

                <!-- Feature 6: Inactive Status (click to show explanation) -->
                <div class="feature-card" style="background: linear-gradient(135deg, rgba(239,68,68,0.1), rgba(239,68,68,0.05)); border: 1px solid rgba(239,68,68,0.3); border-radius: 16px; padding: 1.5rem;">
                    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 1rem;">
                        <div style="display: flex; align-items: center; gap: 0.5rem;">
                            <span style="font-size: 2rem;">⚠️</span>
                            <h3 style="font-size: 1.3rem; color: #ef4444;">Feature 6: Inactive Status</h3>
                        </div>
                        <button type="button" class="explanation-trigger" onclick="toggleTimestampExplanation('expl-inactive', this)">Show explanation</button>
                    </div>
                    <div class="explanation-content" id="expl-inactive" style="margin-bottom: 1rem;"><strong>Time &amp; date:</strong> Your open_date range is ${firstDate} to ${lastDate}. <strong>Workflow:</strong> We flag accounts opened more than 365 days ago with no recent activity. ${data.used_multi_table_activity ? ' We used customer_id to link activity from other uploaded files (logical join, not SQL). ' : ''}${inactiveCustomers.count > 0 ? `We found ${inactiveCustomers.count} inactive — good candidates for re-engagement.` : 'No accounts older than 365 days, so all are considered active.'}</div>
                    <div style="padding: 1rem; background: rgba(239,68,68,0.1); border-radius: 8px;">
                        ${data.used_multi_table_activity ? '<p style="font-size: 0.85rem; color: var(--text-muted); margin-bottom: 0.75rem;">🔗 Multi-table: activity (login/transaction) was linked from other files by customer_id.</p>' : ''}
                        ${inactiveCustomers.count > 0 ? `
                            <div style="font-size: 2rem; color: #ef4444; font-weight: 700; margin-bottom: 0.5rem;">${inactiveCustomers.count} inactive</div>
                            <div style="color: var(--text-primary);">${inactiveCustomers.insight}</div>
                            <p style="margin-top: 0.8rem; color: #fca5a5; font-size: 0.9rem;">💡 Consider a re-engagement email campaign</p>
                        ` : `
                            <div style="color: #10b981; font-weight: 600; font-size: 1.1rem;">✓ All Customers Active</div>
                            <p style="color: var(--text-muted); margin-top: 0.5rem;">${inactiveCustomers.insight}</p>
                        `}
                    </div>
                </div>

                <!-- Multi-Account Holders -->
                ${multiAccount && multiAccount.length > 0 ? `
                    <div class="feature-card" style="background: linear-gradient(135deg, rgba(102,126,234,0.1), rgba(102,126,234,0.05)); border: 1px solid rgba(102,126,234,0.3); border-radius: 16px; padding: 1.5rem;">
                        <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 1rem;">
                            <span style="font-size: 2rem;">👥</span>
                            <h3 style="font-size: 1.3rem; color: #667eea;">Bonus: Multi-Account</h3>
                        </div>
                        <p style="color: var(--text-secondary); margin-bottom: 1rem; line-height: 1.6;">
                            Customers managing multiple accounts:
                        </p>
                        <div style="padding: 1rem; background: rgba(102,126,234,0.1); border-radius: 8px;">
                            <div style="font-size: 2rem; color: #667eea; font-weight: 700; margin-bottom: 0.5rem;">
                                ${multiAccount.length}
                            </div>
                            <p style="color: var(--text-muted); font-size: 0.9rem;">
                                These customers likely have savings + current accounts
                            </p>
                        </div>
                    </div>
                ` : ''}
                
                <!-- Feature 7: Same-Day Multiple Accounts (NEW) -->
                ${data.same_day_accounts && data.same_day_accounts.total_affected > 0 ? `
                    <div class="feature-card" style="background: linear-gradient(135deg, rgba(236,72,153,0.1), rgba(236,72,153,0.05)); border: 1px solid rgba(236,72,153,0.3); border-radius: 16px; padding: 1.5rem;">
                        <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 1rem;">
                            <span style="font-size: 2rem;">🕐</span>
                            <h3 style="font-size: 1.3rem; color: #ec4899;">Feature 7: Same-Day Accounts</h3>
                        </div>
                        <p style="color: var(--text-secondary); margin-bottom: 1rem; line-height: 1.6;">
                            Customers who created multiple accounts on the same day:
                        </p>
                        <div style="padding: 1rem; background: rgba(236,72,153,0.1); border-radius: 8px;">
                            <div style="font-size: 2rem; color: #ec4899; font-weight: 700; margin-bottom: 0.5rem;">
                                ${data.same_day_accounts.total_affected}
                            </div>
                            <p style="color: var(--text-primary); margin-bottom: 0.8rem;">${data.same_day_accounts.explanation}</p>
                            
                            ${data.same_day_accounts.same_day_customers.length > 0 ? `
                                <div style="margin-top: 1rem; padding-top: 1rem; border-top: 1px solid rgba(255,255,255,0.1);">
                                    <p style="color: var(--text-muted); font-size: 0.85rem; margin-bottom: 0.5rem;">Top examples:</p>
                                    ${data.same_day_accounts.same_day_customers.slice(0, 3).map(c => `
                                        <div style="background: rgba(236,72,153,0.15); padding: 0.6rem; border-radius: 6px; margin-bottom: 0.5rem;">
                                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                                <span style="font-weight: 600; color: #ec4899;">Customer ${c.customer_id}</span>
                                                <span style="color: var(--text-muted); font-size: 0.85rem;">${c.date}</span>
                                            </div>
                                            <div style="font-size: 0.85rem; color: var(--text-muted); margin-top: 0.3rem;">
                                                ${c.account_count} accounts at: ${c.timestamps.join(', ')}
                                                ${c.suspicious ? ' <span style="color: #f87171;">⚠️ Suspicious</span>' : ''}
                                            </div>
                                        </div>
                                    `).join('')}
                                </div>
                            ` : ''}
                        </div>
                    </div>
                ` : ''}
                

            </div>

            <!-- Open Date Timeline: Start ----|----|---- End, click date → big explanation panel -->
            ${(data.open_date_timeline && data.open_date_timeline.length > 0) ? (function() {
                const daily = data.open_date_timeline;
                window.openDateDailyData = daily;
                const firstEntry = daily[0];
                const lastEntry = daily[daily.length - 1];
                const sum = data.timeline_diagram_summary || {};
                const totalAcc = sum.total_accounts != null ? sum.total_accounts : (ageAnalysis.counts.NEW + ageAnalysis.counts.ACTIVE + ageAnalysis.counts.TRUSTED);
                const activeAcc = sum.active_count != null ? sum.active_count : totalAcc;
                const inactiveAcc = sum.inactive_count != null ? sum.inactive_count : (inactiveCustomers && inactiveCustomers.count) || 0;
                const timelineBrief = (sum.timeline_brief || '').replace(/"/g, '&quot;');
                const timelineFull = (sum.timeline_full || '').replace(/"/g, '&quot;');
                const peakDt = sum.peak_date_time || firstEntry?.date;
                const peakCnt = sum.peak_count != null ? sum.peak_count : (daily.find(e => e.is_peak_day) || {}).count;
                const peakBrief = (sum.peak_brief || '').replace(/"/g, '&quot;');
                const peakFull = (sum.peak_full || peakBrief).replace(/"/g, '&quot;');
                const multiBrief = (sum.multi_account_brief || (data.same_day_accounts && data.same_day_accounts.brief_explanation) || '').replace(/"/g, '&quot;');
                const multiFull = (sum.multi_account_full || (data.same_day_accounts && data.same_day_accounts.full_explanation) || '').replace(/"/g, '&quot;');
                const multiExists = sum.multi_account_exists === true || (data.same_day_accounts && (data.same_day_accounts.total_affected || 0) > 0);
                return `
            <div class="open-date-timeline-diagram timeline-line-diagram" style="background: linear-gradient(135deg, rgba(59,130,246,0.08), rgba(139,92,246,0.06)); border: 1px solid rgba(59,130,246,0.25); border-radius: 16px; padding: 1.5rem; margin-bottom: 2rem;">
                <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 1rem;">
                    <h2 style="font-size: 1.4rem; color: #3b82f6;">📅 Open Date Timeline</h2>
                    <button type="button" class="explanation-trigger" onclick="toggleTimestampExplanation('timeline-full-theory', this)">Show full deep explanation</button>
                </div>
                <div style="margin-bottom: 1rem; padding: 0.75rem 1rem; background: rgba(255,255,255,0.5); border-radius: 10px; border: 1px solid rgba(59,130,246,0.2); font-size: 0.9rem; color: var(--text-secondary); line-height: 1.5;">
                    <span id="timeline-brief-text">${timelineBrief}</span>
                    <div class="explanation-content" id="timeline-full-theory" style="margin-top: 0.5rem; font-size: 0.9rem; color: var(--text-secondary); line-height: 1.6;">${timelineFull}</div>
                </div>
                <p style="font-size: 0.9rem; color: #475569; margin-bottom: 0.75rem;"><strong>Click a date point</strong> to see: this date, customer ID, time (Morning/Afternoon/Evening/Night), one customer 2+ accounts.</p>
                <div class="timeline-track-wrap" style="overflow-x: auto; padding: 0.5rem 0;">
                    <div class="timeline-track-line" style="display: flex; align-items: flex-start; min-width: max-content; gap: 0;">
                        <div class="timeline-node-wrap timeline-node-start" style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 70px; cursor: pointer;" onclick="showOpenDateExplanation(-1, this)" role="button" tabindex="0">
                            <div class="timeline-node-dot" style="width: 14px; height: 14px; border-radius: 50%; background: #10b981; border: 2px solid #059669; margin-bottom: 0.3rem;"></div>
                            <div style="font-size: 0.7rem; font-weight: 700; color: #10b981;">Start</div>
                            <div style="font-size: 0.8rem; font-weight: 600; color: var(--text-primary);">${(firstEntry && firstEntry.date) || firstDate}</div>
                            <div style="font-size: 0.65rem; color: var(--text-muted);">Active: ${activeAcc} · Inactive: ${inactiveAcc}</div>
                        </div>
                        <div class="timeline-line-seg" style="flex: 1; min-width: 16px; width: 24px; height: 24px; border-bottom: 3px solid rgba(59,130,246,0.5); align-self: flex-start; margin-top: 6px;"></div>
                        ${daily.map((entry, i) => `
                        <div class="timeline-node-wrap timeline-node-mid open-date-day-node ${entry.is_peak_day ? 'timeline-node-peak' : ''}" style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 72px; cursor: pointer;" onclick="showOpenDateExplanation(${i}, this)" role="button" tabindex="0" data-day-index="${i}">
                            <div class="timeline-node-dot" style="width: 12px; height: 12px; border-radius: 50%; background: ${entry.is_peak_day ? '#a855f7' : '#8b5cf6'}; border: 2px solid ${entry.is_peak_day ? '#7c3aed' : '#6d28d9'}; margin-bottom: 0.25rem;"></div>
                            ${entry.is_peak_day ? '<div style="font-size: 0.65rem; font-weight: 700; color: #a855f7;">Peak</div>' : ''}
                            <div style="font-size: 0.72rem; font-weight: 600; color: var(--text-primary);">${entry.date}</div>
                            <div style="font-size: 0.75rem; color: #8b5cf6; font-weight: 700;">${entry.count}</div>
                            ${entry.multi_create_same_day ? '<div style="font-size: 0.6rem; color: #ec4899;">👥 2+</div>' : ''}
                        </div>
                        ${i < daily.length - 1 ? '<div class="timeline-line-seg" style="flex: 1; min-width: 16px; width: 24px; height: 24px; border-bottom: 3px solid rgba(59,130,246,0.5); align-self: flex-start; margin-top: 6px;"></div>' : ''}
                        `).join('')}
                        ${daily.length > 0 ? '<div class="timeline-line-seg" style="flex: 1; min-width: 16px; width: 24px; height: 24px; border-bottom: 3px solid rgba(59,130,246,0.5); align-self: flex-start; margin-top: 6px;"></div>' : ''}
                        <div class="timeline-node-wrap timeline-node-end" style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 70px; cursor: pointer;" onclick="showOpenDateExplanation(-2, this)" role="button" tabindex="0">
                            <div class="timeline-node-dot" style="width: 14px; height: 14px; border-radius: 50%; background: #ef4444; border: 2px solid #dc2626; margin-bottom: 0.3rem;"></div>
                            <div style="font-size: 0.7rem; font-weight: 700; color: #ef4444;">End</div>
                            <div style="font-size: 0.8rem; font-weight: 600; color: var(--text-primary);">${(lastEntry && lastEntry.date) || lastDate}</div>
                            <div style="font-size: 0.65rem; color: var(--text-muted);">Total: ${totalAcc}</div>
                        </div>
                    </div>
                </div>
                <div id="open-date-explanation-panel" class="open-date-explanation-panel" style="margin-top: 1.25rem; padding: 1.25rem 1.5rem; background: #fff; border: 2px solid rgba(59,130,246,0.35); border-radius: 12px; min-height: 100px; box-shadow: 0 4px 12px rgba(0,0,0,0.06);">
                    <div id="open-date-explanation-placeholder" style="font-size: 1rem; color: #64748b; font-weight: 500;">👆 Click a date point above. Each creation: Customer ID, time (Morning/Afternoon/Evening/Night), one customer 2+ accounts this day.</div>
                    <div id="open-date-explanation-content" style="display: none;"></div>
                </div>
                <div class="timeline-peak-expl" style="margin-top: 1rem; padding: 0.75rem; background: rgba(168,85,247,0.1); border-radius: 10px; border: 1px solid rgba(168,85,247,0.3);">
                    <strong style="color: #a855f7;">Peak activity:</strong> <span style="color: var(--text-secondary);">${peakDt}</span> — <strong>${peakCnt}</strong> account(s) opened.
                    <span style="color: var(--text-muted); font-size: 0.85rem; margin-left: 0.25rem;">${peakBrief}</span>
                    <button type="button" class="explanation-trigger" style="margin-left: 0.5rem; font-size: 0.8rem;" onclick="toggleTimestampExplanation('timeline-peak-reason', this)">Full deep explanation</button>
                    <div class="explanation-content" id="timeline-peak-reason" style="margin-top: 0.5rem; font-size: 0.85rem; color: var(--text-muted); line-height: 1.5;">${peakFull}</div>
                </div>
                ${multiExists ? `
                <div class="timeline-multi-account-feature" style="margin-top: 1rem; padding: 0.75rem 1rem; background: rgba(236,72,153,0.1); border: 1px solid rgba(236,72,153,0.35); border-radius: 10px;">
                    <strong style="color: #ec4899;">👥 One user multiple accounts created:</strong>
                    <span style="color: var(--text-secondary); font-size: 0.9rem; margin-left: 0.25rem;">${multiBrief}</span>
                    <button type="button" class="explanation-trigger" style="margin-left: 0.5rem; font-size: 0.8rem;" onclick="toggleTimestampExplanation('timeline-multi-full', this)">Full deep explanation</button>
                    <div class="explanation-content" id="timeline-multi-full" style="margin-top: 0.5rem; font-size: 0.85rem; color: var(--text-muted); line-height: 1.5;">${multiFull}</div>
                </div>
                ` : ''}
            </div>
            `;
            })() : ''}

            <!-- Login Users: Start ----|----|---- End, click date → big explanation panel -->
            ${(data.daily_login_analysis && data.daily_login_analysis.has_data && data.daily_login_analysis.daily && data.daily_login_analysis.daily.length > 0) ? (function() {
                const dl = data.daily_login_analysis;
                const daily = dl.daily;
                window.loginDailyData = daily;
                const firstDay = daily[0];
                const lastDay = daily[daily.length - 1];
                return `
            <div class="login-users-diagram timeline-line-diagram" style="background: linear-gradient(135deg, rgba(34,197,94,0.08), rgba(59,130,246,0.06)); border: 1px solid rgba(34,197,94,0.3); border-radius: 16px; padding: 1.5rem; margin-bottom: 2rem;">
                <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 1rem;">
                    <h2 style="font-size: 1.4rem; color: #22c55e;">🔐 Login Users</h2>
                    <button type="button" class="explanation-trigger" onclick="toggleTimestampExplanation('login-users-full-theory', this)">Show full deep explanation</button>
                </div>
                <div style="margin-bottom: 1rem; padding: 0.75rem 1rem; background: rgba(255,255,255,0.5); border-radius: 10px; border: 1px solid rgba(34,197,94,0.2); font-size: 0.9rem; color: var(--text-secondary); line-height: 1.5;">
                    <span>${(dl.brief || '').replace(/"/g, '&quot;')}</span>
                    <div class="explanation-content" id="login-users-full-theory" style="margin-top: 0.5rem; font-size: 0.9rem; color: var(--text-secondary); line-height: 1.6;">${(dl.full_explanation || '').replace(/"/g, '&quot;')}</div>
                </div>
                <p style="font-size: 0.9rem; color: #475569; margin-bottom: 0.75rem;"><strong>Click a date point</strong> to see: this date, this time, this user login. Morning/Afternoon/Evening/Night.</p>
                <div class="timeline-track-wrap" style="overflow-x: auto; padding: 0.5rem 0;">
                    <div class="timeline-track-line" style="display: flex; align-items: flex-start; min-width: max-content; gap: 0;">
                        <div class="timeline-node-wrap timeline-node-start" style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 70px; cursor: pointer;" onclick="showLoginDayExplanation(-1, this)" role="button" tabindex="0">
                            <div class="timeline-node-dot" style="width: 14px; height: 14px; border-radius: 50%; background: #22c55e; border: 2px solid #16a34a; margin-bottom: 0.3rem;"></div>
                            <div style="font-size: 0.7rem; font-weight: 700; color: #22c55e;">Start</div>
                            <div style="font-size: 0.8rem; font-weight: 600; color: var(--text-primary);">${firstDay ? firstDay.date : ''}</div>
                            <div style="font-size: 0.65rem; color: var(--text-muted);">${dl.total_logins || 0} logins</div>
                        </div>
                        <div class="timeline-line-seg" style="flex: 1; min-width: 16px; width: 24px; height: 24px; border-bottom: 3px solid rgba(34,197,94,0.5); align-self: flex-start; margin-top: 6px;"></div>
                        ${daily.map((entry, i) => `
                        <div class="timeline-node-wrap timeline-node-mid login-day-node" style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 72px; cursor: pointer;" onclick="showLoginDayExplanation(${i}, this)" role="button" tabindex="0" data-day-index="${i}">
                            <div class="timeline-node-dot" style="width: 12px; height: 12px; border-radius: 50%; background: #3b82f6; border: 2px solid #2563eb; margin-bottom: 0.25rem;"></div>
                            <div style="font-size: 0.72rem; font-weight: 600; color: var(--text-primary);">${entry.date}</div>
                            <div style="font-size: 0.75rem; color: #3b82f6; font-weight: 700;">${entry.login_count}</div>
                            <div style="font-size: 0.6rem; color: var(--text-muted);">N:${entry.new_account_logins} O:${entry.old_account_logins}</div>${entry.multi_login_same_day ? '<div style="font-size: 0.6rem; color: #ec4899;">👥 2+</div>' : ''}
                        </div>
                        ${i < daily.length - 1 ? '<div class="timeline-line-seg" style="flex: 1; min-width: 16px; width: 24px; height: 24px; border-bottom: 3px solid rgba(34,197,94,0.5); align-self: flex-start; margin-top: 6px;"></div>' : ''}
                        `).join('')}
                        ${daily.length > 0 ? '<div class="timeline-line-seg" style="flex: 1; min-width: 16px; width: 24px; height: 24px; border-bottom: 3px solid rgba(34,197,94,0.5); align-self: flex-start; margin-top: 6px;"></div>' : ''}
                        <div class="timeline-node-wrap timeline-node-end" style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 70px; cursor: pointer;" onclick="showLoginDayExplanation(-2, this)" role="button" tabindex="0">
                            <div class="timeline-node-dot" style="width: 14px; height: 14px; border-radius: 50%; background: #ef4444; border: 2px solid #dc2626; margin-bottom: 0.3rem;"></div>
                            <div style="font-size: 0.7rem; font-weight: 700; color: #ef4444;">End</div>
                            <div style="font-size: 0.8rem; font-weight: 600; color: var(--text-primary);">${lastDay ? lastDay.date : ''}</div>
                            <div style="font-size: 0.65rem; color: var(--text-muted);">${dl.total_logins || 0} logins</div>
                        </div>
                    </div>
                </div>
                <div id="login-explanation-panel" class="login-explanation-panel" style="margin-top: 1.25rem; padding: 1.25rem 1.5rem; background: #fff; border: 2px solid rgba(34,197,94,0.35); border-radius: 12px; min-height: 100px; box-shadow: 0 4px 12px rgba(0,0,0,0.06);">
                    <div id="login-explanation-placeholder" style="font-size: 1rem; color: #64748b; font-weight: 500;">👆 Click a date point above. Each login shows: this user, this time, morning/afternoon/evening/night, account created date.</div>
                    <div id="login-explanation-content" style="display: none;"></div>
                </div>
                <div style="margin-top: 1rem; padding: 0.75rem; background: rgba(34,197,94,0.08); border-radius: 10px; border: 1px solid rgba(34,197,94,0.25);">
                    <strong style="color: #22c55e;">Table: Date | Account | Login Time | Time of Day | Account Created</strong>
                    <div style="overflow-x: auto; margin-top: 0.5rem;">
                        <table style="width: 100%; font-size: 0.9rem; border-collapse: collapse;">
                            <thead><tr style="border-bottom: 2px solid var(--border);"><th style="padding: 0.4rem;">Date</th><th style="padding: 0.4rem;">Account</th><th style="padding: 0.4rem;">Login Time</th><th style="padding: 0.4rem;">Time of Day</th><th style="padding: 0.4rem;">Account Created</th></tr></thead>
                            <tbody>
                                ${(dl.table_detail || []).slice(0, 50).map(r => `<tr style="border-bottom: 1px solid rgba(0,0,0,0.08);"><td style="padding: 0.4rem;">${r.Date}</td><td style="padding: 0.4rem;">${r.Account}</td><td style="padding: 0.4rem; font-weight: 600;">${r['Login Time'] || '—'}</td><td style="padding: 0.4rem;">${r['Time of Day'] || '—'}</td><td style="padding: 0.4rem;">${r['Account Created'] || '—'}</td></tr>`).join('')}
                            </tbody>
                        </table>
                    </div>
                    ${(dl.table_detail || []).length > 50 ? `<p style="font-size: 0.85rem; color: #475569; margin-top: 0.5rem;">Showing first 50 of ${dl.table_detail.length} logins.</p>` : ''}
                    <p style="margin-top: 0.75rem; font-size: 0.9rem; color: #475569; line-height: 1.6;">${(dl.story || '').replace(/"/g, '&quot;')}</p>
                </div>
            </div>
            `;
            })() : ''}

            <!-- Transaction Details: Start ----|----|---- End, click date → big explanation panel below -->
            ${(data.transaction_timeline && data.transaction_timeline.has_data && data.transaction_timeline.daily && data.transaction_timeline.daily.length > 0) ? (function() {
                const tt = data.transaction_timeline;
                const daily = tt.daily;
                window.txnDailyData = daily;
                const firstDay = daily[0];
                const lastDay = daily[daily.length - 1];
                return `
            <div class="transaction-details-diagram timeline-line-diagram" style="background: linear-gradient(135deg, rgba(245,158,11,0.08), rgba(234,88,12,0.06)); border: 1px solid rgba(245,158,11,0.35); border-radius: 16px; padding: 1.5rem; margin-bottom: 2rem;">
                <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 1rem;">
                    <h2 style="font-size: 1.4rem; color: #f59e0b;">💰 Transaction Details</h2>
                    <button type="button" class="explanation-trigger" onclick="toggleTimestampExplanation('txn-full-theory', this)">Show full deep explanation</button>
                </div>
                <div style="margin-bottom: 1rem; padding: 0.75rem 1rem; background: rgba(255,255,255,0.5); border-radius: 10px; border: 1px solid rgba(245,158,11,0.25);">
                    <span style="font-size: 0.95rem; color: #1e293b; font-weight: 500;">${(tt.brief || '').replace(/"/g, '&quot;')}</span>
                    <div class="explanation-content transaction-explanation-content" id="txn-full-theory" style="margin-top: 0.5rem;">${(tt.full_explanation || '').replace(/"/g, '&quot;')}</div>
                </div>
                <p style="font-size: 0.9rem; color: #475569; margin-bottom: 0.75rem;"><strong>Click a date point</strong> to see explanations. Status: <strong style="color: #059669;">PASS</strong> = SUCCESS, <strong style="color: #dc2626;">FAIL</strong> = DECLINED/BLOCKED.</p>
                <div class="timeline-track-wrap" style="overflow-x: auto; padding: 0.5rem 0;">
                    <div class="timeline-track-line" style="display: flex; align-items: flex-start; min-width: max-content; gap: 0;">
                        <div class="timeline-node-wrap timeline-node-start" style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 70px; cursor: pointer;" onclick="showTxnDayExplanation(-1, this)" role="button" tabindex="0" title="First date">
                            <div class="timeline-node-dot" style="width: 14px; height: 14px; border-radius: 50%; background: #22c55e; border: 2px solid #16a34a; margin-bottom: 0.3rem;"></div>
                            <div style="font-size: 0.7rem; font-weight: 700; color: #22c55e;">Start</div>
                            <div style="font-size: 0.8rem; font-weight: 600; color: var(--text-primary);">${firstDay ? firstDay.date : ''}</div>
                            <div style="font-size: 0.65rem; color: var(--text-muted);">${tt.total_transactions || 0} txns</div>
                        </div>
                        <div class="timeline-line-seg" style="flex: 1; min-width: 16px; width: 24px; height: 24px; border-bottom: 3px solid rgba(245,158,11,0.5); align-self: flex-start; margin-top: 6px;"></div>
                        ${daily.map((entry, i) => `
                        <div class="timeline-node-wrap timeline-node-mid txn-day-node" style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 72px; cursor: pointer;" onclick="showTxnDayExplanation(${i}, this)" role="button" tabindex="0" data-day-index="${i}" title="Click to explain">
                            <div class="timeline-node-dot" style="width: 12px; height: 12px; border-radius: 50%; background: #f59e0b; border: 2px solid #d97706; margin-bottom: 0.25rem;"></div>
                            <div style="font-size: 0.72rem; font-weight: 600; color: var(--text-primary);">${entry.date}</div>
                            <div style="font-size: 0.75rem; color: #f59e0b; font-weight: 700;">${entry.transaction_count}</div>
                            ${entry.multi_user_same_day ? '<div style="font-size: 0.6rem; color: #ec4899;">👥 2+</div>' : ''}<div style="font-size: 0.6rem; color: var(--text-muted);">P:${entry.pass_count || 0} F:${entry.fail_count || 0}</div>
                        </div>
                        ${i < daily.length - 1 ? '<div class="timeline-line-seg" style="flex: 1; min-width: 16px; width: 24px; height: 24px; border-bottom: 3px solid rgba(245,158,11,0.5); align-self: flex-start; margin-top: 6px;"></div>' : ''}
                        `).join('')}
                        ${daily.length > 0 ? '<div class="timeline-line-seg" style="flex: 1; min-width: 16px; width: 24px; height: 24px; border-bottom: 3px solid rgba(245,158,11,0.5); align-self: flex-start; margin-top: 6px;"></div>' : ''}
                        <div class="timeline-node-wrap timeline-node-end" style="flex-shrink: 0; display: flex; flex-direction: column; align-items: center; min-width: 70px; cursor: pointer;" onclick="showTxnDayExplanation(-2, this)" role="button" tabindex="0" title="Last date">
                            <div class="timeline-node-dot" style="width: 14px; height: 14px; border-radius: 50%; background: #ef4444; border: 2px solid #dc2626; margin-bottom: 0.3rem;"></div>
                            <div style="font-size: 0.7rem; font-weight: 700; color: #ef4444;">End</div>
                            <div style="font-size: 0.8rem; font-weight: 600; color: var(--text-primary);">${lastDay ? lastDay.date : ''}</div>
                            <div style="font-size: 0.65rem; color: var(--text-muted);">${tt.total_transactions || 0} txns</div>
                        </div>
                    </div>
                </div>
                <div id="txn-explanation-panel" class="txn-explanation-panel" style="margin-top: 1.25rem; padding: 1.25rem 1.5rem; background: #fff; border: 2px solid rgba(245,158,11,0.4); border-radius: 12px; min-height: 140px; box-shadow: 0 4px 12px rgba(0,0,0,0.06);">
                    <div id="txn-explanation-placeholder" style="font-size: 1rem; color: #64748b; font-weight: 500;">👆 Click a date point above. Each transaction shows status: PASS (success) or FAIL (declined/blocked).</div>
                    <div id="txn-explanation-content" style="display: none;"></div>
                </div>
            </div>
            `;
            })() : ''}

            <!-- Detailed Account Table -->
            <div style="background: var(--bg-card); border: 1px solid var(--border); border-radius: 16px; overflow: hidden;">
                <div style="padding: 1.5rem; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center;">
                    <h2 style="font-size: 1.5rem;">📋 Detailed Account Table</h2>
                    <div style="display: flex; gap: 0.5rem;">
                        <button class="seg-tab active" onclick="renderAgeTable('ALL')">All (${ageAnalysis.counts.NEW + ageAnalysis.counts.ACTIVE + ageAnalysis.counts.TRUSTED})</button>
                        <button class="seg-tab" onclick="renderAgeTable('NEW')">👶 New (${ageAnalysis.counts.NEW})</button>
                        <button class="seg-tab" onclick="renderAgeTable('ACTIVE')">✅ Active (${ageAnalysis.counts.ACTIVE})</button>
                        <button class="seg-tab" onclick="renderAgeTable('TRUSTED')">🏆 Trusted (${ageAnalysis.counts.TRUSTED})</button>
                    </div>
                </div>
                <div id="ageTableContainer" style="max-height: 500px; overflow: auto;">
                    <!-- Table will be inserted here -->
                </div>
            </div>

            <div style="margin-top: 2rem; text-align: center;">
                <button class="btn-secondary" onclick="location.reload()" style="padding: 1rem 2rem;">
                    ← Start New Analysis
                </button>
            </div>
        </div >
    `;

    // Store data globally for table rendering
    window.accountAgeData = data;

    // Render initial table
    renderAgeTable('ALL');
}

// Render Age Table
window.renderAgeTable = function (filter) {
    const data = window.accountAgeData;
    if (!data) return;

    const ageAnalysis = data.age_analysis;
    const container = document.getElementById('ageTableContainer');

    // Update button states
    document.querySelectorAll('.seg-tab').forEach(btn => btn.classList.remove('active'));
    event?.target?.classList?.add('active') || document.querySelector('.seg-tab').classList.add('active');

    let rows = [];
    if (filter === 'ALL') {
        rows = [...ageAnalysis.segments.NEW, ...ageAnalysis.segments.ACTIVE, ...ageAnalysis.segments.TRUSTED];
    } else {
        rows = ageAnalysis.segments[filter] || [];
    }

    rows.sort((a, b) => (a.__age_days || 0) - (b.__age_days || 0));

    if (rows.length === 0) {
        container.innerHTML = `<div style="padding: 2rem; color: var(--text-muted); text-align: center;">No accounts in this category</div>`;
        return;
    }

    const keys = Object.keys(rows[0]).filter(k => !['group', 'meaning', 'action', '__age_days'].includes(k));

    container.innerHTML = `
    <table class="data-table">
            <thead style="position: sticky; top: 0; background: var(--bg-card); z-index: 10;">
                <tr>
                    ${keys.map(k => `<th>${k}</th>`).join('')}
                    <th>Status</th>
                    <th>Business Meaning</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody>
                ${rows.map(row => {
        const g = row.group;
        const colors = { NEW: '#059669', ACTIVE: '#1E40AF', OLD: '#D97706' };
        const color = colors[g] || '#64748B';

        return `
                        <tr>
                            ${keys.map(k => `<td>${row[k]}</td>`).join('')}
                            <td><span style="background: ${color}20; color: ${color}; padding: 0.25rem 0.5rem; border-radius: 4px; font-weight: 600; font-size: 0.8rem;">${g}</span></td>
                            <td style="color: var(--text-secondary); font-size: 0.9rem;">${row.meaning}</td>
                            <td style="font-weight: 500; font-size: 0.9rem; color: ${color};">${row.action}</td>
                        </tr>
                    `;
    }).join('')}
            </tbody>
        </table>
    `;
};
