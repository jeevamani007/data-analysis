// Healthcare Date Details Drill-Down
window.showHealthcareDateDetails = function (dateIndex) {
    const dateInfo = window.healthcareTimelineData[dateIndex];
    if (!dateInfo) return;

    const detailsDiv = document.getElementById('date-details');
    if (!detailsDiv) return;

    let html = `
        <h3 style="font-size: 1.5rem; margin-bottom: 1.5rem; color: #14B8A6;">
            📅 ${dateInfo.date} - Detailed Breakdown
        </h3>
        
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; margin-bottom: 2rem;">
            <div>
                <h4 style="font-size: 1.1rem; margin-bottom: 1rem; color: var(--text-primary);">Visit Count by Time Slot</h4>
                <div style="display: flex; flex-direction: column; gap: 0.75rem;">
    `;

    const timeSlots = dateInfo.time_slots || {};
    const slotIcons = { Morning: '🌅', Afternoon: '☀️', Evening: '🌆', Night: '🌙' };

    Object.entries(timeSlots).forEach(([slot, count]) => {
        const icon = slotIcons[slot] || '⏰';
        html += `
            <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.75rem; background: var(--bg-page); border-radius: 8px;">
                <span>${icon} ${slot}</span>
                <span style="font-weight: 700; color: #14B8A6;">${count} visits</span>
            </div>
        `;
    });

    html += `
                </div>
            </div>
            
            <div>
                <h4 style="font-size: 1.1rem; margin-bottom: 1rem; color: var(--text-primary);">Top Departments</h4>
                <div style="display: flex; flex-direction: column; gap: 0.75rem;">
    `;

    const depts = dateInfo.departments || {};
    Object.entries(depts).slice(0, 5).forEach(([dept, count], idx) => {
        html += `
            <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.75rem; background: var(--bg-page); border-radius: 8px;">
                <span>${idx + 1}. ${dept}</span>
                <span style="font-weight: 700; color: #14B8A6;">${count}</span>
            </div>
        `;
    });

    html += `
                </div>
            </div>
        </div>
        
        <div>
            <h4 style="font-size: 1.1rem; margin-bottom: 1rem; color: var(--text-primary);">Top Diagnoses</h4>
            <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 1rem;">
    `;

    const diagnoses = dateInfo.diagnoses || {};
    Object.entries(diagnoses).forEach(([diag, count]) => {
        html += `
            <div style="padding: 1rem; background: rgba(20, 184, 166, 0.1); border: 1px solid rgba(20, 184, 166, 0.3); border-radius: 8px;">
                <div style="font-weight: 600; color: var(--text-primary); margin-bottom: 0.25rem;">${diag}</div>
                <div style="font-size: 1.5rem; font-weight: 700; color: #14B8A6;">${count}</div>
            </div>
        `;
    });

    html += `
            </div>
        </div>
        
        <button 
            onclick="document.getElementById('date-details').style.display='none'"
            class="btn-secondary"
            style="margin-top: 1.5rem;">
            Close Details
        </button>
    `;

    detailsDiv.innerHTML = html;
    detailsDiv.style.display = 'block';

    // Scroll to details
    detailsDiv.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
};
