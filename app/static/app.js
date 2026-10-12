/* TerraSignal client: plain JS, explicit API errors, no mock login. */
const $ = (s, root=document) => root.querySelector(s);
const $$ = (s, root=document) => [...root.querySelectorAll(s)];
let token = null, currentUser = null, alerts = [], overviewMap = null, explorerMap = null, overviewMarkers = [], explorerMarkers = [], toastTimer = null;
const COLORS = {Critical:"#bd5e4d",High:"#d98a61",Medium:"#d3b052",Low:"#5f9a82"};
const ICONS = {Deforestation:"trees", "Water stress":"droplets", "Urban growth":"building-2", "Coastal change":"waves", Drought:"sun", "Land cover":"layers-3"};
function toast(message, bad=false){const el=$("#toast");el.textContent=message;el.style.background=bad?"#8e3f35":"#1d302b";el.classList.add("show");clearTimeout(toastTimer);toastTimer=setTimeout(()=>el.classList.remove("show"),3200)}
function renderIcons(){if(window.lucide)lucide.createIcons({attrs:{"stroke-width":1.8}})}
function initials(name="TS"){return name.trim().split(/\s+/).slice(0,2).map(x=>x[0]).join("").toUpperCase()}
async function api(path, options={}){const headers={"Content-Type":"application/json",...(options.headers||{})};if(token)headers.Authorization=`Bearer ${token}`;const res=await fetch(path,{...options,headers});if(!res.ok){let msg=`Request failed (${res.status})`;try{msg=(await res.json()).detail||msg}catch{};throw new Error(msg)}if(res.status===204)return null;return res.json()}
function setAuthTab(tab){const isLogin=tab==="login";$("#login-form-wrap").hidden=!isLogin;$("#register-form-wrap").hidden=isLogin;$$("[data-auth-tab]").forEach(b=>b.classList.toggle("active",b.dataset.authTab===tab));}
$$("[data-auth-tab]").forEach(b=>b.addEventListener("click",()=>setAuthTab(b.dataset.authTab)));
$("#login-form").addEventListener("submit",async e=>{e.preventDefault();const button=e.submitter;button.disabled=true;try{const data=Object.fromEntries(new FormData(e.currentTarget));const out=await api("/api/auth/login",{method:"POST",body:JSON.stringify(data)});acceptSession(out)}catch(err){toast(err.message,true)}finally{button.disabled=false}});
$("#register-form").addEventListener("submit",async e=>{e.preventDefault();const button=e.submitter;button.disabled=true;try{const out=await api("/api/auth/register",{method:"POST",body:JSON.stringify(Object.fromEntries(new FormData(e.currentTarget)))});acceptSession(out)}catch(err){toast(err.message,true)}finally{button.disabled=false}});
function acceptSession(out){token=out.access_token;currentUser=out.user;$("#auth-screen").hidden=true;$("#app-screen").hidden=false;$("#user-name").textContent=currentUser.full_name;$("#user-role").textContent=currentUser.role;$("#avatar").textContent=initials(currentUser.full_name);$("#top-avatar").textContent=initials(currentUser.full_name);$("#settings-name").textContent=currentUser.full_name;$("#settings-email").textContent=currentUser.email;$("#settings-role").textContent=currentUser.role;$$(".admin-only").forEach(el=>el.hidden=currentUser.role!=="admin");navigate("overview");loadAll();renderIcons();toast(`Welcome back, ${currentUser.full_name.split(" ")[0]}`)}
$("#logout").addEventListener("click",()=>{token=null;currentUser=null;alerts=[];if(overviewMap){overviewMap.remove();overviewMap=null}if(explorerMap){explorerMap.remove();explorerMap=null}$("#app-screen").hidden=true;$("#auth-screen").hidden=false;$("#login-form").reset();$("#register-form").reset();toast("You have signed out")});
function navigate(page){const target=$("#page-"+page);if(!target){toast("This page is not available",true);return}if(page==="admin"&&currentUser?.role!=="admin"){toast("Administrator access is required",true);return}$$(".page").forEach(p=>p.classList.remove("active"));target.classList.add("active");$$(".nav-link").forEach(n=>n.classList.toggle("active",n.dataset.page===page));$("#crumb").textContent=({"overview":"Overview","explorer":"Signal explorer","alerts":"Alert inbox","admin":"Admin console","research":"Research lab","models":"Model registry","organization":"Organization","settings":"Settings","guide":"Field guide"})[page]||page;$("#sidebar").classList.remove("open");if(page==="explorer"){setTimeout(()=>{ensureMap("explorer");explorerMap.invalidateSize();renderMap("explorer")},80);renderAlertsTable()}if(page==="overview"){setTimeout(()=>{ensureMap("overview");overviewMap.invalidateSize();renderMap("overview")},80)}if(page==="alerts")renderInbox();if(page==="admin")loadAdmin();renderIcons()}
document.addEventListener("click",e=>{const el=e.target.closest("[data-page]");if(el){e.preventDefault();navigate(el.dataset.page)}});
$("#menu-toggle").addEventListener("click",()=>$("#sidebar").classList.toggle("open"));
$("#refresh").addEventListener("click",()=>loadAll(true));
$("#dismiss-banner").addEventListener("click",()=>$(".demo-banner").remove());
async function loadAll(showToast=false){try{const [o,a]=await Promise.all([api("/api/overview"),api("/api/alerts")]);alerts=a;$("#metric-total").textContent=o.total_alerts;$("#metric-high").textContent=o.high+o.critical;$("#metric-confidence").textContent=o.avg_confidence;$("#confidence-fill").style.width=`${o.avg_confidence}%`;$("#metric-regions").textContent=o.regions_monitored;renderPriority();renderComposition();renderAlertsTable();renderInbox();if(overviewMap)renderMap("overview");if(explorerMap)renderMap("explorer");if(showToast)toast("Workspace data refreshed")}catch(err){toast(err.message,true)}}
function ensureMap(which){if(!window.L)return;let map=which==="overview"?overviewMap:explorerMap;if(map)return;const id=which==="overview"?"overview-map":"explorer-map";map=L.map(id,{zoomControl:true,scrollWheelZoom:false}).setView([15,30],2);L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",{maxZoom:18,attribution:'&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noreferrer">OpenStreetMap</a> contributors'}).addTo(map);if(which==="overview")overviewMap=map;else explorerMap=map}
function markerIcon(severity){const c=COLORS[severity]||COLORS.Low;return L.divIcon({className:"ts-marker-wrap",html:`<span style="display:block;width:15px;height:15px;border-radius:50%;background:${c};border:3px solid white;box-shadow:0 1px 7px #1b302b66;outline:2px solid ${c}55"></span>`,iconSize:[17,17],iconAnchor:[8,8]})}
function filteredAlerts(){const q=($("#search-alerts")?.value||"").toLowerCase().trim(),cat=$("#filter-category")?.value||"All categories",sev=$("#filter-severity")?.value||"All severities";return alerts.filter(a=>(cat==="All categories"||a.category===cat)&&(sev==="All severities"||a.severity===sev)&&(!q||`${a.title} ${a.region} ${a.category}`.toLowerCase().includes(q)))}
function renderMap(which){if(!window.L)return;const map=which==="overview"?overviewMap:explorerMap;if(!map)return;const filtered=which==="explorer"?filteredAlerts():alerts;const old=which==="overview"?overviewMarkers:explorerMarkers;old.forEach(m=>m.remove());const next=filtered.map(a=>L.marker([a.latitude,a.longitude],{icon:markerIcon(a.severity)}).addTo(map).bindPopup(`<strong>${escapeHtml(a.title)}</strong><br>${escapeHtml(a.region)}<br><span style="color:${COLORS[a.severity]}">${escapeHtml(a.severity)}</span> · ${a.confidence}% illustrative confidence`));if(which==="overview")overviewMarkers=next;else explorerMarkers=next;if(filtered.length&&which==="explorer"){const group=L.featureGroup(next);map.fitBounds(group.getBounds().pad(.15),{maxZoom:4})}}
function escapeHtml(s=""){return String(s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]))}
function iconFor(a){return ICONS[a.category]||"map-pin"}
function renderPriority(){const host=$("#priority-list");const rows=[...alerts].sort((a,b)=>b.confidence-a.confidence).slice(0,4);host.innerHTML=rows.map(a=>`<div class="priority-item"><span class="priority-symbol"><i data-lucide="${iconFor(a)}"></i></span><div class="priority-main"><strong>${escapeHtml(a.title)}</strong><p>${escapeHtml(a.region)} · ${escapeHtml(a.category)}</p></div><span class="priority-score">${a.confidence}%</span></div>`).join("")||'<p class="muted">No signals match.</p>';renderIcons()}
function renderComposition(){const host=$("#composition");const counts={};alerts.forEach(a=>counts[a.category]=(counts[a.category]||0)+1);const max=Math.max(1,...Object.values(counts));host.innerHTML=Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,5).map(([name,count])=>`<div class="composition-row"><div class="composition-label"><span>${escapeHtml(name)}</span><span>${count} signal${count===1?"":"s"}</span></div><div class="composition-track"><span style="width:${count/max*100}%"></span></div></div>`).join("")}
function statusBadge(status){
  const s = (status || "Pending Review").toLowerCase();
  let cls = "status-badge-pending";
  if(s.includes("verified")) cls = "status-badge-verified";
  else if(s.includes("action")) cls = "status-badge-action";
  else if(s.includes("investigat")) cls = "status-badge-investigating";
  else if(s.includes("false") || s.includes("reject")) cls = "status-badge-rejected";
  return `<span class="status-badge ${cls}">${escapeHtml(status || "Pending Review")}</span>`;
}

function filteredAlerts(){
  const q=($("#search-alerts")?.value||"").toLowerCase().trim(),
        cat=$("#filter-category")?.value||"All categories",
        sev=$("#filter-severity")?.value||"All severities",
        st=$("#filter-status")?.value||"All statuses";
  return alerts.filter(a=>(cat==="All categories"||a.category===cat)&&
                          (sev==="All severities"||a.severity===sev)&&
                          (st==="All statuses"||a.status===st)&&
                          (!q||`${a.title} ${a.region} ${a.category} ${a.status}`.toLowerCase().includes(q)));
}

function renderAlertsTable(){
  const host=$("#alerts-table");
  if(!host)return;
  const rows=filteredAlerts();
  $("#result-count").textContent=`${rows.length} record${rows.length===1?"":"s"} · human-verified pipeline`;
  host.innerHTML=rows.map(a=>`<tr>
    <td><div class="signal-cell"><span><i data-lucide="${iconFor(a)}"></i></span>${escapeHtml(a.title)}</div></td>
    <td>${escapeHtml(a.region)}</td>
    <td><span class="severity-pill severity-${a.severity.toLowerCase()}">${escapeHtml(a.severity)}</span></td>
    <td><div class="confidence-cell"><span>${a.confidence}%</span><span class="confidence-mini"><span style="width:${a.confidence}%"></span></span></div></td>
    <td>${statusBadge(a.status)}</td>
    <td><button class="btn-table-action" onclick="openReviewModal(${a.id})"><i data-lucide="check-square"></i> Review & Action</button></td>
  </tr>`).join("")||'<tr><td colspan="6">No matching signals. Try clearing filters.</td></tr>';
  renderIcons();
}

function renderInbox(){
  const host=$("#inbox-list");
  if(!host)return;
  const q=($("#inbox-search")?.value||"").toLowerCase(),
        sev=$("#inbox-severity")?.value||"All severities",
        st=$("#inbox-status")?.value||"All statuses";
  const rows=alerts.filter(a=>(sev==="All severities"||a.severity===sev)&&
                              (st==="All statuses"||a.status===st)&&
                              `${a.title} ${a.region} ${a.category} ${a.status} ${a.description||''}`.toLowerCase().includes(q))
                   .sort((a,b)=>b.confidence-a.confidence);
  host.innerHTML=rows.map(a=>{
    let reviewSnippet = '';
    if(a.review_notes || a.action_taken){
      reviewSnippet = `<div class="inbox-review-box">
        <strong><i data-lucide="shield-check" style="width:12px;height:12px;display:inline-block;vertical-align:middle"></i> Review Assessment:</strong>
        ${a.review_notes ? `<div>Note: ${escapeHtml(a.review_notes)}</div>` : ''}
        ${a.action_taken ? `<div>Action: <em>${escapeHtml(a.action_taken)}</em></div>` : ''}
        <small style="color:#788b82;display:block;margin-top:4px">Reviewed by ${escapeHtml(a.reviewed_by || 'Review Team')} on ${a.reviewed_at ? new Date(a.reviewed_at).toLocaleDateString() : ''}</small>
      </div>`;
    }
    return `<article class="inbox-card">
      <span class="priority-symbol"><i data-lucide="${iconFor(a)}"></i></span>
      <div class="inbox-body">
        <div class="inbox-top">
          <h3>${escapeHtml(a.title)}</h3>
          <span class="severity-pill severity-${a.severity.toLowerCase()}">${escapeHtml(a.severity)}</span>
        </div>
        <div class="inbox-meta">
          <span>${escapeHtml(a.region)}</span><span>·</span>
          <span>${escapeHtml(a.category)}</span>
          ${a.reported_by_email ? `<span>· Reported by: ${escapeHtml(a.reported_by_email)}</span>` : ''}
        </div>
        <p>${escapeHtml(a.description || 'No additional telemetry notes.')}</p>
        <div class="inbox-meta">
          <span class="inbox-confidence">${a.confidence}% confidence</span><span>·</span>
          ${statusBadge(a.status)}
        </div>
        ${reviewSnippet}
        <div class="inbox-card-actions">
          <span style="font-size:10px;color:#87938b">${a.created_at ? new Date(a.created_at).toLocaleString() : ''}</span>
          <button class="btn btn-secondary btn-table-action" onclick="openReviewModal(${a.id})"><i data-lucide="check-check"></i> Review & Action</button>
        </div>
      </div>
    </article>`;
  }).join("")||'<div class="panel empty-state"><h2>No matching alerts</h2><p>Try another search or severity filter.</p></div>';
  renderIcons();
}

["search-alerts","filter-category","filter-severity","filter-status"].forEach(id=>$("#"+id)?.addEventListener(id==="search-alerts"?"input":"change",()=>{renderAlertsTable();if(explorerMap)renderMap("explorer")}));
$("#clear-filters").addEventListener("click",()=>{$("#search-alerts").value="";$("#filter-category").selectedIndex=0;$("#filter-severity").selectedIndex=0;if($("#filter-status"))$("#filter-status").selectedIndex=0;renderAlertsTable();if(explorerMap)renderMap("explorer")});
["inbox-search","inbox-severity","inbox-status"].forEach(id=>$("#"+id)?.addEventListener(id==="inbox-search"?"input":"change",renderInbox));

/* Modal Handlers */
function openModal(id){const el=$("#"+id);if(el)el.hidden=false;renderIcons();}
function closeModal(id){const el=$("#"+id);if(el)el.hidden=true;}
$$("[data-close-modal]").forEach(btn=>btn.addEventListener("click",()=>closeModal(btn.dataset.closeModal)));
$$(".modal-backdrop").forEach(b=>b.addEventListener("click",e=>{if(e.target===b)b.hidden=true}));

["open-new-alert-overview","open-new-alert-explorer","open-new-alert-inbox"].forEach(id=>{
  $("#"+id)?.addEventListener("click",()=>openModal("modal-enter-alert"));
});

/* Enter Alert Form Submission */
$("#form-enter-alert")?.addEventListener("submit",async e=>{
  e.preventDefault();
  const form=e.currentTarget, submitBtn=form.querySelector("button[type='submit']");
  submitBtn.disabled=true;
  try{
    const fd=new FormData(form);
    const payload={
      title: fd.get("title"),
      category: fd.get("category"),
      severity: fd.get("severity"),
      region: fd.get("region"),
      latitude: parseFloat(fd.get("latitude")),
      longitude: parseFloat(fd.get("longitude")),
      confidence: parseInt(fd.get("confidence"),10),
      description: fd.get("description") || ""
    };
    await api("/api/alerts",{method:"POST",body:JSON.stringify(payload)});
    toast("Alert submitted successfully! Forwarded to Review Team.");
    form.reset();
    closeModal("modal-enter-alert");
    await loadAll();
    navigate("alerts");
  }catch(err){
    toast(err.message,true);
  }finally{
    submitBtn.disabled=false;
  }
});

/* Review Modal Trigger & Submission */
window.openReviewModal=function(alertId){
  const alertItem = alerts.find(a=>a.id===alertId);
  if(!alertItem){ toast("Alert not found",true); return; }
  $("#review-alert-id").value = alertItem.id;
  $("#review-alert-summary").innerHTML = `
    <div class="review-details-top">
      <strong>${escapeHtml(alertItem.title)}</strong>
      <span class="severity-pill severity-${alertItem.severity.toLowerCase()}">${escapeHtml(alertItem.severity)}</span>
    </div>
    <div><strong>Region:</strong> ${escapeHtml(alertItem.region)} &nbsp;|&nbsp; <strong>Category:</strong> ${escapeHtml(alertItem.category)}</div>
    <div><strong>Coordinates:</strong> Lat ${alertItem.latitude.toFixed(4)}, Lon ${alertItem.longitude.toFixed(4)}</div>
    <div><strong>Current Status:</strong> ${statusBadge(alertItem.status)}</div>
    <div style="color:#57665f;margin-top:4px">${escapeHtml(alertItem.description||'No additional telemetry notes recorded.')}</div>
  `;
  const form=$("#form-review-alert");
  form.elements["status"].value = ["Verified","Action Taken","Investigating","False Alarm","Rejected"].includes(alertItem.status) ? alertItem.status : "Verified";
  form.elements["review_notes"].value = alertItem.review_notes || "";
  form.elements["action_taken"].value = alertItem.action_taken || "";
  openModal("modal-review-action");
};

$("#form-review-alert")?.addEventListener("submit",async e=>{
  e.preventDefault();
  const form=e.currentTarget, submitBtn=form.querySelector("button[type='submit']");
  const alertId=$("#review-alert-id").value;
  submitBtn.disabled=true;
  try{
    const fd=new FormData(form);
    const payload={
      status: fd.get("status"),
      review_notes: fd.get("review_notes"),
      action_taken: fd.get("action_taken")
    };
    await api(`/api/alerts/${alertId}/review`,{method:"PATCH",body:JSON.stringify(payload)});
    toast("Review decision & mitigation action saved!");
    closeModal("modal-review-action");
    await loadAll();
  }catch(err){
    toast(err.message,true);
  }finally{
    submitBtn.disabled=false;
  }
});

function exportCsv(){if(!token){toast("Sign in to export records",true);return}window.open("/api/export/alerts.csv?token="+encodeURIComponent(token),"_blank");}
async function secureExport(){try{const res=await fetch("/api/export/alerts.csv",{headers:{Authorization:`Bearer ${token}`}});if(!res.ok)throw new Error("Export failed");const blob=await res.blob(),url=URL.createObjectURL(blob),a=document.createElement("a");a.href=url;a.download="terrasignal-alerts.csv";a.click();URL.revokeObjectURL(url);toast("CSV export ready")}catch(e){toast(e.message,true)}}
["export-btn","explorer-export","alerts-export"].forEach(id=>$("#"+id)&&($("#"+id).onclick=secureExport));

async function loadAdmin(){try{const [users,events]=await Promise.all([api("/api/admin/users"),api("/api/admin/audit")]);$("#users-table").innerHTML=users.map(u=>`<tr><td>${u.id}</td><td>${escapeHtml(u.full_name)}</td><td>${escapeHtml(u.email)}</td><td><span class="severity-pill ${u.role==="admin"?"severity-medium":"severity-low"}">${escapeHtml(u.role)}</span></td><td>${new Date(u.created_at).toLocaleDateString()}</td></tr>`).join("")||'<tr><td colspan="5">No users</td></tr>';$("#audit-table").innerHTML=events.map(r=>`<tr><td>${new Date(r.created_at).toLocaleString()}</td><td>${r.user_id??"—"}</td><td>${escapeHtml(r.action)}</td><td>${escapeHtml(r.detail||"—")}</td></tr>`).join("")||'<tr><td colspan="4">No audit events</td></tr>'}catch(e){toast(e.message,true);navigate("overview")}}
$("#refresh-admin")?.addEventListener("click",loadAdmin);
renderIcons();
