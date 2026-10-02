"use strict";
(() => {
  const report = JSON.parse(document.getElementById("report-data").textContent);
  const all = report.jobs, postings = report.postings, meta = report.metadata;
  const names = {saramin:"사람인",jobkorea:"잡코리아",incruit:"인크루트",linkareer:"링커리어"};
  const roleNames = {BE:"백엔드",DA:"데이터 분석",DE:"데이터 엔지니어링"};
  const careerNames = {new:"신입",exp:"경력",new_or_exp:"신입·경력",any:"경력 무관",none:"경력 무관",unknown:"경력 미확인"};
  const deadlineNames = {date:"날짜 지정",always:"상시 채용",until_filled:"채용 시 마감",unknown:"마감 미확인"};
  const el = id => document.getElementById(id);
  const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  const fmt = number => Number(number).toLocaleString("ko-KR");
  const date = value => value ? new Date(String(value).replace(" ","T")) : null;
  const time = (value, short = false) => {
    const d = date(value);
    if (!d || Number.isNaN(d.getTime())) return "미확인";
    const opts = short ? {month:"2-digit",day:"2-digit"} : {year:"numeric",month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit",hour12:false};
    const parts = Object.fromEntries(new Intl.DateTimeFormat("ko-KR", {...opts,timeZone:"Asia/Seoul"}).formatToParts(d).filter(part => part.type !== "literal").map(part => [part.type,part.value]));
    return short ? `${parts.month}.${parts.day}` : `${parts.year}.${parts.month}.${parts.day} ${parts.hour}:${parts.minute}`;
  };
  const sortableDate = (value, fallback) => {
    const d = date(value); return d && !Number.isNaN(d.getTime()) ? d.getTime() : fallback;
  };
  const regions = job => job.sido ? [job.sido] : job.regions?.length ? job.regions : job.region ? [job.region] : ["미확인"];
  const careerType = job => job.career_type || "unknown";
  const deadlineType = job => job.deadline_kind || job.source_links?.find(s => s.deadline_kind)?.deadline_kind || "unknown";
  const careerLabel = job => {
    if (job.career_raw) return job.career_raw;
    const years = job.career_min_yr != null ? ` ${job.career_min_yr}년${job.career_max_yr != null ? `–${job.career_max_yr}년` : " 이상"}` : "";
    return (careerNames[careerType(job)] || "경력 미확인") + years;
  };
  const deadlineLabel = job => job.deadline_at ? `${time(job.deadline_at, true)} 마감` : (deadlineNames[deadlineType(job)] || "마감 미확인");
  const storageKey = "careerradar.saved.v1";
  let saved = new Set(), storageAvailable = true;
  try { const previous = JSON.parse(localStorage.getItem(storageKey) || "[]"); saved = new Set(Array.isArray(previous) ? previous.map(String) : []); }
  catch (_) { storageAvailable = false; }
  let page = 0, filtered = [], toastTimer;
  const stateIds = ["search","role","source","region","career","deadline","open","skill","saved-only"];
  const bookmarkIcon = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 4h12v17l-6-4-6 4V4Z"/></svg>';
  const externalIcon = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M14 4h6v6m0-6-10 10M10 4H4v16h16v-6"/></svg>';
  const fullScans = meta.operations?.full_scans || [];
  const fullScanText = job => job.sources.map(source => {
    const scans = fullScans.filter(scan => scan.platform === source).map(scan => scan.slot_kst).filter(Boolean).sort();
    return scans.length ? `${names[source] || source} ${time(scans[0])}${scans[0] !== scans.at(-1) ? ` ~ ${time(scans.at(-1))}` : ""}` : `${names[source] || source} 완료 전수 시각 미확인`;
  }).join(" / ");

  all.forEach(job => {
    job.sources ||= [];
    job.job_groups ||= [];
    job.skills ||= [];
    job.source_links ||= [];
    job._search = [job.company_name,job.title,...job.skills,...job.job_groups,job.region,job.career_raw].filter(Boolean).join(" ").toLocaleLowerCase("ko-KR");
  });
  el("nav-count").textContent = fmt(all.length);
  el("total-jobs").textContent = fmt(all.length);
  el("total-postings").textContent = fmt(postings.length);
  el("total-companies").textContent = fmt(new Set(all.map(job => job.company_name).filter(Boolean)).size);
  el("total-shared").textContent = fmt(all.filter(job => job.sources.length > 1).length);
  el("asof").textContent = time(meta.asof);
  el("result-total").textContent = `건 / 전체 ${fmt(all.length)}건`;
  el("dataset-status").textContent = meta.final_slot ? "최종 슬롯 반영" : "부분 관측";
  el("dataset-status").classList.toggle("complete",meta.final_slot);
  el("dataset-status").classList.toggle("partial",!meta.final_slot);
  if (meta.final_slot) {
    el("partial-notice").querySelector("strong").textContent = "20:00 최종 슬롯이 반영되었습니다.";
    el("partial-text").textContent = "수집 시작 전 구간은 결측으로 남아 있습니다. 날짜 전체 또는 전체 채용 시장을 관측한 데이터가 아닙니다.";
  }
  el("source-nav").innerHTML = Object.entries(names).map(([id,label]) => `<button type="button" class="source-${id}" data-source="${id}"><span class="source-dot"></span>${label}<small>${fmt(meta.source_counts[id] || 0)}</small></button>`).join("");
  const uniqueRegions = [...new Set(all.flatMap(regions))].sort((a,b) => a.localeCompare(b,"ko"));
  uniqueRegions.forEach(region => el("region").add(new Option(region,region)));
  const uniqueSkills = [...new Set(all.flatMap(job => job.skills))].sort((a,b) => a.localeCompare(b,"ko"));
  uniqueSkills.forEach(skill => el("skill").add(new Option(skill,skill)));
  const conditionsNote = document.createElement("p");
  conditionsNote.className = "conditions-note";
  conditionsNote.textContent = "지역·경력·마감 조건은 대표 원천의 수집값입니다. 미확인 값은 임의로 추정하지 않습니다.";
  el("detail-filters").append(conditionsNote);

  function announce(message) {
    el("toast").textContent = message; el("toast").hidden = false;
    clearTimeout(toastTimer); toastTimer = setTimeout(() => { el("toast").hidden = true; }, 2200);
  }
  function renderRow(job) {
    const id = String(job.canonical_id), isSaved = saved.has(id);
    const firstUrl = job.source_links[0]?.url;
    const title = firstUrl ? `<a class="posting-title" href="${esc(firstUrl)}" target="_blank" rel="noopener noreferrer">${esc(job.title)}</a>` : `<span class="posting-title">${esc(job.title)}</span>`;
    const roleTags = job.job_groups.map(role => `<span class="tag role-${esc(role)}" title="${esc(roleNames[role] || role)}">${esc(role)}</span>`).join("");
    const skillTag = skill => `<span class="tag skill-tag">${esc(skill)}</span>`;
    const skillTags = job.skills.length ? `<div class="tags skill-tags">${job.skills.slice(0,5).map(skillTag).join("")}${job.skills.length > 5 ? `<details class="more-skills"><summary class="tag skill-tag">+${job.skills.length - 5}</summary><div class="tags">${job.skills.slice(5).map(skillTag).join("")}</div></details>` : ""}</div>` : '<span class="no-skills">기술 미검출</span>';
    const sources = job.source_links.map(source => `<a class="source-link source-${esc(source.platform)}" href="${esc(source.url)}" target="_blank" rel="noopener noreferrer" aria-label="${esc(names[source.platform] || source.platform)} 원천 공고 ${esc(source.posting_id)}"><span>${esc(names[source.platform] || source.platform)}</span><small>#${esc(source.posting_id)}</small>${externalIcon}</a>`).join("");
    const state = job.is_open === true ? ["yes","열림"] : job.is_open === false ? ["no","관측 이탈"] : ["unknown","판정 불가"];
    const representative = names[job.representative_platform] || "대표 원천";
    return `<tr data-id="${esc(id)}"><td class="company-cell"><div class="company-name">${esc(job.company_name || "회사 미확인")}</div><div class="company-sub">통합 ID ${esc(id)}</div></td><td class="posting-cell">${title}${job.sources.length > 1 ? `<span class="merged-badge">${job.sources.length}개 원천 통합</span>` : ""}<div class="condition-line" title="${esc(representative)}의 수집 조건"><span>${esc(job.region || regions(job).join(" · "))}</span><span>${esc(careerLabel(job))}</span></div><div class="deadline-line">${esc(deadlineLabel(job))}${job.education ? ` · ${esc(job.education)}` : ""}</div></td><td class="skills-cell"><div class="tags">${roleTags || '<span class="no-skills">직무 미확인</span>'}</div>${skillTags}</td><td class="sources-cell"><div class="source-links">${sources}</div></td><td class="state-cell"><span class="open-state ${state[0]}" title="${esc(fullScanText(job))}">${state[1]}</span><small class="state-note">전수 관측 기준</small></td><td class="save-cell"><button type="button" class="save-button${isSaved ? " saved" : ""}" data-save="${esc(id)}" aria-pressed="${isSaved}" aria-label="${esc(job.company_name)} 공고 ${isSaved ? "저장 해제" : "저장"}">${bookmarkIcon}</button></td></tr>`;
  }
  function getFiltered() {
    const tokens = el("search").value.toLocaleLowerCase("ko-KR").trim().split(/\s+/).filter(Boolean);
    const role = el("role").value, source = el("source").value, region = el("region").value, career = el("career").value;
    const deadline = el("deadline").value, open = el("open").value, skill = el("skill").value;
    return all.filter(job => tokens.every(token => job._search.includes(token)) && (!role || job.job_groups.includes(role)) && (!source || job.sources.includes(source))
      && (!region || regions(job).includes(region)) && (!career || careerType(job) === career) && (!deadline || deadlineType(job) === deadline)
      && (!open || (open === "unknown" ? job.is_open == null : job.is_open === (open === "true")))
      && (!skill || (skill === "__unknown" ? !job.skills.length : job.skills.includes(skill))) && (!el("saved-only").checked || saved.has(String(job.canonical_id))));
  }
  function sortRows(rows) {
    const sort = el("sort").value;
    const sorter = {
      "seen-desc": (a,b) => sortableDate(b.last_seen_at,0) - sortableDate(a.last_seen_at,0),
      "posted-desc": (a,b) => sortableDate(b.first_posted_at,0) - sortableDate(a.first_posted_at,0),
      "deadline-asc": (a,b) => sortableDate(a.deadline_at,Infinity) - sortableDate(b.deadline_at,Infinity),
      "company-asc": (a,b) => (a.company_name || "").localeCompare(b.company_name || "","ko"),
      "sources-desc": (a,b) => b.sources.length - a.sources.length
    }[sort];
    return rows.sort((a,b) => sorter(a,b) || Number(b.canonical_id) - Number(a.canonical_id));
  }
  function render() {
    filtered = sortRows(getFiltered());
    const pageSize = Number(el("page-size").value), pages = Math.max(1,Math.ceil(filtered.length / pageSize));
    page = Math.max(0,Math.min(page,pages - 1));
    const start = page * pageSize, rows = filtered.slice(start,start + pageSize);
    el("result-count").textContent = fmt(filtered.length);
    el("list-count").textContent = fmt(filtered.length);
    el("rows").innerHTML = rows.map(renderRow).join("");
    el("empty-state").hidden = filtered.length > 0;
    el("job-table").hidden = !filtered.length;
    el("page-range").textContent = filtered.length ? `${fmt(start+1)}–${fmt(start+rows.length)} / ${fmt(filtered.length)}건` : "0건";
    const indexes = new Set([0,pages-1,...[page-1,page,page+1].filter(n => n >= 0 && n < pages)]);
    const previous = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m14 6-6 6 6 6"/></svg>';
    const next = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m10 6 6 6-6 6"/></svg>';
    let buttons = `<button type="button" data-page="${page-1}" aria-label="이전 페이지" ${page === 0 ? "disabled" : ""}>${previous}</button>`, last = -1;
    [...indexes].sort((a,b) => a-b).forEach(index => {
      if (last >= 0 && index - last > 1) buttons += '<span class="page-ellipsis">…</span>';
      buttons += `<button type="button" data-page="${index}" aria-label="${index+1}페이지" ${index === page ? 'aria-current="page"' : ""}>${index+1}</button>`;
      last = index;
    });
    buttons += `<button type="button" data-page="${page+1}" aria-label="다음 페이지" ${page+1 >= pages ? "disabled" : ""}>${next}</button>`;
    el("page-nav").innerHTML = buttons;
    document.querySelectorAll("[data-source]").forEach(button => button.classList.toggle("active",button.dataset.source === el("source").value));
    const detailed = ["region","career","deadline","open","skill"].filter(id => el(id).value).length + Number(el("saved-only").checked);
    el("active-count").hidden = detailed === 0; el("active-count").textContent = detailed;
  }
  function reset() {
    stateIds.forEach(id => { if (el(id).type === "checkbox") el(id).checked = false; else el(id).value = ""; });
    page = 0; render();
  }
  stateIds.forEach(id => el(id).addEventListener(id === "search" ? "input" : "change",() => { page = 0; render(); }));
  el("sort").addEventListener("change",() => { page = 0; render(); });
  el("page-size").addEventListener("change",() => { page = 0; render(); });
  el("reset").addEventListener("click",reset); el("empty-reset").addEventListener("click",reset);
  el("toggle-filters").addEventListener("click",() => { const expanded = el("toggle-filters").getAttribute("aria-expanded") === "true"; el("detail-filters").hidden = expanded; el("toggle-filters").setAttribute("aria-expanded",String(!expanded)); });
  el("page-nav").addEventListener("click",event => { const button = event.target.closest("[data-page]"); if (!button || button.disabled) return; page = Number(button.dataset.page); render(); });
  el("rows").addEventListener("click",event => {
    const button = event.target.closest("[data-save]"); if (!button) return;
    const id = button.dataset.save; saved.has(id) ? saved.delete(id) : saved.add(id);
    try { localStorage.setItem(storageKey,JSON.stringify([...saved])); } catch (_) { storageAvailable = false; }
    render(); announce(saved.has(id) ? (storageAvailable ? "공고를 이 브라우저에 저장했습니다" : "현재 페이지 동안 공고를 저장합니다") : "공고 저장을 해제했습니다");
  });
  el("source-nav").addEventListener("click",event => { const button = event.target.closest("[data-source]"); if (!button) return; el("source").value = button.dataset.source; page = 0; location.hash = "list"; activatePanel(); render(); });
  window.addEventListener("keydown",event => { if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") { event.preventDefault(); location.hash = "list"; activatePanel(); el("search").focus(); } });

  function download(rows, filename) {
    if (!rows.length) { announce("다운로드할 공고가 없습니다"); return; }
    const columns = Object.keys(rows[0]).filter(key => !key.startsWith("_"));
    const cell = value => {
      const content = value == null ? "" : typeof value === "object" ? JSON.stringify(value) : String(value);
      const safe = /^[=+\-@\t\r]/.test(content) ? "'" + content : content;
      return '"' + safe.replace(/"/g,'""') + '"';
    };
    const csv = "\ufeff" + [columns.map(cell).join(","),...rows.map(row => columns.map(key => cell(row[key])).join(","))].join("\r\n");
    const url = URL.createObjectURL(new Blob([csv],{type:"text/csv;charset=utf-8;"}));
    const link = document.createElement("a"); link.href = url; link.download = filename; link.click(); setTimeout(() => URL.revokeObjectURL(url),1000);
    announce(`${fmt(rows.length)}건의 CSV를 다운로드합니다`);
  }
  el("export-top").addEventListener("click",() => download(all,"CareerRadar_all_jobs.csv"));
  el("export-filtered").addEventListener("click",() => download(filtered,"CareerRadar_filtered_jobs.csv"));
  el("export-sources").addEventListener("click",() => download(postings,"CareerRadar_all_platform_postings.csv"));

  function barChart(id, entries, palette = {}) {
    const max = Math.max(1,...entries.map(([,value]) => value));
    el(id).innerHTML = entries.map(([label,value]) => `<div class="bar-row"><span class="bar-label">${esc(label)}</span><span class="bar-track"><span class="bar-fill" style="display:block;width:${value/max*100}%;background:${palette[label] || "#876cce"}"></span></span><span class="bar-value">${fmt(value)}건</span></div>`).join("");
  }
  barChart("role-chart",Object.keys(roleNames).map(role => [role,all.filter(job => job.job_groups.includes(role)).length]),{BE:"#876cce",DA:"#68a98d",DE:"#6b96c8"});
  barChart("source-chart",Object.entries(names).map(([source,label]) => [label,meta.source_counts[source] || 0]),{"사람인":"#7391d1","잡코리아":"#6c93c3","인크루트":"#dba86d","링커리어":"#6ca997"});
  const skillCounts = new Map(); all.forEach(job => new Set(job.skills).forEach(skill => skillCounts.set(skill,(skillCounts.get(skill) || 0)+1)));
  barChart("skill-chart",[...skillCounts].sort((a,b) => b[1]-a[1] || a[0].localeCompare(b[0])).slice(0,10));
  el("skill-note").textContent = `${fmt(all.filter(job => job.skills.length).length)}개 공고에서 기술이 검출되었습니다. 텍스트·태그의 사전 검색 빈도이며 미검출은 요구 기술이 없다는 뜻이 아닙니다.`;
  barChart("open-chart",[["전수 기준 열림",all.filter(job => job.is_open === true).length],["관측 이탈",all.filter(job => job.is_open === false).length],["판정 불가",all.filter(job => job.is_open == null).length]],{"전수 기준 열림":"#71ad95","관측 이탈":"#bf99a9","판정 불가":"#b0b9c8"});
  const operations = meta.operations || {}, slots = operations.slots || [];
  el("operations-asof").textContent = `상태 확인 ${time(operations.checked_at)} · 정적 스냅샷`;
  const slotClasses = {SUCCESS:"slot-success",LEFT_TRUNCATED:"slot-missing",FUTURE:"slot-future"};
  const slotNames = {SUCCESS:"성공",LEFT_TRUNCATED:"수집 시작 전",FUTURE:"미래 슬롯"};
  el("slot-grid").innerHTML = slots.map(slot => `<span class="${slotClasses[slot.state] || "slot-failed"}" role="img" aria-label="${esc(time(slot.slot_kst))} ${esc(slotNames[slot.state] || slot.state)}" title="${esc(time(slot.slot_kst))} · ${esc(slotNames[slot.state] || slot.state)}"></span>`).join("");
  const counts = operations.counts || {};
  el("slot-note").textContent = `${fmt(slots.length)}개 계획 슬롯 중 성공 ${fmt(counts.SUCCESS || 0)}개 · 수집 시작 전 ${fmt(counts.LEFT_TRUNCATED || 0)}개 · 미래 ${fmt(counts.FUTURE || 0)}개. 결측 슬롯은 0건으로 대체하지 않습니다.`;
  el("source-status").innerHTML = Object.entries(names).map(([source,label]) => {
    const status = (operations.sources || []).find(row => row.platform === source);
    const scans = fullScans.filter(scan => scan.platform === source);
    const scanText = scans.length ? `마지막 전수 ${scans.map(scan => `${scan.query_key || "목록"} ${time(scan.slot_kst)}`).join(" · ")}` : "완료 전수 시각 미확인";
    return `<div class="source-status-row"><strong class="source-${source}"><span class="source-dot"></span>${label}</strong><div><span class="source-state-label${status?.allowed !== true ? " blocked" : ""}">${status?.allowed === true ? `접근 확인 · HTTP ${status.robots_status ?? "미확인"}` : "접근 미확인"}</span><p>${esc(scanText)}</p></div></div>`;
  }).join("");
  const checks = {...(meta.report_checks || {}),"원천 링크 전부 연결":meta.source_links === meta.platform_rows,"외부 CDN 없이 동작":true};
  el("report-checks").innerHTML = Object.entries(checks).map(([label,passed]) => `<div class="check-item"><span class="check-indicator${passed ? "" : " pending"}">${passed ? "✓" : "…"}</span>${esc(label)}<span class="sr-only">${passed ? "통과" : "미충족 또는 대기"}</span></div>`).join("");
  function activatePanel() {
    const hash = location.hash.slice(1), selected = ["list","overview","collection"].includes(hash) ? hash : "list";
    ["list","overview","collection"].forEach(panel => el(`panel-${panel}`).hidden = panel !== selected);
    document.querySelectorAll("[data-panel]").forEach(link => { const active = link.dataset.panel === selected; link.classList.toggle("active",active); if (active) link.setAttribute("aria-current","page"); else link.removeAttribute("aria-current"); });
    const titles = {list:["직무별 채용 공고 전체 목록","여러 채용 사이트의 공고를 한곳에서, 원하는 조건으로 살펴보세요."],overview:["채용 공고 관측 요약","원천과 직무별 분포를 실제 수집 데이터로 비교합니다."],collection:["데이터 수집 상태","관측한 범위와 아직 관측하지 못한 구간을 함께 확인하세요."]};
    el("page-title").textContent = titles[selected][0]; el("page-description").textContent = titles[selected][1];
  }
  window.addEventListener("hashchange",activatePanel);
  activatePanel(); render();
})();
