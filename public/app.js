const API_BASE = window.location.origin && window.location.origin.startsWith('http')
  ? window.location.origin
  : 'http://127.0.0.1:3000';
const DEFAULT_SHOP = '拼多多旗舰店';
const DEFAULT_TITLE_PROMPT = '请将下面口播文案总结成一个完整的中文标题句子，长度控制在{min_len}-{max_len}字以内。';

const clientIdInput = document.getElementById('clientId');
const clientSecretInput = document.getElementById('clientSecret');
const redirectInput = document.getElementById('redirectUri');
const authBaseInput = document.getElementById('authBase');
const requireAuthInput = document.getElementById('requireAuth');
const goodsIdInput = document.getElementById('goodsId');
const productGoodsMapInput = document.getElementById('productGoodsMap');
const productTitleTagsInput = document.getElementById('productTitleTags');
const hotTitleTagsInput = document.getElementById('hotTitleTags');
const productShopSelect = document.getElementById('productShopSelect');
const shopListInput = document.getElementById('shopList');
const videoDescInput = document.getElementById('videoDesc');
const asrEnabledInput = document.getElementById('asrEnabled');
const titleModelInput = document.getElementById('titleModel');
const titlePromptInput = document.getElementById('titlePrompt');
const publishTimeSlotsInput = document.getElementById('publishTimeSlots');
const publishRatioInput = document.getElementById('publishRatio');
const dashscopeApiKeyInput = document.getElementById('dashscopeApiKey');
const dashscopeAsrModelInput = document.getElementById('dashscopeAsrModel');
const asrMaxSecondsInput = document.getElementById('asrMaxSeconds');
const asrContextInput = document.getElementById('asrContext');
const titleMinLenInput = document.getElementById('titleMinLen');
const titleMaxLenInput = document.getElementById('titleMaxLen');
const downloadEnabledInput = document.getElementById('downloadEnabled');
const downloadTimeInput = document.getElementById('downloadTime');
const downloadRemoteRootInput = document.getElementById('downloadRemoteRoot');
const downloadLocalRootInput = document.getElementById('downloadLocalRoot');
const baiduCliPathInput = document.getElementById('baiduCliPath');
const feishuWebhookInput = document.getElementById('feishuWebhook');
const feishuTestBtn = document.getElementById('feishuTestBtn');
const statusEl = document.getElementById('status');
const authArea = document.getElementById('authArea');
const authUrlEl = document.getElementById('authUrl');
const tokenAccess = document.getElementById('tokenAccess');
const tokenRefresh = document.getElementById('tokenRefresh');
const tokenExpire = document.getElementById('tokenExpire');
const authShopList = document.getElementById('authShopList');
const logArea = document.getElementById('logArea');
const logScopeTabs = document.querySelectorAll('[data-log-scope]');
const taskTable = document.getElementById('taskTable').querySelector('tbody');
const taskPrevBtn = document.getElementById('taskPrevBtn');
const taskNextBtn = document.getElementById('taskNextBtn');
const taskPageInfo = document.getElementById('taskPageInfo');
const startTimeInput = document.getElementById('startTime');
const intervalInput = document.getElementById('interval');
const dailyLimitInput = document.getElementById('dailyLimit');
const videoRootInput = document.getElementById('videoRoot');
const authCodeInput = document.getElementById('authCode');
const authStateInput = document.getElementById('authState');
const ffmpegStatus = document.getElementById('ffmpegStatus');
const ffmpegPath = document.getElementById('ffmpegPath');

const baiduStatus = document.getElementById('baiduStatus');
const baiduStatusDetail = document.getElementById('baiduStatusDetail');
const baiduActionStatus = document.getElementById('baiduActionStatus');
const baiduLoginOutput = document.getElementById('baiduLoginOutput');
const baiduBdussInput = document.getElementById('baiduBdussInput');
const baiduStokenInput = document.getElementById('baiduStokenInput');
const baiduBdussLoginBtn = document.getElementById('baiduBdussLoginBtn');
const baiduLogoutBtn = document.getElementById('baiduLogoutBtn');

const autoRunBtn = document.getElementById('autoRunBtn');
const autoRunStatus = document.getElementById('autoRunStatus');
const autoRunEnabledInput = document.getElementById('autoRunEnabled');
const downloadSuccessCount = document.getElementById('downloadSuccessCount');
const uploadSuccessCount = document.getElementById('uploadSuccessCount');
const downloadActionStatus = document.getElementById('downloadActionStatus');
const uploadActionStatus = document.getElementById('uploadActionStatus');
const autoPauseBtn = document.getElementById('autoPauseBtn');
const uploadPauseToggle = document.getElementById('uploadPauseToggle');
const downloadPauseToggle = document.getElementById('downloadPauseToggle');
const downloadProgressEl = document.getElementById('downloadProgress');
const uploadProgressEl = document.getElementById('uploadProgress');
const uploadShopList = document.getElementById('uploadShopList');
const scanBtnIgnore = document.getElementById('scanBtnIgnore');

const defaultRedirect = `${API_BASE}/auth/callback`;
const defaultRedirectEl = document.getElementById('defaultRedirect');
const stepRedirectEl = document.getElementById('stepRedirect');
if (defaultRedirectEl) defaultRedirectEl.innerText = defaultRedirect;
if (stepRedirectEl) stepRedirectEl.innerText = defaultRedirect;

let logScope = 'today';
let serverLogs = [];
let cachedConfig = {};
let cachedSchedule = {};
let productGoodsMapByShop = {};
let productTitleTagsByShop = {};
let activeProductShop = '';
let tokenShopMap = {};
let tokenLastAuth = {};
let tokenLastAuthShop = '';
let activeAuthShop = '';
let taskPage = 1;
let taskPageSize = 15;
let taskTotalPages = 1;
let taskItems = [];
const dirtyFields = new Set();

function markDirty(el) {
  if (el && el.id) dirtyFields.add(el.id);
}

function clearDirty(el) {
  if (el && el.id) dirtyFields.delete(el.id);
}

function isDirty(el) {
  return !!(el && el.id && dirtyFields.has(el.id));
}

function setValueIfClean(el, value) {
  if (!el || isDirty(el) || document.activeElement === el) return;
  el.value = value;
}

function setCheckedIfClean(el, value) {
  if (!el || isDirty(el) || document.activeElement === el) return;
  el.checked = value;
}

function apiFetch(path, options) {
  return fetch(`${API_BASE}${path}`, options);
}

async function apiJson(path, options) {
  try {
    const res = await apiFetch(path, options);
    const data = await res.json().catch(() => ({}));
    return { res, data };
  } catch (err) {
    return { res: null, data: { error: String(err) } };
  }
}

function trimToken(token) {
  if (!token) return '';
  const str = String(token);
  if (str.length <= 12) return str;
  return `${str.slice(0, 6)}...${str.slice(-4)}`;
}

function parseLines(text) {
  return String(text || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
}

function getScheduleShopNames() {
  const shops = cachedSchedule.shops || {};
  const names = Object.keys(shops);
  if (names.length) return names;
  if (shopListInput) {
    const fromInput = parseLines(shopListInput.value);
    if (fromInput.length) return fromInput;
  }
  return [DEFAULT_SHOP];
}

function renderShopOptions(selectEl, shopNames, selected) {
  if (!selectEl) return;
  const current = selected && shopNames.includes(selected) ? selected : shopNames[0] || '';
  selectEl.innerHTML = '';
  shopNames.forEach((name) => {
    const option = document.createElement('option');
    option.value = name;
    option.textContent = name;
    if (name === current) option.selected = true;
    selectEl.appendChild(option);
  });
}

function renderShopCheckboxes(container, shopNames, selectedSet) {
  if (!container) return;
  container.innerHTML = '';
  shopNames.forEach((name) => {
    const label = document.createElement('label');
    label.className = 'shop-chip';
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.value = name;
    checkbox.checked = selectedSet ? selectedSet.has(name) : true;
    label.appendChild(checkbox);
    const span = document.createElement('span');
    span.textContent = name;
    label.appendChild(span);
    container.appendChild(label);
  });
}

function getSelectedUploadShops() {
  if (!uploadShopList) return [];
  const inputs = uploadShopList.querySelectorAll('input[type="checkbox"]');
  const selected = [];
  inputs.forEach((input) => {
    if (input.checked) selected.push(input.value);
  });
  return selected;
}

function getShopTokenSummary(shopName) {
  const shopToken = (tokenShopMap && tokenShopMap[shopName]) || {};
  const last = shopToken.lastAuth || {};
  let authorized = !!last.access_token;
  let accessToken = last.access_token || '';
  let refreshToken = last.refresh_token || '';
  let expiresAt = last.expiresAtIso || '';
  return {
    authorized,
    expiresAt,
    accessToken,
    refreshToken,
  };
}

function updateTokenDisplay(shopName) {
  const summary = getShopTokenSummary(shopName);
  tokenAccess.textContent = summary.accessToken ? trimToken(summary.accessToken) : '-';
  tokenRefresh.textContent = summary.refreshToken ? trimToken(summary.refreshToken) : '-';
  tokenExpire.textContent = summary.expiresAt || '-';
}

function renderAuthShopList(shopNames) {
  if (!authShopList) return;
  authShopList.innerHTML = '';
  if (!shopNames.length) {
    authShopList.textContent = "暂无店铺，请先在店铺配置中添加。";
    return;
  }
  shopNames.forEach((name) => {
    const summary = getShopTokenSummary(name);
    const row = document.createElement('div');
    row.className = 'shop-row';
    const left = document.createElement('div');
    const title = document.createElement('div');
    title.textContent = name;
    const meta = document.createElement('div');
    meta.className = 'shop-meta';
    meta.textContent = summary.authorized
      ? `已授权 · 过期时间：${summary.expiresAt || '-'}`
      : "未授权";
    left.appendChild(title);
    left.appendChild(meta);

    const actions = document.createElement('div');
    actions.className = 'shop-actions';
    const authBtn = document.createElement('button');
    authBtn.className = 'btn ghost small';
    authBtn.textContent = "生成授权链接";
    authBtn.addEventListener('click', () => {
      generateAuthForShop(name);
    });
    const exchangeBtn = document.createElement('button');
    exchangeBtn.className = 'btn secondary small';
    exchangeBtn.textContent = "换取 Token";
    exchangeBtn.addEventListener('click', () => {
      exchangeTokenForShop(name);
    });
    const clearBtn = document.createElement('button');
    clearBtn.className = 'btn ghost small';
    clearBtn.textContent = "清除授权";
    clearBtn.addEventListener('click', () => {
      clearTokenForShop(name);
    });
    actions.appendChild(authBtn);
    actions.appendChild(exchangeBtn);
    actions.appendChild(clearBtn);

    row.appendChild(left);
    row.appendChild(actions);
    authShopList.appendChild(row);
  });
}

function updateProductMapView(shopName, force = false) {
  if (!productGoodsMapInput) return;
  const map = (productGoodsMapByShop && productGoodsMapByShop[shopName]) || {};
  if (!isDirty(productGoodsMapInput) || force) {
    productGoodsMapInput.value = mapToLines(map);
  }
  if (productTitleTagsInput) {
    const tagMap = (productTitleTagsByShop && productTitleTagsByShop[shopName]) || {};
    if (!isDirty(productTitleTagsInput) || force) {
      productTitleTagsInput.value = mapToTagLines(tagMap);
    }
  }
}

function syncShopUI() {
  const shopNames = getScheduleShopNames();
  renderShopOptions(productShopSelect, shopNames, productShopSelect?.value);
  renderShopCheckboxes(uploadShopList, shopNames);
  const productShop = productShopSelect?.value || shopNames[0];
  activeProductShop = productShop || '';
  updateProductMapView(productShop);
}

function showStatus(message, isError) {
  if (!statusEl) return;
  if (!message) {
    statusEl.style.display = 'none';
    statusEl.textContent = '';
    return;
  }
  statusEl.style.display = 'block';
  statusEl.textContent = message;
  statusEl.style.color = isError ? '#b42318' : 'var(--muted)';
}

function setActionStatus(el, message, isError, isSuccess) {
  if (!el) return;
  el.textContent = message || '';
  el.classList.toggle('error', !!isError);
  el.classList.toggle('success', !!isSuccess);
}

function setDotStatus(container, ok, label) {
  if (!container) return;
  const dot = container.querySelector('.dot');
  const spans = container.querySelectorAll('span');
  if (dot) {
    dot.classList.toggle('ok', ok === true);
    dot.classList.toggle('bad', ok === false);
  }
  if (spans.length > 1) {
    spans[1].textContent = label || '';
  }
}

function mapToLines(map) {
  if (!map || typeof map !== 'object') return '';
  return Object.entries(map)
    .map(([name, goodsId]) => `${name}=${goodsId}`)
    .join('\n');
}

function mapToTagLines(map) {
  if (!map || typeof map !== 'object') return '';
  return Object.entries(map)
    .map(([name, tags]) => `${name}=${(Array.isArray(tags) ? tags : []).join(',')}`)
    .join('\n');
}

function parseProductGoodsMap(text) {
  const raw = (text || '').trim();
  if (!raw) return {};
  if (raw.startsWith('{') && raw.endsWith('}')) {
    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === 'object' ? parsed : {};
    } catch (err) {
      return {};
    }
  }
  const map = {};
  raw.split(/\r?\n/).forEach((line) => {
    const trimmed = line.trim();
    if (!trimmed) return;
    let idx = trimmed.indexOf('=');
    if (idx < 0) idx = trimmed.indexOf(':');
    if (idx < 0) idx = trimmed.indexOf('：');
    if (idx < 0) return;
    const name = trimmed.slice(0, idx).trim();
    const goodsId = trimmed.slice(idx + 1).trim();
    if (name && goodsId) {
      map[name] = goodsId;
    }
  });
  return map;
}

function parseProductTagsMap(text) {
  const raw = (text || '').trim();
  if (!raw) return {};
  const map = {};
  raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .forEach((line) => {
      const parts = line.split('=');
      if (parts.length < 2) return;
      const key = parts[0].trim();
      if (!key) return;
      const tags = parts
        .slice(1)
        .join('=')
        .split(/[,，]/)
        .map((item) => item.trim().replace(/^#/, ''))
        .filter(Boolean);
      if (tags.length) map[key] = tags;
    });
  return map;
}

function parseTagLines(text) {
  return (text || '')
    .split(/\n+/)
    .map((line) => line.trim())
    .filter(Boolean)
    .flatMap((line) => line.split(/[,，]/))
    .map((tag) => tag.trim().replace(/^#/, ''))
    .filter(Boolean);
}

function formatTagLines(tags) {
  return (tags || [])
    .map((tag) => String(tag || '').trim())
    .filter(Boolean)
    .map((tag) => (tag.startsWith('#') ? tag.slice(1) : tag))
    .join('\n');
}

function normalizeRemoteRoot(input) {
  let value = (input || '').trim();
  if (!value) return '';
  const match = value.match(/path=([^&#]+)/);
  if (match) {
    try {
      value = decodeURIComponent(match[1]);
    } catch (err) {
      value = match[1];
    }
  }
  if (!value.startsWith('/')) {
    value = `/${value}`;
  }
  return value;
}


function getTodayDate() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function filterLogsByScope(logs, scope) {
  if (scope !== 'today') return logs;
  const today = getTodayDate();
  return logs.filter((entry) => String(entry.ts || '').startsWith(today));
}

function renderLogs() {
  const logs = filterLogsByScope(serverLogs, logScope);
  if (logArea) {
    const ordered = logs.slice().reverse();
    logArea.textContent = ordered.length
      ? ordered.map((entry) => `[${entry.ts}] ${entry.message}`).join('\n')
      : '暂无日志';
    logArea.scrollTop = logArea.scrollHeight;
  }
}

async function loadLogs() {
  const { res, data } = await apiJson('/api/logs');
  if (!res || !res.ok) return;
  serverLogs = Array.isArray(data.logs) ? data.logs : [];
  renderLogs();
}

async function clearLogs() {
  const { res, data } = await apiJson('/api/logs/clear', { method: 'POST' });
  if (!res || !res.ok) {
    showStatus(data.error || '清空日志失败', true);
    return;
  }
  showStatus('已清空页面日志', false);
  await loadLogs();
}

async function loadConfig() {
  const { res, data } = await apiJson('/api/config');
  if (!res || !res.ok) return;
  cachedConfig = data || {};
  setValueIfClean(clientIdInput, data.clientId || '');
  setValueIfClean(clientSecretInput, data.clientSecret || '');
  setValueIfClean(redirectInput, data.redirectUri || defaultRedirect);
  setValueIfClean(authBaseInput, data.authBase || 'https://mms.pinduoduo.com/open.html');
  setCheckedIfClean(requireAuthInput, data.requireAuth !== false);
  setValueIfClean(goodsIdInput, data.goodsId || '861017472489');
  productGoodsMapByShop = data.productGoodsMapByShop || {};
  productTitleTagsByShop = data.productTitleTagsByShop || {};
  if (hotTitleTagsInput) {
    setValueIfClean(hotTitleTagsInput, formatTagLines(data.hotTitleTags || []));
  }
  if (!productShopSelect && productGoodsMapInput && !isDirty(productGoodsMapInput)) {
    productGoodsMapInput.value = mapToLines(data.productGoodsMap || {});
  }
  setValueIfClean(videoDescInput, data.videoDesc || '');
  setCheckedIfClean(asrEnabledInput, data.asrEnabled === true);
  setValueIfClean(titleModelInput, data.titleModel || 'qwen-flash');
  setValueIfClean(titlePromptInput, data.titlePrompt || DEFAULT_TITLE_PROMPT);
  setValueIfClean(dashscopeApiKeyInput, data.dashscopeApiKey || '');
  setValueIfClean(dashscopeAsrModelInput, data.dashscopeAsrModel || 'qwen3-asr-flash');
  setValueIfClean(asrMaxSecondsInput, data.asrMaxSeconds || 60);
  setValueIfClean(asrContextInput, data.asrContext || '');
  if (publishTimeSlotsInput) {
    const slots = Array.isArray(data.publishTimeSlots) ? data.publishTimeSlots : [];
    setValueIfClean(publishTimeSlotsInput, slots.join('\n'));
  }
  if (publishRatioInput) {
    const ratios = Array.isArray(data.publishRatio) ? data.publishRatio : [];
    setValueIfClean(publishRatioInput, ratios.join(','));
  }
  setValueIfClean(titleMinLenInput, data.titleMinLen || 10);
  setValueIfClean(titleMaxLenInput, data.titleMaxLen || 20);
  setCheckedIfClean(downloadEnabledInput, data.downloadEnabled !== false);
  setValueIfClean(downloadTimeInput, data.downloadTime || '08:30');
  setValueIfClean(downloadRemoteRootInput, data.downloadRemoteRoot || '');
  setValueIfClean(downloadLocalRootInput, data.downloadLocalRoot || '');
  setValueIfClean(baiduCliPathInput, data.baiduCliPath || '');
  setValueIfClean(feishuWebhookInput, data.feishuWebhook || '');
  if (autoRunEnabledInput) setCheckedIfClean(autoRunEnabledInput, data.autoRunEnabled === true);
  const shopNames = getScheduleShopNames();
  if (productShopSelect) {
    renderShopOptions(productShopSelect, shopNames, productShopSelect.value || shopNames[0]);
    activeProductShop = productShopSelect.value || shopNames[0] || '';
    updateProductMapView(activeProductShop);
  }
}

async function saveConfig() {
  showStatus('', false);
  const shopNames = getScheduleShopNames();
  const currentShop = (productShopSelect && productShopSelect.value) || shopNames[0] || DEFAULT_SHOP;
  const hotTitleTags = hotTitleTagsInput ? parseTagLines(hotTitleTagsInput.value) : [];
  if (currentShop) {
    productGoodsMapByShop = {
      ...productGoodsMapByShop,
      [currentShop]: parseProductGoodsMap(productGoodsMapInput.value),
    };
    if (productTitleTagsInput) {
      productTitleTagsByShop = {
        ...productTitleTagsByShop,
        [currentShop]: parseProductTagsMap(productTitleTagsInput.value),
      };
    }
  }
  const payload = {
    clientId: clientIdInput.value.trim(),
    clientSecret: clientSecretInput.value.trim(),
    redirectUri: redirectInput.value.trim() || defaultRedirect,
    authBase: authBaseInput.value.trim() || 'https://mms.pinduoduo.com/open.html',
    requireAuth: requireAuthInput.checked,
    goodsId: goodsIdInput.value.trim() || '861017472489',
    productGoodsMap: {},
    productGoodsMapByShop,
    productTitleTagsByShop,
    hotTitleTags,
    videoDesc: videoDescInput.value.trim(),
    asrEnabled: asrEnabledInput.checked,
    titleEnabled: true,
    titleModel: titleModelInput.value.trim() || 'qwen-flash',
    titlePrompt: titlePromptInput.value.trim(),
    publishTimeSlots: publishTimeSlotsInput ? parseLines(publishTimeSlotsInput.value) : [],
    publishRatio: publishRatioInput
      ? parseLines(publishRatioInput.value.replace(/\\s+/g, '')).join(',').split(',').filter(Boolean).map((n) => parseInt(n, 10)).filter((n) => !Number.isNaN(n) && n > 0)
      : [],
    dashscopeApiKey: dashscopeApiKeyInput.value.trim(),
    dashscopeAsrModel: dashscopeAsrModelInput.value.trim() || 'qwen3-asr-flash',
    asrMaxSeconds: parseInt(asrMaxSecondsInput.value.trim() || '60', 10),
    asrContext: asrContextInput.value.trim(),
    titleMinLen: parseInt(titleMinLenInput.value.trim() || '10', 10),
    titleMaxLen: parseInt(titleMaxLenInput.value.trim() || '20', 10),
    downloadEnabled: downloadEnabledInput.checked,
    downloadTime: downloadTimeInput.value.trim() || '08:30',
    downloadRemoteRoot: normalizeRemoteRoot(downloadRemoteRootInput.value.trim()),
    downloadLocalRoot: downloadLocalRootInput.value.trim(),
    baiduCliPath: baiduCliPathInput.value.trim(),
    feishuWebhook: feishuWebhookInput ? feishuWebhookInput.value.trim() : '',
    autoRunEnabled: autoRunEnabledInput ? autoRunEnabledInput.checked : cachedConfig.autoRunEnabled || false,
    autoRunShops: getSelectedUploadShops(),
  };
  const { res, data } = await apiJson('/api/config', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res || !res.ok) {
    showStatus(data.error || '保存配置失败', true);
    return;
  }
  cachedConfig = data.config || payload;
  [
    clientIdInput,
    clientSecretInput,
    redirectInput,
    authBaseInput,
    goodsIdInput,
    productGoodsMapInput,
    productTitleTagsInput,
    hotTitleTagsInput,
    videoDescInput,
    titleModelInput,
    titlePromptInput,
    publishTimeSlotsInput,
    publishRatioInput,
    dashscopeApiKeyInput,
    dashscopeAsrModelInput,
    asrMaxSecondsInput,
    asrContextInput,
    titleMinLenInput,
    titleMaxLenInput,
    downloadRemoteRootInput,
    downloadLocalRootInput,
    baiduCliPathInput,
    feishuWebhookInput,
  ].forEach(clearDirty);
  [requireAuthInput, asrEnabledInput, downloadEnabledInput, autoRunEnabledInput].forEach(
    clearDirty,
  );
  showStatus('配置已保存', false);
}

async function loadTokens() {
  const { res, data } = await apiJson('/api/tokens');
  if (!res || !res.ok) return;
  tokenShopMap = data.shops || {};
  tokenLastAuth = data.lastAuth || {};
  tokenLastAuthShop = data.lastAuthShop || '';
  const shopNames = getScheduleShopNames();
  renderAuthShopList(shopNames);
  const focusShop =
    (activeAuthShop && shopNames.includes(activeAuthShop) && activeAuthShop) ||
    (tokenLastAuthShop && shopNames.includes(tokenLastAuthShop) && tokenLastAuthShop) ||
    shopNames[0] ||
    DEFAULT_SHOP;
  updateTokenDisplay(focusShop);
}

async function generateAuthForShop(shopName) {
  showStatus('', false);
  if (!shopName) return;
  activeAuthShop = shopName;
  const url = `/api/auth/url?shop=${encodeURIComponent(shopName)}`;
  const { res, data } = await apiJson(url);
  if (!res || !res.ok) {
    showStatus(data.error || '生成授权链接失败', true);
    if (authArea) authArea.style.display = 'none';
    return;
  }
  if (authArea) authArea.style.display = 'block';
  if (authUrlEl) {
    authUrlEl.textContent = data.url || '';
    authUrlEl.onclick = () => window.open(data.url, '_blank');
  }
  if (authStateInput && data.state) authStateInput.value = data.state;
  if (authCodeInput) authCodeInput.value = '';
  showStatus(`已生成授权链接（店铺：${shopName}）`, false);
}

async function exchangeTokenForShop(shopName) {
  const code = authCodeInput.value.trim();
  let state = authStateInput.value.trim();
  if (!code) {
    showStatus('请先填写 code', true);
    return;
  }
  if (!shopName) return;
  activeAuthShop = shopName;
  if (!state && tokenShopMap && tokenShopMap[shopName] && tokenShopMap[shopName].lastAuthState) {
    state = tokenShopMap[shopName].lastAuthState;
    if (authStateInput) authStateInput.value = state;
  }
  const { res, data } = await apiJson('/api/oauth/exchange', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ code, state, shop: shopName }),
  });
  if (!res || !res.ok) {
    showStatus(data.error || '换取 Token 失败', true);
    return;
  }
  const savedShop = (data && data.shop) || shopName;
  showStatus(savedShop ? `换取 Token 成功（店铺：${savedShop}）` : '换取 Token 成功', false);
  if (data && data.token) {
    const shopKey = savedShop;
    if (shopKey) {
      tokenShopMap = tokenShopMap || {};
      tokenShopMap[shopKey] = tokenShopMap[shopKey] || {};
      tokenShopMap[shopKey].lastAuth = data.token;
    }
  }
  activeAuthShop = savedShop || activeAuthShop;
  await loadTokens();
}

async function loadSchedule() {
  const { res, data } = await apiJson('/api/schedule');
  if (!res || !res.ok) return;
  cachedSchedule = data || {};
  const shops = data.shops || {};
  const shopNames = Object.keys(shops).length ? Object.keys(shops) : [DEFAULT_SHOP];
  const shopCfg = shops[shopNames[0]] || {};
  setValueIfClean(startTimeInput, shopCfg.start_time || '09:00');
  setValueIfClean(intervalInput, shopCfg.interval_seconds || 300);
  setValueIfClean(dailyLimitInput, shopCfg.daily_limit || 50);
  setValueIfClean(videoRootInput, data.video_root || 'video');
  if (shopListInput) {
    setValueIfClean(shopListInput, shopNames.join('\n'));
  }
  syncShopUI();
  renderAuthShopList(shopNames);
  const focusShop =
    (activeAuthShop && shopNames.includes(activeAuthShop) && activeAuthShop) ||
    shopNames[0] ||
    DEFAULT_SHOP;
  updateTokenDisplay(focusShop);
}

async function saveSchedule() {
  showStatus('', false);
  const shopLines = shopListInput ? parseLines(shopListInput.value) : [];
  const shopNames = shopLines.length ? shopLines : [DEFAULT_SHOP];
  const startTime = (startTimeInput && startTimeInput.value.trim()) || '09:00';
  const interval = parseInt((intervalInput && intervalInput.value) || '300', 10);
  const dailyLimit = parseInt((dailyLimitInput && dailyLimitInput.value) || '50', 10);
  const videoRoot = videoRootInput ? videoRootInput.value.trim() : 'video';
  const existing = (cachedSchedule && cachedSchedule.shops) || {};
  const shops = {};
  shopNames.forEach((name) => {
    const key = name.trim();
    if (!key) return;
    shops[key] = {
      start_time: startTime,
      interval_seconds: Number.isNaN(interval) ? 300 : interval,
      daily_limit: Number.isNaN(dailyLimit) ? 50 : dailyLimit,
      enabled: true,
      ...(existing[key] || {}),
    };
    shops[key].start_time = startTime;
    shops[key].interval_seconds = Number.isNaN(interval) ? 300 : interval;
    shops[key].daily_limit = Number.isNaN(dailyLimit) ? 50 : dailyLimit;
    shops[key].enabled = existing[key]?.enabled !== false;
  });
  const payload = {
    video_root: videoRoot || 'video',
    time_zone: (cachedSchedule && cachedSchedule.time_zone) || 'Asia/Shanghai',
    shops,
  };
  const { res, data } = await apiJson('/api/schedule', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res || !res.ok) {
    showStatus(data.error || '保存排期失败', true);
    return;
  }
  cachedSchedule = data.schedule || payload;
  [shopListInput, startTimeInput, intervalInput, dailyLimitInput, videoRootInput].forEach(clearDirty);
  showStatus('排期已保存', false);
  syncShopUI();
  renderAuthShopList(shopNames);
 }

async function clearTokenForShop(shopName) {
  if (!shopName) return;
  if (!confirm(`确定清除店铺【${shopName}】的授权信息吗？`)) return;
  const { res, data } = await apiJson('/api/tokens/clear', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ shop: shopName }),
  });
  if (!res || !res.ok) {
    showStatus(data.error || "清除授权失败", true);
    return;
  }
  showStatus(`已清除授权（店铺：${shopName}）`, false);
  await loadTokens();
}



function renderTasks(tasks) {
  if (!taskTable) return;
  const today = getTodayDate().replace(/-/g, '');
  const rows = Array.isArray(tasks) ? tasks : [];
  const filtered = rows.filter((task) => !task.date || task.date === today);
  taskItems = filtered.slice().reverse();
  renderTaskPage();
}

function renderTaskPage() {
  if (!taskTable) return;
  taskTotalPages = Math.max(1, Math.ceil(taskItems.length / taskPageSize));
  if (taskPage > taskTotalPages) taskPage = taskTotalPages;
  const start = (taskPage - 1) * taskPageSize;
  const pageItems = taskItems.slice(start, start + taskPageSize);
  taskTable.innerHTML = '';
  pageItems.forEach((task) => {
    const tr = document.createElement('tr');
    const status = task.status || '-';
    const badgeClass =
      status === 'done' ? 'badge success' : status === 'failed' ? 'badge failed' : status === 'processing' ? 'badge processing' : 'badge';
    const statusText =
      status === 'done' ? '成功' : status === 'failed' ? '失败' : status === 'processing' ? '进行中' : status === 'paused' ? '已暂停' : status;
    tr.innerHTML = `
        <td>${task.shop || '-'}</td>
        <td>${task.filename || '-'}</td>
        <td><span class="${badgeClass}">${statusText}</span></td>
        <td>${task.message || task.video_id || ''}</td>
        <td>${task.started_at || '-'}</td>
        <td>${task.ended_at || '-'}</td>
      `;
    taskTable.appendChild(tr);
  });
  if (taskPageInfo) {
    taskPageInfo.textContent = `第 ${taskPage} / ${taskTotalPages} 页`;
  }
  if (taskPrevBtn) taskPrevBtn.disabled = taskPage <= 1;
  if (taskNextBtn) taskNextBtn.disabled = taskPage >= taskTotalPages;
}

function goTaskPage(next) {
  if (next) {
    taskPage = Math.min(taskTotalPages, taskPage + 1);
  } else {
    taskPage = Math.max(1, taskPage - 1);
  }
  renderTaskPage();
}

async function loadUploadStatus() {
  const { res, data } = await apiJson('/api/upload/status');
  if (!res || !res.ok) return;
  renderTasks(data.tasks || []);
}

async function loadPauseStatus() {
  const { res, data } = await apiJson('/api/pause/status');
  if (!res || !res.ok) return;
  const uploadPaused = data.uploadPaused === true;
  const downloadPaused = data.downloadPaused === true;
  if (uploadPauseToggle) {
    uploadPauseToggle.textContent = uploadPaused ? '继续上传' : '暂停上传';
    uploadPauseToggle.classList.toggle('secondary', uploadPaused);
  }
  if (downloadPauseToggle) {
    downloadPauseToggle.textContent = downloadPaused ? '继续下载' : '暂停下载';
    downloadPauseToggle.classList.toggle('secondary', downloadPaused);
  }
  if (autoPauseBtn) {
    const paused = uploadPaused && downloadPaused;
    autoPauseBtn.textContent = paused ? '继续任务' : '暂停任务';
    autoPauseBtn.classList.toggle('secondary', paused);
  }
}

async function loadStats() {
  const { res, data } = await apiJson('/api/stats/today');
  if (!res || !res.ok) return;
  downloadSuccessCount.textContent = data.downloadSuccess ?? 0;
  uploadSuccessCount.textContent = data.uploadSuccess ?? 0;
}

async function loadProgress() {
  const { res, data } = await apiJson('/api/progress');
  if (!res || !res.ok) return;
  const download = data.download || {};
  const upload = data.upload || {};
  const dTotal = Number(download.total || 0);
  const dCurrent = Number(download.current || 0);
  const uTotal = Number(upload.total || 0);
  const uCurrent = Number(upload.current || 0);
  if (downloadProgressEl) {
    const name = download.file ? ` ${download.file}` : '';
    downloadProgressEl.textContent = `下载进度：${dCurrent}/${dTotal}${name}`;
  }
  if (uploadProgressEl) {
    const name = upload.file ? ` ${upload.file}` : '';
    uploadProgressEl.textContent = `上传进度：${uCurrent}/${uTotal}${name}`;
  }
  if (dTotal > 0 && dCurrent >= dTotal && !download.file) {
    setActionStatus(downloadActionStatus, `下载完成（${dCurrent}/${dTotal}）`, false, true);
  }
  if (uTotal > 0 && uCurrent >= uTotal && !upload.file) {
    setActionStatus(uploadActionStatus, `上传完成（${uCurrent}/${uTotal}）`, false, true);
  }
}

async function loadAutoStatus() {
  const { res, data } = await apiJson('/api/auto/status');
  if (!res || !res.ok) return;
  const running = data.running === true;
  autoRunBtn.textContent = running ? '运行中' : '一键执行';
  autoRunBtn.disabled = running;
  autoRunStatus.textContent = running
    ? `自动化执行中，开始于 ${data.lastRun || '今天'}，下载完成后自动上传。`
    : data.enabled
      ? '已启用自动化' : '';
}

async function startAutoRun() {
  setActionStatus(downloadActionStatus, '正在启动一键执行...', false, true);
  const selectedShops = getSelectedUploadShops();
  const payload = { shops: selectedShops };
  const { res, data } = await apiJson('/api/auto/start', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res || !res.ok) {
    setActionStatus(downloadActionStatus, data.error || '一键执行失败', true, false);
    return;
  }
  await loadAutoStatus();
}

async function loadBaiduStatus() {
  const { res, data } = await apiJson('/api/baidu/status');
  if (!res || !res.ok) {
    setDotStatus(baiduStatus, false, '异常');
    baiduStatusDetail.textContent = data.error || '读取登录状态失败';
    return false;
  }
  if (data.available === false) {
    setDotStatus(baiduStatus, false, '不可用');
  } else if (data.logged_in === true) {
    setDotStatus(baiduStatus, true, '已登录');
  } else if (data.logged_in === false) {
    setDotStatus(baiduStatus, false, '未登录');
  } else {
    setDotStatus(baiduStatus, null, '未检查');
  }
  baiduStatusDetail.textContent = data.message || '';
  return true;
}

async function refreshBaiduStatus() {
  setActionStatus(baiduActionStatus, '正在刷新状态...', false, false);
  const ok = await loadBaiduStatus();
  if (!ok) {
    setActionStatus(baiduActionStatus, '刷新失败，请检查 BaiduPCS-Go', true, false);
    return;
  }
  setActionStatus(baiduActionStatus, '状态已刷新', false, true);
}

function appendBaiduLoginOutput(message) {
  if (!baiduLoginOutput) return;
  baiduLoginOutput.style.display = 'block';
  baiduLoginOutput.textContent = `${baiduLoginOutput.textContent}${message}\n`;
  baiduLoginOutput.scrollTop = baiduLoginOutput.scrollHeight;
}

async function triggerBaiduLogin() {
  if (!window.electronAPI || !window.electronAPI.loginBaiduWithBduss) {
    appendBaiduLoginOutput('当前不是 Electron 环境，无法直接调用 BDUSS 登录。');
    return;
  }
  const bduss = baiduBdussInput.value.trim();
  const stoken = baiduStokenInput.value.trim();
  const cliPath = baiduCliPathInput.value.trim();
  if (!bduss) {
    appendBaiduLoginOutput('请先填写 BDUSS。');
    return;
  }
  setActionStatus(baiduActionStatus, '正在提交 BDUSS 登录...', false, false);
  baiduLoginOutput.textContent = '';
  window.electronAPI.loginBaiduWithBduss(cliPath, bduss, stoken);
}

async function logoutBaidu() {
  setActionStatus(baiduActionStatus, '正在退出登录...', false, false);
  const { res, data } = await apiJson('/api/baidu/logout', { method: 'POST' });
  if (!res || !res.ok) {
    setActionStatus(baiduActionStatus, data.error || '退出登录失败', true, false);
    return;
  }
  setActionStatus(baiduActionStatus, '已退出登录', false, true);
  await loadBaiduStatus();
}

async function checkFfmpeg() {
  const { res, data } = await apiJson('/api/system/ffmpeg');
  if (!res || !res.ok) return;
  if (data.available) {
    setDotStatus(ffmpegStatus, true, 'ffmpeg: 可用');
    ffmpegPath.textContent = data.path || data.version || '';
  } else {
    setDotStatus(ffmpegStatus, false, 'ffmpeg: 未找到');
    ffmpegPath.textContent = '';
  }
}

async function triggerManualDownload() {
  setActionStatus(downloadActionStatus, '已发送手动下载请求', false, true);
  const { res, data } = await apiJson('/api/download/manual', { method: 'POST' });
  if (!res || !res.ok) {
    setActionStatus(downloadActionStatus, data.error || '手动下载失败', true, false);
    return;
  }
  await loadLogs();
  await loadProgress();
}

async function triggerManualUpload(ignoreSlot) {
  const hint = ignoreSlot ? '（忽略分段）' : '（按分段）';
  setActionStatus(uploadActionStatus, `已发送手动上传请求 ${hint}`, false, true);
  const selectedShops = getSelectedUploadShops();
  const { res, data } = await apiJson('/api/upload/scan', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ shops: selectedShops, ignoreSlot: !!ignoreSlot }),
  });
  if (!res || !res.ok) {
    setActionStatus(uploadActionStatus, data.error || '手动上传失败', true, false);
    return;
  }
  await loadLogs();
  await loadProgress();
}

async function pauseUpload() {
  const { res, data } = await apiJson('/api/upload/pause', { method: 'POST' });
  if (!res || !res.ok) {
    setActionStatus(uploadActionStatus, data.error || '暂停上传失败', true, false);
    return;
  }
  setActionStatus(uploadActionStatus, '上传已暂停', false, true);
  await loadPauseStatus();
}

async function resumeUpload() {
  const { res, data } = await apiJson('/api/upload/resume', { method: 'POST' });
  if (!res || !res.ok) {
    setActionStatus(uploadActionStatus, data.error || '继续上传失败', true, false);
    return;
  }
  setActionStatus(uploadActionStatus, '上传已恢复', false, true);
  await loadPauseStatus();
}

async function pauseDownload() {
  const { res, data } = await apiJson('/api/download/pause', { method: 'POST' });
  if (!res || !res.ok) {
    setActionStatus(downloadActionStatus, data.error || '暂停下载失败', true, false);
    return;
  }
  setActionStatus(downloadActionStatus, '下载已暂停', false, true);
  await loadPauseStatus();
}

async function resumeDownload() {
  const { res, data } = await apiJson('/api/download/resume', { method: 'POST' });
  if (!res || !res.ok) {
    setActionStatus(downloadActionStatus, data.error || '继续下载失败', true, false);
    return;
  }
  setActionStatus(downloadActionStatus, '下载已恢复', false, true);
  await loadPauseStatus();
}

async function pauseAll() {
  await apiJson('/api/download/pause', { method: 'POST' });
  await apiJson('/api/upload/pause', { method: 'POST' });
  await loadPauseStatus();
  setActionStatus(downloadActionStatus, '任务已暂停', false, true);
}

async function resumeAll() {
  await apiJson('/api/download/resume', { method: 'POST' });
  await apiJson('/api/upload/resume', { method: 'POST' });
  await loadPauseStatus();
  setActionStatus(downloadActionStatus, '任务已继续', false, true);
}

async function resetUploads() {
  const { res, data } = await apiJson('/api/upload/reset', { method: 'POST' });
  if (!res || !res.ok) {
    setActionStatus(uploadActionStatus, data.error || '清空上传记录失败', true, false);
    return;
  }
  setActionStatus(uploadActionStatus, '已清空上传记录', false, true);
  await loadUploadStatus();
}

async function refreshAllStatus() {
  await Promise.all([loadUploadStatus(), loadPauseStatus(), loadBaiduStatus(), loadAutoStatus(), loadStats()]);
  setActionStatus(uploadActionStatus, '状态已刷新', false, true);
}

function bindTabs(tabs) {
  tabs.forEach((tab) => {
    tab.addEventListener('click', () => {
      tabs.forEach((item) => item.classList.remove('active'));
      tab.classList.add('active');
      logScope = tab.dataset.logScope || 'today';
      renderLogs();
    });
  });
}

function setupCollapseButtons() {
  document.querySelectorAll('.toggle-collapse').forEach((btn) => {
    const target = btn.dataset.target;
    const card = document.querySelector(`[data-card="${target}"]`);
    if (!card) return;
    const updateText = () => {
      btn.textContent = card.classList.contains('collapsed') ? '展开' : '收起';
    };
    updateText();
    btn.addEventListener('click', () => {
      card.classList.toggle('collapsed');
      updateText();
    });
  });
}

function setupElectronEvents() {
  if (!window.electronAPI || !window.electronAPI.onBaiduLoginEvent) return;
  window.electronAPI.onBaiduLoginEvent((event) => {
    if (!event) return;
    if (event.type === 'start') {
      appendBaiduLoginOutput(event.message || '开始登录...');
    } else if (event.type === 'line') {
      appendBaiduLoginOutput(event.message || '');
    } else if (event.type === 'end') {
      appendBaiduLoginOutput(`登录流程结束，返回码 ${event.code}`);
      setActionStatus(baiduActionStatus, '登录流程结束，请刷新状态确认', false, true);
      loadBaiduStatus();
    } else if (event.type === 'error') {
      appendBaiduLoginOutput(event.message || '登录失败');
      setActionStatus(baiduActionStatus, event.message || '登录失败', true, false);
    }
  });
}

function bindDirtyTracking() {
  document.querySelectorAll('input, textarea, select').forEach((el) => {
    const handler = () => markDirty(el);
    el.addEventListener('input', handler);
    el.addEventListener('change', handler);
  });
}

document.getElementById('saveBtn').addEventListener('click', async () => {
  await saveConfig();
});
const saveAllBtn = document.getElementById('saveAllConfigBtn');
if (saveAllBtn) {
  saveAllBtn.addEventListener('click', async () => {
    await saveConfig();
    await saveSchedule();
  });
}
const saveShopsBtn = document.getElementById('saveShopsBtn');
if (saveShopsBtn) {
  saveShopsBtn.addEventListener('click', async () => {
    await saveSchedule();
  });
}
document.getElementById('ffmpegCheckBtn').addEventListener('click', async () => {
  await checkFfmpeg();
});
if (feishuTestBtn) {
  feishuTestBtn.addEventListener('click', async () => {
    const { res, data } = await apiJson('/api/feishu/test', { method: 'POST' });
    if (!res || !res.ok) {
      showStatus(data.error || '发送测试消息失败', true);
      return;
    }
    showStatus('测试消息已发送', false);
  });
}
document.getElementById('scanBtn').addEventListener('click', async () => {
  await triggerManualUpload(false);
});
if (scanBtnIgnore) {
  scanBtnIgnore.addEventListener('click', async () => {
    await triggerManualUpload(true);
  });
}
document.getElementById('downloadBtn').addEventListener('click', async () => {
  await triggerManualDownload();
});
uploadPauseToggle.addEventListener('click', async () => {
  const { data } = await apiJson('/api/pause/status');
  if (data && data.uploadPaused) {
    await resumeUpload();
  } else {
    await pauseUpload();
  }
});
downloadPauseToggle.addEventListener('click', async () => {
  const { data } = await apiJson('/api/pause/status');
  if (data && data.downloadPaused) {
    await resumeDownload();
  } else {
    await pauseDownload();
  }
});
document.getElementById('refreshStatusBtn').addEventListener('click', async () => {
  await refreshAllStatus();
});
document.getElementById('resetBtn').addEventListener('click', async () => {
  await resetUploads();
});
document.getElementById('clearLogsBtn').addEventListener('click', async () => {
  await clearLogs();
});
document.getElementById('autoRunBtn').addEventListener('click', async () => {
  await startAutoRun();
});
autoPauseBtn.addEventListener('click', async () => {
  const { data } = await apiJson('/api/pause/status');
  if (data && data.uploadPaused && data.downloadPaused) {
    await resumeAll();
  } else {
    await pauseAll();
  }
});
document.getElementById('refreshBaiduBtn').addEventListener('click', async () => {
  await refreshBaiduStatus();
});
document.getElementById('baiduBdussLoginBtn').addEventListener('click', async () => {
  await triggerBaiduLogin();
});
document.getElementById('baiduLogoutBtn').addEventListener('click', async () => {
  await logoutBaidu();
});
if (productShopSelect) {
  productShopSelect.addEventListener('change', () => {
    if (productGoodsMapInput && activeProductShop) {
      productGoodsMapByShop = {
        ...productGoodsMapByShop,
        [activeProductShop]: parseProductGoodsMap(productGoodsMapInput.value),
      };
    }
  if (productTitleTagsInput && activeProductShop) {
      productTitleTagsByShop = {
        ...productTitleTagsByShop,
        [activeProductShop]: parseProductTagsMap(productTitleTagsInput.value),
      };
    }
    activeProductShop = productShopSelect.value;
    updateProductMapView(activeProductShop, true);
  });
}
if (taskPrevBtn) {
  taskPrevBtn.addEventListener('click', () => {
    goTaskPage(false);
  });
}
if (taskNextBtn) {
  taskNextBtn.addEventListener('click', () => {
    goTaskPage(true);
  });
}

setupCollapseButtons();
setupElectronEvents();
bindTabs(logScopeTabs);
bindDirtyTracking();

(async () => {
  await loadConfig();
  await loadSchedule();
  await loadTokens();
  await loadBaiduStatus();
  await loadUploadStatus();
  await loadPauseStatus();
  await loadStats();
  await loadProgress();
  await loadAutoStatus();
  await loadLogs();
  await checkFfmpeg();

  setInterval(() => {
    loadLogs();
    loadSchedule();
    loadUploadStatus();
    loadPauseStatus();
    loadBaiduStatus();
    loadAutoStatus();
    loadTokens();
    loadStats();
    loadProgress();
  }, 10000);
})();
