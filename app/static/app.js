'use strict'

// ─────────────────────────────────────────────────────────────────────────────
// State
// ─────────────────────────────────────────────────────────────────────────────

const S = {
  token: localStorage.getItem('llm_token'),
  me: null,
  users: [],
  buckets: [],
  contentFilters: [],
  multiclusterModels: [],
  clusters: [],
  selectedClusterModel: null,
  userQuery: '',
  bucketQuery: '',
  contentFilterQuery: '',
  // Pagination state
  usersPagination: {
    skip: 0,
    limit: 20,
    total: 0,
  },
  // Cluster edit modal state
  currentModelId: null,
  currentClusterData: [],
  editingClusterId: null,
  // Polling state for multicluster
  clusterRefreshInterval: null,
  drainPollIntervals: {}, // Map of "modelId:clusterId" -> intervalId
  lastClusterStatus: {}, // Map of "modelId:clusterId" -> status data
  isRefreshing: false, // Flag to prevent concurrent refresh calls
  lastRefreshTime: null, // Timestamp of last successful refresh
}

// ─────────────────────────────────────────────────────────────────────────────
// API client
// ─────────────────────────────────────────────────────────────────────────────

const api = {
  _headers() {
    const h = { 'Content-Type': 'application/json' }
    if (S.token) h['Authorization'] = `Bearer ${S.token}`
    return h
  },
  async _call(method, url, body) {
    const opts = { method, headers: this._headers() }
    if (body !== undefined) opts.body = JSON.stringify(body)
    const r = await fetch(url, opts)
    if (r.status === 204) return null
    const ct = r.headers.get('content-type') || ''
    const data = ct.includes('json') ? await r.json() : null
    if (!r.ok) throw new Error(data?.detail || `HTTP ${r.status}`)
    return data
  },
  get: (url) => api._call('GET', url),
  post: (url, body) => api._call('POST', url, body),
  patch: (url, body) => api._call('PATCH', url, body),
  delete: (url) => api._call('DELETE', url),
}

// ─────────────────────────────────────────────────────────────────────────────
// Cluster Polling Management
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Start continuous refresh of cluster load metrics
 * Polls every 3 seconds when on the multicluster page
 */
function startClusterRefresh() {
  // Clear any existing interval
  stopClusterRefresh()
  
  // Start new refresh interval (every 3 seconds)
  S.clusterRefreshInterval = setInterval(async () => {
    if (S.currentModelId) {
      await refreshClusterData()
    }
  }, 3000)
}

/**
 * Stop the continuous cluster refresh
 */
function stopClusterRefresh() {
  if (S.clusterRefreshInterval) {
    clearInterval(S.clusterRefreshInterval)
    S.clusterRefreshInterval = null
  }
}

/**
 * Refresh cluster data without full re-render
 * Updates only the changed metrics in the UI
 */
async function refreshClusterData() {
  if (!S.currentModelId || S.isRefreshing) return
  
  S.isRefreshing = true
  
  // Add refreshing class to all cluster cards for loading indicator
  document.querySelectorAll('.cluster-card').forEach(card => {
    card.classList.add('refreshing')
  })
  
  try {
    const response = await api.get(`/admin/models/${encodeURIComponent(S.currentModelId)}/clusters`)
    const clusters = response.clusters || response.data || []
    
    // Update stored data
    S.currentClusterData = clusters
    S.clusters = clusters
    S.lastRefreshTime = new Date()
    
    // Clear any error states
    document.querySelectorAll('.cluster-card').forEach(card => {
      card.classList.remove('error')
    })
    
    // Update UI for each cluster
    clusters.forEach(cluster => {
      updateClusterCardUI(cluster)
    })
  } catch (e) {
    // Show error state on cards but keep showing last known data
    console.warn('Failed to refresh cluster data:', e)
    document.querySelectorAll('.cluster-card').forEach(card => {
      card.classList.add('error')
    })
  } finally {
    S.isRefreshing = false
    // Remove refreshing class from all cards
    document.querySelectorAll('.cluster-card').forEach(card => {
      card.classList.remove('refreshing')
    })
  }
}

/**
 * Update a single cluster card's UI elements without full re-render
 * @param {Object} cluster - The cluster data
 */
function updateClusterCardUI(cluster) {
  const clusterId = cluster.cluster_id || cluster.id
  const card = document.querySelector(`.cluster-card[data-cluster-id="${clusterId}"]`)
  if (!card) return
  
  // Update status indicator
  const statusIndicator = card.querySelector('.status-indicator')
  const statusText = card.querySelector('.status-text')
  if (statusIndicator && statusText) {
    // Check if this cluster is currently draining (being polled)
    const isDraining = S.drainPollIntervals[`${S.currentModelId}:${clusterId}`] !== undefined
    
    if (isDraining) {
      // Don't override draining state if we're actively polling
      return
    }
    
    const isEnabled = cluster.is_enabled
    statusIndicator.className = `status-indicator ${isEnabled ? 'status-active' : 'status-inactive'}`
    statusText.textContent = isEnabled ? 'Active' : 'Disabled'
  }
  
  // Update load stats
  const loadValue = card.querySelector('.stat-value.load-ratio')
  if (loadValue) {
    const loadRatio = cluster.load_ratio || (cluster.current_load || 0) / 100
    loadValue.textContent = formatPercent(loadRatio)
    loadValue.className = `stat-value load-ratio ${getStatusColor(loadRatio)}`
  }
  
  // Update in-flight tokens
  const inflightValue = card.querySelector('.stat-value.inflight-tokens')
  if (inflightValue) {
    inflightValue.textContent = formatNumber(cluster.inflight_tokens || 0)
  }
  
  // Update adaptive capacity
  const capacityValue = card.querySelector('.stat-value.adaptive-capacity')
  if (capacityValue) {
    capacityValue.textContent = formatNumber(cluster.inferred_capacity || 0)
  }
  
  // Update EWMA latency
  const latencyValue = card.querySelector('.stat-value.ewma-latency')
  if (latencyValue) {
    const latency = cluster.ewma_latency
    latencyValue.textContent = latency ? latency.toFixed(2) + 's' : '—'
  }
  
  // Store last known status
  S.lastClusterStatus[`${S.currentModelId}:${clusterId}`] = cluster
}

/**
 * Start polling for drain status on a specific cluster
 * @param {string} modelId - The model ID
 * @param {string} clusterId - The cluster ID
 */
function startDrainPolling(modelId, clusterId) {
  const key = `${modelId}:${clusterId}`
  
  // Clear existing poll for this cluster
  stopDrainPolling(modelId, clusterId)
  
  // Start new poll (every 2 seconds)
  S.drainPollIntervals[key] = setInterval(async () => {
    await pollDrainStatus(modelId, clusterId)
  }, 2000)
  
  // Show initial draining state in UI
  updateClusterDrainState(modelId, clusterId, 'DRAINING')
}

/**
 * Stop polling for drain status on a specific cluster
 * @param {string} modelId - The model ID
 * @param {string} clusterId - The cluster ID
 */
function stopDrainPolling(modelId, clusterId) {
  const key = `${modelId}:${clusterId}`
  if (S.drainPollIntervals[key]) {
    clearInterval(S.drainPollIntervals[key])
    delete S.drainPollIntervals[key]
  }
}

/**
 * Stop all drain polling (called when leaving multicluster page)
 */
function stopAllDrainPolling() {
  Object.keys(S.drainPollIntervals).forEach(key => {
    clearInterval(S.drainPollIntervals[key])
  })
  S.drainPollIntervals = {}
}

/**
 * Poll the drain status endpoint for a specific cluster
 * @param {string} modelId - The model ID
 * @param {string} clusterId - The cluster ID
 */
async function pollDrainStatus(modelId, clusterId) {
  try {
    const status = await api.get(
      `/admin/models/${encodeURIComponent(modelId)}/clusters/${encodeURIComponent(clusterId)}/status`
    )
    
    // Update UI with drain progress
    updateDrainProgressUI(modelId, clusterId, status)
    
    // Check if drain is complete
    if (status.state === 'DISABLED' || status.is_drain_complete) {
      stopDrainPolling(modelId, clusterId)
      toast(`Cluster "${clusterId}" disabled successfully`, 'success')

      // Update local state immediately for responsive UI
      const clusterIndex = S.currentClusterData.findIndex(c => (c.cluster_id || c.id) === clusterId)
      if (clusterIndex !== -1) {
        S.currentClusterData[clusterIndex].is_enabled = false
        S.clusters[clusterIndex].is_enabled = false
      }

      // Re-render immediately with local state to update the Enable button
      const container = document.getElementById('clusters-container')
      if (container) {
        renderClusterCardsUI(container)
      }

      // Then refresh from API in background to sync with backend
      await refreshClusterData()
    }
  } catch (e) {
    console.warn('Failed to poll drain status:', e)
    // Don't stop polling on error - keep trying
  }
}

/**
 * Update the cluster card to show draining state
 * @param {string} modelId - The model ID
 * @param {string} clusterId - The cluster ID
 * @param {string} state - The drain state ('DRAINING' or 'DISABLED')
 */
function updateClusterDrainState(modelId, clusterId, state) {
  const card = document.querySelector(`.cluster-card[data-cluster-id="${clusterId}"]`)
  if (!card) return
  
  const statusIndicator = card.querySelector('.status-indicator')
  const statusText = card.querySelector('.status-text')
  
  if (state === 'DRAINING') {
    if (statusIndicator) {
      statusIndicator.className = 'status-indicator status-draining'
    }
    if (statusText) {
      statusText.textContent = 'Draining...'
    }
    card.classList.add('draining')
  } else if (state === 'DISABLED') {
    if (statusIndicator) {
      statusIndicator.className = 'status-indicator status-inactive'
    }
    if (statusText) {
      statusText.textContent = 'Disabled'
    }
    card.classList.remove('draining')
  }
}

/**
 * Update UI with drain progress information
 * @param {string} modelId - The model ID
 * @param {string} clusterId - The cluster ID
 * @param {Object} status - The status response from API
 */
function updateDrainProgressUI(modelId, clusterId, status) {
  const card = document.querySelector(`.cluster-card[data-cluster-id="${clusterId}"]`)
  if (!card) return
  
  const statusText = card.querySelector('.status-text')
  const inflightValue = card.querySelector('.stat-value.inflight-tokens')
  
  if (status.state === 'DRAINING') {
    const inflight = status.inflight_tokens || 0
    if (statusText) {
      statusText.innerHTML = `Draining... <span class="drain-count">${inflight} requests remaining</span>`
    }
    if (inflightValue) {
      inflightValue.textContent = formatNumber(inflight)
    }
    card.classList.add('draining')
  } else if (status.state === 'DISABLED') {
    if (statusText) {
      statusText.textContent = 'Disabled'
    }
    card.classList.remove('draining')
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────

function esc(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;')
}

function badgeStatus(active) {
  return active
    ? '<span class="badge badge-green">Active</span>'
    : '<span class="badge badge-red">Inactive</span>'
}

function badgeRole(isAdmin) {
  return isAdmin
    ? '<span class="badge badge-blue">Admin</span>'
    : '<span class="badge badge-gray">User</span>'
}

// ─────────────────────────────────────────────────────────────────────────────
// Toasts
// ─────────────────────────────────────────────────────────────────────────────

function toast(msg, type = 'info') {
  const el = document.createElement('div')
  el.className = `toast toast-${type}`
  el.textContent = msg
  document.getElementById('toasts').appendChild(el)
  requestAnimationFrame(() => requestAnimationFrame(() => el.classList.add('toast-show')))
  setTimeout(() => {
    el.classList.remove('toast-show')
    setTimeout(() => el.remove(), 250)
  }, 3000)
}

// ─────────────────────────────────────────────────────────────────────────────
// Modal
// ─────────────────────────────────────────────────────────────────────────────

function openModal(title, bodyHTML, onSubmit, submitLabel = 'Save') {
  document.getElementById('modal-title').textContent = title
  document.getElementById('modal-body').innerHTML = bodyHTML
  document.getElementById('modal-overlay').classList.remove('hidden')
  const btn = document.getElementById('modal-submit')
  btn.textContent = submitLabel
  btn.onclick = async () => {
    btn.disabled = true
    try {
      await onSubmit()
      closeModal()
    } catch (e) {
      toast(e.message, 'error')
    } finally {
      btn.disabled = false
    }
  }
}

function closeModal() {
  document.getElementById('modal-overlay').classList.add('hidden')
}

// Close modal on backdrop click
document.getElementById('modal-overlay').addEventListener('click', e => {
  if (e.target === e.currentTarget) closeModal()
})

// ─────────────────────────────────────────────────────────────────────────────
// Shell
// ─────────────────────────────────────────────────────────────────────────────

function renderShell() {
  const me = S.me
  document.getElementById('app').innerHTML = `
    <aside class="sidebar">
      <div class="sidebar-brand">
        <span class="brand-icon">⚡</span>
        <span>LLM Router</span>
      </div>
      <nav class="sidebar-nav">
        <a class="nav-item" data-nav="dashboard" href="#/dashboard"
           onclick="navigate('dashboard', event)">
          <span>👤</span> My Profile
        </a>
        ${me.is_admin ? `
        <div class="nav-section">Admin</div>
        <a class="nav-item" data-nav="users" href="#/users"
           onclick="navigate('users', event)">
          <span>👥</span> Users
        </a>
        <a class="nav-item" data-nav="buckets" href="#/buckets"
           onclick="navigate('buckets', event)">
          <span>🪣</span> Buckets
        </a>
        <a class="nav-item" data-nav="content-filters" href="#/content-filters"
           onclick="navigate('content-filters', event)">
          <span>🛡️</span> Content Filters
        </a>
        <a class="nav-item" data-nav="multicluster" href="#/multicluster"
           onclick="navigate('multicluster', event)">
          <span>🌐</span> Multicluster
        </a>` : ''}
      </nav>
      <div class="sidebar-footer">
        <div class="sidebar-user">${esc(me.username)}</div>
        <button class="btn-logout" onclick="logout()">Sign out</button>
      </div>
    </aside>
    <main class="content" id="content"></main>
  `
}

function setContent(html) {
  const el = document.getElementById('content')
  if (el) el.innerHTML = html
}

function setActiveNav(id) {
  document.querySelectorAll('.nav-item').forEach(el =>
    el.classList.toggle('active', el.dataset.nav === id)
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Router
// ─────────────────────────────────────────────────────────────────────────────

const VALID_VIEWS = ['dashboard', 'users', 'buckets', 'content-filters', 'multicluster']

function parseHash() {
  const hash = window.location.hash
  if (!hash) return null
  // Parse hash format: #/users -> users
  const view = hash.replace(/^#\//, '')
  return VALID_VIEWS.includes(view) ? view : null
}

async function navigate(view, event = null) {
  console.log('[Router] navigate() called with view:', view, 'event:', event)

  // Prevent default link behavior if called from onclick
  if (event) {
    event.preventDefault()
    event.stopPropagation()
  }
  
  // Cleanup polling when leaving multicluster view
  if (view !== 'multicluster') {
    stopClusterRefresh()
    stopAllDrainPolling()
  }

  setActiveNav(view)
  // Store current view in localStorage for page refresh persistence
  localStorage.setItem('llm_current_view', view)
  // Update URL hash for bookmarkable/shareable URLs (only if different)
  const expectedHash = `#/${view}`
  console.log('[Router] Current hash:', window.location.hash, 'Expected:', expectedHash)
  if (window.location.hash !== expectedHash) {
    console.log('[Router] Setting hash to:', expectedHash)
    window.location.hash = expectedHash
  }
  const views = { dashboard: showDashboard, users: showUsers, buckets: showBuckets, 'content-filters': showContentFilters, multicluster: showMulticluster }
  await views[view]?.()
}

// Handle browser back/forward navigation via hash changes
window.addEventListener('hashchange', async () => {
  console.log('[Router] Hash changed to:', window.location.hash)
  const view = parseHash()
  console.log('[Router] Parsed view:', view)
  if (view && S.token) {
    setActiveNav(view)
    localStorage.setItem('llm_current_view', view)
    const views = { dashboard: showDashboard, users: showUsers, buckets: showBuckets, 'content-filters': showContentFilters, multicluster: showMulticluster }
    await views[view]?.()
  }
})

// ─────────────────────────────────────────────────────────────────────────────
// Login
// ─────────────────────────────────────────────────────────────────────────────

function renderLogin() {
  document.getElementById('app').innerHTML = `
    <div class="login-page">
      <div class="login-card">
        <div class="login-brand">
          <div class="brand-icon-lg">⚡</div>
          <h1>LLM Router</h1>
          <p>Sign in with your LDAP credentials</p>
        </div>
        <form onsubmit="handleLogin(event)">
          <div class="form-group">
            <label>Username</label>
            <input id="f-user" type="text" required autocomplete="username" />
          </div>
          <div class="form-group">
            <label>Password</label>
            <input id="f-pass" type="password" required autocomplete="current-password" />
          </div>
          <button id="login-btn" type="submit" class="btn btn-primary btn-block">
            Sign in
          </button>
        </form>
      </div>
    </div>
  `
}

async function handleLogin(e) {
  e.preventDefault()
  const btn = document.getElementById('login-btn')
  btn.disabled = true
  btn.textContent = 'Signing in…'
  try {
    const { access_token } = await api.post('/auth/login', {
      username: document.getElementById('f-user').value,
      password: document.getElementById('f-pass').value,
    })
    S.token = access_token
    localStorage.setItem('llm_token', access_token)
    S.me = await api.get('/auth/me')
    renderShell()
    await navigate('dashboard')
  } catch (err) {
    toast(err.message, 'error')
    btn.disabled = false
    btn.textContent = 'Sign in'
  }
}

function logout() {
  S.token = null; S.me = null; S.users = []; S.buckets = []
  localStorage.removeItem('llm_token')
  renderLogin()
}

// ─────────────────────────────────────────────────────────────────────────────
// Dashboard (My Profile)
// ─────────────────────────────────────────────────────────────────────────────

async function showDashboard() {
  try { S.me = await api.get('/auth/me') } catch { }

  // Fetch accessible models
  let accessibleModels = []
  try {
    accessibleModels = await api.get('/auth/me/models')
  } catch { }

  // Fetch default model from /v1/models
  let defaultModel = null
  try {
    const modelsResp = await api.get('/v1/models')
    defaultModel = modelsResp.default_model || null
  } catch { }

  const me = S.me

  // Default model badge
  const defaultModelBadge = defaultModel
    ? `<span class="badge badge-blue">${esc(defaultModel)}</span>`
    : '<span style="color:var(--muted)">Not set</span>'

  // Build model cards HTML
  const modelCardsHtml = accessibleModels.length
    ? accessibleModels.map(modelId => `
        <div class="model-card" onclick="copyModelConfig('${esc(modelId)}')" title="Click to copy config">
          <div class="model-icon">🤖</div>
          <div class="model-name" title="${esc(modelId)}">${esc(modelId)}</div>
        </div>
      `).join('')
    : '<p class="empty-models">No models accessible. Contact an administrator.</p>'

  setContent(`
    <div class="page">
      <div class="page-header"><h2>My Profile</h2></div>
      <div class="card-grid">
        <div class="card">
          <div class="card-label">Username</div>
          <div class="card-value">${esc(me.username)}</div>
        </div>
        <div class="card">
          <div class="card-label">Email</div>
          <div class="card-value">${esc(me.email || '—')}</div>
        </div>
        <div class="card">
          <div class="card-label">Status</div>
          <div class="card-value">${badgeStatus(me.is_active)}</div>
        </div>
        <div class="card">
          <div class="card-label">Role</div>
          <div class="card-value">${badgeRole(me.is_admin)}</div>
        </div>
        <div class="card">
          <div class="card-label">Max Concurrent</div>
          <div class="card-value">${me.max_concurrent_requests}</div>
        </div>
        <div class="card">
          <div class="card-label">Default Model</div>
          <div class="card-value">${defaultModelBadge}</div>
        </div>
      </div>
      <div class="section">
        <h3>Accessible Models <span class="count">${accessibleModels.length}</span></h3>
        <div class="models-grid">
          ${modelCardsHtml}
        </div>
      </div>
      <div class="section">
        <h3>Bearer Token</h3>
        <div class="token-box">
          <code class="token-text" id="token-display">${esc(S.token)}</code>
          <button class="btn btn-secondary btn-sm" onclick="copyToken()">Copy</button>
          <button class="btn btn-primary btn-sm" onclick="regenerateToken()">Regenerate Token</button>
        </div>
      </div>
    </div>
  `)
}

async function copyToken() {
  // Try modern clipboard API first
  if (navigator.clipboard && window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(S.token)
      showCopySuccess()
      return
    } catch (err) {
      console.log('Clipboard API failed, trying fallback')
    }
  }

  // Fallback: use textarea and execCommand
  try {
    const textarea = document.createElement('textarea')
    textarea.value = S.token
    textarea.style.position = 'fixed'
    textarea.style.left = '-9999px'
    textarea.style.top = '0'
    document.body.appendChild(textarea)
    textarea.focus()
    textarea.select()

    const successful = document.execCommand('copy')
    document.body.removeChild(textarea)

    if (successful) {
      showCopySuccess()
    } else {
      toast('Failed to copy token', 'error')
    }
  } catch (err) {
    toast('Failed to copy token', 'error')
  }
}

function showCopySuccess() {
  toast('Token copied to clipboard', 'success')

  // Visual feedback animation on button
  const btn = document.querySelector('.token-box .btn')
  if (btn) {
    const originalText = btn.textContent
    btn.textContent = 'Copied!'
    btn.classList.add('btn-success')
    btn.classList.remove('btn-secondary')

    setTimeout(() => {
      btn.textContent = originalText
      btn.classList.remove('btn-success')
      btn.classList.add('btn-secondary')
    }, 1500)
  }
}

async function regenerateToken() {
  try {
    const response = await api.post('/auth/refresh-token')
    S.token = response.access_token
    localStorage.setItem('llm_token', response.access_token)
    
    // Update the displayed token
    const tokenDisplay = document.getElementById('token-display')
    if (tokenDisplay) {
      tokenDisplay.textContent = response.access_token
    }
    
    toast('Token regenerated successfully', 'success')
  } catch (err) {
    toast(err.message || 'Failed to regenerate token', 'error')
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Model Config Copy
// ─────────────────────────────────────────────────────────────────────────────

function toCamelCase(str) {
  // Convert model ID to a valid camelCase identifier
  // e.g., "gpt-4o-mini" -> "gpt4oMini", "meta/llama-3-8b" -> "llama3_8b"
  return str
    .replace(/[^a-zA-Z0-9]+(.)/g, (_, chr) => chr.toUpperCase())
    .replace(/^[^a-zA-Z_]/, '_') // prefix with underscore if starts with number
}

function getShortName(modelId) {
  // Extract a condensed name from model ID
  // e.g., "gpt-4o-mini" -> "GPT 4o Mini"
  // e.g., "meta/llama-3-8b" -> "Llama 3 8B"
  const cleaned = modelId
    .replace(/^[^/]+\//, '')               // Remove namespace prefix like meta/
    .replace(/[-_]/g, ' ')                 // Replace dashes/underscores with spaces
    .replace(/([a-z])([A-Z])/g, '$1 $2')   // Split camelCase

  // Capitalize each word
  return cleaned
    .split(' ')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ')
}

async function copyModelConfig(modelId) {
  // Get the current base URL (without /ui path)
  const currentUrl = window.location.origin
  const baseURL = `${currentUrl}/v1`

  // Create a camelCase key for the config
  const configKey = toCamelCase(modelId)

  // Create a condensed display name
  const condensedName = getShortName(modelId)

  // Build the config object in the requested format
  const config = {
    [configKey]: {
      name: "Local-On-Prem",
      npm: "@ai-sdk/openai-compatible",
      models: {
        [modelId]: {
          name: condensedName
        }
      },
      options: {
        baseURL: baseURL,
        apiKey: S.token
      }
    }
  }

  // Convert to JSON string with pretty formatting
  const configText = JSON.stringify(config, null, 2)

  // Try modern clipboard API first
  if (navigator.clipboard && window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(configText)
      showModelConfigCopySuccess(modelId)
      return
    } catch (err) {
      console.log('Clipboard API failed, trying fallback')
    }
  }

  // Fallback: use textarea and execCommand
  try {
    const textarea = document.createElement('textarea')
    textarea.value = configText
    textarea.style.position = 'fixed'
    textarea.style.left = '-9999px'
    textarea.style.top = '0'
    document.body.appendChild(textarea)
    textarea.focus()
    textarea.select()

    const successful = document.execCommand('copy')
    document.body.removeChild(textarea)

    if (successful) {
      showModelConfigCopySuccess(modelId)
    } else {
      toast(`Failed to copy config for ${modelId}`, 'error')
    }
  } catch (err) {
    toast(`Failed to copy config for ${modelId}`, 'error')
  }
}

function showModelConfigCopySuccess(modelId) {
  toast(`Config for "${modelId}" copied to clipboard`, 'success')
}

// ─────────────────────────────────────────────────────────────────────────────
// Users
// ─────────────────────────────────────────────────────────────────────────────

async function fetchUsers(search = '', skip = 0, limit = 20) {
  const params = new URLSearchParams({ skip: String(skip), limit: String(limit) })
  if (search) params.append('search', search)
  const response = await api.get(`/admin/users?${params.toString()}`)
  return response
}

async function showUsers() {
  S.userQuery = ''
  S.usersPagination = { skip: 0, limit: 20, total: 0 }
  setContent('<div class="loading">Loading…</div>')
  try {
    const [usersResponse, buckets] = await Promise.all([
      fetchUsers(),
      api.get('/admin/buckets'),
    ])
    S.users = usersResponse.data || []
    S.usersPagination.total = usersResponse.total || 0
    S.usersPagination.skip = usersResponse.skip || 0
    S.usersPagination.limit = usersResponse.limit || 20
    S.buckets = buckets
    renderUsersTable()
  } catch (e) { toast(e.message, 'error') }
}

const _searchIcon = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none"
  stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
  <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
</svg>`

function renderUsersTable() {
  const rows = S.users.map(u => `
    <tr data-username="${esc(u.username)}" data-email="${esc(u.email || '')}">
      <td><span class="cell-text-sm">${u.id}</span></td>
      <td><span class="cell-text" title="${esc(u.username)}">${esc(u.username)}</span></td>
      <td><span class="cell-text" title="${esc(u.email || '—')}">${esc(u.email || '—')}</span></td>
      <td>${badgeStatus(u.is_active)}</td>
      <td>${badgeRole(u.is_admin)}</td>
      <td style="text-align:center">${u.max_concurrent_requests}</td>
      <td class="actions">
        <button class="btn btn-sm ${u.is_active ? 'btn-warning' : 'btn-success'}"
                onclick="toggleActive(${u.id},${u.is_active})">
          ${u.is_active ? 'Deactivate' : 'Activate'}
        </button>
        <button class="btn btn-sm btn-secondary"
                onclick="toggleAdmin(${u.id},${u.is_admin})">
          ${u.is_admin ? 'Remove Admin' : 'Make Admin'}
        </button>
        <button class="btn btn-sm btn-secondary"
                onclick="editMaxConcurrent(${u.id},${u.max_concurrent_requests})">
          Limit
        </button>
        <button class="btn btn-sm btn-secondary"
                onclick="manageBuckets(${u.id})">
          Buckets
        </button>
      </td>
    </tr>`).join('')

  const { skip, limit, total } = S.usersPagination
  const currentPage = Math.floor(skip / limit) + 1
  const totalPages = Math.ceil(total / limit) || 1
  const startItem = total > 0 ? skip + 1 : 0
  const endItem = Math.min(skip + limit, total)

  // Pagination controls
  const renderPageButton = (pageNum, label = null, isActive = false) => {
    const pageSkip = (pageNum - 1) * limit
    const activeClass = isActive ? 'active' : ''
    return `<button class="btn btn-sm ${activeClass}" onclick="goToPage(${pageSkip})" ${isActive ? 'disabled' : ''}>${label || pageNum}</button>`
  }

  let paginationHtml = ''
  if (total > limit) {
    const pages = []
    // Previous button
    pages.push(`<button class="btn btn-sm" onclick="goToPage(${Math.max(0, skip - limit)})" ${skip === 0 ? 'disabled' : ''}>← Prev</button>`)

    // Page numbers (simplified: show first, last, current, and neighbors)
    const maxVisible = 5
    let startPage = Math.max(1, currentPage - Math.floor(maxVisible / 2))
    let endPage = Math.min(totalPages, startPage + maxVisible - 1)
    if (endPage - startPage < maxVisible - 1) {
      startPage = Math.max(1, endPage - maxVisible + 1)
    }

    if (startPage > 1) {
      pages.push(renderPageButton(1))
      if (startPage > 2) pages.push('<span class="pagination-ellipsis">…</span>')
    }

    for (let i = startPage; i <= endPage; i++) {
      pages.push(renderPageButton(i, i, i === currentPage))
    }

    if (endPage < totalPages) {
      if (endPage < totalPages - 1) pages.push('<span class="pagination-ellipsis">…</span>')
      pages.push(renderPageButton(totalPages))
    }

    // Next button
    pages.push(`<button class="btn btn-sm" onclick="goToPage(${Math.min(skip + limit, (totalPages - 1) * limit)})" ${skip + limit >= total ? 'disabled' : ''}>Next →</button>`)

    paginationHtml = `
      <div class="pagination-controls">
        ${pages.join('')}
      </div>
    `
  }

  setContent(`
    <div class="page">
      <div class="page-header">
        <h2>Users <span class="count">${total}</span></h2>
        <button class="btn btn-primary" onclick="openCreateUser()">+ New User</button>
      </div>
      <div class="toolbar">
        <div class="search-wrap">
          ${_searchIcon}
          <input class="search-input" type="search" id="user-search" 
                 placeholder="Search by username or email…"
                 value="${esc(S.userQuery)}"
                 oninput="handleUserSearch(this.value)" />
        </div>
        <span class="search-count" id="user-search-count">${total > 0 ? `Showing ${startItem}–${endItem} of ${total}` : ''}</span>
      </div>
      <div class="card-table table-wrap">
        <table class="table">
          <thead>
            <tr>
              <th>ID</th><th>Username</th><th>Email</th>
              <th>Status</th><th>Role</th><th>Max Concurrent</th><th>Actions</th>
            </tr>
          </thead>
          <tbody id="user-tbody">
            ${rows || `<tr class="no-results"><td colspan="7">${S.userQuery ? 'No users found matching your search.' : 'No users yet.'}</td></tr>`}
          </tbody>
        </table>
      </div>
      ${paginationHtml}
    </div>
  `)
}

// Debounce function for search input
function debounce(fn, delay) {
  let timer = null
  return function (...args) {
    clearTimeout(timer)
    timer = setTimeout(() => fn.apply(this, args), delay)
  }
}

// Debounced search handler (300ms delay)
const debouncedUserSearch = debounce(async (val) => {
  S.userQuery = val
  S.usersPagination.skip = 0  // Reset to first page on search
  await loadUsersPage()
}, 300)

async function handleUserSearch(val) {
  debouncedUserSearch(val)
}

async function goToPage(skip) {
  S.usersPagination.skip = skip
  await loadUsersPage()
}

async function loadUsersPage() {
  try {
    const response = await fetchUsers(S.userQuery, S.usersPagination.skip, S.usersPagination.limit)
    S.users = response.data || []
    S.usersPagination.total = response.total || 0
    S.usersPagination.skip = response.skip || 0
    S.usersPagination.limit = response.limit || 20
    renderUsersTable()
  } catch (e) {
    toast(e.message, 'error')
  }
}

async function toggleActive(userId, currentlyActive) {
  try {
    await api.patch(`/admin/users/${userId}/active`, { is_active: !currentlyActive })
    // Refresh the current page to show updated data
    await loadUsersPage()
    toast(`User ${currentlyActive ? 'deactivated' : 'activated'}`, 'success')
  } catch (e) { toast(e.message, 'error') }
}

async function toggleAdmin(userId, currentlyAdmin) {
  try {
    await api.patch(`/admin/users/${userId}/admin`, { is_admin: !currentlyAdmin })
    // Refresh the current page to show updated data
    await loadUsersPage()
    toast(`Admin ${currentlyAdmin ? 'revoked' : 'granted'}`, 'success')
  } catch (e) { toast(e.message, 'error') }
}

function editMaxConcurrent(userId, current) {
  openModal('Set Max Concurrent Requests', `
    <div class="form-group">
      <label>Max concurrent in-flight requests</label>
      <input id="m-max" type="number" min="1" value="${current}" />
      <div class="form-hint">Minimum: 1</div>
    </div>
  `, async () => {
    const val = parseInt(document.getElementById('m-max').value, 10)
    if (!val || val < 1) throw new Error('Value must be ≥ 1')
    await api.patch(`/admin/users/${userId}/max-concurrent`, { max_concurrent_requests: val })
    // Refresh the current page to show updated data
    await loadUsersPage()
    toast('Updated', 'success')
  })
}

async function manageBuckets(userId) {
  const user = S.users.find(u => u.id === userId)
  if (!user) return
  let currentBucketIds
  try {
    currentBucketIds = await api.get(`/admin/users/${userId}/buckets`)
  } catch (e) { toast(e.message, 'error'); return }

  // Filter to only show buckets that have models (buckets with all models available in backend)
  const validBuckets = S.buckets.filter(b => b.models && b.models.length > 0)

  const checkboxes = validBuckets.length
    ? validBuckets.map(b => `
      <label class="checkbox-item">
        <input type="checkbox" value="${b.id}"
               ${currentBucketIds.includes(b.id) ? 'checked' : ''} />
        <span>${esc(b.name)}${b.is_default
        ? ' <span class="badge badge-blue" style="font-size:10px;padding:1px 5px">default</span>'
        : ''}</span>
      </label>`).join('')
    : '<p style="color:var(--muted);font-size:13px">No buckets with available models.</p>'

  openModal(`Bucket Access — ${esc(user.username)}`, `
    <p style="color:var(--muted);font-size:12px;margin-bottom:12px">
      Select which buckets this user can access.
    </p>
    <div class="checkbox-group">${checkboxes}</div>
  `, async () => {
    const checked = [...document.querySelectorAll('#modal-body input[type=checkbox]:checked')]
      .map(el => parseInt(el.value))
    const toGrant = checked.filter(id => !currentBucketIds.includes(id))
    const toRevoke = currentBucketIds.filter(id => !checked.includes(id))
    await Promise.all([
      ...toGrant.map(bid => api.post(`/admin/users/${userId}/buckets/${bid}`)),
      ...toRevoke.map(bid => api.delete(`/admin/users/${userId}/buckets/${bid}`)),
    ])
    toast('Bucket access updated', 'success')
  })
}

function openCreateUser() {
  openModal('Create User', `
    <div class="form-group">
      <label>Username</label>
      <input id="m-uname" type="text" required placeholder="LDAP username" />
      <div class="form-hint">User must already exist in LDAP.</div>
    </div>
    <div class="form-group">
      <label>Email <span style="font-weight:400;color:var(--muted)">(optional)</span></label>
      <input id="m-email" type="email" placeholder="Override LDAP email" />
    </div>
  `, async () => {
    const username = document.getElementById('m-uname').value.trim()
    const email = document.getElementById('m-email').value.trim() || undefined
    if (!username) throw new Error('Username is required')
    await api.post('/admin/users', { username, email })
    // Refresh the first page to show the new user
    S.usersPagination.skip = 0
    S.userQuery = ''
    await loadUsersPage()
    toast(`User "${username}" created`, 'success')
  }, 'Create')
}

// ─────────────────────────────────────────────────────────────────────────────
// Buckets
// ─────────────────────────────────────────────────────────────────────────────

async function showBuckets() {
  S.bucketQuery = ''
  setContent('<div class="loading">Loading…</div>')
  try {
    S.buckets = await api.get('/admin/buckets')
    const defaultBucket = S.buckets.find(b => b.is_default)
    S.allModels = defaultBucket ? defaultBucket.models.map(m => m.model_id) : []
    renderBucketsTable()
  } catch (e) { toast(e.message, 'error') }
}

function renderBucketsTable() {
  const rows = S.buckets.map(b => {
    // Build default model dropdown for default bucket
    let defaultModelControls = '<span style="color:var(--muted)">—</span>'
    if (b.is_default && b.models.length > 0) {
      const selectId = `dm-select-${b.id}`
      const dm = b.default_model || ''
      const options = b.models.map(m => {
        const selected = m.model_id === dm ? 'selected' : ''
        return `<option value="${esc(m.model_id)}" ${selected}>${esc(m.model_id)}</option>`
      }).join('')
      defaultModelControls = `
        <div style="display:flex;align-items:center;gap:6px;margin-top:4px">
          <select id="${selectId}" onchange="setDefaultModel(${b.id}, this.value)" style="width:100%;padding:4px 8px;font-size:13px;border:1px solid var(--border);border-radius:6px;background:var(--bg-elevated);color:var(--text)">
            ${options}
          </select>
        </div>`
    }

    return `<tr data-name="${esc(b.name)}" data-desc="${esc(b.description || '')}">
      <td><span class="cell-text-sm">${b.id}</span></td>
      <td><span class="cell-text" title="${esc(b.name)}">${esc(b.name)}</span></td>
      <td><span class="cell-text" title="${esc(b.description || '—')}">${esc(b.description || '—')}</span></td>
      <td>${b.is_default
      ? '<span class="badge badge-blue">Default</span>'
      : '<span class="badge badge-gray">No</span>'}</td>
      <td style="max-width:220px">${b.models.map(m =>
        `<span class="badge badge-gray" style="margin:1px 2px">${esc(m.model_id)}</span>`
      ).join('') || '<span style="color:var(--muted)">—</span>'}</td>
      <td>
        ${defaultModelControls}
      </td>
      <td class="actions">
        <button class="btn btn-sm btn-secondary"
                onclick="openEditBucket(${b.id})">Edit</button>
        ${!b.is_default
      ? `<button class="btn btn-sm btn-secondary"
                     onclick="setDefaultBucket(${b.id})">Set Default</button>`
      : '<span class="btn-placeholder"></span>'}
      </td>
    </tr>`
  }).join('')

  setContent(`
    <div class="page">
      <div class="page-header">
        <h2>Buckets <span class="count">${S.buckets.length}</span></h2>
        <button class="btn btn-primary" onclick="openCreateBucket()">+ New Bucket</button>
      </div>
      <div class="toolbar">
        <div class="search-wrap">
          ${_searchIcon}
          <input class="search-input" type="search" id="bucket-search"
                 placeholder="Search by name or description…"
                 value="${esc(S.bucketQuery)}"
                 oninput="filterBuckets(this.value)" />
        </div>
        <span class="search-count" id="bucket-search-count"></span>
      </div>
      <div class="card-table table-wrap">
        <table class="table">
          <thead>
            <tr>
              <th>ID</th><th>Name</th><th>Description</th>
              <th>Default</th><th>Models</th><th>Default Model</th><th>Actions</th>
            </tr>
          </thead>
          <tbody id="bucket-tbody">
            ${rows || '<tr class="no-results"><td colspan="7">No buckets yet.</td></tr>'}
          </tbody>
        </table>
      </div>
    </div>
  `)
  applyBucketFilter(S.bucketQuery)
}

function filterBuckets(val) {
  S.bucketQuery = val
  applyBucketFilter(val)
}

function applyBucketFilter(q) {
  const query = q.toLowerCase().trim()
  let visible = 0
  document.querySelectorAll('#bucket-tbody tr[data-name]').forEach(tr => {
    const match = !query ||
      tr.dataset.name.toLowerCase().includes(query) ||
      tr.dataset.desc.toLowerCase().includes(query)
    tr.style.display = match ? '' : 'none'
    if (match) visible++
  })
  const countEl = document.getElementById('bucket-search-count')
  if (countEl && query) countEl.textContent = `${visible} of ${S.buckets.length} shown`
  else if (countEl) countEl.textContent = ''
}

async function setDefaultBucket(bucketId) {
  try {
    await api.patch(`/admin/buckets/${bucketId}/default`)
    S.buckets.forEach(b => b.is_default = b.id === bucketId)
    renderBucketsTable()
    toast('Default bucket updated', 'success')
  } catch (e) { toast(e.message, 'error') }
}

async function setDefaultModel(bucketId, modelId) {
  try {
    await api.patch(`/admin/buckets/${bucketId}/default-model`, { model_id: modelId })
    S.buckets.forEach(b => {
      if (b.id === bucketId) b.default_model = modelId
    })
    toast('Default model updated: ' + modelId, 'success')
  } catch (e) { toast(e.message, 'error') }
}

async function openCreateBucket() {
  let models = []
  try { models = (await api.get('/admin/models')).data } catch { }

  const checkboxes = models.length
    ? models.map(m => `
        <label class="checkbox-item">
          <input type="checkbox" name="bmodel" value="${esc(m.id)}" />
          ${esc(m.id)}
        </label>`).join('')
    : '<span style="color:var(--muted);font-size:12px">No models available</span>'

  openModal('Create Bucket', `
    <div class="form-group">
      <label>Name</label>
      <input id="m-bname" type="text" required placeholder="e.g. gpt4-users" />
    </div>
    <div class="form-group">
      <label>Description <span style="font-weight:400;color:var(--muted)">(optional)</span></label>
      <input id="m-bdesc" type="text" placeholder="Optional description" />
    </div>
    <div class="form-group">
      <label>Models</label>
      <div class="checkbox-group">${checkboxes}</div>
    </div>
  `, async () => {
    const name = document.getElementById('m-bname').value.trim()
    const description = document.getElementById('m-bdesc').value.trim() || undefined
    const model_ids = [...document.querySelectorAll('input[name=bmodel]:checked')]
      .map(el => el.value)
    if (!name) throw new Error('Name is required')
    await api.post('/admin/buckets', { name, description, model_ids })
    toast(`Bucket "${name}" created`, 'success')
    await showBuckets()
  }, 'Create')
}

async function openEditBucket(bucketId) {
  const bucket = S.buckets.find(b => b.id === bucketId)
  if (!bucket) {
    toast('Bucket not found', 'error')
    return
  }

  // Fetch available models
  let models = []
  try { models = (await api.get('/admin/models')).data } catch { }

  // Get current bucket's model IDs
  const currentModelIds = bucket.models.map(m => m.model_id)

  const checkboxes = models.length
    ? models.map(m => `
        <label class="checkbox-item">
          <input type="checkbox" name="bmodel" value="${esc(m.id)}"
                 ${currentModelIds.includes(m.id) ? 'checked' : ''} />
          ${esc(m.id)}
        </label>`).join('')
    : '<span style="color:var(--muted);font-size:12px">No models available</span>'

  openModal(`Edit Bucket — ${esc(bucket.name)}`, `
    <div class="form-group">
      <label>Name</label>
      <input id="m-bname" type="text" required value="${esc(bucket.name)}" />
    </div>
    <div class="form-group">
      <label>Description <span style="font-weight:400;color:var(--muted)">(optional)</span></label>
      <input id="m-bdesc" type="text" value="${esc(bucket.description || '')}" />
    </div>
    <div class="form-group">
      <label>Models</label>
      <div class="checkbox-group">${checkboxes}</div>
      <div class="form-hint">Select models for this bucket (replaces all existing assignments)</div>
    </div>
  `, async () => {
    const name = document.getElementById('m-bname').value.trim()
    const description = document.getElementById('m-bdesc').value.trim() || undefined
    const model_ids = [...document.querySelectorAll('input[name=bmodel]:checked')]
      .map(el => el.value)

    if (!name) throw new Error('Name is required')
    if (model_ids.length === 0) throw new Error('At least one model must be selected')

    await api.patch(`/admin/buckets/${bucketId}`, { name, description, model_ids })
    toast(`Bucket "${name}" updated`, 'success')
    await showBuckets()
  }, 'Save')
}

// ─────────────────────────────────────────────────────────────────────────────
// Init
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// Content Filters
// ─────────────────────────────────────────────────────────────────────────────

function getRuleTypeBadge(type) {
  const colors = {
    'REGEX': '#f59e0b',
    'KEYWORD': '#10b981',
    'EXTERNAL_URL': '#6366f1'
  }
  const color = colors[type] || '#6b7280'
  return `<span class="badge" style="background:${color};color:white">${type}</span>`
}

function getActionBadge(isBlocking) {
  return isBlocking
    ? '<span class="badge" style="background:#ef4444;color:white">Block</span>'
    : '<span class="badge" style="background:#f59e0b;color:white">Audit</span>'
}

async function showContentFilters() {
  S.contentFilterQuery = ''
  setContent('<div class="loading">Loading…</div>')
  try {
    S.contentFilters = await api.get('/admin/content-filters')
    renderContentFiltersTable()
  } catch (e) { toast(e.message, 'error') }
}

function renderContentFiltersTable() {
  const rows = S.contentFilters.map(r => `
    <tr data-name="${esc(r.name)}" data-desc="${esc(r.description || '')}" data-type="${esc(r.rule_type)}">
      <td><span class="cell-text-sm">${r.id}</span></td>
      <td><span class="cell-text" title="${esc(r.name)}">${esc(r.name)}</span></td>
      <td><span class="cell-text" title="${esc(r.description || '—')}">${esc(r.description || '—')}</span></td>
      <td>${getRuleTypeBadge(r.rule_type)}</td>
      <td>${badgeStatus(r.is_active)}</td>
      <td>${getActionBadge(r.is_blocking)}</td>
      <td class="actions">
        <button class="btn btn-sm btn-secondary" onclick="testFilterRule('${esc(r.name)}')">Test</button>
        <button class="btn btn-sm ${r.is_active ? 'btn-warning' : 'btn-success'}"
                onclick="toggleFilterActive(${r.id}, ${r.is_active})">
          ${r.is_active ? 'Deactivate' : 'Activate'}
        </button>
        <button class="btn btn-sm btn-secondary" onclick="editFilterRule(${r.id})">Edit</button>
        <button class="btn btn-sm btn-danger" onclick="deleteFilterRule(${r.id})">Delete</button>
      </td>
    </tr>`).join('')

  setContent(`
    <div class="page">
      <div class="page-header">
        <h2>Content Filters <span class="count">${S.contentFilters.length}</span></h2>
        <button class="btn btn-primary" onclick="openCreateContentFilter()">+ New Filter</button>
      </div>
      <div class="toolbar">
        <div class="search-wrap">
          ${_searchIcon}
          <input class="search-input" type="search" id="filter-search"
                 placeholder="Search by name or description…"
                 value="${esc(S.contentFilterQuery)}"
                 oninput="filterContentFilters(this.value)" />
        </div>
        <span class="search-count" id="filter-search-count"></span>
      </div>
      <div class="card-table table-wrap">
        <table class="table">
          <thead>
            <tr>
              <th>ID</th><th>Name</th><th>Description</th>
              <th>Type</th><th>Status</th><th>Action</th><th>Actions</th>
            </tr>
          </thead>
          <tbody id="filter-tbody">
            ${rows || '<tr class="no-results"><td colspan="7">No content filters yet.</td></tr>'}
          </tbody>
        </table>
      </div>
    </div>
  `)
  applyContentFilterFilter(S.contentFilterQuery)
}

function filterContentFilters(val) {
  S.contentFilterQuery = val
  applyContentFilterFilter(val)
}

function applyContentFilterFilter(q) {
  const query = q.toLowerCase().trim()
  let visible = 0
  document.querySelectorAll('#filter-tbody tr[data-name]').forEach(tr => {
    const match = !query ||
      tr.dataset.name.toLowerCase().includes(query) ||
      tr.dataset.desc.toLowerCase().includes(query) ||
      tr.dataset.type.toLowerCase().includes(query)
    tr.style.display = match ? '' : 'none'
    if (match) visible++
  })
  const countEl = document.getElementById('filter-search-count')
  if (countEl && query) countEl.textContent = `${visible} of ${S.contentFilters.length} shown`
  else if (countEl) countEl.textContent = ''
}

async function toggleFilterActive(ruleId, currentlyActive) {
  try {
    await api.patch(`/admin/content-filters/${ruleId}`, { is_active: !currentlyActive })
    await showContentFilters()
    toast(`Filter ${currentlyActive ? 'deactivated' : 'activated'}`, 'success')
  } catch (e) { toast(e.message, 'error') }
}

async function deleteFilterRule(ruleId) {
  if (!confirm('Are you sure you want to delete this content filter?')) return
  try {
    await api.delete(`/admin/content-filters/${ruleId}`)
    await showContentFilters()
    toast('Filter deleted', 'success')
  } catch (e) { toast(e.message, 'error') }
}

function openCreateContentFilter() {
  openModal('Create Content Filter Rule', `
    <div class="form-group">
      <label>Name</label>
      <input id="m-name" type="text" required placeholder="e.g. profanity-filter" />
    </div>
    <div class="form-group">
      <label>Description <span style="font-weight:400;color:var(--muted)">(optional)</span></label>
      <input id="m-desc" type="text" placeholder="Brief description of this rule" />
    </div>
    <div class="form-group">
      <label>Rule Type</label>
      <select id="m-type" onchange="onFilterTypeChange(this.value)">
        <option value="REGEX">REGEX - Pattern matching</option>
        <option value="KEYWORD">KEYWORD - Simple word match</option>
        <option value="EXTERNAL_URL">EXTERNAL_URL - External API check</option>
      </select>
    </div>
    <div class="form-group" id="pattern-group">
      <label>Pattern/Keyword</label>
      <input id="m-pattern" type="text" placeholder="e.g. (bad|worse|worst)word" />
      <div class="form-hint" id="pattern-hint">Regex pattern or keyword to match</div>
    </div>
    <div class="form-group hidden" id="url-group">
      <label>External URL</label>
      <input id="m-url" type="url" placeholder="https://api.example.com/check" />
      <div class="form-hint">URL of external content filtering API</div>
    </div>
    <div class="form-group hidden" id="extra-params-group">
      <label>Extra Parameters (JSON) <span style="font-weight:400;color:var(--muted)">(optional)</span></label>
      <textarea id="m-extra-params" rows="3" placeholder='{"api_key": "secret", "strict_mode": true}'></textarea>
      <div class="form-hint">Additional JSON parameters to include in the request payload</div>
    </div>
    <div class="form-group">
      <label>Fields to Check</label>
      <input id="m-fields" type="text" value="content" placeholder="content,system_content" />
      <div class="form-hint">Comma-separated list of message fields to check</div>
    </div>
    <div class="form-group">
      <label>Violation Message <span style="font-weight:400;color:var(--muted)">(optional)</span></label>
      <input id="m-violation" type="text" placeholder="Custom message shown when content is blocked" />
    </div>
    <div class="form-group">
      <label class="checkbox-item" style="display:flex;align-items:center;gap:8px">
        <input type="checkbox" id="m-active" checked />
        <span>Active</span>
      </label>
    </div>
    <div class="form-group">
      <label class="checkbox-item" style="display:flex;align-items:center;gap:8px">
        <input type="checkbox" id="m-blocking" checked />
        <span>Blocking (if unchecked, only logs violations)</span>
      </label>
    </div>
  `, async () => {
    const name = document.getElementById('m-name').value.trim()
    const description = document.getElementById('m-desc').value.trim() || undefined
    const rule_type = document.getElementById('m-type').value
    const pattern = document.getElementById('m-pattern').value.trim() || undefined
    const external_url = document.getElementById('m-url').value.trim() || undefined
    const extra_params_raw = document.getElementById('m-extra-params').value.trim()
    const fields_to_check = document.getElementById('m-fields').value.trim() || 'content'
    const is_active = document.getElementById('m-active').checked
    const is_blocking = document.getElementById('m-blocking').checked
    const violation_message = document.getElementById('m-violation').value.trim() || undefined

    if (!name) throw new Error('Name is required')
    if (rule_type !== 'EXTERNAL_URL' && !pattern) {
      throw new Error('Pattern is required for REGEX and KEYWORD types')
    }
    if (rule_type === 'EXTERNAL_URL' && !external_url) {
      throw new Error('External URL is required for EXTERNAL_URL type')
    }

    // Parse extra_params if provided
    let extra_params = undefined
    if (rule_type === 'EXTERNAL_URL' && extra_params_raw) {
      try {
        extra_params = JSON.parse(extra_params_raw)
      } catch (e) {
        throw new Error('Invalid JSON in Extra Parameters: ' + e.message)
      }
    }

    await api.post('/admin/content-filters', {
      name,
      description,
      rule_type,
      pattern,
      external_url,
      extra_params,
      fields_to_check,
      is_active,
      is_blocking,
      violation_message
    })
    toast(`Filter "${name}" created`, 'success')
    await showContentFilters()
  }, 'Create')
}

function onFilterTypeChange(type) {
  const patternGroup = document.getElementById('pattern-group')
  const urlGroup = document.getElementById('url-group')
  const extraParamsGroup = document.getElementById('extra-params-group')
  const patternHint = document.getElementById('pattern-hint')

  if (type === 'EXTERNAL_URL') {
    patternGroup.classList.add('hidden')
    urlGroup.classList.remove('hidden')
    extraParamsGroup.classList.remove('hidden')
  } else {
    patternGroup.classList.remove('hidden')
    urlGroup.classList.add('hidden')
    extraParamsGroup.classList.add('hidden')
    patternHint.textContent = type === 'REGEX'
      ? 'Regex pattern to match (e.g., (bad|worse|worst)word)'
      : 'Keyword to match (case-insensitive)'
  }
}

async function editFilterRule(ruleId) {
  const rule = S.contentFilters.find(r => r.id === ruleId)
  if (!rule) return

  const extraParamsValue = rule.extra_params ? JSON.stringify(rule.extra_params, null, 2) : ''

  openModal(`Edit Filter - ${esc(rule.name)}`, `
    <div class="form-group">
      <label>Name</label>
      <input id="m-name" type="text" required value="${esc(rule.name)}" />
    </div>
    <div class="form-group">
      <label>Description <span style="font-weight:400;color:var(--muted)">(optional)</span></label>
      <input id="m-desc" type="text" value="${esc(rule.description || '')}" />
    </div>
    <div class="form-group">
      <label>Rule Type</label>
      <select id="m-type" onchange="onFilterTypeChange(this.value)">
        <option value="REGEX" ${rule.rule_type === 'REGEX' ? 'selected' : ''}>REGEX - Pattern matching</option>
        <option value="KEYWORD" ${rule.rule_type === 'KEYWORD' ? 'selected' : ''}>KEYWORD - Simple word match</option>
        <option value="EXTERNAL_URL" ${rule.rule_type === 'EXTERNAL_URL' ? 'selected' : ''}>EXTERNAL_URL - External API check</option>
      </select>
    </div>
    <div class="form-group" id="pattern-group">
      <label>Pattern/Keyword</label>
      <input id="m-pattern" type="text" value="${esc(rule.pattern || '')}" />
    </div>
    <div class="form-group ${rule.rule_type === 'EXTERNAL_URL' ? '' : 'hidden'}" id="url-group">
      <label>External URL</label>
      <input id="m-url" type="url" value="${esc(rule.external_url || '')}" />
    </div>
    <div class="form-group ${rule.rule_type === 'EXTERNAL_URL' ? '' : 'hidden'}" id="extra-params-group">
      <label>Extra Parameters (JSON) <span style="font-weight:400;color:var(--muted)">(optional)</span></label>
      <textarea id="m-extra-params" rows="3" placeholder='{"api_key": "secret", "strict_mode": true}'>${esc(extraParamsValue)}</textarea>
      <div class="form-hint">Additional JSON parameters to include in the request payload</div>
    </div>
    <div class="form-group">
      <label>Fields to Check</label>
      <input id="m-fields" type="text" value="${esc(rule.fields_to_check || 'content')}" />
    </div>
    <div class="form-group">
      <label>Violation Message <span style="font-weight:400;color:var(--muted)">(optional)</span></label>
      <input id="m-violation" type="text" value="${esc(rule.violation_message || '')}" />
    </div>
    <div class="form-group">
      <label class="checkbox-item" style="display:flex;align-items:center;gap:8px">
        <input type="checkbox" id="m-active" ${rule.is_active ? 'checked' : ''} />
        <span>Active</span>
      </label>
    </div>
    <div class="form-group">
      <label class="checkbox-item" style="display:flex;align-items:center;gap:8px">
        <input type="checkbox" id="m-blocking" ${rule.is_blocking ? 'checked' : ''} />
        <span>Blocking</span>
      </label>
    </div>
  `, async () => {
    const name = document.getElementById('m-name').value.trim()
    const description = document.getElementById('m-desc').value.trim() || undefined
    const rule_type = document.getElementById('m-type').value
    const pattern = document.getElementById('m-pattern').value.trim() || undefined
    const external_url = document.getElementById('m-url').value.trim() || undefined
    const extra_params_raw = document.getElementById('m-extra-params').value.trim()
    const fields_to_check = document.getElementById('m-fields').value.trim() || undefined
    const is_active = document.getElementById('m-active').checked
    const is_blocking = document.getElementById('m-blocking').checked
    const violation_message = document.getElementById('m-violation').value.trim() || undefined

    if (!name) throw new Error('Name is required')

    // Parse extra_params if provided
    let extra_params = undefined
    if (rule_type === 'EXTERNAL_URL' && extra_params_raw) {
      try {
        extra_params = JSON.parse(extra_params_raw)
      } catch (e) {
        throw new Error('Invalid JSON in Extra Parameters: ' + e.message)
      }
    }

    await api.patch(`/admin/content-filters/${ruleId}`, {
      name,
      description,
      rule_type,
      pattern,
      external_url,
      extra_params,
      fields_to_check,
      is_active,
      is_blocking,
      violation_message
    })
    toast(`Filter "${name}" updated`, 'success')
    await showContentFilters()
  })
}

async function testFilterRule(ruleName) {
  // Create modal content with test button and close button inside the body
  const modalContent = `
    <div class="form-group">
      <label>Test Content</label>
      <textarea id="m-content" rows="4" placeholder="Enter text to test against the filter..."></textarea>
    </div>
    <div style="margin-top:12px;display:flex;gap:8px;">
      <button id="test-btn" class="btn btn-primary">Test</button>
      <button id="close-test-btn" class="btn btn-secondary">Close</button>
    </div>
    <div id="test-status" style="margin-top:12px;padding:12px;border-radius:6px;background:#f3f4f6;border:1px solid #d1d5db;">
      <strong style="color:#6b7280;">📝 Ready to Test</strong><br/>
      Enter text above and click Test to check against filter rules.
    </div>
    <div id="test-result" style="margin-top:12px;padding:12px;border-radius:6px;display:none"></div>
  `

  // Set up the modal
  document.getElementById('modal-title').textContent = `Test Filter - ${esc(ruleName)}`
  document.getElementById('modal-body').innerHTML = modalContent
  document.getElementById('modal-overlay').classList.remove('hidden')

  // Hide the modal action buttons since we have our own
  const submitBtn = document.getElementById('modal-submit')
  const cancelBtn = submitBtn.previousElementSibling
  submitBtn.style.display = 'none'
  cancelBtn.style.display = 'none'

  // Set up the close button handler
  document.getElementById('close-test-btn').onclick = () => {
    closeModal()
  }

  // Set up the test handler
  document.getElementById('test-btn').onclick = async () => {
    const content = document.getElementById('m-content').value.trim()
    if (!content) {
      // Show error in the status area
      const statusEl = document.getElementById('test-status')
      statusEl.style.background = '#fef3c7'
      statusEl.style.border = '1px solid #f59e0b'
      statusEl.innerHTML = `<strong style="color:#d97706">⚠️ Please enter content to test</strong>`
      return
    }

    // Show testing in progress
    const statusEl = document.getElementById('test-status')
    statusEl.style.background = '#eff6ff'
    statusEl.style.border = '1px solid #3b82f6'
    statusEl.innerHTML = `<strong style="color:#2563eb">🔄 Testing...</strong>`

    try {
      const result = await api.post('/admin/content-filters/test', { content })
      const resultEl = document.getElementById('test-result')

      if (result.is_violation) {
        // Update status to show violation detected
        statusEl.style.background = '#fef2f2'
        statusEl.style.border = '1px solid #ef4444'
        statusEl.innerHTML = `<strong style="color:#dc2626">🚫 FILTER BLOCKED</strong>`

        // Show detailed result
        resultEl.style.display = 'block'
        resultEl.style.background = '#fef2f2'
        resultEl.style.border = '1px solid #ef4444'
        resultEl.innerHTML = `
          <div style="margin-bottom:8px;"><strong style="color:#dc2626;font-size:16px;">⚠️ RESTRICTED CONTENT DETECTED</strong></div>
          <div style="margin-bottom:4px;"><strong>Rule Name:</strong> ${esc(result.rule_name || 'Unknown')}</div>
          <div style="margin-bottom:4px;"><strong>Rule Type:</strong> ${esc(result.rule_type || 'N/A')}</div>
          <div style="margin-bottom:4px;"><strong>Matched Text:</strong> <code style="background:#fee2e2;padding:2px 6px;border-radius:3px;">${esc(result.matched_text || 'N/A')}</code></div>
          <div><strong>Message:</strong> ${esc(result.message || 'Content violates filter rules')}</div>
        `
      } else {
        // Update status to show passed
        statusEl.style.background = '#f0fdf4'
        statusEl.style.border = '1px solid #22c55e'
        statusEl.innerHTML = `<strong style="color:#16a34a">✅ CONTENT ALLOWED</strong>`

        // Show detailed result
        resultEl.style.display = 'block'
        resultEl.style.background = '#f0fdf4'
        resultEl.style.border = '1px solid #22c55e'
        resultEl.innerHTML = `
          <div style="margin-bottom:8px;"><strong style="color:#16a34a;font-size:16px;">✅ NO RESTRICTED CONTENT FOUND</strong></div>
          <div>The text passed all active filter checks. No violations detected.</div>
        `
      }
    } catch (error) {
      statusEl.style.background = '#fef2f2'
      statusEl.style.border = '1px solid #ef4444'
      statusEl.innerHTML = `<strong style="color:#dc2626">❌ Error: ${esc(error.message)}</strong>`
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Multicluster
// ─────────────────────────────────────────────────────────────────────────────

// Helper functions for formatting
function formatNumber(num) {
  if (num === null || num === undefined) return '—'
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',')
}

function formatPercent(num) {
  if (num === null || num === undefined) return '—'
  return `${(num * 100).toFixed(1)}%`
}

function getStatusColor(loadRatio) {
  if (loadRatio === null || loadRatio === undefined) return ''
  if (loadRatio < 0.5) return 'text-success'
  if (loadRatio < 0.8) return 'text-warning'
  return 'text-danger'
}

async function showMulticluster() {
  setContent('<div class="loading">Loading…</div>')
  try {
    // Fetch models that have multicluster support
    const models = await api.get('/admin/models')
    // Filter to only models with cluster support (has_clusters flag or similar)
    S.multiclusterModels = models.data.filter(m => m.supports_clusters || m.has_clusters || true) // Show all for now

    if (S.multiclusterModels.length === 0) {
      setContent(`
        <div class="page">
          <div class="page-header">
            <h2>Multicluster</h2>
          </div>
          <div class="empty-state">
            <div class="empty-icon">🌐</div>
            <p>No models with cluster support available.</p>
          </div>
        </div>
      `)
      return
    }

    // Default to first model
    S.selectedClusterModel = S.multiclusterModels[0].id
    S.currentModelId = S.multiclusterModels[0].id

    renderMulticlusterPage()
    
    // Start continuous refresh
    startClusterRefresh()
  } catch (e) {
    toast(e.message, 'error')
    setContent(`
      <div class="page">
        <div class="page-header">
          <h2>Multicluster</h2>
        </div>
        <div class="error-state">
          <p>Failed to load multicluster data: ${esc(e.message)}</p>
        </div>
      </div>
    `)
  }
}

function renderMulticlusterPage() {
  const modelOptions = S.multiclusterModels.map(m =>
    `<option value="${esc(m.id)}" ${m.id === S.selectedClusterModel ? 'selected' : ''}>${esc(m.id)}</option>`
  ).join('')

  setContent(`
    <div class="page">
      <div class="page-header">
        <h2>Multicluster</h2>
      </div>
      <div class="toolbar">
        <div class="form-group" style="margin:0;min-width:300px">
          <label style="font-size:12px;color:var(--muted);margin-bottom:4px;display:block">Select Model</label>
          <select id="cluster-model-select" class="search-input" onchange="onClusterModelChange(this.value)" style="width:100%">
            ${modelOptions}
          </select>
        </div>
      </div>
      <div id="clusters-container" class="clusters-grid">
        <div class="loading">Loading clusters…</div>
      </div>
    </div>
  `)

  // Load clusters for selected model
  loadAndRenderClusters(S.selectedClusterModel)
}

async function onClusterModelChange(modelId) {
  S.selectedClusterModel = modelId
  S.currentModelId = modelId
  S.currentClusterData = []
  
  // Stop all drain polling when switching models
  stopAllDrainPolling()
  
  const container = document.getElementById('clusters-container')
  if (container) {
    container.innerHTML = '<div class="loading">Loading clusters…</div>'
  }
  await loadAndRenderClusters(modelId)
}

/**
 * Fetch clusters for the current model and render them
 * @param {string} modelId - The model ID to fetch clusters for
 */
async function loadAndRenderClusters(modelId) {
  const container = document.getElementById('clusters-container')
  if (!container) return

  try {
    // Fetch clusters using the model-specific endpoint
    const response = await api.get(`/admin/models/${encodeURIComponent(modelId)}/clusters`)
    S.currentClusterData = response.clusters || response.data || []
    S.clusters = S.currentClusterData

    renderClusterCardsUI(container)
  } catch (e) {
    container.innerHTML = `
      <div class="error-state">
        <p>Failed to load clusters: ${esc(e.message)}</p>
      </div>
    `
  }
}

/**
 * Render cluster cards in the provided container
 * @param {HTMLElement} container - The container element to render into
 */
function renderClusterCardsUI(container) {
  if (!S.currentClusterData || S.currentClusterData.length === 0) {
    container.innerHTML = `
      <div class="empty-state">
        <div class="empty-icon">🏠</div>
        <p>No clusters configured for this model.</p>
      </div>
    `
    return
  }

  const cardsHtml = S.currentClusterData.map(cluster => `
    <div class="cluster-card ${cluster.is_local ? 'local' : ''}" data-cluster-id="${esc(cluster.cluster_id || cluster.id)}">
      <div class="loading-indicator"></div>
      <div class="cluster-header">
        <div class="cluster-title">
          <span class="cluster-icon">${cluster.is_local ? '🏠' : '🌐'}</span>
          <span class="cluster-name">${esc(cluster.cluster_id || cluster.id)}</span>
          ${cluster.is_local ? '<span class="badge badge-blue">Local</span>' : ''}
        </div>
        <div class="cluster-status">
          <span class="status-indicator ${cluster.is_enabled ? 'status-active' : 'status-inactive'}"></span>
          <span class="status-text">${cluster.is_enabled ? 'Active' : 'Disabled'}</span>
        </div>
      </div>
      <div class="cluster-stats">
        <div class="stat-row">
          <div class="stat">
            <span class="stat-label">Replicas</span>
            <span class="stat-value">${formatNumber(cluster.number_of_replicas || cluster.replicas)}</span>
          </div>
          <div class="stat">
            <span class="stat-label">Per-pod Capacity</span>
            <span class="stat-value">${formatNumber(cluster.seed_capacity_tokens_per_pod || cluster.capacity_per_pod)}</span>
          </div>
        </div>
        <div class="stat-row">
          <div class="stat">
            <span class="stat-label">Adaptive Capacity</span>
            <span class="stat-value adaptive-capacity">${formatNumber(cluster.inferred_capacity)}</span>
          </div>
          <div class="stat">
            <span class="stat-label">In-flight Tokens</span>
            <span class="stat-value inflight-tokens">${formatNumber(cluster.inflight_tokens || 0)}</span>
          </div>
        </div>
        <div class="stat-row">
          <div class="stat">
            <span class="stat-label">EWMA Latency</span>
            <span class="stat-value ewma-latency">${cluster.ewma_latency ? cluster.ewma_latency.toFixed(2) + 's' : '—'}</span>
          </div>
          <div class="stat">
            <span class="stat-label">Load</span>
            <span class="stat-value load-ratio ${getStatusColor(cluster.load_ratio || (cluster.current_load || 0) / 100)}">${formatPercent(cluster.load_ratio || (cluster.current_load || 0) / 100)}</span>
          </div>
        </div>
        ${cluster.priority !== undefined ? `
        <div class="stat-row">
          <div class="stat">
            <span class="stat-label">Priority</span>
            <span class="stat-value">${cluster.priority}</span>
          </div>
          <div class="stat">
            <span class="stat-label">Auto-adapt</span>
            <span class="stat-value">${cluster.auto_adapt_capacity ? 'Yes' : 'No'}</span>
          </div>
        </div>
        ` : ''}
      </div>
      <div class="cluster-actions">
        <button class="btn btn-primary" onclick="openEditClusterModal('${esc(cluster.cluster_id || cluster.id)}')">Edit</button>
        ${cluster.is_enabled 
          ? `<button class="btn btn-warning" onclick="disableCluster('${esc(cluster.cluster_id || cluster.id)}')">Disable</button>`
          : `<button class="btn btn-success" onclick="enableCluster('${esc(cluster.cluster_id || cluster.id)}')">Enable</button>`}
      </div>
    </div>
  `).join('')

  container.innerHTML = cardsHtml
}

/**
 * Enable a cluster via API
 * POST /admin/models/${currentModelId}/clusters/${clusterId}/enable
 * @param {string} clusterId - The ID of the cluster to enable
 */
async function enableCluster(clusterId) {
  if (!S.currentModelId) {
    toast('No model selected', 'error')
    return
  }

  try {
    // Stop any drain polling for this cluster (in case it was draining)
    stopDrainPolling(S.currentModelId, clusterId)
    
    await api.post(`/admin/models/${encodeURIComponent(S.currentModelId)}/clusters/${encodeURIComponent(clusterId)}/enable`)
    toast(`Cluster "${clusterId}" enabled`, 'success')

    // Update local state immediately for responsive UI
    const clusterIndex = S.currentClusterData.findIndex(c => (c.cluster_id || c.id) === clusterId)
    if (clusterIndex !== -1) {
      S.currentClusterData[clusterIndex].is_enabled = true
      S.clusters[clusterIndex].is_enabled = true
    }

    // Re-render immediately with local state (optimistic update)
    const container = document.getElementById('clusters-container')
    if (container) {
      renderClusterCardsUI(container)
    }

    // Then refresh from API in background to sync with backend
    loadAndRenderClusters(S.currentModelId)
  } catch (e) {
    toast(e.message, 'error')
  }
}

/**
 * Disable a cluster via API (with confirmation and drain polling)
 * POST /admin/models/${currentModelId}/clusters/${clusterId}/disable
 * @param {string} clusterId - The ID of the cluster to disable
 */
async function disableCluster(clusterId) {
  if (!S.currentModelId) {
    toast('No model selected', 'error')
    return
  }

  // Confirmation dialog
  if (!confirm(`Are you sure you want to disable cluster "${clusterId}"? This will drain active requests first.`)) {
    return
  }

  try {
    // Immediately show DRAINING state in UI
    updateClusterDrainState(S.currentModelId, clusterId, 'DRAINING')
    
    // Start polling for drain progress
    startDrainPolling(S.currentModelId, clusterId)
    
    // Call API to start drain
    await api.post(`/admin/models/${encodeURIComponent(S.currentModelId)}/clusters/${encodeURIComponent(clusterId)}/disable`)
    
    // Show toast that drain has started
    toast(`Cluster "${clusterId}" is draining...`, 'info')
    
    // Note: We don't update local state yet - the polling will handle that
    // when the drain completes or we refresh the data
    
  } catch (e) {
    // On error, stop polling and show error
    stopDrainPolling(S.currentModelId, clusterId)
    toast(e.message, 'error')
    
    // Refresh to get actual state
    await loadAndRenderClusters(S.currentModelId)
  }
}

/**
 * Open the cluster edit modal with current cluster data
 * Uses the modal structure specified in requirements with real-time computed values
 * @param {string} clusterId - The ID of the cluster to edit
 */
function openEditClusterModal(clusterId) {
  if (!S.currentModelId) {
    toast('No model selected', 'error')
    return
  }

  const cluster = S.currentClusterData.find(c => (c.cluster_id || c.id) === clusterId)
  if (!cluster) {
    toast('Cluster not found', 'error')
    return
  }

  // Store the editing cluster ID
  S.editingClusterId = clusterId

  // Calculate initial computed values
  const replicas = cluster.number_of_replicas || cluster.replicas || 1
  const capacityPerPod = cluster.seed_capacity_tokens_per_pod || cluster.capacity_per_pod || 10000
  // Use inferred_capacity from cluster data if available, otherwise calculate from input values
  const totalCapacity = cluster.inferred_capacity !== undefined ? cluster.inferred_capacity : (replicas * capacityPerPod)
  const currentLoad = cluster.current_load !== undefined ? cluster.current_load : 0

  // Build modal content with computed values section
  const modalContent = `
    <form id="edit-cluster-form">
      <div class="form-group">
        <label>Cluster ID</label>
        <input type="text" value="${esc(clusterId)}" disabled />
        <div class="form-hint">Cluster ID cannot be changed</div>
      </div>
      <div class="form-group">
        <label>Number of Replicas</label>
        <input type="number" id="edit-replicas" min="1" max="100" value="${replicas}" required>
        <span class="form-hint">Current pod count in the cluster</span>
      </div>
      <div class="form-group">
        <label>Per-pod Capacity (tokens)</label>
        <input type="number" id="edit-capacity" min="1000" step="1000" value="${capacityPerPod}" required>
        <span class="form-hint">Estimated token capacity per pod</span>
      </div>
      <div class="form-group form-group-inline">
        <label style="display:flex;align-items:center;gap:8px;cursor:pointer;">
          <input type="checkbox" id="edit-auto-adapt" ${cluster.auto_adapt_capacity ? 'checked' : ''}>
          <span>Auto-adapt Capacity</span>
        </label>
      </div>
      <div class="form-group">
        <label>Priority Rank</label>
        <select id="edit-priority">
          <option value="0" ${(cluster.priority || 0) === 0 ? 'selected' : ''}>0 - Highest</option>
          <option value="1" ${cluster.priority === 1 ? 'selected' : ''}>1</option>
          <option value="2" ${cluster.priority === 2 ? 'selected' : ''}>2</option>
          <option value="3" ${cluster.priority === 3 ? 'selected' : ''}>3 - Lowest</option>
        </select>
      </div>
      <div class="form-group">
        <label class="checkbox-item" style="display:flex;align-items:center;gap:8px;cursor:pointer;">
          <input type="checkbox" id="edit-is-local" ${cluster.is_local ? 'checked' : ''}>
          <span>Local Cluster</span>
        </label>
      </div>
      <div class="computed-values" style="background:#f8fafc;border:1px solid var(--border);border-radius:var(--radius);padding:12px;margin-top:16px;">
        <h4 style="font-size:12px;font-weight:600;margin-bottom:8px;color:var(--muted);">Computed Values</h4>
        <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
          <span>Total Capacity:</span>
          <strong id="computed-capacity">${formatNumber(totalCapacity)}</strong> tokens
        </div>
        <div style="display:flex;justify-content:space-between;">
          <span>Current Load:</span>
          <strong id="computed-load">${currentLoad}%</strong>
        </div>
      </div>
    </form>
  `

  // Set up the modal
  document.getElementById('modal-title').textContent = `Edit Cluster: ${esc(cluster.name || clusterId)}`
  document.getElementById('modal-body').innerHTML = modalContent
  document.getElementById('modal-overlay').classList.remove('hidden')

  // Update submit button
  const submitBtn = document.getElementById('modal-submit')
  submitBtn.textContent = 'Save Changes'
  submitBtn.style.display = ''
  submitBtn.onclick = async () => {
    submitBtn.disabled = true
    try {
      await saveClusterChanges()
      closeModal()
    } catch (e) {
      toast(e.message, 'error')
    } finally {
      submitBtn.disabled = false
    }
  }

  // Ensure cancel button is visible
  const cancelBtn = submitBtn.previousElementSibling
  if (cancelBtn) cancelBtn.style.display = ''

  // Set up real-time computed value updates
  setupComputedValueListeners(cluster.current_load)
}

/**
 * Set up event listeners to update computed values in real-time
 * @param {number} currentLoad - The current load percentage to display
 */
function setupComputedValueListeners(currentLoad) {
  const replicasInput = document.getElementById('edit-replicas')
  const capacityInput = document.getElementById('edit-capacity')
  const computedCapacityEl = document.getElementById('computed-capacity')

  if (replicasInput && capacityInput && computedCapacityEl) {
    const updateComputed = () => {
      const replicas = parseInt(replicasInput.value, 10) || 1
      const capacity = parseInt(capacityInput.value, 10) || 0
      computedCapacityEl.textContent = formatNumber(replicas * capacity)
    }

    replicasInput.addEventListener('input', updateComputed)
    capacityInput.addEventListener('input', updateComputed)
  }
}

/**
 * Close the cluster edit modal
 */
function closeClusterModal() {
  closeModal()
  S.editingClusterId = null
}

/**
 * Save cluster changes to the API
 * PATCH /admin/models/${currentModelId}/clusters/${clusterId}
 */
async function saveClusterChanges() {
  if (!S.editingClusterId || !S.currentModelId) {
    throw new Error('No cluster selected for editing')
  }

  const number_of_replicas = parseInt(document.getElementById('edit-replicas').value, 10)
  const seed_capacity_tokens_per_pod = parseInt(document.getElementById('edit-capacity').value, 10)
  const auto_adapt_capacity = document.getElementById('edit-auto-adapt').checked
  const priority = parseInt(document.getElementById('edit-priority').value, 10)
  const is_local = document.getElementById('edit-is-local').checked

  // Validation
  if (!number_of_replicas || number_of_replicas < 1 || number_of_replicas > 100) {
    throw new Error('Replicas must be between 1 and 100')
  }
  if (!seed_capacity_tokens_per_pod || seed_capacity_tokens_per_pod < 1000) {
    throw new Error('Per-pod capacity must be at least 1000 tokens')
  }

  try {
    const response = await api.patch(
      `/admin/models/${encodeURIComponent(S.currentModelId)}/clusters/${encodeURIComponent(S.editingClusterId)}`,
      {
        number_of_replicas,
        seed_capacity_tokens_per_pod,
        auto_adapt_capacity,
        priority,
        is_local
      }
    )

    // Update local state
    const clusterIndex = S.currentClusterData.findIndex(c => (c.cluster_id || c.id) === S.editingClusterId)
    if (clusterIndex !== -1) {
      S.currentClusterData[clusterIndex] = { ...S.currentClusterData[clusterIndex], ...response }
    }

    // Re-render cluster cards
    const container = document.getElementById('clusters-container')
    if (container) {
      renderClusterCardsUI(container)
    }

    toast('Cluster updated successfully', 'success')
  } catch (error) {
    throw new Error(`Failed to update cluster: ${error.message}`)
  }
}

/**
 * Legacy editCluster function - redirects to openEditClusterModal
 * Kept for backward compatibility with existing onclick handlers
 * @param {string} clusterId - The ID of the cluster to edit
 */
function editCluster(clusterId) {
  openEditClusterModal(clusterId)
}

// ─────────────────────────────────────────────────────────────────────────────
// Init
// ─────────────────────────────────────────────────────────────────────────────

async function init() {
  if (!S.token) { renderLogin(); return }
  try {
    S.me = await api.get('/auth/me')
    renderShell()
    // Check URL hash first for shareable/bookmarkable URLs
    const hashView = parseHash()
    // If hash exists and is valid, use it; otherwise fall back to localStorage, then default
    let targetView = hashView
    if (!targetView) {
      targetView = localStorage.getItem('llm_current_view') || 'dashboard'
    }
    await navigate(targetView)
  } catch {
    // Token invalid or expired
    localStorage.removeItem('llm_token')
    S.token = null
    renderLogin()
  }
}

// Cleanup intervals when page is unloaded
window.addEventListener('beforeunload', () => {
  stopClusterRefresh()
  stopAllDrainPolling()
})

init()
