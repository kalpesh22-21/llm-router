'use strict'

// ─────────────────────────────────────────────────────────────────────────────
// State
// ─────────────────────────────────────────────────────────────────────────────

const S = {
  token: localStorage.getItem('llm_token'),
  me: null,
  users: [],
  buckets: [],
  userQuery: '',
  bucketQuery: '',
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
  get:    (url)       => api._call('GET',    url),
  post:   (url, body) => api._call('POST',   url, body),
  patch:  (url, body) => api._call('PATCH',  url, body),
  delete: (url)       => api._call('DELETE', url),
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
        <a class="nav-item" data-nav="dashboard" href="#"
           onclick="navigate('dashboard');return false">
          <span>👤</span> My Profile
        </a>
        ${me.is_admin ? `
        <div class="nav-section">Admin</div>
        <a class="nav-item" data-nav="users" href="#"
           onclick="navigate('users');return false">
          <span>👥</span> Users
        </a>
        <a class="nav-item" data-nav="buckets" href="#"
           onclick="navigate('buckets');return false">
          <span>🪣</span> Buckets
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

async function navigate(view) {
  setActiveNav(view)
  const views = { dashboard: showDashboard, users: showUsers, buckets: showBuckets }
  await views[view]?.()
}

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
  try { S.me = await api.get('/auth/me') } catch {}
  const me = S.me
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
      </div>
      <div class="section">
        <h3>Bearer Token</h3>
        <div class="token-box">
          <code class="token-text">${esc(S.token)}</code>
          <button class="btn btn-secondary btn-sm" onclick="copyToken()">Copy</button>
        </div>
      </div>
    </div>
  `)
}

function copyToken() {
  navigator.clipboard.writeText(S.token)
    .then(() => toast('Token copied to clipboard', 'success'))
}

// ─────────────────────────────────────────────────────────────────────────────
// Users
// ─────────────────────────────────────────────────────────────────────────────

async function showUsers() {
  S.userQuery = ''
  setContent('<div class="loading">Loading…</div>')
  try {
    const [users, buckets] = await Promise.all([
      api.get('/admin/users'),
      api.get('/admin/buckets'),
    ])
    S.users = users
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

  setContent(`
    <div class="page">
      <div class="page-header">
        <h2>Users <span class="count">${S.users.length}</span></h2>
        <button class="btn btn-primary" onclick="openCreateUser()">+ New User</button>
      </div>
      <div class="toolbar">
        <div class="search-wrap">
          ${_searchIcon}
          <input class="search-input" type="search" id="user-search"
                 placeholder="Search by username or email…"
                 value="${esc(S.userQuery)}"
                 oninput="filterUsers(this.value)" />
        </div>
        <span class="search-count" id="user-search-count"></span>
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
            ${rows || '<tr class="no-results"><td colspan="7">No users yet.</td></tr>'}
          </tbody>
        </table>
      </div>
    </div>
  `)
  applyUserFilter(S.userQuery)
}

function filterUsers(val) {
  S.userQuery = val
  applyUserFilter(val)
}

function applyUserFilter(q) {
  const query = q.toLowerCase().trim()
  let visible = 0
  document.querySelectorAll('#user-tbody tr[data-username]').forEach(tr => {
    const match = !query ||
      tr.dataset.username.toLowerCase().includes(query) ||
      tr.dataset.email.toLowerCase().includes(query)
    tr.style.display = match ? '' : 'none'
    if (match) visible++
  })
  const countEl = document.getElementById('user-search-count')
  if (countEl && query) countEl.textContent = `${visible} of ${S.users.length} shown`
  else if (countEl) countEl.textContent = ''
}

async function toggleActive(userId, currentlyActive) {
  try {
    await api.patch(`/admin/users/${userId}/active`, { is_active: !currentlyActive })
    const u = S.users.find(u => u.id === userId)
    if (u) u.is_active = !currentlyActive
    renderUsersTable()
    toast(`User ${currentlyActive ? 'deactivated' : 'activated'}`, 'success')
  } catch (e) { toast(e.message, 'error') }
}

async function toggleAdmin(userId, currentlyAdmin) {
  try {
    await api.patch(`/admin/users/${userId}/admin`, { is_admin: !currentlyAdmin })
    const u = S.users.find(u => u.id === userId)
    if (u) u.is_admin = !currentlyAdmin
    renderUsersTable()
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
    const u = S.users.find(u => u.id === userId)
    if (u) u.max_concurrent_requests = val
    renderUsersTable()
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

  const checkboxes = S.buckets.length
    ? S.buckets.map(b => `
      <label class="checkbox-item">
        <input type="checkbox" value="${b.id}"
               ${currentBucketIds.includes(b.id) ? 'checked' : ''} />
        <span>${esc(b.name)}${b.is_default
          ? ' <span class="badge badge-blue" style="font-size:10px;padding:1px 5px">default</span>'
          : ''}</span>
      </label>`).join('')
    : '<p style="color:var(--muted);font-size:13px">No buckets created yet.</p>'

  openModal(`Bucket Access — ${esc(user.username)}`, `
    <p style="color:var(--muted);font-size:12px;margin-bottom:12px">
      Select which buckets this user can access.
    </p>
    <div class="checkbox-group">${checkboxes}</div>
  `, async () => {
    const checked = [...document.querySelectorAll('#modal-body input[type=checkbox]:checked')]
      .map(el => parseInt(el.value))
    const toGrant  = checked.filter(id => !currentBucketIds.includes(id))
    const toRevoke = currentBucketIds.filter(id => !checked.includes(id))
    await Promise.all([
      ...toGrant.map(bid  => api.post(`/admin/users/${userId}/buckets/${bid}`)),
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
    const email    = document.getElementById('m-email').value.trim() || undefined
    if (!username) throw new Error('Username is required')
    const newUser = await api.post('/admin/users', { username, email })
    S.users.push(newUser)
    renderUsersTable()
    toast(`User "${newUser.username}" created`, 'success')
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
    renderBucketsTable()
  } catch (e) { toast(e.message, 'error') }
}

function renderBucketsTable() {
  const rows = S.buckets.map(b => `
    <tr data-name="${esc(b.name)}" data-desc="${esc(b.description || '')}">
      <td><span class="cell-text-sm">${b.id}</span></td>
      <td><span class="cell-text" title="${esc(b.name)}">${esc(b.name)}</span></td>
      <td><span class="cell-text" title="${esc(b.description || '—')}">${esc(b.description || '—')}</span></td>
      <td>${b.is_default
        ? '<span class="badge badge-blue">Default</span>'
        : '<span class="badge badge-gray">No</span>'}</td>
      <td style="max-width:220px">${b.models.map(m =>
        `<span class="badge badge-gray" style="margin:1px 2px">${esc(m.model_id)}</span>`
      ).join('') || '<span style="color:var(--muted)">—</span>'}</td>
      <td class="actions">
        ${!b.is_default
          ? `<button class="btn btn-sm btn-secondary"
                     onclick="setDefaultBucket(${b.id})">Set Default</button>`
          : ''}
      </td>
    </tr>`).join('')

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
              <th>Default</th><th>Models</th><th>Actions</th>
            </tr>
          </thead>
          <tbody id="bucket-tbody">
            ${rows || '<tr class="no-results"><td colspan="6">No buckets yet.</td></tr>'}
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

async function openCreateBucket() {
  let models = []
  try { models = (await api.get('/admin/models')).data } catch {}

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
    const name        = document.getElementById('m-bname').value.trim()
    const description = document.getElementById('m-bdesc').value.trim() || undefined
    const model_ids   = [...document.querySelectorAll('input[name=bmodel]:checked')]
      .map(el => el.value)
    if (!name) throw new Error('Name is required')
    await api.post('/admin/buckets', { name, description, model_ids })
    toast(`Bucket "${name}" created`, 'success')
    await showBuckets()
  }, 'Create')
}

// ─────────────────────────────────────────────────────────────────────────────
// Init
// ─────────────────────────────────────────────────────────────────────────────

async function init() {
  if (!S.token) { renderLogin(); return }
  try {
    S.me = await api.get('/auth/me')
    renderShell()
    await navigate('dashboard')
  } catch {
    // Token invalid or expired
    localStorage.removeItem('llm_token')
    S.token = null
    renderLogin()
  }
}

init()
