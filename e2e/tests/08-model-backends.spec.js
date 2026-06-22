// @ts-check
/**
 * Spec 08 — Dynamic model backends (DB-backed, UI-managed).
 *
 * Proves the full lifecycle: UI-create → registered in registry → routable
 * via the proxy → disable removes routing → re-enable restores routing →
 * delete-guard fires when a bucket still references the backend.
 *
 * Stack requirements (verified against live docker-compose.e2e.yml stack):
 *   - DB_BACKENDS_ENABLED=true
 *   - mock-upstream reachable from the app container at http://mock-upstream:9000/v1
 *     (base_url must include /v1 so DiscoveryService probes GET {base_url}/models
 *     → GET /v1/models → 200, making the backend `available: true`)
 *
 * test-connection UI endpoint behaviour:
 *   - The admin UI test-connection endpoint appends /models to the given base_url
 *     (base_url is expected to already include the API-version path, e.g. /v1).
 *   - So http://mock-upstream:9000/v1 → probes http://mock-upstream:9000/v1/models → 200.
 *   - This matches the health probe (validate_api_key) and proxy (StaticBackend.forward)
 *     convention: all three use {base_url}/models and {base_url}/chat/completions.
 *   - For Test 4 (test-connection UI): use http://mock-upstream:9000/v1 so the probe
 *     hits /v1/models → 200.
 *
 * All tests are independent (each creates its own named resources via uid()).
 */

const { test, expect } = require('@playwright/test')
const {
  TEST_MODEL,
  uid,
  login,
  gotoView,
  settle,
  submitModalAndClose,
  expectRowVisible,
  expectRowGone,
  bucketRow,
  serviceRow,
} = require('./helpers')

// The base_url to use when creating a dynamic backend for routing.
// Must include /v1 so DiscoveryService's GET {base_url}/models probes
// GET /v1/models → 200 → backend.is_available = true.
const MOCK_BASE_URL = 'http://mock-upstream:9000/v1'

/** Locate a model-backend row by model_id in the #mb-tbody table. */
function backendRow(page, modelId) {
  return page.locator(`#mb-tbody tr[data-model-id="${modelId}"]`)
}

/**
 * Poll the model-backends table (re-fetching on each attempt) until the
 * specified model shows a green "registered" badge.
 *
 * Uses waitForSelector('#mb-tbody') instead of networkidle — the discovery
 * service keeps background probes running so networkidle can stall for seconds.
 */
async function waitForRegistered(page, modelId, timeout = 15_000) {
  await expect.poll(
    async () => {
      // Re-click the nav item to trigger a fresh showModelBackends() fetch.
      await page.click('.nav-item[data-nav="model-backends"]')
      // Wait for the table to render (not networkidle — discovery probes block that).
      await page.waitForSelector('#mb-tbody', { timeout: 5_000 }).catch(() => {})
      const badge = backendRow(page, modelId).locator('.badge-green', { hasText: 'registered' })
      return badge.count()
    },
    {
      message: `backend ${modelId} should show green "registered" badge`,
      timeout,
      intervals: [500, 1000, 1000, 2000],
    },
  ).toBeGreaterThan(0)
}

// ---------------------------------------------------------------------------
// Test 1 — UI-added backend is routable end-to-end (headline proof)
// ---------------------------------------------------------------------------

test('Test 1: UI-added backend is routable end-to-end', async ({ page, request }) => {
  const modelId = uid('e2e-db-model')
  const bucketName = uid('db-bucket')
  const svcName = uid('db-svc')
  const keyName = uid('db-key')

  await login(page)

  // ---- 1a. Create the model backend via the UI ----
  await gotoView(page, 'model-backends')
  await page.waitForSelector('#mb-tbody')

  await page.click('button:has-text("+ Add Backend")')
  await expect(page.locator('#modal-overlay')).not.toHaveClass(/hidden/)

  await page.fill('#mb-model-id', modelId)
  await page.fill('#mb-base-url', MOCK_BASE_URL)
  // Leave api_key blank — falls back to global API_KEY which the mock accepts.

  await submitModalAndClose(page)

  // ---- 1b. Assert row is visible and status is Enabled ----
  const mbRow = backendRow(page, modelId)
  await expectRowVisible(page, 'model-backends', mbRow)
  await expect(mbRow.locator('.badge', { hasText: 'Enabled' })).toBeVisible()

  // ---- 1c. Assert backend becomes registered (inline reconcile on create) ----
  // The create endpoint reconciles inline on the writer pod, so the backend
  // starts _available = True immediately. Poll until the green badge appears.
  await waitForRegistered(page, modelId)

  // ---- 2. Create a bucket containing this new model ----
  await gotoView(page, 'buckets')
  await page.waitForSelector('#bucket-tbody')
  await page.click('button:has-text("+ New Bucket")')
  await page.fill('#m-bname', bucketName)

  // The bucket create modal fetches /admin/models live — the new model should
  // be present since inline reconcile has already registered it.
  // Retry: re-open the modal if the checkbox isn't there yet.
  await expect.poll(
    async () => {
      const cb = page.locator(`input[name=bmodel][value="${modelId}"]`)
      const count = await cb.count()
      if (count === 0) {
        const cancelBtn = page.locator('#modal-overlay button:has-text("Cancel")')
        if (await cancelBtn.isVisible()) await cancelBtn.click()
        await settle(page)
        await page.click('button:has-text("+ New Bucket")')
        await page.fill('#m-bname', bucketName)
      }
      return count
    },
    { message: `checkbox for ${modelId} should appear in create-bucket modal`, timeout: 15_000 },
  ).toBeGreaterThan(0)

  await page.check(`input[name=bmodel][value="${modelId}"]`)
  await submitModalAndClose(page)
  await expectRowVisible(page, 'buckets', bucketRow(page, bucketName))

  // ---- 3. Create a service account granting that bucket ----
  await gotoView(page, 'service-accounts')
  await page.waitForSelector('#sa-tbody')
  await page.click('button:has-text("+ Create Service Account")')
  await page.fill('#sa-name', svcName)
  await page.locator('label.checkbox-item', { hasText: bucketName })
    .locator('input[name=sa-bucket]').check()
  await submitModalAndClose(page)

  const saRow = serviceRow(page, svcName)
  await expectRowVisible(page, 'service-accounts', saRow)
  await expect(saRow.locator('.badge', { hasText: 'Active' })).toBeVisible()

  // ---- 4. Issue an API key restricted to the new model ----
  await saRow.locator('button:has-text("Manage Keys")').click()
  await expect(page.locator('#modal-overlay')).not.toHaveClass(/hidden/)
  await page.click('button:has-text("+ Issue Key")')

  await page.fill('#ik-name', keyName)

  const ensureChecked = async (locator) => {
    await expect(async () => {
      if (!(await locator.isChecked())) await locator.click()
      await expect(locator).toBeChecked({ timeout: 500 })
    }).toPass({ timeout: 5_000 })
  }
  await ensureChecked(page.locator('input[name=ik-scope][value="chat"]'))
  await ensureChecked(page.locator(`input[name=ik-model][value="${modelId}"]`))

  await page.click('#modal-submit') // "Issue Key"

  // Capture the one-time-reveal key.
  const revealed = page.locator('#ik-revealed-key')
  await expect(revealed).toBeVisible()
  const apiKey = await revealed.inputValue()
  expect(apiKey).toMatch(/^llmr_sk_/)

  // Close the reveal modal.
  await page.click('#modal-submit') // "Done"

  // ---- 5. HEADLINE PROOF: key routes through the proxy to the mock upstream ----
  const chatResp = await request.post('/v1/chat/completions', {
    headers: { Authorization: `Bearer ${apiKey}` },
    data: { model: modelId, messages: [{ role: 'user', content: 'ping' }] },
  })
  expect(chatResp.status(), 'issued key with new DB backend should return 200').toBe(200)
  const chatBody = await chatResp.json()
  expect(JSON.stringify(chatBody)).toContain('pong from mock upstream')
})

// ---------------------------------------------------------------------------
// Test 2 — disable removes routing; re-enable restores it
// ---------------------------------------------------------------------------

test('Test 2: disable removes routing; re-enable restores it', async ({ page, request }) => {
  const modelId = uid('e2e-toggle')
  const bucketName = uid('tog-bucket')
  const svcName = uid('tog-svc')
  const keyName = uid('tog-key')

  await login(page)

  // ---- Create the backend ----
  await gotoView(page, 'model-backends')
  await page.waitForSelector('#mb-tbody')
  await page.click('button:has-text("+ Add Backend")')
  await page.fill('#mb-model-id', modelId)
  await page.fill('#mb-base-url', MOCK_BASE_URL)
  await submitModalAndClose(page)

  await expectRowVisible(page, 'model-backends', backendRow(page, modelId))
  await waitForRegistered(page, modelId)

  // ---- Create bucket + SA + key ----
  await gotoView(page, 'buckets')
  await page.waitForSelector('#bucket-tbody')
  await page.click('button:has-text("+ New Bucket")')
  await page.fill('#m-bname', bucketName)

  await expect.poll(
    async () => {
      const cb = page.locator(`input[name=bmodel][value="${modelId}"]`)
      const count = await cb.count()
      if (count === 0) {
        const cancelBtn = page.locator('#modal-overlay button:has-text("Cancel")')
        if (await cancelBtn.isVisible()) await cancelBtn.click()
        await settle(page)
        await page.click('button:has-text("+ New Bucket")')
        await page.fill('#m-bname', bucketName)
      }
      return count
    },
    { timeout: 15_000 },
  ).toBeGreaterThan(0)

  await page.check(`input[name=bmodel][value="${modelId}"]`)
  await submitModalAndClose(page)
  await expectRowVisible(page, 'buckets', bucketRow(page, bucketName))

  await gotoView(page, 'service-accounts')
  await page.waitForSelector('#sa-tbody')
  await page.click('button:has-text("+ Create Service Account")')
  await page.fill('#sa-name', svcName)
  await page.locator('label.checkbox-item', { hasText: bucketName })
    .locator('input[name=sa-bucket]').check()
  await submitModalAndClose(page)
  await expectRowVisible(page, 'service-accounts', serviceRow(page, svcName))

  const saRow = serviceRow(page, svcName)
  await saRow.locator('button:has-text("Manage Keys")').click()
  await expect(page.locator('#modal-overlay')).not.toHaveClass(/hidden/)
  await page.click('button:has-text("+ Issue Key")')

  await page.fill('#ik-name', keyName)

  const ensureChecked = async (locator) => {
    await expect(async () => {
      if (!(await locator.isChecked())) await locator.click()
      await expect(locator).toBeChecked({ timeout: 500 })
    }).toPass({ timeout: 5_000 })
  }
  await ensureChecked(page.locator('input[name=ik-scope][value="chat"]'))
  await ensureChecked(page.locator(`input[name=ik-model][value="${modelId}"]`))
  await page.click('#modal-submit') // "Issue Key"

  const revealed = page.locator('#ik-revealed-key')
  await expect(revealed).toBeVisible()
  const apiKey = await revealed.inputValue()
  expect(apiKey).toMatch(/^llmr_sk_/)

  // Click "Done" — this closes the reveal and re-opens the key-list modal.
  // Wait for the key-list modal (submit button is hidden; cancel becomes "Close").
  await page.click('#modal-submit') // "Done"
  // The key-list modal is now open; close it via the Cancel/Close button.
  const closeKeyListBtn = page.locator('#modal-overlay button:has-text("Close")')
  await expect(closeKeyListBtn).toBeVisible({ timeout: 5_000 })
  await closeKeyListBtn.click()
  await expect(page.locator('#modal-overlay')).toHaveClass(/hidden/, { timeout: 10_000 })

  // Confirm routing is live before disabling.
  await expect.poll(
    async () => {
      const r = await request.post('/v1/chat/completions', {
        headers: { Authorization: `Bearer ${apiKey}` },
        data: { model: modelId, messages: [{ role: 'user', content: 'ping' }] },
      })
      return r.status()
    },
    { message: 'backend should be routable before disable', timeout: 15_000 },
  ).toBe(200)

  // ---- Disable via the UI ----
  await gotoView(page, 'model-backends')
  await page.waitForSelector('#mb-tbody')
  await expectRowVisible(page, 'model-backends', backendRow(page, modelId))

  await backendRow(page, modelId).locator('button:has-text("Disable")').click()

  // Badge should flip to Disabled in the UI.
  await expect(backendRow(page, modelId).locator('.badge', { hasText: 'Disabled' })).toBeVisible({
    timeout: 10_000,
  })

  // ---- Assert proxy stops serving the model ----
  // Disabling unregisters the backend inline on the writer pod → 404 immediately.
  await expect.poll(
    async () => {
      const r = await request.post('/v1/chat/completions', {
        headers: { Authorization: `Bearer ${apiKey}` },
        data: { model: modelId, messages: [{ role: 'user', content: 'ping' }] },
      })
      return r.status()
    },
    {
      message: 'disabled backend should no longer route (expect non-200)',
      timeout: 15_000,
      intervals: [500, 500, 1000, 1000],
    },
  ).not.toBe(200)

  // ---- Re-enable via the UI ----
  await gotoView(page, 'model-backends')
  await page.waitForSelector('#mb-tbody')
  await expectRowVisible(page, 'model-backends', backendRow(page, modelId))
  await backendRow(page, modelId).locator('button:has-text("Enable")').click()

  // Badge should flip back to Enabled.
  await expect(backendRow(page, modelId).locator('.badge', { hasText: 'Enabled' })).toBeVisible({
    timeout: 10_000,
  })

  // ---- Assert proxy resumes serving the model ----
  // After re-enable, the backend is immediately re-registered (inline reconcile).
  // However, the SA key identity is cached in Redis for up to 60s; the cache
  // payload may exclude the model from accessible_models if it was cached during
  // the disabled window.  Poll with a generous 75s timeout to cover that TTL.
  await expect.poll(
    async () => {
      const r = await request.post('/v1/chat/completions', {
        headers: { Authorization: `Bearer ${apiKey}` },
        data: { model: modelId, messages: [{ role: 'user', content: 'ping' }] },
      })
      return r.status()
    },
    {
      message: 're-enabled backend should route again (expect 200)',
      timeout: 75_000,
      intervals: [1000, 2000, 2000, 2000],
    },
  ).toBe(200)
})

// ---------------------------------------------------------------------------
// Test 3 — delete-guard: 409 when backend is referenced by a bucket
// ---------------------------------------------------------------------------

test('Test 3: delete-guard fires when backend is referenced by a bucket', async ({ page }) => {
  const modelId = uid('e2e-guard')
  const bucketName = uid('guard-bucket')

  await login(page)

  // ---- Create the backend ----
  await gotoView(page, 'model-backends')
  await page.waitForSelector('#mb-tbody')
  await page.click('button:has-text("+ Add Backend")')
  await page.fill('#mb-model-id', modelId)
  await page.fill('#mb-base-url', MOCK_BASE_URL)
  await submitModalAndClose(page)
  await expectRowVisible(page, 'model-backends', backendRow(page, modelId))

  // Wait for registration so the model appears in /admin/models for the bucket modal.
  await waitForRegistered(page, modelId)

  // ---- Create a bucket referencing this model ----
  await gotoView(page, 'buckets')
  await page.waitForSelector('#bucket-tbody')
  await page.click('button:has-text("+ New Bucket")')
  await page.fill('#m-bname', bucketName)

  await expect.poll(
    async () => {
      const cb = page.locator(`input[name=bmodel][value="${modelId}"]`)
      const count = await cb.count()
      if (count === 0) {
        const cancelBtn = page.locator('#modal-overlay button:has-text("Cancel")')
        if (await cancelBtn.isVisible()) await cancelBtn.click()
        await settle(page)
        await page.click('button:has-text("+ New Bucket")')
        await page.fill('#m-bname', bucketName)
      }
      return count
    },
    { timeout: 15_000 },
  ).toBeGreaterThan(0)

  await page.check(`input[name=bmodel][value="${modelId}"]`)
  await submitModalAndClose(page)
  await expectRowVisible(page, 'buckets', bucketRow(page, bucketName))

  // ---- Attempt to delete the backend while it's still in the bucket ----
  await gotoView(page, 'model-backends')
  await page.waitForSelector('#mb-tbody')
  await expectRowVisible(page, 'model-backends', backendRow(page, modelId))

  // Accept the native confirm() dialog.
  page.once('dialog', (d) => d.accept())
  await backendRow(page, modelId).locator('button:has-text("Delete")').click()

  // The service returns 409; the UI surfaces it as a toast "Cannot delete: ..."
  const errorToast = page.locator('#toasts .toast', { hasText: 'Cannot delete' })
  await expect(errorToast).toBeVisible({ timeout: 10_000 })

  // The row must still be present — delete was rejected.
  await expect(backendRow(page, modelId)).toBeVisible()

  // ---- Remove the model from the bucket: add test-model so bucket isn't empty ----
  // The edit-bucket form requires at least one model, so we include test-model
  // alongside removing the guard model.
  await gotoView(page, 'buckets')
  await page.waitForSelector('#bucket-tbody')
  await expectRowVisible(page, 'buckets', bucketRow(page, bucketName))

  await bucketRow(page, bucketName).locator('button:has-text("Edit")').click()
  await expect(page.locator('#modal-overlay')).not.toHaveClass(/hidden/)

  // Uncheck the guard model and check test-model to keep the bucket non-empty.
  // Use the retry pattern from 02-service-accounts.spec.js to handle layout-shift
  // clicks that don't register on label-wrapped checkboxes.
  const guardCb = page.locator(`input[name=bmodel][value="${modelId}"]`)
  const testModelCb = page.locator(`input[name=bmodel][value="${TEST_MODEL}"]`)

  const ensureUnchecked = async (locator) => {
    await expect(async () => {
      if (await locator.isChecked()) await locator.click()
      await expect(locator).not.toBeChecked({ timeout: 500 })
    }).toPass({ timeout: 5_000 })
  }

  const ensureChecked = async (locator) => {
    await expect(async () => {
      if (!(await locator.isChecked())) await locator.click()
      await expect(locator).toBeChecked({ timeout: 500 })
    }).toPass({ timeout: 5_000 })
  }

  // Uncheck the guard model (may already be unchecked due to read-after-write race).
  await ensureUnchecked(guardCb)

  // Check test-model to satisfy the "at least one model required" constraint.
  await ensureChecked(testModelCb)

  await submitModalAndClose(page)

  // ---- Now delete should succeed ----
  await gotoView(page, 'model-backends')
  await page.waitForSelector('#mb-tbody')
  await expectRowVisible(page, 'model-backends', backendRow(page, modelId))

  page.once('dialog', (d) => d.accept())
  await backendRow(page, modelId).locator('button:has-text("Delete")').click()

  await expectRowGone(page, 'model-backends', backendRow(page, modelId))
})

// ---------------------------------------------------------------------------
// Test 4 — test-connection in the create modal shows "Reachable (HTTP 200)"
// ---------------------------------------------------------------------------

test('Test 4: test-connection in create modal shows reachable indicator', async ({ page }) => {
  // The test-connection endpoint appends /models to the given base_url.
  // base_url must include /v1, so the probe hits /v1/models → 200.
  const TEST_CONN_URL = 'http://mock-upstream:9000/v1'

  await login(page)
  await gotoView(page, 'model-backends')
  await page.waitForSelector('#mb-tbody')

  // Open the Add Backend modal.
  await page.click('button:has-text("+ Add Backend")')
  await expect(page.locator('#modal-overlay')).not.toHaveClass(/hidden/)

  // Fill only the base URL.
  await page.fill('#mb-base-url', TEST_CONN_URL)

  // Click "Test Connection" and wait for the result div.
  await page.click('button:has-text("Test Connection")')

  const resultEl = page.locator('#mb-test-result')
  // The UI renders "Reachable (HTTP 200)" on success.
  await expect(resultEl).toContainText('Reachable', { timeout: 10_000 })
  await expect(resultEl).toContainText('200')

  // Close without saving.
  const cancelBtn = page.locator('#modal-overlay button:has-text("Cancel")')
  await cancelBtn.click()
  await expect(page.locator('#modal-overlay')).toHaveClass(/hidden/)
})
