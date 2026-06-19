// @ts-check
const { test, expect } = require('@playwright/test')
const {
  TEST_MODEL, uid, login, gotoView, submitModalAndClose, expectRowVisible,
  bucketRow, serviceRow,
} = require('./helpers')

/**
 * Headline end-to-end flow requested by the user:
 *   create a service account in the UI  →  issue an API key in the UI  →
 *   prove the issued key actually works against /v1/chat/completions  →
 *   revoke it  →  prove the revoked key no longer works.
 *
 * A dedicated bucket containing `test-model` is created through the UI first so
 * the account has model access (a key only grants buckets ∩ key-model-subset).
 */
test.describe('Service accounts: create → issue key → key works → revoke', () => {
  test('full lifecycle with a working API key', async ({ page, request }) => {
    const bucketName = uid('sa-bucket')
    const svcName = uid('svc')
    const keyName = uid('key')

    await login(page)

    // ---- 1. Create a bucket with test-model (prerequisite for model access) ----
    await gotoView(page, 'buckets')
    await page.waitForSelector('#bucket-tbody')
    await page.click('button:has-text("+ New Bucket")')
    await page.fill('#m-bname', bucketName)
    await page.check(`input[name=bmodel][value="${TEST_MODEL}"]`)
    await submitModalAndClose(page)
    await expectRowVisible(page, 'buckets', bucketRow(page, bucketName))

    // ---- 2. Create the service account, granting it that bucket ----
    await gotoView(page, 'service-accounts')
    await page.waitForSelector('#sa-tbody')
    await page.click('button:has-text("+ Create Service Account")')
    await page.fill('#sa-name', svcName)
    await page.fill('#sa-team', 'platform-eng')
    // Bucket checkbox is labelled by the bucket name in the create modal.
    await page.locator('label.checkbox-item', { hasText: bucketName })
      .locator('input[name=sa-bucket]').check()
    await submitModalAndClose(page)

    const row = serviceRow(page, svcName)
    await expectRowVisible(page, 'service-accounts', row)
    await expect(row.locator('.badge', { hasText: 'Active' })).toBeVisible()

    // ---- 3. Issue an API key (chat scope, restricted to test-model) ----
    await row.locator('button:has-text("Manage Keys")').click()
    await expect(page.locator('#modal-overlay')).not.toHaveClass(/hidden/)
    await page.click('button:has-text("+ Issue Key")')

    await page.fill('#ik-name', keyName)
    // Custom label-wrapped checkboxes can drop a single click during a layout
    // shift; click until the checkbox reports checked.
    const ensureChecked = async (locator) => {
      await expect(async () => {
        if (!(await locator.isChecked())) await locator.click()
        await expect(locator).toBeChecked({ timeout: 500 })
      }).toPass({ timeout: 5_000 })
    }
    await ensureChecked(page.locator('input[name=ik-scope][value="chat"]'))
    await ensureChecked(page.locator(`input[name=ik-model][value="${TEST_MODEL}"]`))
    await page.click('#modal-submit') // "Issue Key"

    // One-time reveal: capture the full llmr_sk_ key from the readonly input.
    const revealed = page.locator('#ik-revealed-key')
    await expect(revealed).toBeVisible()
    const apiKey = await revealed.inputValue()
    expect(apiKey).toMatch(/^llmr_sk_/)

    // ---- 4. The created key WORKS against the proxy ----
    const ok = await request.post('/v1/chat/completions', {
      headers: { Authorization: `Bearer ${apiKey}` },
      data: { model: TEST_MODEL, messages: [{ role: 'user', content: 'ping' }] },
    })
    expect(ok.status(), 'issued key should be accepted by the proxy').toBe(200)
    const body = await ok.json()
    expect(JSON.stringify(body)).toContain('pong from mock upstream')

    // ---- 4b. Scope/model restriction is enforced (embeddings scope denied) ----
    const scopeDenied = await request.post('/v1/embeddings', {
      headers: { Authorization: `Bearer ${apiKey}` },
      data: { model: TEST_MODEL, input: 'hello' },
    })
    expect(scopeDenied.status(), 'chat-only key must be denied on embeddings').toBe(403)

    // Close the reveal ("Done") → returns to the key list.
    await page.click('#modal-submit') // "Done"
    await expect(page.locator('table.table')).toBeVisible() // key list modal

    // The issued key shows up in the list (by name) and is revocable.
    const keyRow = page.locator('#modal-body tr', { hasText: keyName })
    await expect(keyRow).toBeVisible()

    // ---- 5. Revoke the key (native confirm dialog) ----
    page.once('dialog', (d) => d.accept())
    await keyRow.locator('button:has-text("Revoke")').click()
    await expect(page.locator('#modal-body', { hasText: keyName })
      .locator('.badge-red', { hasText: 'revoked' })).toBeVisible()

    // ---- 6. The revoked key NO LONGER works ----
    await expect.poll(async () => {
      const r = await request.post('/v1/chat/completions', {
        headers: { Authorization: `Bearer ${apiKey}` },
        data: { model: TEST_MODEL, messages: [{ role: 'user', content: 'ping' }] },
      })
      return r.status()
    }, { message: 'revoked key should be rejected' }).toBe(401)
  })

  test('deactivating a service account flips its status badge', async ({ page }) => {
    const svcName = uid('svc-toggle')
    await login(page)
    await gotoView(page, 'service-accounts')
    await page.waitForSelector('#sa-tbody')

    await page.click('button:has-text("+ Create Service Account")')
    await page.fill('#sa-name', svcName)
    await submitModalAndClose(page)

    const row = serviceRow(page, svcName)
    await expectRowVisible(page, 'service-accounts', row)
    await expect(row.locator('.badge', { hasText: 'Active' })).toBeVisible()

    await row.locator('button:has-text("Deactivate")').click()
    await expect(serviceRow(page, svcName).locator('.badge', { hasText: 'Inactive' })).toBeVisible()
  })
})
