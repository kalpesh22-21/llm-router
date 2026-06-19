// @ts-check
const { test, expect } = require('@playwright/test')
const { uid, login, gotoView, settle, expectRowVisible, expectRowGone } = require('./helpers')

const filterRow = (page, name) => page.locator(`#filter-tbody tr[data-name="${name}"]`)

test.describe('Content filters CRUD + live test', () => {
  test.beforeEach(async ({ page }) => {
    await login(page)
    await gotoView(page, 'content-filters')
    await page.waitForSelector('#filter-tbody')
  })

  test('create a KEYWORD filter, test it blocks, toggle, edit, delete', async ({ page }) => {
    const name = uid('filter')
    const keyword = `zzblock${Date.now().toString(36)}`

    // ---- Create (KEYWORD, blocking, active) ----
    await page.click('button:has-text("+ New Filter")')
    await page.fill('#m-name', name)
    await page.fill('#m-desc', 'e2e keyword filter')
    await page.selectOption('#m-type', 'KEYWORD')
    await page.fill('#m-pattern', keyword)
    await page.click('#modal-submit')
    await expect(page.locator('#modal-overlay')).toHaveClass(/hidden/)

    const row = filterRow(page, name)
    await expectRowVisible(page, 'content-filters', row)
    await expect(row.locator('.badge', { hasText: 'KEYWORD' })).toBeVisible()
    await expect(row.locator('.badge', { hasText: 'Active' })).toBeVisible()
    await expect(row.locator('.badge', { hasText: 'Block' })).toBeVisible()

    // ---- Test: content containing the keyword is BLOCKED ----
    // Rules are served from an in-memory cache refreshed every
    // CONTENT_FILTER_REFRESH seconds (3s in the e2e env), so a just-created rule
    // becomes visible to the Test endpoint with a short eventual-consistency
    // delay. Retry the Test action until the rule is loaded.
    await row.locator('button:has-text("Test")').click()
    await page.fill('#m-content', `please ${keyword} this text`)
    await expect(async () => {
      await page.click('#test-btn')
      await expect(page.locator('#test-status')).toContainText('FILTER BLOCKED', { timeout: 2_000 })
    }).toPass({ timeout: 20_000 })
    await expect(page.locator('#test-result')).toContainText(name)
    // Benign content is allowed.
    await page.fill('#m-content', 'a perfectly innocent sentence')
    await page.click('#test-btn')
    await expect(page.locator('#test-status')).toContainText('CONTENT ALLOWED')
    await page.click('#close-test-btn')
    await expect(page.locator('#modal-overlay')).toHaveClass(/hidden/)

    // ---- Toggle active → inactive ----
    await filterRow(page, name).locator('button:has-text("Deactivate")').click()
    await expectRowVisible(page, 'content-filters', filterRow(page, name).locator('.badge', { hasText: 'Inactive' }))

    // ---- Edit description ----
    await filterRow(page, name).locator('button:has-text("Edit")').click()
    await expect(page.locator('#m-name')).toHaveValue(name)
    await page.fill('#m-desc', 'edited description')
    await page.click('#modal-submit')
    await expect(page.locator('#modal-overlay')).toHaveClass(/hidden/)
    await expectRowVisible(page, 'content-filters', filterRow(page, name).locator('.cell-text', { hasText: 'edited description' }))

    // ---- Delete (native confirm) ----
    page.once('dialog', (d) => d.accept())
    await filterRow(page, name).locator('button:has-text("Delete")').click()
    await expectRowGone(page, 'content-filters', filterRow(page, name))
  })

  test('REGEX filter creation validates required pattern', async ({ page }) => {
    await page.click('button:has-text("+ New Filter")')
    await page.fill('#m-name', uid('regex-filter'))
    await page.selectOption('#m-type', 'REGEX')
    // Leave pattern empty → onSubmit throws → error toast, modal stays open.
    await page.click('#modal-submit')
    await expect(page.locator('#toasts .toast-error')).toContainText('Pattern is required')
    await expect(page.locator('#modal-overlay')).not.toHaveClass(/hidden/)
    await page.locator('#modal-overlay .modal-actions .btn-secondary').click()
  })
})
