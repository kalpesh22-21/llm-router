// @ts-check
const { test, expect } = require('@playwright/test')
const { uid, login, gotoView, refetchAssert } = require('./helpers')

/**
 * NOTE on environment: this e2e stack runs WITHOUT an LDAP server, and
 * `POST /admin/users` requires the username to exist in LDAP
 * (verify_ldap_user_exists). So new-user creation cannot succeed here — we test
 * that the UI surfaces that error gracefully, then exercise management actions
 * against the existing `admin` user. We deliberately never deactivate/demote
 * admin (it is the only login).
 */
test.describe('Users management', () => {
  test.beforeEach(async ({ page }) => {
    await login(page)
    await gotoView(page, 'users')
    await page.waitForSelector('#user-tbody')
  })

  test('create-user is gated by LDAP and shows an error', async ({ page }) => {
    await page.click('button:has-text("+ New User")')
    await page.fill('#m-uname', uid('ghost-user'))
    await page.click('#modal-submit')
    // The POST fails (400) → error toast; the modal stays open.
    await expect(page.locator('#toasts .toast-error')).toContainText('LDAP')
    await expect(page.locator('#modal-overlay')).not.toHaveClass(/hidden/)
    // Close the modal.
    await page.locator('#modal-overlay .modal-actions .btn-secondary').click()
    await expect(page.locator('#modal-overlay')).toHaveClass(/hidden/)
  })

  test('server-side search locates the admin user', async ({ page }) => {
    await page.fill('#user-search', 'admin')
    const row = page.locator('#user-tbody tr[data-username="admin"]')
    await expect(row).toBeVisible()
    await expect(row.locator('.badge', { hasText: 'Admin' })).toBeVisible()
  })

  test('edit max-concurrent limit for admin (and revert)', async ({ page }) => {
    await page.fill('#user-search', 'admin')
    const row = page.locator('#user-tbody tr[data-username="admin"]')
    await expect(row).toBeVisible()
    const cell = row.locator('td[style*="text-align:center"]')

    await row.locator('button:has-text("Limit")').click()
    const original = await page.inputValue('#m-max')
    const next = original === '5' ? '6' : '5'

    await page.fill('#m-max', next)
    await page.click('#modal-submit')
    await expect(page.locator('#modal-overlay')).toHaveClass(/hidden/)
    // Re-fetch until the cell reflects the new value — the edit's list refetch
    // can race the commit (bug B), so a plain assertion is flaky.
    await refetchAssert(page, 'users', async () => {
      await expect(page.locator('#user-tbody tr[data-username="admin"] td[style*="text-align:center"]')).toHaveText(next, { timeout: 1_500 })
    })

    // Revert to the original value.
    await page.locator('#user-tbody tr[data-username="admin"]').locator('button:has-text("Limit")').click()
    await page.fill('#m-max', original)
    await page.click('#modal-submit')
    await expect(page.locator('#modal-overlay')).toHaveClass(/hidden/)
    await refetchAssert(page, 'users', async () => {
      await expect(page.locator('#user-tbody tr[data-username="admin"] td[style*="text-align:center"]')).toHaveText(original, { timeout: 1_500 })
    })
  })

  test('manage-buckets modal lists buckets with checkboxes', async ({ page }) => {
    await page.fill('#user-search', 'admin')
    const row = page.locator('#user-tbody tr[data-username="admin"]')
    await row.locator('button:has-text("Buckets")').click()
    await expect(page.locator('#modal-overlay')).not.toHaveClass(/hidden/)
    // At least one bucket checkbox is offered.
    await expect(page.locator('#modal-body .checkbox-item input[type=checkbox]').first()).toBeVisible()
    // Close without modifying admin's access.
    await page.locator('#modal-overlay .modal-actions .btn-secondary').click()
    await expect(page.locator('#modal-overlay')).toHaveClass(/hidden/)
  })
})
