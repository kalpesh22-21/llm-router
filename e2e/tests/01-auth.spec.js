// @ts-check
const { test, expect } = require('@playwright/test')
const { ADMIN, login, getToken } = require('./helpers')

test.describe('Authentication & shell', () => {
  test('rejects bad credentials and stays on login', async ({ page }) => {
    await page.goto('/ui/')
    await page.fill('#f-user', 'admin')
    await page.fill('#f-pass', 'wrong-password')
    await page.click('#login-btn')

    // Error toast appears; the login form is still present (no sidebar).
    await expect(page.locator('#toasts .toast-error')).toBeVisible()
    await expect(page.locator('#login-btn')).toBeVisible()
    await expect(page.locator('.sidebar')).toHaveCount(0)
    expect(await getToken(page)).toBeNull()
  })

  test('logs in as admin and renders the admin shell', async ({ page }) => {
    await login(page)

    // Token persisted, profile loaded, admin nav present.
    expect(await getToken(page)).not.toBeNull()
    await expect(page.locator('.sidebar-user')).toHaveText(ADMIN.username)
    for (const nav of ['dashboard', 'users', 'buckets', 'content-filters', 'multicluster', 'service-accounts']) {
      await expect(page.locator(`.nav-item[data-nav="${nav}"]`)).toBeVisible()
    }
    // Dashboard is the landing view.
    await expect(page.locator('#content h2')).toHaveText('My Profile')
  })

  test('dashboard shows a bearer token that can be regenerated', async ({ page }) => {
    await login(page)
    await expect(page.locator('#token-display')).toBeVisible()
    const before = (await page.locator('#token-display').textContent())?.trim()
    expect(before && before.length).toBeTruthy()

    await page.click('button:has-text("Regenerate Token")')
    await expect(page.locator('#toasts .toast')).toBeVisible()
    // localStorage token is refreshed.
    await expect.poll(async () => getToken(page)).not.toBeNull()
  })

  test('logout returns to the login screen', async ({ page }) => {
    await login(page)
    await page.click('button.btn-logout')
    await expect(page.locator('#f-user')).toBeVisible()
    await expect(page.locator('.sidebar')).toHaveCount(0)
    expect(await getToken(page)).toBeNull()
  })
})
