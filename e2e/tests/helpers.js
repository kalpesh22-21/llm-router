// @ts-check
const { expect } = require('@playwright/test')

const ADMIN = { username: 'admin', password: 'admin123' }

// The static model wired to the mock upstream in docker-compose.e2e.yml.
const TEST_MODEL = 'test-model'

/** Unique-ish suffix for resource names so reruns don't collide. */
function uid(prefix = 'pw') {
  return `${prefix}-${Date.now().toString(36)}-${Math.floor(Math.random() * 1e4)}`
}

/**
 * Log in through the UI as the env admin and wait for the shell to render.
 * Each test gets a fresh browser context, so there's no pre-existing token.
 */
async function login(page, creds = ADMIN) {
  await page.goto('/ui/')
  await page.waitForSelector('#f-user')
  await page.fill('#f-user', creds.username)
  await page.fill('#f-pass', creds.password)
  await page.click('#login-btn')
  // renderShell() runs on success → sidebar appears.
  await page.waitForSelector('.sidebar', { timeout: 15_000 })
  await expect(page.locator('.nav-item[data-nav="dashboard"]')).toBeVisible()
}

/**
 * Let in-flight view fetches settle.
 *
 * The SPA renders a view TWICE per navigation — the nav link's onclick calls
 * navigate()→showX(), and setting window.location.hash fires a `hashchange`
 * listener that calls showX() again. Two concurrent fetch+render cycles race,
 * and a late one can clobber a fresh post-mutation render. Waiting for the
 * network to go idle ensures both cycles finish before we assert. The
 * multicluster view polls every 3s, but there is still a >500ms idle window
 * between polls, so networkidle is reached there too.
 */
async function settle(page) {
  await page.waitForLoadState('networkidle').catch(() => {})
}

/** Click a sidebar nav item, wait for it to become active, and let it settle. */
async function gotoView(page, view) {
  await page.click(`.nav-item[data-nav="${view}"]`)
  await expect(page.locator(`.nav-item[data-nav="${view}"]`)).toHaveClass(/active/)
  await settle(page)
}

/** Submit the shared modal, wait for it to close, and let the re-render settle. */
async function submitModalAndClose(page) {
  await page.click('#modal-submit')
  await expect(page.locator('#modal-overlay')).toHaveClass(/hidden/, { timeout: 15_000 })
  await settle(page)
}

/**
 * Force a fresh fetch of the current list view, then assert a row is present —
 * retrying the refetch until it appears.
 *
 * Works around a read-after-write race (documented in e2e/README.md, bug B):
 * the app's GracefulShutdownMiddleware is a Starlette BaseHTTPMiddleware, which
 * defers get_db()'s post-yield commit until after the POST response is
 * delivered. The SPA's immediate post-create refresh can therefore read
 * pre-commit data and render a stale list (e.g. "Buckets 9" when 10 exist).
 * Re-clicking the (already-active) nav re-fetches AFTER the commit has landed.
 */
async function expectRowVisible(page, view, rowLocator) {
  await expect(async () => {
    await page.click(`.nav-item[data-nav="${view}"]`)
    await settle(page)
    await expect(rowLocator).toBeVisible({ timeout: 1_500 })
  }).toPass({ timeout: 15_000 })
}

/**
 * Re-fetch a list view (re-click its nav) and run an arbitrary assertion,
 * retrying until it passes. For post-mutation checks on inner cell text/state
 * that race the commit (bug B) and aren't a simple row presence/absence.
 */
async function refetchAssert(page, view, assertFn) {
  await expect(async () => {
    await page.click(`.nav-item[data-nav="${view}"]`)
    await settle(page)
    await assertFn()
  }).toPass({ timeout: 15_000 })
}

/** Like expectRowVisible but asserts the row/locator is ABSENT (e.g. after delete). */
async function expectRowGone(page, view, rowLocator) {
  await expect(async () => {
    await page.click(`.nav-item[data-nav="${view}"]`)
    await settle(page)
    await expect(rowLocator).toHaveCount(0, { timeout: 1_500 })
  }).toPass({ timeout: 15_000 })
}

/** A bucket row in the buckets table located by its name. */
function bucketRow(page, name) {
  return page.locator(`#bucket-tbody tr[data-name="${name}"]`)
}

/** A service-account row located by its name. */
function serviceRow(page, name) {
  return page.locator(`#sa-tbody tr[data-name="${name}"]`)
}

/** Read the JWT bearer token the SPA stored in localStorage. */
async function getToken(page) {
  return page.evaluate(() => localStorage.getItem('llm_token'))
}

module.exports = {
  ADMIN,
  TEST_MODEL,
  uid,
  login,
  gotoView,
  settle,
  submitModalAndClose,
  expectRowVisible,
  expectRowGone,
  refetchAssert,
  bucketRow,
  serviceRow,
  getToken,
}
