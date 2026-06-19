// @ts-check
const { test, expect } = require('@playwright/test')
const { login, gotoView } = require('./helpers')

/**
 * NOTE on environment: the static e2e stack provisions no cluster_config rows,
 * so `/admin/models/<id>/clusters` returns []. The multicluster view therefore
 * renders its empty-state rather than cluster cards. We verify the page loads,
 * the model selector is populated, and switching models works. Cluster-card
 * edit/enable/disable can't be exercised without provisioned clusters.
 */
test.describe('Multicluster view', () => {
  test('loads, lists models, and switches model selection', async ({ page }) => {
    await login(page)
    await gotoView(page, 'multicluster')

    await expect(page.locator('#content h2')).toHaveText('Multicluster')
    const select = page.locator('#cluster-model-select')
    // The SPA double-fetches on navigation; if a stale render landed on the
    // empty-state, a full reload re-inits with a single clean render.
    await expect(async () => {
      if (await select.count() === 0) {
        await page.reload()
        await page.waitForSelector('.sidebar')
      }
      await expect(select).toBeVisible({ timeout: 3_000 })
    }).toPass({ timeout: 20_000 })

    // Both configured models are selectable.
    const options = await select.locator('option').allTextContents()
    expect(options).toContain('test-model')
    expect(options).toContain('k8s-discovered-model')

    // Clusters container resolves to a real state (empty-state here, no cards).
    await expect(page.locator('#clusters-container .empty-state, #clusters-container .cluster-card').first()).toBeVisible()

    // Switching the model re-renders the clusters container without error.
    await select.selectOption('k8s-discovered-model')
    await expect(page.locator('#clusters-container')).toBeVisible()
    await expect(page.locator('#clusters-container .error-state')).toHaveCount(0)
  })
})
