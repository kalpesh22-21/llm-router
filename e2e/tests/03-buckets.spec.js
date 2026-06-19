// @ts-check
const { test, expect } = require('@playwright/test')
const {
  TEST_MODEL, uid, login, gotoView, submitModalAndClose,
  expectRowVisible, expectRowGone, bucketRow,
} = require('./helpers')

test.describe('Buckets CRUD', () => {
  test.beforeEach(async ({ page }) => {
    await login(page)
    await gotoView(page, 'buckets')
    await page.waitForSelector('#bucket-tbody')
  })

  test('create, edit, and delete a (non-default) bucket', async ({ page }) => {
    const name = uid('bucket')

    // Create with test-model attached.
    await page.click('button:has-text("+ New Bucket")')
    await page.fill('#m-bname', name)
    await page.fill('#m-bdesc', 'created by e2e')
    await page.check(`input[name=bmodel][value="${TEST_MODEL}"]`)
    await submitModalAndClose(page)

    const row = bucketRow(page, name)
    await expectRowVisible(page, 'buckets', row)
    await expect(row.locator('.badge', { hasText: TEST_MODEL })).toBeVisible()
    await expect(row.locator('.cell-text', { hasText: 'created by e2e' })).toBeVisible()

    // Edit the description.
    await row.locator('button:has-text("Edit")').click()
    await expect(page.locator('#m-bname')).toHaveValue(name)
    await page.fill('#m-bdesc', 'edited by e2e')
    await submitModalAndClose(page)
    await expectRowVisible(page, 'buckets', bucketRow(page, name).locator('.cell-text', { hasText: 'edited by e2e' }))

    // Delete it (confirmation modal with a "Delete" submit button).
    await bucketRow(page, name).locator('button:has-text("Delete")').click()
    await expect(page.locator('#modal-title')).toContainText('Delete Bucket')
    await submitModalAndClose(page)
    await expectRowGone(page, 'buckets', bucketRow(page, name))
  })

  test('set a bucket as default and pick its default model', async ({ page }) => {
    const name = uid('default-bucket')

    await page.click('button:has-text("+ New Bucket")')
    await page.fill('#m-bname', name)
    await page.check(`input[name=bmodel][value="${TEST_MODEL}"]`)
    await submitModalAndClose(page)

    const row = bucketRow(page, name)
    await expectRowVisible(page, 'buckets', row)
    await expect(row.locator('.badge', { hasText: 'No' })).toBeVisible()

    // Promote to default.
    await row.locator('button:has-text("Set Default")').click()
    await expect(bucketRow(page, name).locator('.badge-blue', { hasText: 'Default' })).toBeVisible()

    // The default-model <select> only renders for the default bucket.
    const select = bucketRow(page, name).locator('select[id^="dm-select-"]')
    await expect(select).toBeVisible()
    await select.selectOption(TEST_MODEL)
    await expect(page.locator('#toasts .toast', { hasText: 'Default model updated' })).toBeVisible()
    await expect(select).toHaveValue(TEST_MODEL)
  })

  test('the default bucket cannot be deleted', async ({ page }) => {
    const defaultRow = page.locator('#bucket-tbody tr', {
      has: page.locator('.badge-blue', { hasText: 'Default' }),
    }).first()
    await expect(defaultRow).toBeVisible()
    // No Delete / Set Default buttons are rendered for the default bucket.
    await expect(defaultRow.locator('button:has-text("Delete")')).toHaveCount(0)
    await expect(defaultRow.locator('button:has-text("Set Default")')).toHaveCount(0)
  })

  test('search filters the bucket table', async ({ page }) => {
    const name = uid('searchable')
    await page.click('button:has-text("+ New Bucket")')
    await page.fill('#m-bname', name)
    await page.check(`input[name=bmodel][value="${TEST_MODEL}"]`)
    await submitModalAndClose(page)
    await expectRowVisible(page, 'buckets', bucketRow(page, name))

    await page.fill('#bucket-search', name)
    await expect(bucketRow(page, name)).toBeVisible()
    // A non-matching query hides our row.
    await page.fill('#bucket-search', 'zzz-nonexistent-zzz')
    await expect(bucketRow(page, name)).toBeHidden()
  })
})
