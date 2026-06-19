// @ts-check
const { defineConfig, devices } = require('@playwright/test')

/**
 * Playwright config for the LLM Router admin SPA E2E suite.
 *
 * Targets the running docker-compose.e2e.yml stack (app on :8000).
 * Tests run SERIALLY (workers: 1) because they mutate shared backend state
 * (the single MySQL instance) — e.g. the "default bucket" is global, and
 * parallel writers would race on it.
 */
module.exports = defineConfig({
  testDir: './tests',
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: 0,
  timeout: 45_000,
  expect: { timeout: 10_000 },
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: process.env.BASE_URL || 'http://localhost:8000',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
    // Grant clipboard so copy buttons don't throw (we read inputs directly,
    // but this keeps the UI handlers happy).
    permissions: ['clipboard-read', 'clipboard-write'],
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
})
