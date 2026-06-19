// @ts-check
const { test, expect } = require('@playwright/test')
const { ADMIN, TEST_MODEL } = require('./helpers')

/**
 * API-surface checks for the OpenAI-compatible proxy, driven directly through
 * the request fixture (no browser). Complements the UI flows by proving the
 * router's auth + model-access + upstream proxying behave end-to-end.
 */
test.describe('Proxy API surface', () => {
  /** @type {string} */
  let token

  test.beforeAll(async ({ request }) => {
    const r = await request.post('/auth/login', {
      data: { username: ADMIN.username, password: ADMIN.password },
    })
    expect(r.ok()).toBeTruthy()
    token = (await r.json()).access_token
    expect(token).toBeTruthy()
  })

  test('GET /v1/models lists the configured model', async ({ request }) => {
    const r = await request.get('/v1/models', {
      headers: { Authorization: `Bearer ${token}` },
    })
    expect(r.status()).toBe(200)
    const ids = (await r.json()).data.map((m) => m.id)
    expect(ids).toContain(TEST_MODEL)
  })

  test('POST /v1/chat/completions proxies to the mock upstream (200)', async ({ request }) => {
    const r = await request.post('/v1/chat/completions', {
      headers: { Authorization: `Bearer ${token}` },
      data: { model: TEST_MODEL, messages: [{ role: 'user', content: 'ping' }] },
    })
    expect(r.status()).toBe(200)
    expect(JSON.stringify(await r.json())).toContain('pong from mock upstream')
  })

  test('an inaccessible model is rejected with 403', async ({ request }) => {
    const r = await request.post('/v1/chat/completions', {
      headers: { Authorization: `Bearer ${token}` },
      data: { model: 'no-such-model', messages: [{ role: 'user', content: 'hi' }] },
    })
    expect(r.status()).toBe(403)
  })

  test('missing auth is rejected with 401', async ({ request }) => {
    const r = await request.post('/v1/chat/completions', {
      data: { model: TEST_MODEL, messages: [{ role: 'user', content: 'hi' }] },
    })
    expect(r.status()).toBe(401)
  })
})
