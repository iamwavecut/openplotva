const { test, expect } = require('@playwright/test');

const baseURL = process.env.OPENPLOTVA_WEB_UI_BASE_URL;
const adminSessionCookie = process.env.OPENPLOTVA_WEB_UI_ADMIN_COOKIE;
const settingsInitData42 = process.env.OPENPLOTVA_WEB_UI_SETTINGS_INIT_DATA_42 || '';
const settingsInitData7 = process.env.OPENPLOTVA_WEB_UI_SETTINGS_INIT_DATA_7 || '';
const browserPath = process.env.OPENPLOTVA_WEB_UI_BROWSER || '';
const headless = process.env.OPENPLOTVA_WEB_UI_HEADLESS !== '0';

if (!baseURL) {
  throw new Error('OPENPLOTVA_WEB_UI_BASE_URL is required');
}
if (!adminSessionCookie) {
  throw new Error('OPENPLOTVA_WEB_UI_ADMIN_COOKIE is required');
}

test.use({
  baseURL,
  headless,
  launchOptions: browserPath ? { executablePath: browserPath } : {},
});

test.setTimeout(60_000);

function watchPageErrors(page) {
  const errors = [];
  page.on('pageerror', (error) => {
    errors.push(error.message);
  });
  return () => {
    expect(errors).toEqual([]);
  };
}

async function stubTelegramWebApp(page, userId, initData) {
  if (!Number.isSafeInteger(userId) || !initData) {
    throw new Error('settings UI smoke requires a user ID and signed Telegram initData');
  }
  await page.route('https://telegram.org/js/telegram-web-app.js', async (route) => {
    await route.fulfill({
      contentType: 'application/javascript',
      body: `
        window.Telegram = {
          WebApp: {
            initData: ${JSON.stringify(initData)},
            initDataUnsafe: { user: { id: ${userId}, first_name: 'Smoke' } },
            colorScheme: 'light',
            platform: 'web',
            version: 'service-smoke',
            headerColor: 'bg_color',
            backgroundColor: 'bg_color',
            ready() {},
            expand() {},
            setHeaderColor() {},
            setBackgroundColor() {},
            enableClosingConfirmation() {},
            disableClosingConfirmation() {},
            onEvent() {},
            showAlert(message) { window.__lastTelegramAlert = String(message || ''); }
          }
        };
      `,
    });
  });
}

async function stubAdminExternalScripts(page) {
  await page.route('https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js', async (route) => {
    await route.fulfill({
      contentType: 'application/javascript',
      body: `
        window.__charts = [];
        window.Chart = class {
          constructor(ctx, config) {
            this.canvasID = ctx && ctx.canvas ? ctx.canvas.id : '';
            this.data = config && config.data ? config.data : { labels: [], datasets: [] };
            this.options = config && config.options ? config.options : {};
            window.__charts.push(this);
          }
          update() {
            window.__charts.push(this);
          }
          destroy() {}
        };
      `,
    });
  });
}

test('admin login gate and authenticated shell render', async ({ page, context }) => {
  const assertNoPageErrors = watchPageErrors(page);
  await stubAdminExternalScripts(page);

  await page.goto('/admin/login.html', { waitUntil: 'domcontentloaded' });
  await expect(page).toHaveTitle(/Plotva Admin/);
  await expect(page.locator('h1')).toHaveText('Plotva Admin');
  await expect(page.locator('#telegram-login-widget')).toBeVisible();
  await expect(page.locator('#telegram-login-widget script[data-telegram-login="SmokePlotvaBot"]'))
    .toHaveCount(1);

  await page.goto('/admin/', { waitUntil: 'domcontentloaded' });
  await expect(page).toHaveURL(/\/admin\/login\.html$/);

  await context.addCookies([{
    name: 'admin_session',
    value: adminSessionCookie,
    url: baseURL,
  }]);
  await page.goto('/admin/', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('.brand-title')).toHaveText('Plotva');
  await expect(page.locator('#page-title')).toHaveText('Dashboard');
  await expect(page.locator('#dashboard')).toBeVisible();
  await page.locator('pl-button[data-tab="settings"]').click();
  await expect(page.locator('#page-title')).toHaveText('Settings');
  await expect(page.locator('#log-level')).toHaveValue('info');
  await expect(page.locator('#queue-stats')).toContainText('{');

  await page.locator('#log-level').selectOption('debug');
  const loglevelResponse = page.waitForResponse((response) => {
    return response.url().endsWith('/admin/api/loglevel')
      && response.request().method() === 'POST';
  });
  await page.locator('pl-button[data-action="saveLogLevel"]').click();
  await expect((await loglevelResponse).status()).toBe(200);
  await expect(page.locator('.pl-toast', { hasText: 'Log level updated to debug.' })).toBeVisible();

  await page.locator('pl-button[data-tab="redis"]').click();
  await expect(page.locator('#page-title')).toHaveText('Redis');
  await page.locator('#redis-pattern-search').fill('*');
  const redisListResponse = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/redis/list?')
      && response.request().method() === 'GET';
  });
  await page.locator('#redis pl-button[data-action="loadRedisKeys"]').click();
  const redisList = await (await redisListResponse).json();
  expect(Array.isArray(redisList.keys)).toBe(true);

  const memoryCardsResponse = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/memory/cards?')
      && response.request().method() === 'GET';
  });
  const memoryRunsResponse = page.waitForResponse((response) => {
    return response.url().endsWith('/admin/api/memory/runs?limit=100')
      && response.request().method() === 'GET';
  });
  await page.locator('pl-button[data-tab="memory"]').click();
  const memoryCards = await (await memoryCardsResponse).json();
  await memoryRunsResponse;
  expect(Array.isArray(memoryCards.cards)).toBe(true);
  await expect(page.locator('#memory-meta')).toContainText('cards');
  await expect(page.locator('#memory-table-body')).toContainText('Smoke Group likes real DB settings smoke.');
  await expect(page.locator('#memory-runs-meta')).toContainText('runs');

  const shieldResponse = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/shield/documents?')
      && response.request().method() === 'GET';
  });
  await page.locator('pl-button[data-tab="shield"]').click();
  const shieldDocs = await (await shieldResponse).json();
  expect(Array.isArray(shieldDocs.documents)).toBe(true);
  await expect(page.locator('#shield-meta')).toContainText('documents');

  await page.locator('pl-button[data-tab="logs"]').click();
  await expect(page.locator('#page-title')).toHaveText('Real-time Logs');
  await expect(page.locator('#logs-status')).toHaveClass(/status-connected/);

  const chatsResponse = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/chats/search_by_member?user_id=7')
      && response.request().method() === 'GET';
  });
  await page.locator('pl-button[data-tab="chats"]').click();
  await page.locator('#chat-search-mode').selectOption('member');
  await page.locator('#chat-search-input').fill('7');
  await page.locator('#btn-search-chats').click();
  const chats = await (await chatsResponse).json();
  expect(Array.isArray(chats.chats)).toBe(true);
  await expect(page.locator('#chat-list')).toContainText('Smoke Group');

  const chatResponse = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/chat?chat_id=-100777')
      && response.request().method() === 'GET';
  });
  await page.locator('#chat-list .list-item').first().click();
  await chatResponse;
  await expect(page.locator('#chat-details')).toContainText('Smoke Group');

  const chatMembersResponse = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/chat/members?chat_id=-100777')
      && response.request().method() === 'GET';
  });
  await page.locator('#pane-chats-details pl-button', { hasText: 'Load Members' }).click();
  await chatMembersResponse;
  await expect(page.locator('#chat-members-list')).toContainText('Owner');

  await page.locator('#chat-mood').fill('browser-smoke-mood');
  await page.locator('#chat-persona').fill('browser smoke persona');
  await page.locator('#chat-reactivity').fill('88');
  await page.locator('#chat-proactivity').fill('22');
  await page.locator('#chat-daily-theme').fill('browser-smoke-theme');
  await page.locator('#chat-greeting-html').fill('<b>browser hello</b>');
  // pl-toggle is an accessible switch (role=switch), not a native checkbox input:
  // click it to the desired state rather than using setChecked().
  for (const [id, want] of [['chat-draw-reply', true], ['chat-obscenifier', true], ['chat-profanity', false]]) {
    const toggle = page.locator(`#${id}`);
    if ((await toggle.getAttribute('aria-checked')) !== String(want)) {
      await toggle.click();
    }
  }
  const chatSettingsResponse = page.waitForResponse((response) => {
    return response.url().endsWith('/admin/api/chat/settings')
      && response.request().method() === 'POST';
  });
  await page.locator('#pane-chats-details pl-button', { hasText: 'Save Settings' }).click();
  expect(await (await chatSettingsResponse).json()).toMatchObject({ ok: true });
  await expect(page.locator('.pl-toast', { hasText: 'Settings saved' })).toBeVisible();
  const chatReloadAfterSettings = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/chat?chat_id=-100777')
      && response.request().method() === 'GET';
  });
  await page.locator('#pane-chats-details pl-button').filter({ hasText: /^Load$/ }).click();
  await chatReloadAfterSettings;
  await expect(page.locator('#chat-mood')).toHaveValue('browser-smoke-mood');
  await expect(page.locator('#chat-persona')).toHaveValue('browser smoke persona');
  await expect(page.locator('#chat-daily-theme')).toHaveValue('browser-smoke-theme');

  const chatBlockResponse = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/chat/block?chat_id=-100777')
      && response.request().method() === 'POST';
  });
  const chatReloadAfterBlock = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/chat?chat_id=-100777')
      && response.request().method() === 'GET';
  });
  await page.locator('#pane-chats-details pl-button', { hasText: 'Block 10m' }).click();
  expect(await (await chatBlockResponse).json()).toHaveProperty('ok', true);
  await expect(page.locator('.pl-toast', { hasText: 'Chat blocked' })).toBeVisible();
  await chatReloadAfterBlock;
  await expect(page.locator('#chat-details')).toContainText('"blocked": true');

  const chatUnblockResponse = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/chat/unblock?chat_id=-100777')
      && response.request().method() === 'DELETE';
  });
  const chatReloadAfterUnblock = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/chat?chat_id=-100777')
      && response.request().method() === 'GET';
  });
  await page.locator('#pane-chats-details pl-button', { hasText: 'Unblock' }).click();
  expect(await (await chatUnblockResponse).json()).toMatchObject({ ok: true });
  await expect(page.locator('.pl-toast', { hasText: 'Chat unblocked' })).toBeVisible();
  await chatReloadAfterUnblock;
  await expect(page.locator('#chat-details')).toContainText('"blocked": false');
  await page.locator('#pane-chats-details pl-button', { hasText: 'Close' }).click();
  await expect(page.locator('#pane-chats-details')).toBeHidden();

  const usersResponse = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/users?q=owner')
      && response.request().method() === 'GET';
  });
  await page.locator('pl-button[data-tab="users"]').click();
  await page.locator('#user-search-input').fill('owner');
  await page.locator('#btn-search-users').click();
  const users = await (await usersResponse).json();
  expect(Array.isArray(users.users)).toBe(true);
  await expect(page.locator('#user-list')).toContainText('@owner');

  const userResponse = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/user?id=7')
      && response.request().method() === 'GET';
  });
  await page.locator('#user-list .list-item').first().click();
  await userResponse;
  await expect(page.locator('#user-details')).toContainText('"username": "owner"');

  await page.locator('#vip-days').fill('2');
  await page.locator('#vip-reason').fill('browser vip smoke');
  const grantVipResponse = page.waitForResponse((response) => {
    return response.url().endsWith('/admin/api/user/grant_vip')
      && response.request().method() === 'POST';
  });
  const userReloadAfterGrant = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/user?id=7')
      && response.request().method() === 'GET';
  });
  await page.locator('#pane-users-details pl-button', { hasText: 'Grant VIP' }).click();
  expect(await (await grantVipResponse).json()).toHaveProperty('ok', true);
  await expect(page.locator('.pl-toast', { hasText: 'VIP adjustment recorded' })).toBeVisible();
  await userReloadAfterGrant;
  await expect(page.locator('#user-vip-summary')).toContainText('Active');
  await expect(page.locator('#user-vip-summary')).toContainText('yes');
  await expect(page.locator('#user-vip-events')).toContainText('admin_adjustment');
  await expect(page.locator('#user-vip-events')).toContainText('browser vip smoke');

  await page.locator('#vip-reason').fill('browser vip revoke');
  const revokeVipResponse = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/user/revoke_vip?user_id=7')
      && response.request().method() === 'DELETE';
  });
  const userReloadAfterRevoke = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/user?id=7')
      && response.request().method() === 'GET';
  });
  await page.locator('#pane-users-details pl-button', { hasText: 'Revoke VIP' }).click();
  expect(await (await revokeVipResponse).json()).toMatchObject({ ok: true, revoked: true });
  await expect(page.locator('.pl-toast', { hasText: 'VIP revoked' })).toBeVisible();
  await userReloadAfterRevoke;
  await expect(page.locator('#user-vip-events')).toContainText('admin_revoke');
  await expect(page.locator('#user-vip-events')).toContainText('browser vip revoke');
  await page.locator('#pane-users-details pl-button', { hasText: 'Close' }).click();
  await expect(page.locator('#pane-users-details')).toBeHidden();

  const safetyResponse = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/safety/checks?')
      && response.request().method() === 'GET';
  });
  await page.locator('pl-button[data-tab="safety"]').click();
  const safety = await (await safetyResponse).json();
  expect(Array.isArray(safety.checks)).toBe(true);
  const seededSafety = safety.checks.find((check) => check.external_session_id === 'wc-smoke-ext');
  expect(seededSafety).toMatchObject({
    source: 'service-smoke',
    flagged: true,
    duration_ms: 123,
  });
  await expect(page.locator('#safety-checks-list')).toBeVisible();
  await expect(page.locator('#safety-checks-list')).toContainText('wc-smoke-ext');
  await expect(page.locator('#safety-checks-list')).toContainText('FLAGGED');
  await page.locator('#safety-checks-list .list-item', { hasText: 'wc-smoke-ext' }).click();
  await expect(page.locator('#safety-check-details')).toContainText('"deployment_id": "service-smoke"');
  await expect(page.locator('#safety-check-request')).toContainText('smoke risky text');
  await expect(page.locator('#safety-check-policies')).toContainText('"violence": true');
  await expect(page.locator('#safety-check-response')).toContainText('"flagged": true');
  await page.locator('#pane-safety-details pl-button', { hasText: 'Close' }).click();
  await expect(page.locator('#pane-safety-details')).toBeHidden();

  const analyticsResponse = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/analytics/overview?')
      && response.request().method() === 'GET';
  });
  await page.locator('pl-button[data-tab="analytics"]').click();
  const analytics = await (await analyticsResponse).json();
  expect(analytics).toHaveProperty('range');
  expect(analytics.health).toBeTruthy();
  expect(Number(analytics.health.external_calls)).toBeGreaterThanOrEqual(2);
  expect(Array.isArray(analytics.llm.models)).toBe(true);
  expect(analytics.llm.models.find((row) => row.model === 'smoke-model-a')).toBeTruthy();
  expect(Array.isArray(analytics.llm.providers)).toBe(true);
  expect(analytics.llm.providers.length).toBeGreaterThanOrEqual(1);

  await expect(page.locator('#page-title')).toHaveText('Analytics');
  await expect(page.locator('#an-health .metric-card').first()).toBeVisible();
  await expect(page.locator('#an-health')).toContainText('external calls');
  await expect(page.locator('#an-models')).toContainText('smoke-model-a');
  await expect(page.locator('#analytics canvas').first()).toBeVisible();
  // Range change refetches the snapshot.
  const rangeRefetch = page.waitForResponse((response) => {
    return response.url().includes('/admin/api/analytics/overview?range=7d')
      && response.request().method() === 'GET';
  });
  await page.locator('#an-range').selectOption('7d');
  await rangeRefetch;

  assertNoPageErrors();
});

test('admin LLM dialogs list and detail render agent runs', async ({ page, context }) => {
  const assertNoPageErrors = watchPageErrors(page);
  await stubAdminExternalScripts(page);
  await context.addCookies([{
    name: 'admin_session',
    value: adminSessionCookie,
    url: baseURL,
  }]);

  const runSkeleton = {
    id: 7,
    run_id: 'job-8123',
    kind: 'dialog',
    status: 'completed',
    started_at: '2026-05-25T12:34:56Z',
    duration_ms: 5400,
    preview: 'smoke final answer',
    origin: {
      chat_id: -100,
      chat_title: 'Smoke Chat',
      user_id: 42,
      trigger_message_id: 20,
      trigger_preview: 'когда солнцестояние?',
      queue_name: 'dialog-aifarm',
    },
    totals: { rounds: 2, tool_calls: 1, total_tokens: 1234 },
    tools: [{ name: 'web_search', status: 'ok', count: 1 }],
    outcome: { outcome: 'sent', sent_message_parts: 1 },
  };

  await page.route('**/admin/api/llm/dialogs', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({ count: 1, runs: [runSkeleton] }),
    });
  });
  await page.route('**/admin/api/llm/dialogs/detail?id=7', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        run: {
          ...runSkeleton,
          context: {
            memories: [
              { card_id: 101, salience: 0.9, confidence: 0.8, card_type: 'preference', competing: false, preview: 'smoke prefers tea' },
              { card_id: 102, salience: 0.5, confidence: 0.6, card_type: 'technical_fact', competing: true, preview: 'smoke writes rust' },
            ],
            persona: { name: 'Плотва', mood: 'playful', custom: true, profanity: false, obscenifier: false },
            settings: [{ label: 'reactivity', value: '30%' }],
            history_len: 12,
            tools_offered: true,
            shield_on: true,
            reference_context_chars: 640,
          },
          rounds: [
            {
              seq: 1,
              provider: 'aifarm',
              model: 'smoke-model-a',
              duration_ms: 2600,
              usage: { input_tokens: 900, output_tokens: 120 },
              response_text: 'щас гляну',
              tool_calls: [{
                name: 'web_search',
                status: 'ok',
                duration_ms: 400,
                args_json: { query: 'солнцестояние' },
                result_json: { status: 'ok', message: 'June 21' },
              }],
              raw_source: 'live',
              raw_request: { messages: [{ role: 'user', content: 'hi' }] },
              raw_response: { choices: [] },
            },
            {
              seq: 2,
              provider: 'aifarm',
              model: 'smoke-model-a',
              duration_ms: 2800,
              usage: { input_tokens: 1000, output_tokens: 90 },
              response_text: 'smoke final answer',
              sent: 'final',
              tool_calls: [],
              raw_source: 'rotated_out',
            },
          ],
        },
      }),
    });
  });

  await page.route('**/admin/api/memory/card?id=*', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        card: { id: 101, fact_text: 'smoke prefers strong tea', card_type: 'preference', salience: 0.9, status: 'active' },
        links: [{ peer_card_id: 102, peer_fact_text: 'rust', peer_card_type: 'technical_fact', relation: 'same_topic', confidence: 0.6 }],
      }),
    });
  });

  await page.goto('/admin/', { waitUntil: 'domcontentloaded' });
  const listResponse = page.waitForResponse((response) => {
    return response.url().endsWith('/admin/api/llm/dialogs')
      && response.request().method() === 'GET';
  });
  await page.locator('pl-button[data-tab="llm"]').click();
  const payload = await (await listResponse).json();
  expect(payload.runs).toHaveLength(1);

  await expect(page.locator('#page-title')).toHaveText('LLM Dialogs');
  await expect(page.locator('#llmd-kpis')).toContainText('runs');
  await expect(page.locator('#llmd-list')).toContainText('Smoke Chat');
  await expect(page.locator('#llmd-list')).toContainText('smoke final answer');
  await expect(page.locator('#llmd-list')).toContainText('web_search');

  await page.locator('#llmd-list .llmd-row').first().click();
  await expect(page.locator('#llmd-detail')).toBeVisible();
  await expect(page.locator('#llmd-browse')).toBeHidden();
  await expect(page.locator('#llmd-detail-body')).toContainText('Smoke Chat');
  await expect(page.locator('#llmd-detail-body')).toContainText('smoke final answer');
  await expect(page.locator('#llmd-detail-body')).toContainText('web_search');
  await expect(page.locator('#llmd-detail-body')).toContainText('rotated out');

  // Context X-ray: the recalled-memory constellation + persona/settings.
  await expect(page.locator('#llmd-detail-body .llmd-xray')).toBeVisible();
  await expect(page.locator('#llmd-detail-body')).toContainText('Context X-ray');
  await expect(page.locator('.llmd-xray pl-graph svg')).toBeVisible();
  await expect(page.locator('.llmd-xray-mem')).toContainText('smoke prefers tea');
  await expect(page.locator('.llmd-xray')).toContainText('competing');
  await expect(page.locator('.llmd-xray')).toContainText('playful');
  await expect(page.locator('.llmd-xray')).toContainText('reactivity');

  await page.locator('#llmd-detail [data-action="llmdCloseDetail"]').first().click();
  await expect(page.locator('#llmd-detail')).toBeHidden();
  await expect(page.locator('#llmd-browse')).toBeVisible();
  await expect(page.locator('#llmd-list')).toContainText('Smoke Chat');

  assertNoPageErrors();
});

test('admin Taskman supports filters paging details and mutations', async ({ page, context }) => {
  const assertNoPageErrors = watchPageErrors(page);
  await stubAdminExternalScripts(page);
  await context.addCookies([{
    name: 'admin_session',
    value: adminSessionCookie,
    url: baseURL,
  }]);
  await context.grantPermissions(['clipboard-read', 'clipboard-write'], { origin: baseURL });

  const listRequests = [];
  const mutationRequests = [];
  const job = (id, status = 'pending') => ({
    id,
    queue_name: id === 102 ? 'image-vip' : 'text',
    priority: 10,
    title: `Smoke job ${id}`,
    job_type: 'dialog',
    payload: { type: 'dialog', prompt: `prompt-${id}` },
    status,
    user_id: 42,
    chat_id: -100777,
    trigger_message_id: 77,
    created_at: '2026-08-03T10:00:00Z',
    processing_timeout_seconds: 90,
    preview: `preview-${id}`,
  });

  await page.route('**/admin/api/taskman/jobs?*', async (route) => {
    const url = new URL(route.request().url());
    listRequests.push(Object.fromEntries(url.searchParams.entries()));
    const offset = Number(url.searchParams.get('offset') || 0);
    const item = job(offset >= 200 ? 102 : 101);
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        total: 201,
        offset,
        limit: 200,
        summary: { by_status: { pending: 200, failed: 1 }, by_queue: { text: 200, 'image-vip': 1 } },
        items: [item],
      }),
    });
  });
  await page.route('**/admin/api/taskman/job?id=*', async (route) => {
    const url = new URL(route.request().url());
    const id = Number(url.searchParams.get('id'));
    if (id === 404) {
      await route.fulfill({
        status: 500,
        contentType: 'application/json',
        body: JSON.stringify({ error: 'smoke detail failed' }),
      });
      return;
    }
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        job: job(id, id === 303 ? 'pending' : 'failed'),
        events: [{ at: '2026-08-03T10:01:00Z', event: 'attempt', detail: `event-${id}` }],
        messages: [{ id: 1, job_id: id, message_type: 'result', chat_id: -100777, message_id: 88, created_at: '2026-08-03T10:02:00Z', status: 'sent' }],
      }),
    });
  });
  await page.route('**/admin/api/taskman/job/cancel?*', async (route) => {
    mutationRequests.push({ path: 'cancel', method: route.request().method() });
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify({ ok: true }) });
  });
  await page.route('**/admin/api/taskman/job/restart?*', async (route) => {
    mutationRequests.push({ path: 'restart', method: route.request().method() });
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify({ ok: true, new_job_id: 303 }) });
  });
  await page.route('**/admin/api/taskman/jobs/clear?*', async (route) => {
    mutationRequests.push({ path: 'clear', method: route.request().method() });
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({ ok: true, matched: 201, deleted: 200, deleted_active: 1, skipped_active: 0 }),
    });
  });

  await page.goto('/admin/', { waitUntil: 'domcontentloaded' });
  await page.locator('pl-button[data-tab="taskman"]').click();
  await expect(page.locator('#taskman-jobs-list')).toContainText('Smoke job 101');
  await expect(page.locator('#taskman-summary')).toContainText('201');

  await page.locator('#taskman-search-input').fill('needle');
  await page.locator('#taskman-queue').selectOption('text');
  await page.locator('#taskman-status').selectOption('pending');
  await page.locator('#taskman-chat-id').fill('-100777');
  await page.locator('#taskman-user-id').fill('42');
  await page.locator('#btn-search-taskman').click();
  await expect.poll(() => listRequests.length).toBeGreaterThan(1);
  expect(listRequests.at(-1)).toMatchObject({
    q: 'needle', queue: 'text', status: 'pending', chat_id: '-100777', user_id: '42', offset: '0', limit: '200',
  });

  await page.locator('pl-button[data-action="taskmanNextPage"]').click();
  await expect(page.locator('#taskman-jobs-list')).toContainText('Smoke job 102');
  expect(listRequests.at(-1)).toHaveProperty('offset', '200');
  await page.locator('pl-button[data-action="taskmanPrevPage"]').click();
  await expect(page.locator('#taskman-jobs-list')).toContainText('Smoke job 101');

  await page.locator('#taskman-job-id').fill('202');
  await page.locator('pl-button[data-action="loadTaskmanJobByInput"]').click();
  await expect(page.locator('#pane-taskman-details')).toBeVisible();
  await expect(page.locator('#taskman-job-details')).toContainText('"id": 202');

  await page.locator('pl-button[data-action="copyTaskmanSelectedJob"]').click();
  await expect(page.locator('.pl-toast', { hasText: 'Job JSON copied' })).toBeVisible();
  expect(await page.evaluate(() => navigator.clipboard.readText())).toContain('"id": 202');

  await page.locator('#pane-taskman-details pl-button[data-action="toggleTaskmanDetails"]').click();
  await expect(page.locator('#pane-taskman-details')).toBeHidden();
  await page.locator('#taskman-jobs-list tbody tr').first().click();
  await expect(page.locator('#pane-taskman-details')).toBeVisible();
  await expect(page.locator('#taskman-job-details')).toContainText('"id": 101');
  await expect(page.locator('#taskman-job-events')).toContainText('event-101');
  await expect(page.locator('#taskman-job-messages')).toContainText('sent');

  await page.locator('pl-button[data-action="cancelTaskmanSelectedJob"]').click();
  await expect(page.locator('pl-modal')).toContainText('Cancel job 101');
  await page.locator('pl-modal pl-button', { hasText: 'Cancel job' }).click();
  await expect(page.locator('.pl-toast', { hasText: 'Job 101 cancelled' })).toBeVisible();

  await page.locator('pl-button[data-action="restartTaskmanSelectedJob"]').click();
  await expect(page.locator('pl-modal')).toContainText('Restart job 101');
  await page.locator('pl-modal pl-button', { hasText: 'Restart job' }).click();
  await expect(page.locator('#taskman-job-details')).toContainText('"id": 303');
  await expect(page.locator('.pl-toast', { hasText: 'restarted as 303' })).toBeVisible();

  await page.locator('#pane-taskman-details pl-button[data-action="toggleTaskmanDetails"]').click();
  await expect(page.locator('#pane-taskman-details')).toBeHidden();
  await page.locator('#taskman-status').selectOption('');
  await page.locator('pl-button[data-action="clearTaskmanJobsByFilter"]').click();
  await expect(page.locator('.pl-toast', { hasText: 'Apply the current filters before clearing' })).toBeVisible();
  await expect(page.locator('pl-modal')).toHaveCount(0);
  const requestsBeforeClearSearch = listRequests.length;
  await page.locator('#btn-search-taskman').click();
  await expect.poll(() => listRequests.length).toBeGreaterThan(requestsBeforeClearSearch);
  await page.locator('pl-button[data-action="clearTaskmanJobsByFilter"]').click();
  await expect(page.locator('pl-modal')).toContainText('pending or processing jobs');
  await page.locator('pl-modal pl-button', { hasText: 'Clear filtered' }).click();
  await expect(page.locator('.pl-toast', { hasText: 'Deleted 200 of 201 jobs' })).toBeVisible();

  expect(mutationRequests).toEqual([
    { path: 'cancel', method: 'POST' },
    { path: 'restart', method: 'POST' },
    { path: 'clear', method: 'DELETE' },
  ]);

  await page.locator('#taskman-job-id').fill('404');
  await page.locator('pl-button[data-action="loadTaskmanJobByInput"]').click();
  await expect(page.locator('#taskman-job-details')).toContainText('Could not load Taskman job 404');
  await expect(page.locator('#taskman-job-artifacts')).toBeHidden();
  await expect(page.locator('pl-button[data-action="copyTaskmanSelectedJob"]')).toBeDisabled();
  assertNoPageErrors();
});

test('settings landing loads managed chats from real API', async ({ page }) => {
  const assertNoPageErrors = watchPageErrors(page);
  await stubTelegramWebApp(page, 7, settingsInitData7);

  await page.goto('/settings/index.html?user_id=7&signature=68b3a1ec', {
    waitUntil: 'domcontentloaded',
  });
  await expect(page.locator('#tileChats')).toContainText('Настройки чатов');
  await page.locator('#tileChats').click();
  await expect(page.locator('#chatList')).toContainText('Smoke Group');
  await expect(page.locator('#chatList')).toContainText('👥');

  assertNoPageErrors();
});

test('settings general page renders persisted private settings', async ({ page }) => {
  const assertNoPageErrors = watchPageErrors(page);
  await stubTelegramWebApp(page, 42, settingsInitData42);

  await page.goto('/settings/index.html?user_id=42&signature=780e28cf&mode=general', {
    waitUntil: 'domcontentloaded',
  });
  await expect(page.locator('#pageTitle')).toHaveText('Настройки пользователя');
  await expect(page.locator('textarea[name="custom_persona"]')).toHaveValue('service smoke persona');
  await expect(page.locator('input[name="disable_random_reactivity"]')).toBeChecked();
  await expect(page.locator('input[name="enable_profanity"]')).toBeChecked();

  assertNoPageErrors();
});
