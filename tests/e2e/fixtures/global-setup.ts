import { chromium, FullConfig } from '@playwright/test';

/**
 * Global Setup
 *
 * Runs once before all tests. Used for:
 * - Health checks on API and frontend
 * - Creating authenticated state
 * - Seeding test data
 */

const API_URL = process.env.API_URL || 'http://localhost:8000';
const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:5173';

async function globalSetup(config: FullConfig) {
  console.log('\n[Global Setup] Starting...');

  // Wait for API to be ready
  await waitForService(API_URL + '/health', 'API');

  // Wait for Frontend to be ready
  await waitForService(FRONTEND_URL, 'Frontend');

  // Create authenticated browser state
  const browser = await chromium.launch();
  const context = await browser.newContext();
  const page = await context.newPage();

  try {
    // Perform login and save state
    const response = await context.request.post(`${API_URL}/api/auth/login`, {
      data: {
        username: 'admin',
        password: process.env.ADMIN_PASSWORD || 'admin123',
      },
    });

    if (response.ok()) {
      const data = await response.json();
      // Store auth tokens in storage state
      await context.addCookies([
        {
          name: 'access_token',
          value: data.access_token,
          domain: new URL(FRONTEND_URL).hostname,
          path: '/',
        },
      ]);

      // Save the authenticated state
      await context.storageState({ path: 'playwright/.auth/admin.json' });
      console.log('[Global Setup] Admin authentication state saved');
    } else {
      console.warn('[Global Setup] Login failed, tests will run without authentication');
    }
  } catch (error) {
    console.warn('[Global Setup] Could not authenticate:', error);
  } finally {
    await browser.close();
  }

  console.log('[Global Setup] Complete\n');
}

async function waitForService(url: string, name: string, maxAttempts = 30): Promise<void> {
  console.log(`[Global Setup] Waiting for ${name} at ${url}...`);

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const response = await fetch(url, {
        method: 'GET',
        signal: AbortSignal.timeout(5000),
      });

      if (response.ok || response.status < 500) {
        console.log(`[Global Setup] ${name} is ready`);
        return;
      }
    } catch (error) {
      // Service not ready yet
    }

    if (attempt < maxAttempts) {
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }

  console.warn(`[Global Setup] ${name} may not be ready after ${maxAttempts} attempts`);
}

export default globalSetup;
