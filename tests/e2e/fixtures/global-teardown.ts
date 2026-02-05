import { FullConfig } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Global Teardown
 *
 * Runs once after all tests. Used for:
 * - Cleanup of test data
 * - Report generation finalization
 * - Resource cleanup
 */

const API_URL = process.env.API_URL || 'http://localhost:8000';

async function globalTeardown(config: FullConfig) {
  console.log('\n[Global Teardown] Starting...');

  // Clean up test data created during tests
  await cleanupTestData();

  // Clean up temporary auth files
  const authDir = path.join(__dirname, '../playwright/.auth');
  if (fs.existsSync(authDir)) {
    try {
      fs.rmSync(authDir, { recursive: true, force: true });
      console.log('[Global Teardown] Auth state files cleaned up');
    } catch (error) {
      console.warn('[Global Teardown] Could not clean up auth files:', error);
    }
  }

  console.log('[Global Teardown] Complete\n');
}

async function cleanupTestData(): Promise<void> {
  try {
    // Attempt to clean up any test sources created during E2E tests
    // This requires admin authentication
    const loginResponse = await fetch(`${API_URL}/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        username: 'admin',
        password: process.env.ADMIN_PASSWORD || 'admin123',
      }),
    });

    if (!loginResponse.ok) {
      console.log('[Global Teardown] Could not authenticate for cleanup');
      return;
    }

    const { access_token } = await loginResponse.json();

    // Get all sources with test prefix
    const sourcesResponse = await fetch(`${API_URL}/api/sources`, {
      headers: {
        Authorization: `Bearer ${access_token}`,
      },
    });

    if (sourcesResponse.ok) {
      const sources = await sourcesResponse.json();
      const testSources = sources.filter((s: any) =>
        s.name.startsWith('e2e_test_') || s.name.includes('_test_')
      );

      for (const source of testSources) {
        try {
          await fetch(`${API_URL}/api/sources/${source._id}`, {
            method: 'DELETE',
            headers: {
              Authorization: `Bearer ${access_token}`,
            },
          });
          console.log(`[Global Teardown] Deleted test source: ${source.name}`);
        } catch (error) {
          console.warn(`[Global Teardown] Could not delete source ${source.name}:`, error);
        }
      }
    }
  } catch (error) {
    console.warn('[Global Teardown] Cleanup failed:', error);
  }
}

export default globalTeardown;
