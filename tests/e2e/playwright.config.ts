import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright E2E Test Configuration
 *
 * Configures browser testing for the Airflow Crawler System.
 * - API Base URL: http://localhost:8000
 * - Frontend Base URL: http://localhost:5173
 */

export default defineConfig({
  // Test directory
  testDir: './specs',

  // Test file patterns
  testMatch: '**/*.spec.ts',

  // Run tests in parallel
  fullyParallel: true,

  // Fail the build on CI if you accidentally left test.only in the source code
  forbidOnly: !!process.env.CI,

  // Retry on CI only
  retries: process.env.CI ? 2 : 0,

  // Limit parallel workers on CI
  workers: process.env.CI ? 1 : undefined,

  // Reporter configuration
  reporter: [
    ['html', { outputFolder: 'playwright-report', open: 'never' }],
    ['json', { outputFile: 'test-results/results.json' }],
    ['list'],
  ],

  // Global timeout settings
  timeout: 30000,
  expect: {
    timeout: 10000,
  },

  // Shared settings for all projects
  use: {
    // Base URL for the frontend
    baseURL: process.env.FRONTEND_URL || 'http://localhost:5173',

    // Collect trace when retrying the failed test
    trace: 'on-first-retry',

    // Screenshot on failure
    screenshot: 'only-on-failure',

    // Video recording
    video: 'on-first-retry',

    // Default action timeout
    actionTimeout: 10000,

    // Navigation timeout
    navigationTimeout: 30000,

    // Viewport
    viewport: { width: 1280, height: 720 },

    // Locale
    locale: 'ko-KR',

    // Timezone
    timezoneId: 'Asia/Seoul',
  },

  // Configure projects for major browsers
  projects: [
    // Desktop browsers
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        // Custom API base URL for test fixtures
        extraHTTPHeaders: {
          'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        },
      },
    },
    {
      name: 'firefox',
      use: {
        ...devices['Desktop Firefox'],
      },
    },
    {
      name: 'webkit',
      use: {
        ...devices['Desktop Safari'],
      },
    },

    // Mobile viewports (optional)
    {
      name: 'mobile-chrome',
      use: {
        ...devices['Pixel 5'],
      },
    },
    {
      name: 'mobile-safari',
      use: {
        ...devices['iPhone 12'],
      },
    },

    // API testing project (no browser needed)
    {
      name: 'api',
      testMatch: '**/api-*.spec.ts',
      use: {
        baseURL: process.env.API_URL || 'http://localhost:8000',
      },
    },
  ],

  // Output folder for test artifacts
  outputDir: 'test-results',

  // Web server configuration (optional - start servers before tests)
  webServer: [
    {
      command: 'cd ../.. && npm run dev --prefix frontend',
      url: 'http://localhost:5173',
      timeout: 120000,
      reuseExistingServer: !process.env.CI,
      cwd: __dirname,
    },
    // API server is assumed to be running separately or via docker-compose
  ],

  // Global setup and teardown
  globalSetup: require.resolve('./fixtures/global-setup.ts'),
  globalTeardown: require.resolve('./fixtures/global-teardown.ts'),

  // Metadata for the test report
  metadata: {
    project: 'Airflow Crawler System',
    version: '1.0.0',
  },
});
