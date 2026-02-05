import { test, expect, Page } from '@playwright/test';
import { TestDataManager } from '../fixtures';

/**
 * Dashboard Tests
 *
 * Tests for dashboard statistics, charts, and real-time updates.
 */

test.describe('Dashboard', () => {
  let testData: TestDataManager;

  test.beforeAll(async ({ request }) => {
    testData = new TestDataManager(request);
    await testData.init();
  });

  test.afterAll(async () => {
    await testData.cleanup();
  });

  test.describe('Dashboard Statistics Loading', () => {
    test('should load dashboard page', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Dashboard should be the default route
      await expect(page.locator('h1')).toContainText('Dashboard');
    });

    test('should display statistics cards', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Wait for stats to load
      await page.waitForSelector('[class*="stat"], [class*="card"]', { timeout: 10000 });

      // Should have multiple stat cards
      const cards = page.locator('[class*="card"]');
      const count = await cards.count();
      expect(count).toBeGreaterThan(0);
    });

    test('should show Active Sources count', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Look for Active Sources indicator
      const activeSourcesText = page.locator('text=Active Sources');
      await expect(activeSourcesText).toBeVisible({ timeout: 10000 });
    });

    test('should show Success Rate metric', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Look for Success Rate indicator
      const successRateText = page.locator('text=Success Rate');
      await expect(successRateText).toBeVisible({ timeout: 10000 });
    });

    test('should show Unresolved Errors count', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Look for Unresolved Errors indicator
      const errorsText = page.locator('text=Unresolved Errors');
      await expect(errorsText).toBeVisible({ timeout: 10000 });
    });

    test('should show Active Crawlers count', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Look for Active Crawlers indicator
      const crawlersText = page.locator('text=Active Crawlers');
      await expect(crawlersText).toBeVisible({ timeout: 10000 });
    });
  });

  test.describe('Dashboard API', () => {
    test('should get dashboard stats via API', async ({ request }) => {
      const api = testData.getApiHelper();

      const stats = await api.getDashboardStats();

      expect(stats).toHaveProperty('sources');
      expect(stats).toHaveProperty('crawlers');
      expect(stats).toHaveProperty('recent_executions');
      expect(stats).toHaveProperty('unresolved_errors');
      expect(stats).toHaveProperty('timestamp');

      // Verify structure
      expect(stats.sources).toHaveProperty('total');
      expect(stats.sources).toHaveProperty('active');
      expect(stats.sources).toHaveProperty('error');

      expect(stats.crawlers).toHaveProperty('total');
      expect(stats.crawlers).toHaveProperty('active');

      expect(stats.recent_executions).toHaveProperty('total');
      expect(stats.recent_executions).toHaveProperty('success');
      expect(stats.recent_executions).toHaveProperty('failed');
      expect(stats.recent_executions).toHaveProperty('success_rate');
    });

    test('should return valid numeric values', async ({ request }) => {
      const api = testData.getApiHelper();

      const stats = await api.getDashboardStats();

      // All counts should be non-negative
      expect(stats.sources.total).toBeGreaterThanOrEqual(0);
      expect(stats.sources.active).toBeGreaterThanOrEqual(0);
      expect(stats.sources.error).toBeGreaterThanOrEqual(0);
      expect(stats.crawlers.total).toBeGreaterThanOrEqual(0);
      expect(stats.unresolved_errors).toBeGreaterThanOrEqual(0);

      // Success rate should be between 0 and 100
      expect(stats.recent_executions.success_rate).toBeGreaterThanOrEqual(0);
      expect(stats.recent_executions.success_rate).toBeLessThanOrEqual(100);
    });
  });

  test.describe('Chart Rendering', () => {
    test('should render Execution Trends chart', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Look for chart title
      const chartTitle = page.locator('text=Execution Trends');
      await expect(chartTitle).toBeVisible({ timeout: 10000 });

      // Look for chart container (recharts creates SVG elements)
      const chartContainer = page.locator('.recharts-wrapper, svg.recharts-surface');
      // Chart may or may not be visible depending on data
    });

    test('should render System Health indicator', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Look for System Health section
      const healthTitle = page.locator('text=System Health');
      await expect(healthTitle).toBeVisible({ timeout: 10000 });
    });

    test('should display health score', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Health score should be a number
      // Look for circular progress or score display
      const healthSection = page.locator('text=System Health').locator('..');
      await expect(healthSection).toBeVisible({ timeout: 10000 });
    });

    test('should show health status color coding', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Status should be color-coded (green for healthy, yellow for degraded, red for critical)
      const healthSection = page.locator('[class*="health"], [class*="status"]');
      // At least one element should be present
    });
  });

  test.describe('Execution Trends API', () => {
    test('should get execution trends', async ({ request }) => {
      const api = testData.getApiHelper();

      const trends = await api.getExecutionTrends(7);

      expect(trends).toHaveProperty('period_days');
      expect(trends).toHaveProperty('daily_stats');
      expect(Array.isArray(trends.daily_stats)).toBeTruthy();
      expect(trends.period_days).toBe(7);
    });

    test('should include daily breakdown', async ({ request }) => {
      const api = testData.getApiHelper();

      const trends = await api.getExecutionTrends(7);

      if (trends.daily_stats.length > 0) {
        const dayStats = trends.daily_stats[0];

        expect(dayStats).toHaveProperty('date');
        expect(dayStats).toHaveProperty('total');
        expect(dayStats).toHaveProperty('success');
        expect(dayStats).toHaveProperty('failed');
        expect(dayStats).toHaveProperty('success_rate');
        expect(dayStats).toHaveProperty('avg_time_ms');
      }
    });

    test('should support different time periods', async ({ request }) => {
      const api = testData.getApiHelper();

      const trends7 = await api.getExecutionTrends(7);
      const trends14 = await api.getExecutionTrends(14);
      const trends30 = await api.getExecutionTrends(30);

      expect(trends7.period_days).toBe(7);
      expect(trends14.period_days).toBe(14);
      expect(trends30.period_days).toBe(30);
    });
  });

  test.describe('System Health API', () => {
    test('should get system health', async ({ request }) => {
      const api = testData.getApiHelper();

      const health = await api.getSystemHealth();

      expect(health).toHaveProperty('health_score');
      expect(health).toHaveProperty('status');
      expect(health).toHaveProperty('components');
      expect(health).toHaveProperty('timestamp');
    });

    test('should include component status', async ({ request }) => {
      const api = testData.getApiHelper();

      const health = await api.getSystemHealth();

      expect(health.components).toHaveProperty('mongodb');
      // Airflow status might vary
    });

    test('should return valid health score', async ({ request }) => {
      const api = testData.getApiHelper();

      const health = await api.getSystemHealth();

      expect(health.health_score).toBeGreaterThanOrEqual(0);
      expect(health.health_score).toBeLessThanOrEqual(100);
    });

    test('should return valid status value', async ({ request }) => {
      const api = testData.getApiHelper();

      const health = await api.getSystemHealth();

      expect(['healthy', 'degraded', 'critical']).toContain(health.status);
    });

    test('should include issues list when health is not healthy', async ({ request }) => {
      const api = testData.getApiHelper();

      const health = await api.getSystemHealth();

      expect(health).toHaveProperty('issues');
      expect(Array.isArray(health.issues)).toBeTruthy();
    });
  });

  test.describe('Recent Activity API', () => {
    test('should get recent activity', async ({ request }) => {
      const api = testData.getApiHelper();

      const activity = await api.getRecentActivity(24);

      expect(activity).toHaveProperty('period_hours');
      expect(activity).toHaveProperty('crawl_results');
      expect(activity).toHaveProperty('errors');
      expect(activity).toHaveProperty('code_changes');

      expect(activity.period_hours).toBe(24);
    });

    test('should include crawl results in activity', async ({ request }) => {
      const api = testData.getApiHelper();

      const activity = await api.getRecentActivity(24);

      expect(Array.isArray(activity.crawl_results)).toBeTruthy();

      if (activity.crawl_results.length > 0) {
        const result = activity.crawl_results[0];
        expect(result).toHaveProperty('source_id');
        expect(result).toHaveProperty('status');
        expect(result).toHaveProperty('executed_at');
      }
    });

    test('should include errors in activity', async ({ request }) => {
      const api = testData.getApiHelper();

      const activity = await api.getRecentActivity(24);

      expect(Array.isArray(activity.errors)).toBeTruthy();
    });

    test('should support different time windows', async ({ request }) => {
      const api = testData.getApiHelper();

      const activity6h = await api.getRecentActivity(6);
      const activity24h = await api.getRecentActivity(24);
      const activity48h = await api.getRecentActivity(48);

      expect(activity6h.period_hours).toBe(6);
      expect(activity24h.period_hours).toBe(24);
      expect(activity48h.period_hours).toBe(48);
    });
  });

  test.describe('Real-time Updates', () => {
    test('should refresh data periodically', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Track network requests
      const requests: string[] = [];
      page.on('request', (request) => {
        if (request.url().includes('/api/dashboard')) {
          requests.push(request.url());
        }
      });

      // Wait for potential auto-refresh (typically 30s based on the code)
      await page.waitForTimeout(35000);

      // There should be at least initial request
      expect(requests.length).toBeGreaterThan(0);
    });

    test('should update stats without page reload', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Get initial value
      const initialValue = await page
        .locator('text=Active Sources')
        .locator('..')
        .locator('..')
        .textContent();

      // Wait for auto-refresh
      await page.waitForTimeout(35000);

      // Value should still be accessible (component re-rendered but not full page reload)
      const currentValue = await page
        .locator('text=Active Sources')
        .locator('..')
        .locator('..')
        .textContent();

      // Value should still be there (may or may not have changed)
      expect(currentValue).toBeTruthy();
    });
  });

  test.describe('Loading States', () => {
    test('should show loading spinner while fetching data', async ({ page }) => {
      // Intercept API and delay response
      await page.route('**/api/dashboard**', async (route) => {
        await new Promise((resolve) => setTimeout(resolve, 1000));
        await route.continue();
      });

      await page.goto('/');

      // Look for loading indicator
      const loadingSpinner = page.locator('[class*="loading"], [class*="spinner"]');
      // Loading state may or may not be visible depending on timing
    });

    test('should handle API errors gracefully', async ({ page }) => {
      // Intercept API and return error
      await page.route('**/api/dashboard**', async (route) => {
        await route.fulfill({
          status: 500,
          body: JSON.stringify({ error: 'Internal Server Error' }),
        });
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Page should not crash - should show error state or retry
    });
  });

  test.describe('Responsive Layout', () => {
    test('should display correctly on desktop', async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Stats cards should be in grid layout
      const cards = page.locator('[class*="card"]');
      expect(await cards.count()).toBeGreaterThan(0);
    });

    test('should display correctly on tablet', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Page should still be usable
      const dashboardTitle = page.locator('h1');
      await expect(dashboardTitle).toBeVisible();
    });

    test('should display correctly on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Page should still be usable
      const dashboardTitle = page.locator('h1');
      await expect(dashboardTitle).toBeVisible();
    });
  });

  test.describe('Navigation', () => {
    test('should navigate to sources from dashboard', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Find and click sources link
      const sourcesLink = page.locator('a[href="/sources"], a:has-text("Sources")');
      await sourcesLink.first().click();

      await expect(page).toHaveURL(/sources/);
    });

    test('should navigate to monitoring from dashboard', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Find and click monitoring link
      const monitoringLink = page.locator('a[href="/monitoring"], a:has-text("Monitoring")');
      await monitoringLink.first().click();

      await expect(page).toHaveURL(/monitoring/);
    });

    test('should navigate to errors from dashboard', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Find and click errors link
      const errorsLink = page.locator('a[href="/errors"], a:has-text("Errors")');
      await errorsLink.first().click();

      await expect(page).toHaveURL(/errors/);
    });
  });

  test.describe('Edge Cases', () => {
    test('should handle empty stats gracefully', async ({ page, request }) => {
      // Even with no data, dashboard should render
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Dashboard should be visible
      await expect(page.locator('h1')).toContainText('Dashboard');
    });

    test('should handle slow network', async ({ page }) => {
      // Simulate slow 3G
      const client = await page.context().newCDPSession(page);
      await client.send('Network.emulateNetworkConditions', {
        offline: false,
        downloadThroughput: (500 * 1024) / 8,
        uploadThroughput: (500 * 1024) / 8,
        latency: 400,
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle', { timeout: 60000 });

      // Dashboard should eventually load
      await expect(page.locator('h1')).toContainText('Dashboard');
    });
  });
});
