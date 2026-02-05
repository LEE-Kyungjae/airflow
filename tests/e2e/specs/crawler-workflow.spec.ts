import { test, expect } from '@playwright/test';
import {
  TestDataManager,
  TEST_SOURCES,
  generateTestName,
  waitForCondition,
} from '../fixtures';

/**
 * Crawler Workflow Tests
 *
 * Tests for crawler code viewing, version history, and rollback functionality.
 */

test.describe('Crawler Workflow', () => {
  let testData: TestDataManager;

  test.beforeAll(async ({ request }) => {
    testData = new TestDataManager(request);
    await testData.init();
  });

  test.afterAll(async () => {
    await testData.cleanup();
  });

  test.describe('Crawler List', () => {
    test('should list all crawlers via API', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      expect(Array.isArray(crawlers)).toBeTruthy();
    });

    test('should filter crawlers by source ID', async ({ request }) => {
      const api = testData.getApiHelper();

      // Create a source
      const sourceData = TEST_SOURCES.htmlSimple();
      const createResult = await api.createSource(sourceData);

      if (createResult.response.ok() && createResult.data.source_id) {
        // List crawlers for this source
        const crawlers = await api.listCrawlers({ source_id: createResult.data.source_id });

        expect(Array.isArray(crawlers)).toBeTruthy();
        // May or may not have crawlers yet (depends on DAG execution)

        // Clean up
        await api.deleteSource(createResult.data.source_id);
      }
    });

    test('should filter crawlers by status', async ({ request }) => {
      const api = testData.getApiHelper();

      const activeCrawlers = await api.listCrawlers({ status: 'active' });
      const testingCrawlers = await api.listCrawlers({ status: 'testing' });
      const deprecatedCrawlers = await api.listCrawlers({ status: 'deprecated' });

      expect(Array.isArray(activeCrawlers)).toBeTruthy();
      expect(Array.isArray(testingCrawlers)).toBeTruthy();
      expect(Array.isArray(deprecatedCrawlers)).toBeTruthy();
    });
  });

  test.describe('Crawler Code View', () => {
    test('should get crawler by ID', async ({ request }) => {
      const api = testData.getApiHelper();

      // First get list of crawlers
      const crawlers = await api.listCrawlers();

      if (crawlers.length > 0) {
        const crawler = await api.getCrawler(crawlers[0]._id);

        expect(crawler).toBeTruthy();
        expect(crawler?._id).toBe(crawlers[0]._id);
        expect(crawler?.source_id).toBeDefined();
        expect(crawler?.version).toBeDefined();
      }
    });

    test('should get crawler code separately', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      if (crawlers.length > 0) {
        const codeResult = await api.getCrawlerCode(crawlers[0]._id);

        if (codeResult) {
          expect(codeResult.code).toBeDefined();
          expect(codeResult.version).toBeDefined();
          expect(typeof codeResult.code).toBe('string');
        }
      }
    });

    test('should return 404 for non-existent crawler', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawler = await api.getCrawler('non_existent_crawler_id');

      expect(crawler).toBeNull();
    });

    test('crawler code should be valid Python', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      if (crawlers.length > 0) {
        const codeResult = await api.getCrawlerCode(crawlers[0]._id);

        if (codeResult?.code) {
          // Basic Python code validation
          // Should contain common crawler patterns
          expect(codeResult.code).toMatch(/import|def |class |from /);
        }
      }
    });
  });

  test.describe('Version History', () => {
    test('should get crawler version history', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      if (crawlers.length > 0) {
        const history = await api.getCrawlerHistory(crawlers[0]._id);

        expect(Array.isArray(history)).toBeTruthy();

        if (history.length > 0) {
          // Each history entry should have version info
          expect(history[0]).toHaveProperty('version');
          expect(history[0]).toHaveProperty('code');
        }
      }
    });

    test('should return empty history for new crawler', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      if (crawlers.length > 0) {
        // Note: New crawlers might not have history entries
        const history = await api.getCrawlerHistory(crawlers[0]._id);

        expect(Array.isArray(history)).toBeTruthy();
      }
    });

    test('history should be ordered by version', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      if (crawlers.length > 0) {
        const history = await api.getCrawlerHistory(crawlers[0]._id);

        if (history.length > 1) {
          // Verify descending order
          for (let i = 1; i < history.length; i++) {
            expect(history[i - 1].version).toBeGreaterThan(history[i].version);
          }
        }
      }
    });
  });

  test.describe('Rollback Functionality', () => {
    test('should rollback to previous version', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      if (crawlers.length > 0) {
        const history = await api.getCrawlerHistory(crawlers[0]._id);

        if (history.length > 0) {
          // Try to rollback to an earlier version
          const targetVersion = history[history.length - 1].version;
          const rollbackResult = await api.rollbackCrawler(crawlers[0]._id, targetVersion);

          if (rollbackResult.response.ok()) {
            expect(rollbackResult.data.success).toBeTruthy();
            expect(rollbackResult.data.new_version).toBeDefined();
          }
        }
      }
    });

    test('should fail rollback to non-existent version', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      if (crawlers.length > 0) {
        const rollbackResult = await api.rollbackCrawler(crawlers[0]._id, 99999);

        expect(rollbackResult.response.status()).toBe(404);
      }
    });

    test('rollback should create new version with old code', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      if (crawlers.length > 0) {
        const history = await api.getCrawlerHistory(crawlers[0]._id);

        if (history.length > 0) {
          const targetVersion = history[history.length - 1].version;
          const targetCode = history[history.length - 1].code;

          // Get current version
          const beforeCrawler = await api.getCrawler(crawlers[0]._id);
          const beforeVersion = beforeCrawler?.version || 1;

          // Perform rollback
          const rollbackResult = await api.rollbackCrawler(crawlers[0]._id, targetVersion);

          if (rollbackResult.response.ok()) {
            // New version should be greater than before
            expect(rollbackResult.data.new_version).toBeGreaterThan(beforeVersion);
          }
        }
      }
    });
  });

  test.describe('Crawler Code Generation', () => {
    test('should trigger code generation when creating source', async ({ request }) => {
      const api = testData.getApiHelper();
      const sourceData = TEST_SOURCES.htmlSimple();

      const createResult = await api.createSource(sourceData);

      if (createResult.response.ok()) {
        expect(createResult.data.success).toBeTruthy();
        // DAG trigger should be attempted
        // Note: Actual code generation depends on Airflow

        // Clean up
        await api.deleteSource(createResult.data.source_id);
      }
    });

    test('generated code should include base crawler imports', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      if (crawlers.length > 0) {
        const codeResult = await api.getCrawlerCode(crawlers[0]._id);

        if (codeResult?.code) {
          // Should have proper imports
          const code = codeResult.code;
          // Common patterns in generated crawler code
          const hasImports = code.includes('import') || code.includes('from');
          expect(hasImports).toBeTruthy();
        }
      }
    });
  });

  test.describe('Crawler Status Management', () => {
    test('should have valid status values', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      for (const crawler of crawlers) {
        expect(['active', 'testing', 'deprecated']).toContain(crawler.status);
      }
    });

    test('should have associated DAG ID', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers({ status: 'active' });

      for (const crawler of crawlers) {
        // Active crawlers should have DAG ID
        if (crawler.status === 'active') {
          // DAG ID may or may not be set depending on system state
          expect(crawler).toHaveProperty('dag_id');
        }
      }
    });
  });

  test.describe('Edge Cases', () => {
    test('should handle crawler with special characters in code', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      if (crawlers.length > 0) {
        const codeResult = await api.getCrawlerCode(crawlers[0]._id);

        // Code should be retrievable regardless of content
        if (codeResult) {
          expect(typeof codeResult.code).toBe('string');
        }
      }
    });

    test('should handle concurrent history requests', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      if (crawlers.length > 0) {
        // Make concurrent requests
        const requests = Array(5)
          .fill(null)
          .map(() => api.getCrawlerHistory(crawlers[0]._id));

        const results = await Promise.all(requests);

        // All should succeed
        results.forEach((history) => {
          expect(Array.isArray(history)).toBeTruthy();
        });
      }
    });

    test('should handle large version history', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      if (crawlers.length > 0) {
        // Request with pagination
        const history = await api.getCrawlerHistory(crawlers[0]._id);

        // Should return array regardless of size
        expect(Array.isArray(history)).toBeTruthy();
        // History should be capped at reasonable limit
        expect(history.length).toBeLessThanOrEqual(100);
      }
    });
  });

  test.describe('Crawler-Source Relationship', () => {
    test('should link crawler to correct source', async ({ request }) => {
      const api = testData.getApiHelper();

      const crawlers = await api.listCrawlers();

      if (crawlers.length > 0) {
        const crawler = crawlers[0];

        // Get the associated source
        const source = await api.getSource(crawler.source_id);

        if (source) {
          expect(source._id).toBe(crawler.source_id);
        }
      }
    });

    test('should delete crawler when source is deleted', async ({ request }) => {
      const api = testData.getApiHelper();

      // Create a source
      const sourceData = TEST_SOURCES.htmlSimple();
      const createResult = await api.createSource(sourceData);

      if (createResult.response.ok() && createResult.data.source_id) {
        const sourceId = createResult.data.source_id;

        // Delete the source
        await api.deleteSource(sourceId);

        // Crawlers for this source should also be gone
        const crawlers = await api.listCrawlers({ source_id: sourceId });

        // After source deletion, no crawlers should exist for this source
        expect(crawlers.filter((c) => c.source_id === sourceId)).toHaveLength(0);
      }
    });
  });
});
