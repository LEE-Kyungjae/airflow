import { APIRequestContext } from '@playwright/test';
import { ApiHelper, createAuthenticatedApiHelper, Source, FieldDefinition } from './api.fixture';

/**
 * Test Data Fixture
 *
 * Provides test data generation and cleanup utilities for E2E tests.
 * All test data is prefixed with 'e2e_test_' for easy cleanup.
 */

const TEST_PREFIX = 'e2e_test_';

// ============================================
// Test Data Generators
// ============================================

/**
 * Generate a unique test name with timestamp
 */
export function generateTestName(baseName: string): string {
  const timestamp = Date.now();
  return `${TEST_PREFIX}${baseName}_${timestamp}`;
}

/**
 * Test source data templates
 */
export const TEST_SOURCES = {
  htmlSimple: (name?: string): Partial<Source> => ({
    name: name || generateTestName('html_source'),
    url: 'https://example.com/data',
    type: 'html',
    fields: [
      { name: 'title', selector: 'h1', data_type: 'string' },
      { name: 'content', selector: 'p', data_type: 'string' },
    ],
    schedule: '0 */6 * * *',
  }),

  htmlTable: (name?: string): Partial<Source> => ({
    name: name || generateTestName('html_table'),
    url: 'https://example.com/table',
    type: 'html',
    fields: [
      { name: 'column1', selector: 'td:nth-child(1)', data_type: 'string' },
      { name: 'column2', selector: 'td:nth-child(2)', data_type: 'number' },
      { name: 'column3', selector: 'td:nth-child(3)', data_type: 'date' },
    ],
    schedule: '0 9 * * 1-5',
  }),

  pdfDocument: (name?: string): Partial<Source> => ({
    name: name || generateTestName('pdf_source'),
    url: 'https://example.com/document.pdf',
    type: 'pdf',
    fields: [
      { name: 'title', selector: '//*[@class="title"]', data_type: 'string' },
      { name: 'date', selector: '//*[@class="date"]', data_type: 'date' },
    ],
    schedule: '0 0 * * *',
  }),

  excelFile: (name?: string): Partial<Source> => ({
    name: name || generateTestName('excel_source'),
    url: 'https://example.com/data.xlsx',
    type: 'excel',
    fields: [
      { name: 'name', selector: 'A', data_type: 'string' },
      { name: 'value', selector: 'B', data_type: 'number' },
    ],
    schedule: '0 8 * * 1',
  }),

  csvFile: (name?: string): Partial<Source> => ({
    name: name || generateTestName('csv_source'),
    url: 'https://example.com/data.csv',
    type: 'csv',
    fields: [
      { name: 'id', selector: '0', data_type: 'number' },
      { name: 'name', selector: '1', data_type: 'string' },
    ],
    schedule: '*/30 * * * *',
  }),

  // Invalid sources for negative testing
  invalidUrl: (name?: string): Partial<Source> => ({
    name: name || generateTestName('invalid_url'),
    url: 'not-a-valid-url',
    type: 'html',
    fields: [],
  }),

  missingFields: (name?: string): Partial<Source> => ({
    name: name || generateTestName('missing_fields'),
    url: 'https://example.com/data',
    type: 'html',
    // fields intentionally missing
  }),

  duplicateName: (existingName: string): Partial<Source> => ({
    name: existingName, // Using existing name to test conflict
    url: 'https://example.com/other',
    type: 'html',
    fields: [{ name: 'test', selector: 'div', data_type: 'string' }],
  }),
};

/**
 * Test field definitions
 */
export const TEST_FIELDS: Record<string, FieldDefinition[]> = {
  newsArticle: [
    { name: 'title', selector: '.article-title', data_type: 'string' },
    { name: 'author', selector: '.author-name', data_type: 'string' },
    { name: 'date', selector: '.publish-date', data_type: 'date' },
    { name: 'content', selector: '.article-body', data_type: 'string' },
    { name: 'link', selector: '.article-link', data_type: 'url', attribute: 'href' },
  ],

  financialData: [
    { name: 'symbol', selector: '.stock-symbol', data_type: 'string' },
    { name: 'price', selector: '.current-price', data_type: 'number' },
    { name: 'change', selector: '.price-change', data_type: 'number' },
    { name: 'volume', selector: '.trade-volume', data_type: 'number' },
  ],

  productListing: [
    { name: 'name', selector: '.product-name', data_type: 'string' },
    { name: 'price', selector: '.product-price', data_type: 'number' },
    { name: 'image', selector: '.product-image', data_type: 'url', attribute: 'src' },
    { name: 'description', selector: '.product-desc', data_type: 'string' },
  ],
};

/**
 * Test review data
 */
export const TEST_REVIEWS = {
  pendingReview: {
    review_status: 'pending',
    original_data: {
      title: 'Test Article',
      content: 'Sample content for testing',
      date: '2025-02-05',
    },
    confidence_score: 0.85,
  },

  lowConfidenceReview: {
    review_status: 'pending',
    original_data: {
      value: '123,456',
      name: 'Test Item',
    },
    confidence_score: 0.45,
    needs_number_review: true,
    uncertain_numbers: [{ field: 'value', original: '123,456', confidence: 0.4 }],
  },
};

// ============================================
// Test Data Manager
// ============================================

export class TestDataManager {
  private createdSources: string[] = [];
  private apiHelper: ApiHelper | null = null;

  constructor(private request: APIRequestContext) {}

  /**
   * Initialize with authentication
   */
  async init(): Promise<void> {
    this.apiHelper = await createAuthenticatedApiHelper(this.request);
  }

  /**
   * Create a test source
   */
  async createTestSource(
    template: 'htmlSimple' | 'htmlTable' | 'pdfDocument' | 'excelFile' | 'csvFile' = 'htmlSimple'
  ): Promise<{ id: string; name: string }> {
    if (!this.apiHelper) {
      await this.init();
    }

    const sourceData = TEST_SOURCES[template]();
    const result = await this.apiHelper!.createSource(sourceData);

    if (result.response.ok() && result.data?.source_id) {
      this.createdSources.push(result.data.source_id);
      return { id: result.data.source_id, name: sourceData.name! };
    }

    throw new Error(`Failed to create test source: ${await result.response.text()}`);
  }

  /**
   * Create a test source with custom data
   */
  async createCustomSource(sourceData: Partial<Source>): Promise<{ id: string; name: string }> {
    if (!this.apiHelper) {
      await this.init();
    }

    // Ensure name has test prefix
    if (sourceData.name && !sourceData.name.startsWith(TEST_PREFIX)) {
      sourceData.name = `${TEST_PREFIX}${sourceData.name}`;
    }

    const result = await this.apiHelper!.createSource(sourceData);

    if (result.response.ok() && result.data?.source_id) {
      this.createdSources.push(result.data.source_id);
      return { id: result.data.source_id, name: sourceData.name! };
    }

    throw new Error(`Failed to create custom source: ${await result.response.text()}`);
  }

  /**
   * Get API helper instance
   */
  getApiHelper(): ApiHelper {
    if (!this.apiHelper) {
      throw new Error('TestDataManager not initialized. Call init() first.');
    }
    return this.apiHelper;
  }

  /**
   * Clean up all created test data
   */
  async cleanup(): Promise<void> {
    if (!this.apiHelper) return;

    for (const sourceId of this.createdSources) {
      try {
        await this.apiHelper.deleteSource(sourceId);
      } catch (error) {
        console.warn(`Failed to delete test source ${sourceId}:`, error);
      }
    }

    this.createdSources = [];
  }

  /**
   * Clean up all test data (including data from previous runs)
   */
  async cleanupAllTestData(): Promise<void> {
    if (!this.apiHelper) {
      await this.init();
    }

    try {
      const sources = await this.apiHelper!.listSources();
      const testSources = sources.filter((s) => s.name.startsWith(TEST_PREFIX));

      for (const source of testSources) {
        try {
          await this.apiHelper!.deleteSource(source._id);
          console.log(`Cleaned up test source: ${source.name}`);
        } catch (error) {
          console.warn(`Failed to delete ${source.name}:`, error);
        }
      }
    } catch (error) {
      console.warn('Failed to cleanup test data:', error);
    }
  }
}

// ============================================
// Utility Functions
// ============================================

/**
 * Wait for a condition with timeout
 */
export async function waitForCondition(
  condition: () => Promise<boolean>,
  options: { timeout?: number; interval?: number; message?: string } = {}
): Promise<void> {
  const { timeout = 30000, interval = 1000, message = 'Condition not met' } = options;
  const startTime = Date.now();

  while (Date.now() - startTime < timeout) {
    if (await condition()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, interval));
  }

  throw new Error(`Timeout: ${message}`);
}

/**
 * Generate random string for unique identifiers
 */
export function randomString(length = 8): string {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

/**
 * Format date for test comparison
 */
export function formatTestDate(date: Date = new Date()): string {
  return date.toISOString().split('T')[0];
}
