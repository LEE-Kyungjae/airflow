/**
 * E2E Test Fixtures Index
 *
 * Exports all fixtures for easy importing in test files.
 */

// Authentication fixtures
export {
  test,
  expect,
  loginViaApi,
  refreshToken,
  verifyAuth,
  getCurrentUser,
  setupAuthenticatedContext,
  createAuthHeaders,
  TEST_USERS,
  type AuthTokens,
  type TestUser,
} from './auth.fixture';

// API helper fixtures
export {
  ApiHelper,
  createAuthenticatedApiHelper,
  createApiHelper,
  type Source,
  type FieldDefinition,
  type Crawler,
  type DashboardStats,
  type Review,
} from './api.fixture';

// Test data fixtures
export {
  TestDataManager,
  TEST_SOURCES,
  TEST_FIELDS,
  TEST_REVIEWS,
  generateTestName,
  waitForCondition,
  randomString,
  formatTestDate,
} from './data.fixture';
