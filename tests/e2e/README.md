# E2E Tests - Playwright

End-to-end tests for the Airflow Crawler System using Playwright.

## Prerequisites

1. **Node.js** (v18 or higher)
2. **API Server** running on `http://localhost:8000`
3. **Frontend Server** running on `http://localhost:5173`

## Installation

```bash
# From the frontend directory
cd frontend
npm install

# Install Playwright browsers
npx playwright install
```

## Running Tests

### All Tests

```bash
npm run test:e2e
```

### Interactive UI Mode

```bash
npm run test:e2e:ui
```

### Headed Mode (See Browser)

```bash
npm run test:e2e:headed
```

### Debug Mode

```bash
npm run test:e2e:debug
```

### Specific Browser

```bash
npm run test:e2e:chromium
npm run test:e2e:firefox
npm run test:e2e:webkit
```

### View Test Report

```bash
npm run test:e2e:report
```

## Test Structure

```
tests/e2e/
├── playwright.config.ts    # Playwright configuration
├── tsconfig.json           # TypeScript configuration
├── fixtures/               # Test fixtures and utilities
│   ├── index.ts            # Exports all fixtures
│   ├── auth.fixture.ts     # Authentication helpers
│   ├── api.fixture.ts      # API client helpers
│   ├── data.fixture.ts     # Test data generators
│   ├── global-setup.ts     # Global setup (runs once before all tests)
│   └── global-teardown.ts  # Global teardown (runs once after all tests)
├── specs/                  # Test specifications
│   ├── auth.spec.ts        # Authentication flow tests
│   ├── source-management.spec.ts  # Source CRUD tests
│   ├── crawler-workflow.spec.ts   # Crawler code and version tests
│   ├── review-workflow.spec.ts    # Data review workflow tests
│   └── dashboard.spec.ts   # Dashboard and statistics tests
└── README.md               # This file
```

## Test Categories

### 1. Authentication Tests (`auth.spec.ts`)

- Login success/failure
- JWT token storage and persistence
- Token refresh
- Logout
- Authorization scopes

### 2. Source Management Tests (`source-management.spec.ts`)

- Quick Add workflow
- Source list and filtering
- Source CRUD operations (via API)
- Manual execution trigger
- Field definitions

### 3. Crawler Workflow Tests (`crawler-workflow.spec.ts`)

- Crawler list and filtering
- Code viewing
- Version history
- Rollback functionality

### 4. Review Workflow Tests (`review-workflow.spec.ts`)

- Review dashboard
- Review queue navigation
- Approval/rejection/correction workflows
- Batch operations
- Keyboard shortcuts

### 5. Dashboard Tests (`dashboard.spec.ts`)

- Statistics loading
- Chart rendering
- System health display
- Real-time updates
- Responsive layout

## Configuration

### Environment Variables

```bash
# API server URL (default: http://localhost:8000)
API_URL=http://localhost:8000

# Frontend URL (default: http://localhost:5173)
FRONTEND_URL=http://localhost:5173

# Admin password (default: admin123)
ADMIN_PASSWORD=admin123

# CI mode (set automatically in CI environments)
CI=true
```

### Timeouts

- Default test timeout: 30 seconds
- Expect timeout: 10 seconds
- Action timeout: 10 seconds
- Navigation timeout: 30 seconds

### Browsers

Tests run on:
- Chromium (primary)
- Firefox
- WebKit (Safari)
- Mobile Chrome (Pixel 5)
- Mobile Safari (iPhone 12)

## Writing New Tests

### Basic Test Structure

```typescript
import { test, expect } from '@playwright/test';
import { TestDataManager, createAuthenticatedApiHelper } from '../fixtures';

test.describe('Feature Name', () => {
  let testData: TestDataManager;

  test.beforeAll(async ({ request }) => {
    testData = new TestDataManager(request);
    await testData.init();
  });

  test.afterAll(async () => {
    await testData.cleanup();
  });

  test('should do something', async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('h1')).toContainText('Expected Text');
  });

  test('should call API correctly', async ({ request }) => {
    const api = testData.getApiHelper();
    const result = await api.someMethod();
    expect(result).toBeDefined();
  });
});
```

### Using Fixtures

```typescript
import {
  test,
  expect,
  TestDataManager,
  TEST_SOURCES,
  generateTestName,
} from '../fixtures';

test('should create source', async ({ request }) => {
  const testData = new TestDataManager(request);
  await testData.init();

  const sourceData = TEST_SOURCES.htmlSimple();
  const api = testData.getApiHelper();
  const result = await api.createSource(sourceData);

  expect(result.response.ok()).toBeTruthy();

  await testData.cleanup();
});
```

## Best Practices

1. **Test Isolation**: Each test should be independent and not rely on state from other tests.

2. **Cleanup**: Always clean up test data in `afterAll` or `afterEach` hooks.

3. **Test Data Prefix**: Use `e2e_test_` prefix for all test data to enable easy cleanup.

4. **Waiting**: Use Playwright's built-in waiting mechanisms instead of arbitrary timeouts.

5. **Assertions**: Make specific assertions about what you expect to see.

6. **API vs UI**: Use API tests for business logic, UI tests for user workflows.

## Troubleshooting

### Tests Fail with Network Errors

Ensure both API and frontend servers are running:

```bash
# Terminal 1: Start API
cd api && uvicorn app.main:app --reload

# Terminal 2: Start Frontend
cd frontend && npm run dev
```

### Authentication Failures

Check that the admin password is correctly set:

```bash
export ADMIN_PASSWORD=admin123
```

### Browser Installation

If browsers are not installed:

```bash
npx playwright install
```

### Debugging

Run in debug mode with step-through:

```bash
npm run test:e2e:debug
```

Or use Playwright Inspector:

```bash
PWDEBUG=1 npm run test:e2e
```

## CI/CD Integration

The tests are configured to work in CI environments:

```yaml
# Example GitHub Actions workflow
- name: Run E2E Tests
  run: |
    cd frontend
    npm ci
    npx playwright install --with-deps
    npm run test:e2e
  env:
    CI: true
    API_URL: http://localhost:8000
    FRONTEND_URL: http://localhost:5173
```

## Reports

Test reports are generated in:
- `tests/e2e/playwright-report/` - HTML report
- `tests/e2e/test-results/` - JSON results and artifacts

Screenshots and videos are captured on test failure.
