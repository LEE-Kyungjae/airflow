import { test as base, expect, APIRequestContext, BrowserContext, Page } from '@playwright/test';

/**
 * Authentication Fixture
 *
 * Provides authenticated user context for tests.
 * - authenticatedPage: Page with logged-in user session
 * - adminContext: API context with admin token
 * - authToken: Raw JWT token for API calls
 */

const API_URL = process.env.API_URL || 'http://localhost:8000';

export interface AuthTokens {
  access_token: string;
  refresh_token: string | null;
  token_type: string;
  expires_in: number;
}

export interface TestUser {
  username: string;
  password: string;
  role: 'admin' | 'user';
}

// Default test users
export const TEST_USERS: Record<string, TestUser> = {
  admin: {
    username: 'admin',
    password: process.env.ADMIN_PASSWORD || 'admin123',
    role: 'admin',
  },
};

/**
 * Login via API and get authentication tokens
 */
export async function loginViaApi(
  request: APIRequestContext,
  user: TestUser = TEST_USERS.admin
): Promise<AuthTokens> {
  const response = await request.post(`${API_URL}/api/auth/login`, {
    data: {
      username: user.username,
      password: user.password,
    },
  });

  if (!response.ok()) {
    const errorBody = await response.text();
    throw new Error(`Login failed: ${response.status()} ${errorBody}`);
  }

  return response.json();
}

/**
 * Refresh authentication token
 */
export async function refreshToken(
  request: APIRequestContext,
  refreshToken: string
): Promise<AuthTokens> {
  const response = await request.post(`${API_URL}/api/auth/refresh`, {
    data: {
      refresh_token: refreshToken,
    },
  });

  if (!response.ok()) {
    throw new Error(`Token refresh failed: ${response.status()}`);
  }

  return response.json();
}

/**
 * Verify authentication status
 */
export async function verifyAuth(
  request: APIRequestContext,
  token: string
): Promise<{ authenticated: boolean; user_id: string; scopes: string[] }> {
  const response = await request.get(`${API_URL}/api/auth/verify`, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok()) {
    throw new Error(`Auth verification failed: ${response.status()}`);
  }

  return response.json();
}

/**
 * Get current user information
 */
export async function getCurrentUser(
  request: APIRequestContext,
  token: string
): Promise<any> {
  const response = await request.get(`${API_URL}/api/auth/me`, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok()) {
    throw new Error(`Get user failed: ${response.status()}`);
  }

  return response.json();
}

/**
 * Set up authenticated browser context
 */
export async function setupAuthenticatedContext(
  context: BrowserContext,
  tokens: AuthTokens
): Promise<void> {
  // Store token in localStorage
  await context.addInitScript((token) => {
    window.localStorage.setItem('access_token', token);
  }, tokens.access_token);

  if (tokens.refresh_token) {
    await context.addInitScript((token) => {
      window.localStorage.setItem('refresh_token', token);
    }, tokens.refresh_token);
  }
}

/**
 * Create authenticated API headers
 */
export function createAuthHeaders(token: string): Record<string, string> {
  return {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
  };
}

// Extended test fixture with authentication
type AuthFixtures = {
  authenticatedPage: Page;
  authTokens: AuthTokens;
  adminApiContext: APIRequestContext;
};

export const test = base.extend<AuthFixtures>({
  // Authenticated page fixture
  authenticatedPage: async ({ page, context, request }, use) => {
    // Login and get tokens
    const tokens = await loginViaApi(request);

    // Set up authentication in browser context
    await setupAuthenticatedContext(context, tokens);

    // Navigate to app to apply localStorage
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    await use(page);
  },

  // Auth tokens fixture
  authTokens: async ({ request }, use) => {
    const tokens = await loginViaApi(request);
    await use(tokens);
  },

  // Admin API context fixture
  adminApiContext: async ({ playwright }, use) => {
    const context = await playwright.request.newContext({
      baseURL: API_URL,
    });

    // Login and set authorization header
    const response = await context.post('/api/auth/login', {
      data: {
        username: TEST_USERS.admin.username,
        password: TEST_USERS.admin.password,
      },
    });

    if (!response.ok()) {
      throw new Error('Admin login failed');
    }

    const tokens = await response.json();

    // Create new context with auth header
    const authContext = await playwright.request.newContext({
      baseURL: API_URL,
      extraHTTPHeaders: {
        Authorization: `Bearer ${tokens.access_token}`,
      },
    });

    await use(authContext);

    // Cleanup
    await context.dispose();
    await authContext.dispose();
  },
});

export { expect };
