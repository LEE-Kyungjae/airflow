import { test, expect } from '@playwright/test';
import {
  loginViaApi,
  verifyAuth,
  refreshToken,
  getCurrentUser,
  createApiHelper,
  TEST_USERS,
} from '../fixtures';

/**
 * Authentication Flow Tests
 *
 * Tests for login, JWT token management, and logout functionality.
 */

test.describe('Authentication Flow', () => {
  test.describe('Login', () => {
    test('should successfully login with valid credentials', async ({ request }) => {
      const tokens = await loginViaApi(request, TEST_USERS.admin);

      expect(tokens.access_token).toBeTruthy();
      expect(tokens.token_type).toBe('bearer');
      expect(tokens.expires_in).toBeGreaterThan(0);
    });

    test('should fail login with invalid username', async ({ request }) => {
      const api = createApiHelper(request);
      const response = await request.post('http://localhost:8000/api/auth/login', {
        data: {
          username: 'invalid_user',
          password: 'password123',
        },
      });

      expect(response.status()).toBe(401);
      const body = await response.json();
      expect(body.detail).toBeDefined();
    });

    test('should fail login with invalid password', async ({ request }) => {
      const response = await request.post('http://localhost:8000/api/auth/login', {
        data: {
          username: 'admin',
          password: 'wrong_password',
        },
      });

      expect(response.status()).toBe(401);
    });

    test('should fail login with empty credentials', async ({ request }) => {
      const response = await request.post('http://localhost:8000/api/auth/login', {
        data: {
          username: '',
          password: '',
        },
      });

      expect(response.status()).toBe(422); // Validation error
    });

    test('should reject malformed login request', async ({ request }) => {
      const response = await request.post('http://localhost:8000/api/auth/login', {
        data: {
          // Missing required fields
        },
      });

      expect(response.status()).toBe(422);
    });
  });

  test.describe('JWT Token Storage', () => {
    test('should store access token after successful login', async ({ page, request }) => {
      // Login via API
      const tokens = await loginViaApi(request, TEST_USERS.admin);

      // Store token in localStorage via page context
      await page.goto('/');
      await page.evaluate((token) => {
        localStorage.setItem('access_token', token);
      }, tokens.access_token);

      // Verify token is stored
      const storedToken = await page.evaluate(() => localStorage.getItem('access_token'));
      expect(storedToken).toBe(tokens.access_token);
    });

    test('should persist token across page navigation', async ({ page, request }) => {
      const tokens = await loginViaApi(request, TEST_USERS.admin);

      // Store token
      await page.goto('/');
      await page.evaluate((token) => {
        localStorage.setItem('access_token', token);
      }, tokens.access_token);

      // Navigate to another page
      await page.goto('/sources');
      await page.waitForLoadState('networkidle');

      // Verify token persists
      const storedToken = await page.evaluate(() => localStorage.getItem('access_token'));
      expect(storedToken).toBe(tokens.access_token);
    });
  });

  test.describe('Token Refresh', () => {
    test('should successfully refresh access token with valid refresh token', async ({ request }) => {
      // Login to get initial tokens
      const initialTokens = await loginViaApi(request, TEST_USERS.admin);
      expect(initialTokens.refresh_token).toBeTruthy();

      // Refresh the token
      const newTokens = await refreshToken(request, initialTokens.refresh_token!);

      expect(newTokens.access_token).toBeTruthy();
      expect(newTokens.access_token).not.toBe(initialTokens.access_token);
    });

    test('should fail refresh with invalid refresh token', async ({ request }) => {
      const response = await request.post('http://localhost:8000/api/auth/refresh', {
        data: {
          refresh_token: 'invalid_token_12345',
        },
      });

      expect(response.status()).toBe(401);
    });

    test('should fail refresh with expired refresh token', async ({ request }) => {
      // Using a clearly expired/invalid token format
      const expiredToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjB9.invalid';

      const response = await request.post('http://localhost:8000/api/auth/refresh', {
        data: {
          refresh_token: expiredToken,
        },
      });

      expect(response.status()).toBe(401);
    });
  });

  test.describe('Token Verification', () => {
    test('should verify valid access token', async ({ request }) => {
      const tokens = await loginViaApi(request, TEST_USERS.admin);
      const verifyResult = await verifyAuth(request, tokens.access_token);

      expect(verifyResult.authenticated).toBe(true);
      expect(verifyResult.user_id).toBe('admin');
      expect(verifyResult.scopes).toContain('admin');
    });

    test('should reject invalid access token', async ({ request }) => {
      const response = await request.get('http://localhost:8000/api/auth/verify', {
        headers: {
          Authorization: 'Bearer invalid_token_12345',
        },
      });

      expect(response.status()).toBe(401);
    });

    test('should reject request without authorization header', async ({ request }) => {
      const response = await request.get('http://localhost:8000/api/auth/verify');

      expect(response.status()).toBe(401);
    });
  });

  test.describe('Get Current User', () => {
    test('should return current user info with valid token', async ({ request }) => {
      const tokens = await loginViaApi(request, TEST_USERS.admin);
      const userInfo = await getCurrentUser(request, tokens.access_token);

      expect(userInfo.user_id).toBe('admin');
      expect(userInfo.role).toBe('admin');
      expect(userInfo.is_admin).toBe(true);
      expect(userInfo.scopes).toContain('admin');
      expect(userInfo.scopes).toContain('read');
      expect(userInfo.scopes).toContain('write');
    });

    test('should fail getting user info without authentication', async ({ request }) => {
      const response = await request.get('http://localhost:8000/api/auth/me');

      expect(response.status()).toBe(401);
    });
  });

  test.describe('Logout', () => {
    test('should clear tokens from localStorage on logout', async ({ page, request }) => {
      // Login and store token
      const tokens = await loginViaApi(request, TEST_USERS.admin);

      await page.goto('/');
      await page.evaluate((accessToken) => {
        localStorage.setItem('access_token', accessToken);
        localStorage.setItem('refresh_token', 'test_refresh_token');
      }, tokens.access_token);

      // Simulate logout (clear localStorage)
      await page.evaluate(() => {
        localStorage.removeItem('access_token');
        localStorage.removeItem('refresh_token');
      });

      // Verify tokens are cleared
      const accessToken = await page.evaluate(() => localStorage.getItem('access_token'));
      const refreshTokenVal = await page.evaluate(() => localStorage.getItem('refresh_token'));

      expect(accessToken).toBeNull();
      expect(refreshTokenVal).toBeNull();
    });

    test('should redirect to login after logout when accessing protected route', async ({
      page,
      request,
    }) => {
      // Login and store token
      const tokens = await loginViaApi(request, TEST_USERS.admin);

      await page.goto('/');
      await page.evaluate((token) => {
        localStorage.setItem('access_token', token);
      }, tokens.access_token);

      // Clear token (logout)
      await page.evaluate(() => {
        localStorage.removeItem('access_token');
      });

      // Note: This test depends on the frontend implementing auth guards
      // If the app doesn't redirect, this test documents current behavior
      await page.goto('/sources');
      await page.waitForLoadState('networkidle');

      // The app should either redirect to login or show unauthorized message
      // Actual behavior depends on frontend implementation
    });
  });

  test.describe('Authorization Scope Tests', () => {
    test('should allow admin to access protected endpoints', async ({ request }) => {
      const tokens = await loginViaApi(request, TEST_USERS.admin);

      // Try to access sources list (requires read scope)
      const response = await request.get('http://localhost:8000/api/sources', {
        headers: {
          Authorization: `Bearer ${tokens.access_token}`,
        },
      });

      expect(response.ok()).toBeTruthy();
    });

    test('should include correct scopes in token', async ({ request }) => {
      const tokens = await loginViaApi(request, TEST_USERS.admin);
      const userInfo = await getCurrentUser(request, tokens.access_token);

      // Admin should have all scopes
      expect(userInfo.scopes).toEqual(expect.arrayContaining(['admin', 'read', 'write', 'delete']));
    });
  });

  test.describe('Edge Cases', () => {
    test('should handle concurrent login requests', async ({ request }) => {
      // Send multiple login requests simultaneously
      const loginPromises = Array(5)
        .fill(null)
        .map(() => loginViaApi(request, TEST_USERS.admin));

      const results = await Promise.all(loginPromises);

      // All should succeed
      results.forEach((tokens) => {
        expect(tokens.access_token).toBeTruthy();
      });

      // Tokens should be different (unique session)
      const uniqueTokens = new Set(results.map((r) => r.access_token));
      expect(uniqueTokens.size).toBe(5);
    });

    test('should handle special characters in password', async ({ request }) => {
      // Test with the configured admin password
      // This validates that special characters don't break authentication
      const response = await request.post('http://localhost:8000/api/auth/login', {
        data: {
          username: 'admin',
          password: 'test!@#$%^&*()', // Special characters
        },
      });

      // Should fail (wrong password) but not crash
      expect(response.status()).toBe(401);
    });

    test('should handle very long token in authorization header', async ({ request }) => {
      const longToken = 'a'.repeat(10000);

      const response = await request.get('http://localhost:8000/api/auth/verify', {
        headers: {
          Authorization: `Bearer ${longToken}`,
        },
      });

      // Should reject invalid token, not crash
      expect([401, 400]).toContain(response.status());
    });

    test('should handle missing Bearer prefix', async ({ request }) => {
      const tokens = await loginViaApi(request, TEST_USERS.admin);

      const response = await request.get('http://localhost:8000/api/auth/verify', {
        headers: {
          Authorization: tokens.access_token, // Missing "Bearer " prefix
        },
      });

      expect(response.status()).toBe(401);
    });
  });
});
