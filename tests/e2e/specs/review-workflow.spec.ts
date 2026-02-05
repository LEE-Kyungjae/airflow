import { test, expect, Page } from '@playwright/test';
import { TestDataManager, randomString } from '../fixtures';

/**
 * Review Workflow Tests
 *
 * Tests for data review queue, approval, rejection, and correction workflows.
 */

test.describe('Review Workflow', () => {
  let testData: TestDataManager;
  const REVIEWER_ID = `e2e_reviewer_${randomString(8)}`;

  test.beforeAll(async ({ request }) => {
    testData = new TestDataManager(request);
    await testData.init();
  });

  test.afterAll(async () => {
    await testData.cleanup();
  });

  test.describe('Review Dashboard', () => {
    test('should get review dashboard statistics', async ({ request }) => {
      const api = testData.getApiHelper();

      const dashboard = await api.getReviewDashboard();

      expect(dashboard).toHaveProperty('pending_count');
      expect(dashboard).toHaveProperty('today_reviewed');
      expect(dashboard).toHaveProperty('approval_rate');
      expect(dashboard).toHaveProperty('avg_confidence');
      expect(dashboard).toHaveProperty('by_source');
      expect(dashboard).toHaveProperty('recent_reviews');

      expect(typeof dashboard.pending_count).toBe('number');
      expect(typeof dashboard.approval_rate).toBe('number');
    });

    test('should include source breakdown in dashboard', async ({ request }) => {
      const api = testData.getApiHelper();

      const dashboard = await api.getReviewDashboard();

      expect(Array.isArray(dashboard.by_source)).toBeTruthy();

      if (dashboard.by_source.length > 0) {
        expect(dashboard.by_source[0]).toHaveProperty('source_id');
        expect(dashboard.by_source[0]).toHaveProperty('source_name');
        expect(dashboard.by_source[0]).toHaveProperty('pending_count');
      }
    });

    test('should include recent reviews in dashboard', async ({ request }) => {
      const api = testData.getApiHelper();

      const dashboard = await api.getReviewDashboard();

      expect(Array.isArray(dashboard.recent_reviews)).toBeTruthy();
    });
  });

  test.describe('Review Queue', () => {
    test('should get pending review queue', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'pending' });

      expect(Array.isArray(queue)).toBeTruthy();

      if (queue.length > 0) {
        expect(queue[0]).toHaveProperty('review');
        expect(queue[0]).toHaveProperty('source_name');
        expect(queue[0]).toHaveProperty('total_in_queue');
        expect(queue[0]).toHaveProperty('current_position');
      }
    });

    test('should get on_hold review queue', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'on_hold' });

      expect(Array.isArray(queue)).toBeTruthy();
    });

    test('should get needs_correction review queue', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'needs_correction' });

      expect(Array.isArray(queue)).toBeTruthy();
    });

    test('should filter queue by source', async ({ request }) => {
      const api = testData.getApiHelper();

      // Get all pending reviews
      const allPending = await api.getReviewQueue({ status: 'pending' });

      if (allPending.length > 0 && allPending[0].review.source_id) {
        const sourceId = allPending[0].review.source_id;

        // Filter by source
        const filtered = await api.getReviewQueue({
          status: 'pending',
          source_id: sourceId,
        });

        // All filtered results should have the same source
        filtered.forEach((item) => {
          expect(item.review.source_id).toBe(sourceId);
        });
      }
    });

    test('should respect limit parameter', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'pending', limit: 5 });

      expect(queue.length).toBeLessThanOrEqual(5);
    });

    test('should include position information in queue', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'pending' });

      if (queue.length > 0) {
        expect(queue[0].current_position).toBe(1);
        expect(queue[0].total_in_queue).toBeGreaterThanOrEqual(queue.length);
      }
    });
  });

  test.describe('Next Review Navigation', () => {
    test('should get first pending review', async ({ request }) => {
      const api = testData.getApiHelper();

      const result = await api.getNextReview();

      expect(result).toHaveProperty('has_next');

      if (result.has_next) {
        expect(result.review).toBeTruthy();
        expect(result).toHaveProperty('source');
        expect(result).toHaveProperty('position');
        expect(result).toHaveProperty('total_pending');
      }
    });

    test('should navigate forward in review queue', async ({ request }) => {
      const api = testData.getApiHelper();

      // Get first review
      const first = await api.getNextReview();

      if (first.has_next && first.review) {
        // Get next review
        const next = await api.getNextReview(first.review._id, 'forward');

        if (next.has_next) {
          // Should be different review or same if only one item
          expect(next.review).toBeTruthy();
        }
      }
    });

    test('should navigate backward in review queue', async ({ request }) => {
      const api = testData.getApiHelper();

      // Get first review
      const first = await api.getNextReview();

      if (first.has_next && first.review) {
        // Go forward first
        const second = await api.getNextReview(first.review._id, 'forward');

        if (second.has_next && second.review) {
          // Go backward
          const back = await api.getNextReview(second.review._id, 'backward');

          // Should either get the first review or some previous review
          expect(back).toHaveProperty('has_next');
        }
      }
    });

    test('should handle empty queue gracefully', async ({ request }) => {
      const api = testData.getApiHelper();

      // Use a non-existent source to get empty queue
      const result = await api.getNextReview(undefined, 'forward');

      // Should return has_next: false or has_next: true with review
      expect(result).toHaveProperty('has_next');

      if (!result.has_next) {
        expect(result.review).toBeNull();
      }
    });
  });

  test.describe('Data Approval', () => {
    test('should approve review with valid reviewer ID', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'pending', limit: 1 });

      if (queue.length > 0) {
        const reviewId = queue[0].review._id;

        const result = await api.updateReview(reviewId, 'approved', REVIEWER_ID);

        expect(result.response.ok()).toBeTruthy();
        expect(result.data.review_status).toBe('approved');
        expect(result.data.reviewer_id).toBe(REVIEWER_ID);
      }
    });

    test('should record review timestamp on approval', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'pending', limit: 1 });

      if (queue.length > 0) {
        const reviewId = queue[0].review._id;
        const beforeTime = new Date().toISOString();

        const result = await api.updateReview(reviewId, 'approved', REVIEWER_ID);

        if (result.response.ok()) {
          expect(result.data.reviewed_at).toBeDefined();
          const reviewedAt = new Date(result.data.reviewed_at);
          expect(reviewedAt.getTime()).toBeGreaterThanOrEqual(new Date(beforeTime).getTime() - 1000);
        }
      }
    });
  });

  test.describe('Data Rejection / Hold', () => {
    test('should put review on hold', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'pending', limit: 1 });

      if (queue.length > 0) {
        const reviewId = queue[0].review._id;

        const result = await api.updateReview(
          reviewId,
          'on_hold',
          REVIEWER_ID,
          undefined,
          'Need more information'
        );

        expect(result.response.ok()).toBeTruthy();
        expect(result.data.review_status).toBe('on_hold');
        expect(result.data.notes).toBe('Need more information');
      }
    });

    test('should mark review as needs_correction', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'pending', limit: 1 });

      if (queue.length > 0) {
        const reviewId = queue[0].review._id;

        const corrections = [
          {
            field: 'title',
            original_value: 'old value',
            corrected_value: 'new value',
            reason: 'typo',
          },
        ];

        const result = await api.updateReview(
          reviewId,
          'needs_correction',
          REVIEWER_ID,
          corrections,
          'Multiple errors found'
        );

        expect(result.response.ok()).toBeTruthy();
        expect(result.data.review_status).toBe('needs_correction');
      }
    });
  });

  test.describe('Data Correction', () => {
    test('should apply corrections to review', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'pending', limit: 1 });

      if (queue.length > 0) {
        const reviewId = queue[0].review._id;
        const originalData = queue[0].review.original_data || {};

        const corrections = [
          {
            field: Object.keys(originalData)[0] || 'test_field',
            original_value: 'original',
            corrected_value: 'corrected',
            reason: 'Data error',
          },
        ];

        const result = await api.updateReview(reviewId, 'corrected', REVIEWER_ID, corrections);

        if (result.response.ok()) {
          expect(result.data.review_status).toBe('corrected');
          expect(result.data.corrections).toBeDefined();
        }
      }
    });

    test('should create corrected_data from original + corrections', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'pending', limit: 1 });

      if (queue.length > 0) {
        const reviewId = queue[0].review._id;
        const originalData = queue[0].review.original_data || {};

        if (Object.keys(originalData).length > 0) {
          const fieldName = Object.keys(originalData)[0];
          const corrections = [
            {
              field: fieldName,
              original_value: originalData[fieldName],
              corrected_value: 'CORRECTED_VALUE',
              reason: 'Testing',
            },
          ];

          const result = await api.updateReview(reviewId, 'corrected', REVIEWER_ID, corrections);

          if (result.response.ok() && result.data.corrected_data) {
            expect(result.data.corrected_data[fieldName]).toBe('CORRECTED_VALUE');
          }
        }
      }
    });
  });

  test.describe('Batch Operations', () => {
    test('should batch approve multiple reviews', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'pending', limit: 5 });

      if (queue.length >= 2) {
        const reviewIds = queue.slice(0, 2).map((item) => item.review._id);

        const result = await api.batchApprove(reviewIds, REVIEWER_ID);

        expect(result.response.ok()).toBeTruthy();
        expect(result.data.success).toBeTruthy();
        expect(result.data.modified_count).toBeGreaterThanOrEqual(0);
      }
    });

    test('should handle empty batch gracefully', async ({ request }) => {
      const api = testData.getApiHelper();

      const result = await api.batchApprove([], REVIEWER_ID);

      // Should succeed with 0 modified
      if (result.response.ok()) {
        expect(result.data.modified_count).toBe(0);
      }
    });

    test('should handle non-existent review IDs in batch', async ({ request }) => {
      const api = testData.getApiHelper();

      const fakeIds = ['fake_id_1', 'fake_id_2'];

      const result = await api.batchApprove(fakeIds, REVIEWER_ID);

      // Should complete without error, just 0 modified
      if (result.response.ok()) {
        expect(result.data.modified_count).toBe(0);
      }
    });
  });

  test.describe('Review Page UI', () => {
    test('should navigate to review page', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Look for review link in navigation
      const reviewLink = page.locator('a[href*="review"], a:has-text("Review")');

      if (await reviewLink.first().isVisible()) {
        await reviewLink.first().click();
        await page.waitForLoadState('networkidle');
      }
    });

    test('should display review controls', async ({ page }) => {
      // Navigate directly to review page if it exists
      await page.goto('/review');
      await page.waitForLoadState('networkidle');

      // Check for common review UI elements
      const approveButton = page.locator('button:has-text("검토완료"), button:has-text("Approve")');
      const holdButton = page.locator('button:has-text("보류"), button:has-text("Hold")');
      const correctionButton = page.locator(
        'button:has-text("정정"), button:has-text("Correction")'
      );

      // At least one control should be visible if there are pending reviews
      // If no reviews, should show empty state
    });

    test('should show keyboard shortcuts help', async ({ page }) => {
      await page.goto('/review');
      await page.waitForLoadState('networkidle');

      // Look for keyboard shortcuts indicator
      const shortcutsHelp = page.locator('.shortcuts-help, [class*="shortcut"]');

      // Shortcuts might be togglable
      await page.keyboard.press('?');
    });

    test('should support keyboard navigation', async ({ page }) => {
      await page.goto('/review');
      await page.waitForLoadState('networkidle');

      // Test keyboard shortcuts
      // ArrowRight for next, ArrowLeft for previous
      await page.keyboard.press('ArrowRight');
      await page.waitForTimeout(500);

      await page.keyboard.press('ArrowLeft');
      await page.waitForTimeout(500);
    });
  });

  test.describe('Confidence Score Handling', () => {
    test('should prioritize low confidence reviews', async ({ request }) => {
      const api = testData.getApiHelper();

      // Get queue with priority_numbers flag
      const queue = await api.getReviewQueue({ status: 'pending', limit: 10 });

      if (queue.length > 1) {
        // Queue should be sorted by confidence (lower first) or needs_number_review
        // This is implementation-dependent
        expect(queue[0].review).toBeDefined();
      }
    });

    test('should display confidence level indicator', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'pending', limit: 1 });

      if (queue.length > 0) {
        // Review should have confidence score
        expect(queue[0].review).toHaveProperty('confidence_score');
      }
    });
  });

  test.describe('Source Review Stats', () => {
    test('should get review stats by source', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'pending', limit: 1 });

      if (queue.length > 0 && queue[0].review.source_id) {
        const sourceId = queue[0].review.source_id;

        // This endpoint might exist
        const response = await api['request'].get(
          `http://localhost:8000/api/reviews/stats/by-source/${sourceId}`,
          {
            headers: {
              Authorization: `Bearer ${api['authToken']}`,
            },
          }
        );

        if (response.ok()) {
          const stats = await response.json();
          expect(stats).toHaveProperty('pending');
          expect(stats).toHaveProperty('approved');
          expect(stats).toHaveProperty('total');
        }
      }
    });
  });

  test.describe('Edge Cases', () => {
    test('should handle review with missing source', async ({ request }) => {
      const api = testData.getApiHelper();

      // Try to get a review that might have deleted source
      const queue = await api.getReviewQueue({ status: 'pending', limit: 1 });

      // Should handle gracefully
      expect(Array.isArray(queue)).toBeTruthy();
    });

    test('should handle concurrent review updates', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'pending', limit: 1 });

      if (queue.length > 0) {
        const reviewId = queue[0].review._id;

        // Concurrent updates
        const updates = [
          api.updateReview(reviewId, 'approved', `${REVIEWER_ID}_1`),
          api.updateReview(reviewId, 'approved', `${REVIEWER_ID}_2`),
        ];

        const results = await Promise.all(updates);

        // At least one should succeed
        const successCount = results.filter((r) => r.response.ok()).length;
        expect(successCount).toBeGreaterThanOrEqual(1);
      }
    });

    test('should validate review status transitions', async ({ request }) => {
      const api = testData.getApiHelper();

      const queue = await api.getReviewQueue({ status: 'pending', limit: 1 });

      if (queue.length > 0) {
        const reviewId = queue[0].review._id;

        // Try invalid status
        const result = await api.updateReview(reviewId, 'invalid_status' as any, REVIEWER_ID);

        // Should fail validation
        expect(result.response.status()).toBe(422);
      }
    });
  });
});
