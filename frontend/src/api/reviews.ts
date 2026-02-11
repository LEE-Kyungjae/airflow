import apiClient from './client'
import type {
  ReviewDashboardData,
  ReviewQueueItemData,
  ReviewItem,
  SourceContentData,
} from '@/types'

export async function getReviewDashboard(): Promise<ReviewDashboardData> {
  const { data } = await apiClient.get('/api/reviews/dashboard')
  return data
}

export async function getReviewQueue(params?: {
  source_id?: string
  status?: string
  limit?: number
  offset?: number
}): Promise<ReviewQueueItemData[]> {
  const { data } = await apiClient.get('/api/reviews/queue', { params })
  return data
}

export async function getNextReview(params?: {
  source_id?: string
  current_id?: string
  direction?: 'forward' | 'backward'
}): Promise<{
  has_next: boolean
  review?: ReviewItem
  source?: { name: string; type: string; url: string }
  position?: number
  total_pending?: number
}> {
  const { data } = await apiClient.get('/api/reviews/next', { params })
  return data
}

export async function getResumeInfo(): Promise<{
  has_bookmark: boolean
  last_review_id?: string
  last_reviewed_at?: string
  remaining_after_bookmark?: number
  total_pending: number
}> {
  const { data } = await apiClient.get('/api/reviews/resume')
  return data
}

export async function getReview(reviewId: string): Promise<ReviewItem> {
  const { data } = await apiClient.get(`/api/reviews/${reviewId}`)
  return data
}

export async function getReviewSourceContent(reviewId: string): Promise<SourceContentData> {
  const { data } = await apiClient.get(`/api/reviews/${reviewId}/source-content`)
  return data
}

export async function updateReview(
  reviewId: string,
  update: {
    status: string
    notes?: string
    rejection_reason?: string
    rejection_notes?: string
    review_duration_ms?: number
    corrections?: { field: string; original_value: any; corrected_value: any; reason?: string }[]
  }
): Promise<ReviewItem> {
  const { data } = await apiClient.put(`/api/reviews/${reviewId}`, update)
  return data
}

export async function revertReview(reviewId: string): Promise<ReviewItem> {
  const { data } = await apiClient.put(`/api/reviews/${reviewId}/revert`)
  return data
}

export async function batchApprove(reviewIds: string[]): Promise<{ success: boolean; modified_count: number }> {
  const { data } = await apiClient.post('/api/reviews/batch-approve', reviewIds)
  return data
}

export async function bulkApprove(request: {
  review_ids: string[]
}): Promise<{ total: number; success: number; failed: number }> {
  const { data } = await apiClient.post('/api/reviews/bulk-approve', request)
  return data
}

export async function bulkReject(request: {
  review_ids: string[]
  reason: string
}): Promise<{ total: number; success: number; failed: number }> {
  const { data } = await apiClient.post('/api/reviews/bulk-reject', request)
  return data
}
