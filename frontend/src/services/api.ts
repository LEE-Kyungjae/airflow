import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json'
  }
})

export interface Review {
  _id: string
  crawl_result_id: string
  source_id: string
  data_record_index: number
  review_status: 'pending' | 'approved' | 'on_hold' | 'needs_correction' | 'corrected'
  reviewer_id?: string
  reviewed_at?: string
  original_data: Record<string, any>
  corrected_data?: Record<string, any>
  corrections: FieldCorrection[]
  source_highlights: SourceHighlight[]
  confidence_score?: number
  ocr_confidence?: number
  ai_confidence?: number
  needs_number_review: boolean
  uncertain_numbers: UncertainNumber[]
  notes?: string
  created_at: string
}

export interface FieldCorrection {
  field: string
  original_value: any
  corrected_value: any
  reason?: string
}

export interface SourceHighlight {
  field: string
  bbox?: { x: number; y: number; width: number; height: number }
  page?: number
  selector?: string
}

export interface UncertainNumber {
  value: string
  type: string
  confidence: number
  needs_review: boolean
}

export interface ReviewQueueItem {
  review: Review
  source_name: string
  source_type: string
  source_url: string
  total_in_queue: number
  current_position: number
}

export interface NextReviewResponse {
  has_next: boolean
  review?: Review
  source?: {
    name: string
    type: string
    url: string
  }
  position: number
  total_pending: number
}

export interface SourceContent {
  source_type: string
  source_url: string
  source_name: string
  fields: any[]
  highlights: SourceHighlight[]
  html_snapshot?: string
  raw_data?: any
}

export interface DashboardData {
  pending_count: number
  today_reviewed: number
  approval_rate: number
  avg_confidence: number
  needs_number_review_count: number
  by_source: Array<{ source_id: string; source_name: string; pending_count: number }>
  recent_reviews: Review[]
}

// Dashboard
export const getDashboard = () =>
  api.get<DashboardData>('/reviews/dashboard').then(r => r.data)

// Queue
export const getReviewQueue = (params?: {
  source_id?: string
  status?: string
  limit?: number
  offset?: number
  priority_numbers?: boolean
}) => api.get<ReviewQueueItem[]>('/reviews/queue', { params }).then(r => r.data)

// Next/Previous navigation
export const getNextReview = (params?: {
  source_id?: string
  current_id?: string
  direction?: 'forward' | 'backward'
}) => api.get<NextReviewResponse>('/reviews/next', { params }).then(r => r.data)

// Single review
export const getReview = (id: string) =>
  api.get<Review>(`/reviews/${id}`).then(r => r.data)

// Source content for review
export const getSourceContent = (reviewId: string) =>
  api.get<SourceContent>(`/reviews/${reviewId}/source-content`).then(r => r.data)

// Update review status
export const updateReview = (
  id: string,
  data: {
    status: 'approved' | 'on_hold' | 'needs_correction' | 'corrected'
    corrections?: FieldCorrection[]
    notes?: string
    review_duration_ms?: number
  },
  reviewer_id: string
) => api.put<Review>(`/reviews/${id}?reviewer_id=${reviewer_id}`, data).then(r => r.data)

// Batch approve
export const batchApprove = (reviewIds: string[], reviewer_id: string) =>
  api.post('/reviews/batch-approve', reviewIds, { params: { reviewer_id } }).then(r => r.data)

export default api
