import apiClient from './client'

export interface ValidationIssue {
  rule_name: string
  field_name: string
  severity: 'info' | 'warning' | 'error' | 'critical'
  message: string
  actual_value?: string
  expected?: string
  row_index?: number
  suggestion?: string
}

export interface ValidationResult {
  id: string
  source_id: string
  run_id: string
  total_records: number
  validated_at: string
  is_valid: boolean
  quality_score: number
  issue_summary: {
    total: number
    by_severity: Record<string, number>
    by_rule: Record<string, number>
    by_field: Record<string, number>
  }
  issues: ValidationIssue[]
  field_stats: Record<string, {
    total: number
    null_count: number
    empty_count: number
    unique_count: number
    null_rate: number
    empty_rate: number
  }>
}

export interface QualityTrend {
  source_id: string
  period_start: string
  period_end: string
  avg_quality_score: number
  min_quality_score: number
  max_quality_score: number
  total_validations: number
  total_issues: number
  issue_trend: Array<{
    date: string
    score: number
    issues: number
  }>
  top_issues: Array<{
    rule: string
    count: number
  }>
}

export interface QualityAnomaly {
  id: string
  source_id: string
  detected_at: string
  anomaly_type: string
  severity: string
  description: string
  acknowledged: boolean
}

export interface QualityDashboard {
  overall_stats: {
    total_sources: number
    avg_quality_score: number
    total_issues_24h: number
    anomalies_count: number
    sources_below_threshold: number
  }
  source_scores: Array<{
    source_id: string
    source_name: string
    quality_score: number
    last_validation: string
    trend: 'up' | 'down' | 'stable'
  }>
  recent_anomalies: QualityAnomaly[]
  top_issues: Array<{
    rule: string
    count: number
    affected_sources: number
  }>
}

// Get validation results list
export async function getValidationResults(params?: {
  source_id?: string
  limit?: number
  offset?: number
  days?: number
}): Promise<{ items: ValidationResult[]; total: number }> {
  const { data } = await apiClient.get('/data-quality/validation-results', { params })
  // Map backend response format to frontend expected format
  return {
    items: data.results?.map((r: any) => ({
      id: r.id,
      source_id: r.source_id,
      run_id: r.run_id,
      total_records: r.total_records,
      validated_at: r.validated_at || r.created_at,
      is_valid: r.is_valid,
      quality_score: r.quality_score,
      issue_summary: {
        total: r.issue_count || r.issue_summary?.total || 0,
        by_severity: r.issues_by_severity || r.issue_summary?.by_severity || {},
        by_rule: r.issue_summary?.by_rule || {},
        by_field: r.issue_summary?.by_field || {},
      },
      issues: r.issues || [],
      field_stats: r.field_stats || {},
    })) || [],
    total: data.total || 0,
  }
}

// Get single validation result
export async function getValidationResult(id: string): Promise<ValidationResult> {
  const { data } = await apiClient.get(`/data-quality/validation-results/${id}`)
  return data
}

// Get quality trend for a source
export async function getQualityTrend(sourceId: string, days?: number): Promise<QualityTrend> {
  const { data } = await apiClient.get(`/data-quality/quality-trend/${sourceId}`, {
    params: { days },
  })
  return data
}

// Get anomalies list
export async function getAnomalies(params?: {
  source_id?: string
  hours?: number
  acknowledged?: boolean
}): Promise<QualityAnomaly[]> {
  const { data } = await apiClient.get('/data-quality/anomalies', { params })
  return data
}

// Acknowledge an anomaly
export async function acknowledgeAnomaly(id: string): Promise<{ success: boolean }> {
  const { data } = await apiClient.post(`/data-quality/anomalies/${id}/acknowledge`)
  return data
}

// Get quality dashboard
export async function getQualityDashboard(): Promise<QualityDashboard> {
  const { data } = await apiClient.get('/data-quality/dashboard')
  return data
}

// Manually trigger validation
export async function triggerValidation(params: {
  source_id: string
  run_id: string
  sample_data: Record<string, any>[]
}): Promise<ValidationResult> {
  const { data } = await apiClient.post('/data-quality/validate', params)
  return data
}

// Get quality report
export async function getQualityReport(sourceId: string, days?: number): Promise<any> {
  const { data } = await apiClient.get(`/data-quality/report/${sourceId}`, {
    params: { days },
  })
  return data
}

// Timeline data point
export interface TimelineDataPoint {
  timestamp: string
  label: string
  validation_count: number
  total_records: number
  avg_quality_score: number
  min_quality_score: number
  max_quality_score: number
  total_issues: number
  critical_issues: number
  error_issues: number
  warning_issues: number
}

export interface ValidationTimeline {
  period: {
    start: string
    end: string
    days: number
    interval: string
  }
  summary: {
    total_validations: number
    total_records: number
    avg_quality_score: number
  }
  timeline: TimelineDataPoint[]
}

// Get validation timeline data
export async function getValidationTimeline(params?: {
  source_id?: string
  days?: number
  interval?: 'hour' | 'day'
}): Promise<ValidationTimeline> {
  const { data } = await apiClient.get('/data-quality/timeline', { params })
  return data
}
