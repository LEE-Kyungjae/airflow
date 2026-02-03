import apiClient from './client'
import type { PipelineStatus, HealingSession, ErrorSummary } from '@/types'

export async function getRealtimeStatus(): Promise<{
  timestamp: string
  summary: {
    total: number
    active: number
    failed: number
    healing: number
    pending: number
  }
  pipelines: PipelineStatus[]
}> {
  const { data } = await apiClient.get('/api/monitoring/status/realtime')
  return data
}

export async function getErrorSummary(hours: number = 24): Promise<{
  period_hours: number
  total_errors: number
  unresolved_count: number
  error_summaries: ErrorSummary[]
  hourly_trend: { hour: string; count: number }[]
}> {
  const { data } = await apiClient.get('/api/monitoring/errors/summary', {
    params: { hours },
  })
  return data
}

export async function getHealingSessions(params?: {
  status?: string
  limit?: number
}): Promise<{
  total: number
  status_counts: Record<string, number>
  sessions: HealingSession[]
}> {
  const { data } = await apiClient.get('/api/monitoring/healing/sessions', { params })
  return data
}

export async function approveHealing(
  sessionId: string,
  additionalAttempts: number = 3
) {
  const { data } = await apiClient.post(
    `/api/monitoring/healing/${sessionId}/admin-approve`,
    null,
    { params: { additional_attempts: additionalAttempts } }
  )
  return data
}

export async function getWellknownCases(limit: number = 50) {
  const { data } = await apiClient.get('/api/monitoring/wellknown-cases', {
    params: { limit },
  })
  return data
}
