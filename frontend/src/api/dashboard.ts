import apiClient from './client'
import type { DashboardStats, ExecutionTrend, SystemHealth } from '@/types'

export async function getDashboardStats(): Promise<DashboardStats> {
  const { data } = await apiClient.get('/api/dashboard')
  return data
}

export async function getExecutionTrends(days: number = 7): Promise<ExecutionTrend[]> {
  const { data } = await apiClient.get('/api/dashboard/execution-trends', {
    params: { days },
  })
  return data.trends || []
}

export async function getRecentActivity() {
  const { data } = await apiClient.get('/api/dashboard/recent-activity')
  return data
}

export async function getSystemHealth(): Promise<SystemHealth> {
  const { data } = await apiClient.get('/api/monitoring/health')
  return data
}
