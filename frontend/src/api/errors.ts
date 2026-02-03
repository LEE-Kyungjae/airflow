import apiClient from './client'
import type { ErrorLog } from '@/types'

export async function getErrors(params?: {
  resolved?: boolean
  source_id?: string
  error_code?: string
  limit?: number
}): Promise<ErrorLog[]> {
  const { data } = await apiClient.get('/api/errors', { params })
  return data
}

export async function getUnresolvedErrors(): Promise<ErrorLog[]> {
  const { data } = await apiClient.get('/api/errors/unresolved')
  return data
}

export async function getErrorStats() {
  const { data } = await apiClient.get('/api/errors/stats')
  return data
}

export async function resolveError(id: string, resolution: {
  method: string
  detail?: string
}) {
  const { data } = await apiClient.post(`/api/errors/${id}/resolve`, resolution)
  return data
}

export async function retryError(id: string) {
  const { data } = await apiClient.post(`/api/errors/${id}/retry`)
  return data
}

export async function regenerateCrawler(id: string) {
  const { data } = await apiClient.post(`/api/errors/${id}/regenerate`)
  return data
}
