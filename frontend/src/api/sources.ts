import apiClient from './client'
import type { Source } from '@/types'

export async function getSources(params?: {
  status?: string
  skip?: number
  limit?: number
}): Promise<Source[]> {
  const { data } = await apiClient.get('/api/sources', { params })
  return data
}

export async function getSource(id: string): Promise<Source> {
  const { data } = await apiClient.get(`/api/sources/${id}`)
  return data
}

export async function createSource(source: Partial<Source>): Promise<Source> {
  const { data } = await apiClient.post('/api/sources', source)
  return data
}

export async function updateSource(id: string, source: Partial<Source>): Promise<Source> {
  const { data } = await apiClient.put(`/api/sources/${id}`, source)
  return data
}

export async function deleteSource(id: string): Promise<void> {
  await apiClient.delete(`/api/sources/${id}`)
}

export async function triggerCrawl(id: string) {
  const { data } = await apiClient.post(`/api/sources/${id}/trigger`)
  return data
}

export async function getSourceResults(id: string, params?: { limit?: number }) {
  const { data } = await apiClient.get(`/api/sources/${id}/results`, { params })
  return data
}

// Quick Add
export async function analyzeUrl(url: string) {
  const { data } = await apiClient.post('/api/quick-add/analyze', { url })
  return data
}

export async function quickAddSource(payload: {
  url: string
  name?: string
  hint?: string
  auto_start?: boolean
}) {
  const { data } = await apiClient.post('/api/quick-add', payload)
  return data
}
