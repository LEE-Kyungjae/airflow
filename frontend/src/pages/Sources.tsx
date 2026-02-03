import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, RefreshCw, ExternalLink, Trash2, Play } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { StatusBadge } from '@/components/ui/Badge'
import { LoadingPage } from '@/components/ui/LoadingSpinner'
import { getSources, deleteSource, triggerCrawl } from '@/api/sources'
import { formatRelativeTime } from '@/lib/utils'
import type { Source } from '@/types'

export default function Sources() {
  const [statusFilter, setStatusFilter] = useState<string>('')
  const queryClient = useQueryClient()

  const { data: sources, isLoading } = useQuery({
    queryKey: ['sources', statusFilter],
    queryFn: () => getSources({ status: statusFilter || undefined }),
  })

  const deleteMutation = useMutation({
    mutationFn: deleteSource,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['sources'] })
    },
  })

  const triggerMutation = useMutation({
    mutationFn: triggerCrawl,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['sources'] })
    },
  })

  if (isLoading) return <LoadingPage />

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Sources</h1>
        <Link to="/quick-add">
          <Button>
            <Plus className="w-4 h-4" />
            Add Source
          </Button>
        </Link>
      </div>

      {/* Filters */}
      <Card>
        <CardContent className="py-4">
          <div className="flex items-center gap-4">
            <span className="text-sm font-medium text-gray-700">Status:</span>
            <div className="flex gap-2">
              {['', 'active', 'inactive', 'error'].map((status) => (
                <button
                  key={status}
                  onClick={() => setStatusFilter(status)}
                  className={`px-3 py-1.5 text-sm rounded-lg transition-colors ${
                    statusFilter === status
                      ? 'bg-blue-100 text-blue-700'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  {status || 'All'}
                </button>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Sources Table */}
      <Card>
        <CardContent className="p-0">
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-200 bg-gray-50">
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Name
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Type
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Last Run
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Schedule
                </th>
                <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {sources?.map((source: Source) => (
                <tr key={source._id} className="hover:bg-gray-50">
                  <td className="px-6 py-4">
                    <div>
                      <p className="font-medium text-gray-900">{source.name}</p>
                      <a
                        href={source.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-sm text-blue-600 hover:underline flex items-center gap-1"
                      >
                        {source.url.substring(0, 50)}...
                        <ExternalLink className="w-3 h-3" />
                      </a>
                    </div>
                  </td>
                  <td className="px-6 py-4">
                    <span className="px-2 py-1 bg-gray-100 text-gray-700 text-sm rounded">
                      {source.type.toUpperCase()}
                    </span>
                  </td>
                  <td className="px-6 py-4">
                    <StatusBadge status={source.status} />
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-500">
                    {formatRelativeTime(source.last_run)}
                  </td>
                  <td className="px-6 py-4">
                    <code className="text-sm bg-gray-100 px-2 py-1 rounded">
                      {source.schedule || '-'}
                    </code>
                  </td>
                  <td className="px-6 py-4 text-right">
                    <div className="flex items-center justify-end gap-2">
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => triggerMutation.mutate(source._id)}
                        loading={triggerMutation.isPending}
                      >
                        <Play className="w-4 h-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => {
                          if (confirm('Delete this source?')) {
                            deleteMutation.mutate(source._id)
                          }
                        }}
                      >
                        <Trash2 className="w-4 h-4 text-red-500" />
                      </Button>
                    </div>
                  </td>
                </tr>
              ))}
              {(!sources || sources.length === 0) && (
                <tr>
                  <td colSpan={6} className="px-6 py-12 text-center text-gray-500">
                    No sources found.{' '}
                    <Link to="/quick-add" className="text-blue-600 hover:underline">
                      Add your first source
                    </Link>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </CardContent>
      </Card>
    </div>
  )
}
