import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, ExternalLink, Trash2, Play, Database } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Card, CardContent } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { StatusBadge } from '@/components/ui/Badge'
import { LoadingPage } from '@/components/ui/LoadingSpinner'
import { ConfirmModal } from '@/components/ui/ConfirmModal'
import { useToast } from '@/hooks/useToast'
import { getSources, deleteSource, triggerCrawl } from '@/api/sources'
import { formatRelativeTime } from '@/lib/utils'
import type { Source } from '@/types'

export default function Sources() {
  const [statusFilter, setStatusFilter] = useState<string>('')
  const [deleteTarget, setDeleteTarget] = useState<Source | null>(null)
  const queryClient = useQueryClient()
  const toast = useToast()

  const { data: sources, isLoading } = useQuery({
    queryKey: ['sources', statusFilter],
    queryFn: () => getSources({ status: statusFilter || undefined }),
  })

  const deleteMutation = useMutation({
    mutationFn: deleteSource,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['sources'] })
      toast.success('Source deleted successfully')
    },
    onError: (err: Error) => {
      toast.error(err.message || 'Failed to delete source')
    },
  })

  const triggerMutation = useMutation({
    mutationFn: triggerCrawl,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['sources'] })
      toast.success('Crawl triggered')
    },
    onError: (err: Error) => {
      toast.error(err.message || 'Failed to trigger crawl')
    },
  })

  if (isLoading) return <LoadingPage />

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Sources</h1>
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
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Status:</span>
            <div className="flex gap-2">
              {['', 'active', 'inactive', 'error'].map((status) => (
                <button
                  key={status}
                  onClick={() => setStatusFilter(status)}
                  className={`px-3 py-1.5 text-sm rounded-lg transition-colors ${
                    statusFilter === status
                      ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200 dark:bg-gray-700 dark:text-gray-300 dark:hover:bg-gray-600'
                  }`}
                >
                  {status || 'All'}
                </button>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Mobile card layout */}
      <div className="md:hidden space-y-3">
        {sources?.map((source: Source) => (
          <Card key={source._id}>
            <CardContent className="p-4">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0 flex-1">
                  <Link
                    to={`/sources/${source._id}`}
                    className="font-medium text-gray-900 dark:text-gray-100 hover:text-blue-600 dark:hover:text-blue-400"
                  >
                    {source.name}
                  </Link>
                  <a
                    href={source.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-xs text-blue-600 dark:text-blue-400 hover:underline flex items-center gap-1 mt-0.5"
                  >
                    {source.url.substring(0, 40)}...
                    <ExternalLink className="w-3 h-3" />
                  </a>
                </div>
                <StatusBadge status={source.status} />
              </div>
              <div className="flex items-center gap-4 mt-3 text-xs text-gray-500 dark:text-gray-400">
                <span className="px-2 py-0.5 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded">
                  {source.type.toUpperCase()}
                </span>
                <span>{formatRelativeTime(source.last_run)}</span>
                <code className="bg-gray-100 dark:bg-gray-700 px-1.5 py-0.5 rounded text-gray-900 dark:text-gray-100">
                  {source.schedule || '-'}
                </code>
              </div>
              <div className="flex items-center justify-end gap-2 mt-3 pt-3 border-t border-gray-100 dark:border-gray-700">
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => triggerMutation.mutate(source._id)}
                  loading={triggerMutation.isPending}
                >
                  <Play className="w-4 h-4" />
                  Run
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setDeleteTarget(source)}
                >
                  <Trash2 className="w-4 h-4 text-red-500" />
                  Delete
                </Button>
              </div>
            </CardContent>
          </Card>
        ))}
        {(!sources || sources.length === 0) && (
          <Card>
            <CardContent className="p-12">
              <div className="flex flex-col items-center gap-3">
                <Database className="w-12 h-12 text-gray-300 dark:text-gray-600" />
                <p className="text-gray-500 dark:text-gray-400">No sources found.</p>
                <Link to="/quick-add">
                  <Button variant="outline" size="sm">
                    <Plus className="w-4 h-4" />
                    Add your first source
                  </Button>
                </Link>
              </div>
            </CardContent>
          </Card>
        )}
      </div>

      {/* Desktop table layout */}
      <Card className="hidden md:block">
        <CardContent className="p-0">
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50">
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  Name
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  Type
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  Last Run
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  Schedule
                </th>
                <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
              {sources?.map((source: Source) => (
                <tr key={source._id} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                  <td className="px-6 py-4">
                    <div>
                      <Link
                        to={`/sources/${source._id}`}
                        className="font-medium text-gray-900 dark:text-gray-100 hover:text-blue-600 dark:hover:text-blue-400"
                      >
                        {source.name}
                      </Link>
                      <a
                        href={source.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-sm text-blue-600 dark:text-blue-400 hover:underline flex items-center gap-1"
                      >
                        {source.url.substring(0, 50)}...
                        <ExternalLink className="w-3 h-3" />
                      </a>
                    </div>
                  </td>
                  <td className="px-6 py-4">
                    <span className="px-2 py-1 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 text-sm rounded">
                      {source.type.toUpperCase()}
                    </span>
                  </td>
                  <td className="px-6 py-4">
                    <StatusBadge status={source.status} />
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-500 dark:text-gray-400">
                    {formatRelativeTime(source.last_run)}
                  </td>
                  <td className="px-6 py-4">
                    <code className="text-sm bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded text-gray-900 dark:text-gray-100">
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
                        onClick={() => setDeleteTarget(source)}
                      >
                        <Trash2 className="w-4 h-4 text-red-500" />
                      </Button>
                    </div>
                  </td>
                </tr>
              ))}
              {(!sources || sources.length === 0) && (
                <tr>
                  <td colSpan={6} className="px-6 py-12 text-center">
                    <div className="flex flex-col items-center gap-3">
                      <Database className="w-12 h-12 text-gray-300 dark:text-gray-600" />
                      <p className="text-gray-500 dark:text-gray-400">No sources found.</p>
                      <Link to="/quick-add">
                        <Button variant="outline" size="sm">
                          <Plus className="w-4 h-4" />
                          Add your first source
                        </Button>
                      </Link>
                    </div>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </CardContent>
      </Card>

      {/* Delete Confirmation Modal */}
      <ConfirmModal
        open={!!deleteTarget}
        title="Delete Source"
        message={`Are you sure you want to delete "${deleteTarget?.name}"? This action cannot be undone.`}
        confirmLabel="Delete"
        variant="danger"
        onConfirm={() => {
          if (deleteTarget) deleteMutation.mutate(deleteTarget._id)
          setDeleteTarget(null)
        }}
        onCancel={() => setDeleteTarget(null)}
      />
    </div>
  )
}
