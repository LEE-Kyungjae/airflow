import { useState } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  ArrowLeft,
  ExternalLink,
  Play,
  Trash2,
  Clock,
  CheckCircle,
  AlertCircle,
  Database,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { Badge, StatusBadge } from '@/components/ui/Badge'
import { LoadingPage } from '@/components/ui/LoadingSpinner'
import { ConfirmModal } from '@/components/ui/ConfirmModal'
import { useToast } from '@/hooks/useToast'
import { getSource, getSourceResults, deleteSource, triggerCrawl } from '@/api/sources'
import { formatDate, formatRelativeTime } from '@/lib/utils'

export default function SourceDetail() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const toast = useToast()
  const [showDeleteModal, setShowDeleteModal] = useState(false)

  const { data: source, isLoading } = useQuery({
    queryKey: ['source', id],
    queryFn: () => getSource(id!),
    enabled: !!id,
  })

  const { data: results } = useQuery({
    queryKey: ['sourceResults', id],
    queryFn: () => getSourceResults(id!, { limit: 10 }),
    enabled: !!id,
  })

  const deleteMutation = useMutation({
    mutationFn: deleteSource,
    onSuccess: () => {
      toast.success('Source deleted successfully')
      queryClient.invalidateQueries({ queryKey: ['sources'] })
      navigate('/sources')
    },
    onError: (err: Error) => {
      toast.error(err.message || 'Failed to delete source')
    },
  })

  const triggerMutation = useMutation({
    mutationFn: triggerCrawl,
    onSuccess: () => {
      toast.success('Crawl triggered successfully')
      queryClient.invalidateQueries({ queryKey: ['source', id] })
    },
    onError: (err: Error) => {
      toast.error(err.message || 'Failed to trigger crawl')
    },
  })

  if (isLoading) return <LoadingPage />

  if (!source) {
    return (
      <div className="flex flex-col items-center justify-center h-64 space-y-4">
        <AlertCircle className="w-12 h-12 text-gray-400 dark:text-gray-500" />
        <p className="text-gray-500 dark:text-gray-400">Source not found</p>
        <Link to="/sources">
          <Button variant="outline">Back to Sources</Button>
        </Link>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div className="flex items-center gap-4 min-w-0">
          <Link to="/sources">
            <Button variant="ghost" size="sm">
              <ArrowLeft className="w-4 h-4" />
            </Button>
          </Link>
          <div className="min-w-0">
            <h1 className="text-xl md:text-2xl font-bold text-gray-900 dark:text-gray-100 truncate">
              {source.name}
            </h1>
            <a
              href={source.url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-sm text-blue-600 dark:text-blue-400 hover:underline flex items-center gap-1 mt-1 truncate"
            >
              <span className="truncate">{source.url}</span>
              <ExternalLink className="w-3 h-3 flex-shrink-0" />
            </a>
          </div>
        </div>
        <div className="flex items-center gap-2 ml-14 sm:ml-0">
          <Button
            variant="outline"
            onClick={() => triggerMutation.mutate(id!)}
            loading={triggerMutation.isPending}
          >
            <Play className="w-4 h-4" />
            Run Now
          </Button>
          <Button
            variant="destructive"
            onClick={() => setShowDeleteModal(true)}
          >
            <Trash2 className="w-4 h-4" />
            Delete
          </Button>
        </div>
      </div>

      {/* Info Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="p-4">
            <p className="text-sm text-gray-500 dark:text-gray-400">Status</p>
            <div className="mt-1">
              <StatusBadge status={source.status} />
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <p className="text-sm text-gray-500 dark:text-gray-400">Type</p>
            <p className="mt-1 font-semibold text-gray-900 dark:text-gray-100">
              {source.type.toUpperCase()}
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <p className="text-sm text-gray-500 dark:text-gray-400">Last Run</p>
            <p className="mt-1 font-semibold text-gray-900 dark:text-gray-100">
              {formatRelativeTime(source.last_run)}
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <p className="text-sm text-gray-500 dark:text-gray-400">Errors</p>
            <p className="mt-1 font-semibold text-gray-900 dark:text-gray-100">
              {source.error_count}
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Details */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Fields */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Database className="w-5 h-5" />
              Fields ({source.fields?.length || 0})
            </CardTitle>
          </CardHeader>
          <CardContent>
            {source.fields && source.fields.length > 0 ? (
              <div className="space-y-2">
                {source.fields.map((field, i) => (
                  <div
                    key={i}
                    className="flex items-center justify-between p-2 bg-gray-50 dark:bg-gray-700/50 rounded"
                  >
                    <span className="font-medium text-gray-900 dark:text-gray-100">
                      {field.name}
                    </span>
                    <Badge variant="default">{field.data_type}</Badge>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-sm text-gray-500 dark:text-gray-400">No fields configured</p>
            )}
          </CardContent>
        </Card>

        {/* Schedule & Config */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Clock className="w-5 h-5" />
              Schedule & Config
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Cron Schedule</p>
              <code className="text-sm bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded text-gray-900 dark:text-gray-100">
                {source.schedule || 'Not set'}
              </code>
            </div>
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Created</p>
              <p className="text-sm text-gray-900 dark:text-gray-100">
                {formatDate(source.created_at)}
              </p>
            </div>
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Last Success</p>
              <p className="text-sm text-gray-900 dark:text-gray-100">
                {formatRelativeTime(source.last_success)}
              </p>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Recent Results */}
      <Card>
        <CardHeader>
          <CardTitle>Recent Crawl Results</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50">
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">
                  Time
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">
                  Status
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">
                  Records
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
              {results && Array.isArray(results) && results.length > 0 ? (
                results.map((result: any, i: number) => (
                  <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                    <td className="px-6 py-3 text-sm text-gray-900 dark:text-gray-100">
                      {formatRelativeTime(result.created_at || result.timestamp)}
                    </td>
                    <td className="px-6 py-3">
                      {result.status === 'success' ? (
                        <span className="flex items-center gap-1 text-green-600 dark:text-green-400 text-sm">
                          <CheckCircle className="w-4 h-4" /> Success
                        </span>
                      ) : (
                        <span className="flex items-center gap-1 text-red-600 dark:text-red-400 text-sm">
                          <AlertCircle className="w-4 h-4" /> Failed
                        </span>
                      )}
                    </td>
                    <td className="px-6 py-3 text-sm text-gray-900 dark:text-gray-100">
                      {result.records_count ?? result.record_count ?? '-'}
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={3} className="px-6 py-8 text-center text-gray-500 dark:text-gray-400">
                    No results yet. Run the crawler to see results.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </CardContent>
      </Card>

      {/* Delete Confirmation Modal */}
      <ConfirmModal
        open={showDeleteModal}
        title="Delete Source"
        message={`Are you sure you want to delete "${source.name}"? This action cannot be undone.`}
        confirmLabel="Delete"
        variant="danger"
        onConfirm={() => {
          setShowDeleteModal(false)
          deleteMutation.mutate(id!)
        }}
        onCancel={() => setShowDeleteModal(false)}
      />
    </div>
  )
}
