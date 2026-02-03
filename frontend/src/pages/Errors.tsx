import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { RefreshCw, CheckCircle, RotateCw, Code } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { Badge } from '@/components/ui/Badge'
import { LoadingPage } from '@/components/ui/LoadingSpinner'
import { getErrors, getUnresolvedErrors, resolveError, retryError, regenerateCrawler } from '@/api/errors'
import { getErrorSummary } from '@/api/monitoring'
import { formatRelativeTime } from '@/lib/utils'
import type { ErrorLog } from '@/types'

export default function Errors() {
  const [showResolved, setShowResolved] = useState(false)
  const queryClient = useQueryClient()

  const { data: errors, isLoading } = useQuery({
    queryKey: ['errors', showResolved],
    queryFn: () => (showResolved ? getErrors({ limit: 100 }) : getUnresolvedErrors()),
  })

  const { data: summary } = useQuery({
    queryKey: ['errorSummary'],
    queryFn: () => getErrorSummary(24),
  })

  const resolveMutation = useMutation({
    mutationFn: ({ id, method }: { id: string; method: string }) =>
      resolveError(id, { method }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['errors'] })
      queryClient.invalidateQueries({ queryKey: ['errorSummary'] })
    },
  })

  const retryMutation = useMutation({
    mutationFn: retryError,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['errors'] })
    },
  })

  const regenerateMutation = useMutation({
    mutationFn: regenerateCrawler,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['errors'] })
    },
  })

  if (isLoading) return <LoadingPage />

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Error Management</h1>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setShowResolved(!showResolved)}
            className={`px-3 py-1.5 text-sm rounded-lg transition-colors ${
              showResolved
                ? 'bg-blue-100 text-blue-700'
                : 'bg-gray-100 text-gray-700'
            }`}
          >
            {showResolved ? 'Show All' : 'Unresolved Only'}
          </button>
        </div>
      </div>

      {/* Error Summary */}
      {summary && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card>
            <CardContent className="py-4">
              <p className="text-sm text-gray-500">Total Errors (24h)</p>
              <p className="text-2xl font-bold">{summary.total_errors}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="py-4">
              <p className="text-sm text-gray-500">Unresolved</p>
              <p className="text-2xl font-bold text-red-600">{summary.unresolved_count}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="py-4">
              <p className="text-sm text-gray-500">Error Types</p>
              <p className="text-2xl font-bold">{summary.error_summaries?.length || 0}</p>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="py-4">
              <p className="text-sm text-gray-500">Period</p>
              <p className="text-2xl font-bold">24h</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Error Code Breakdown */}
      {summary?.error_summaries && summary.error_summaries.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Error Breakdown by Code</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
              {summary.error_summaries.map((es) => (
                <div
                  key={es.error_code}
                  className="p-3 bg-gray-50 rounded-lg flex items-center justify-between"
                >
                  <div>
                    <p className="font-medium">{es.error_code}</p>
                    <p className="text-sm text-gray-500">{es.error_type}</p>
                  </div>
                  <div className="text-right">
                    <p className="font-bold">{es.count}</p>
                    <p className="text-xs text-gray-500">
                      {es.unresolved_count} unresolved
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Error List */}
      <Card>
        <CardHeader>
          <CardTitle>
            {showResolved ? 'All Errors' : 'Unresolved Errors'}
          </CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <div className="divide-y divide-gray-200">
            {errors?.map((error: ErrorLog) => (
              <div key={error._id} className="p-4 hover:bg-gray-50">
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      <Badge variant={error.resolved ? 'success' : 'error'}>
                        {error.error_code}
                      </Badge>
                      <span className="text-sm text-gray-500">
                        {formatRelativeTime(error.created_at)}
                      </span>
                      {error.auto_recoverable && (
                        <Badge variant="info">Auto-recoverable</Badge>
                      )}
                    </div>
                    <p className="font-medium text-gray-900 truncate">
                      {error.error_type}
                    </p>
                    <p className="text-sm text-gray-600 line-clamp-2">
                      {error.message}
                    </p>
                  </div>

                  {!error.resolved && (
                    <div className="flex items-center gap-2">
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => retryMutation.mutate(error._id)}
                        loading={retryMutation.isPending}
                      >
                        <RotateCw className="w-4 h-4" />
                        Retry
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => regenerateMutation.mutate(error._id)}
                        loading={regenerateMutation.isPending}
                      >
                        <Code className="w-4 h-4" />
                        Regenerate
                      </Button>
                      <Button
                        variant="secondary"
                        size="sm"
                        onClick={() =>
                          resolveMutation.mutate({
                            id: error._id,
                            method: 'manual',
                          })
                        }
                        loading={resolveMutation.isPending}
                      >
                        <CheckCircle className="w-4 h-4" />
                        Resolve
                      </Button>
                    </div>
                  )}
                </div>
              </div>
            ))}

            {(!errors || errors.length === 0) && (
              <div className="p-12 text-center text-gray-500">
                {showResolved ? 'No errors found' : 'No unresolved errors'}
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
