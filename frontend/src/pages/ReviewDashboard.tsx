import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  ClipboardCheck,
  Clock,
  CheckCircle2,
  TrendingUp,
  AlertTriangle,
  Play,
  RotateCcw,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { Badge } from '@/components/ui/Badge'
import { LoadingPage } from '@/components/ui/LoadingSpinner'
import { getReviewDashboard, getResumeInfo } from '@/api/reviews'
import { cn } from '@/lib/utils'

export default function ReviewDashboard() {
  const navigate = useNavigate()

  const { data: dashboard, isLoading } = useQuery({
    queryKey: ['reviewDashboard'],
    queryFn: getReviewDashboard,
    refetchInterval: 30000,
  })

  const { data: resumeInfo } = useQuery({
    queryKey: ['reviewResume'],
    queryFn: getResumeInfo,
  })

  if (isLoading) return <LoadingPage />

  const stats = [
    {
      label: 'Pending Review',
      value: dashboard?.pending_count ?? 0,
      icon: Clock,
      color: 'text-yellow-600 dark:text-yellow-400',
      bg: 'bg-yellow-50 dark:bg-yellow-900/20',
    },
    {
      label: 'Reviewed Today',
      value: dashboard?.today_reviewed ?? 0,
      icon: CheckCircle2,
      color: 'text-green-600 dark:text-green-400',
      bg: 'bg-green-50 dark:bg-green-900/20',
    },
    {
      label: 'Approval Rate',
      value: `${(dashboard?.approval_rate ?? 0).toFixed(1)}%`,
      icon: TrendingUp,
      color: 'text-blue-600 dark:text-blue-400',
      bg: 'bg-blue-50 dark:bg-blue-900/20',
    },
    {
      label: 'Avg Confidence',
      value: `${((dashboard?.avg_confidence ?? 0) * 100).toFixed(0)}%`,
      icon: ClipboardCheck,
      color: (dashboard?.avg_confidence ?? 0) >= 0.8
        ? 'text-green-600 dark:text-green-400'
        : 'text-yellow-600 dark:text-yellow-400',
      bg: (dashboard?.avg_confidence ?? 0) >= 0.8
        ? 'bg-green-50 dark:bg-green-900/20'
        : 'bg-yellow-50 dark:bg-yellow-900/20',
    },
  ]

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Data Review</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Verify crawled data accuracy before production
          </p>
        </div>
        <div className="flex gap-2">
          {resumeInfo?.has_bookmark && (
            <Button
              variant="outline"
              onClick={() => navigate('/reviews/session', { state: { resume: true } })}
            >
              <RotateCcw className="w-4 h-4 mr-2" />
              Resume ({resumeInfo.remaining_after_bookmark} remaining)
            </Button>
          )}
          <Button
            variant="primary"
            onClick={() => navigate('/reviews/session')}
            disabled={!dashboard?.pending_count}
          >
            <Play className="w-4 h-4 mr-2" />
            Start Review
          </Button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {stats.map((stat) => {
          const Icon = stat.icon
          return (
            <Card key={stat.label}>
              <CardContent className="p-4">
                <div className="flex items-center gap-3">
                  <div className={cn('p-2 rounded-lg', stat.bg)}>
                    <Icon className={cn('w-5 h-5', stat.color)} />
                  </div>
                  <div>
                    <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">{stat.value}</p>
                    <p className="text-xs text-gray-500 dark:text-gray-400">{stat.label}</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          )
        })}
      </div>

      {/* Needs Number Review Alert */}
      {(dashboard?.needs_number_review_count ?? 0) > 0 && (
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <AlertTriangle className="w-5 h-5 text-yellow-500" />
              <div>
                <p className="text-sm font-medium text-gray-900 dark:text-gray-100">
                  {dashboard?.needs_number_review_count} items need number verification
                </p>
                <p className="text-xs text-gray-500 dark:text-gray-400">
                  These records contain uncertain numeric extractions that require manual review
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* By Source Table */}
      <Card>
        <CardHeader>
          <CardTitle>Pending by Source</CardTitle>
        </CardHeader>
        <CardContent>
          {dashboard?.by_source && dashboard.by_source.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200 dark:border-gray-700">
                    <th className="text-left py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Source</th>
                    <th className="text-right py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Pending</th>
                    <th className="text-right py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Action</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
                  {dashboard.by_source.map((source) => (
                    <tr key={source.source_id} className="hover:bg-gray-50 dark:hover:bg-gray-800">
                      <td className="py-2 px-3 text-gray-900 dark:text-gray-100">{source.source_name}</td>
                      <td className="py-2 px-3 text-right">
                        <Badge variant="secondary">{source.pending_count}</Badge>
                      </td>
                      <td className="py-2 px-3 text-right">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() =>
                            navigate('/reviews/session', { state: { sourceId: source.source_id } })
                          }
                        >
                          Review
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="text-sm text-gray-400 dark:text-gray-500 text-center py-8">
              No pending reviews
            </p>
          )}
        </CardContent>
      </Card>

      {/* Recent Reviews */}
      {dashboard?.recent_reviews && dashboard.recent_reviews.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Recent Reviews</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {dashboard.recent_reviews.slice(0, 5).map((review) => (
                <div
                  key={review._id}
                  className="flex items-center justify-between py-2 border-b border-gray-100 dark:border-gray-700 last:border-0"
                >
                  <div className="flex items-center gap-2">
                    <Badge
                      variant={
                        review.review_status === 'approved'
                          ? 'default'
                          : review.review_status === 'rejected'
                            ? 'destructive'
                            : 'secondary'
                      }
                    >
                      {review.review_status}
                    </Badge>
                    <span className="text-sm text-gray-600 dark:text-gray-300 truncate max-w-64">
                      Record #{review.data_record_index} from {review.source_id.slice(-6)}
                    </span>
                  </div>
                  <span className="text-xs text-gray-400 dark:text-gray-500">
                    {review.reviewed_at
                      ? new Date(review.reviewed_at).toLocaleString()
                      : ''}
                  </span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
