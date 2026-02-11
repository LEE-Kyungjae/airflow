import { useState, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  ClipboardCheck,
  Play,
  RotateCcw,
  BarChart3,
  CheckCircle2,
  AlertTriangle,
  TrendingUp,
} from 'lucide-react'
import { Button } from '@/components/ui/Button'
import { Badge } from '@/components/ui/Badge'
import { DataGrid } from '@/components/review/DataGrid'
import { ReviewToolbar } from '@/components/review/ReviewToolbar'
import { SourceViewer } from '@/components/review/SourceViewer'
import { RejectReasonModal } from '@/components/review/RejectReasonModal'
import { useReviewSession } from '@/hooks/useReviewSession'
import { useKeyboardReview } from '@/hooks/useKeyboardReview'
import { getReviewDashboard, getResumeInfo } from '@/api/reviews'
import { cn } from '@/lib/utils'
import type { RejectionReason, ReviewDashboardData } from '@/types'

type ViewMode = 'dashboard' | 'review'

export default function ReviewPage() {
  const [viewMode, setViewMode] = useState<ViewMode>('dashboard')
  const [rejectModalOpen, setRejectModalOpen] = useState(false)

  const session = useReviewSession()

  // Dashboard data
  const { data: dashboard, isLoading: dashboardLoading } = useQuery({
    queryKey: ['reviewDashboard'],
    queryFn: getReviewDashboard,
    enabled: viewMode === 'dashboard',
  })

  // Resume info
  const { data: resumeInfo } = useQuery({
    queryKey: ['reviewResume'],
    queryFn: getResumeInfo,
    enabled: viewMode === 'dashboard',
  })

  // Start review session
  const handleStartReview = useCallback(async () => {
    setViewMode('review')
    await session.loadReview('forward')
  }, [session])

  // Resume from bookmark
  const handleResumeReview = useCallback(async () => {
    if (resumeInfo?.last_review_id) {
      setViewMode('review')
      await session.loadReview('forward', resumeInfo.last_review_id)
    }
  }, [resumeInfo, session])

  // Back to dashboard
  const handleBackToDashboard = useCallback(() => {
    setViewMode('dashboard')
  }, [])

  // Rejection handler
  const handleRejectConfirm = useCallback(
    (_reason: RejectionReason, _notes?: string) => {
      session.rejectRow(session.selectedRowIndex)
      setRejectModalOpen(false)
    },
    [session]
  )

  // Keyboard shortcuts
  useKeyboardReview({
    onMoveUp: session.moveUp,
    onMoveDown: session.moveDown,
    onApprove: () => session.approveRow(session.selectedRowIndex),
    onFlag: () => session.flagRow(session.selectedRowIndex),
    onReject: () => setRejectModalOpen(true),
    onUndo: session.undoLast,
    onApproveAll: session.approveAll,
    onEscape: handleBackToDashboard,
    enabled: viewMode === 'review' && !rejectModalOpen,
  })

  // Get active highlight based on selected row
  const activeHighlight =
    session.review?.source_highlights?.[session.selectedRowIndex] || undefined

  if (viewMode === 'dashboard') {
    return (
      <ReviewDashboard
        dashboard={dashboard}
        resumeInfo={resumeInfo}
        isLoading={dashboardLoading}
        onStart={handleStartReview}
        onResume={handleResumeReview}
      />
    )
  }

  // Review mode
  if (!session.review && !session.isLoading) {
    return (
      <div className="flex flex-col items-center justify-center h-[calc(100vh-8rem)] text-center">
        <CheckCircle2 className="w-16 h-16 text-green-500 mb-4" />
        <h2 className="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-2">
          All reviews complete
        </h2>
        <p className="text-gray-500 dark:text-gray-400 mb-6">
          No more pending items in the queue.
        </p>
        <Button variant="outline" onClick={handleBackToDashboard}>
          Back to Dashboard
        </Button>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-[calc(100vh-5rem)] -m-4 md:-m-6">
      {/* Toolbar */}
      <ReviewToolbar
        position={session.position}
        totalPending={session.totalPending}
        sourceName={session.sourceMeta?.name || 'Loading...'}
        sourceType={session.sourceMeta?.type || ''}
        onPrev={() => session.loadReview('backward', session.review?._id)}
        onNext={() => session.saveAndNext()}
        onApproveAll={session.approveAll}
        onSaveAndNext={session.saveAndNext}
        isLoading={session.isLoading}
      />

      {/* Split panel */}
      <div className="flex-1 flex overflow-hidden">
        {/* Left: Source Viewer */}
        <div className="w-1/2 border-r border-gray-200 dark:border-gray-700">
          <SourceViewer
            content={session.sourceContent}
            activeHighlight={activeHighlight}
            isLoading={session.isLoading}
          />
        </div>

        {/* Right: Data Grid */}
        <div className="w-1/2">
          {session.review ? (
            <DataGrid
              data={session.review.original_data}
              selectedIndex={session.selectedRowIndex}
              rowStatuses={session.rowStatuses}
              onRowClick={(index) => session.selectRow(index)}
              confidenceScore={session.review.confidence_score}
            />
          ) : (
            <div className="flex items-center justify-center h-full">
              <div className="animate-spin w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full" />
            </div>
          )}
        </div>
      </div>

      {/* Reject Reason Modal */}
      <RejectReasonModal
        isOpen={rejectModalOpen}
        onClose={() => setRejectModalOpen(false)}
        onConfirm={handleRejectConfirm}
      />
    </div>
  )
}

// ============================================================
// Review Dashboard Sub-component
// ============================================================

interface ReviewDashboardProps {
  dashboard?: ReviewDashboardData
  resumeInfo?: {
    has_bookmark: boolean
    last_review_id?: string
    last_reviewed_at?: string
    remaining_after_bookmark?: number
    total_pending: number
  }
  isLoading: boolean
  onStart: () => void
  onResume: () => void
}

function ReviewDashboard({ dashboard, resumeInfo, isLoading, onStart, onResume }: ReviewDashboardProps) {
  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full" />
      </div>
    )
  }

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Data Review</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Verify and approve extracted data before production
          </p>
        </div>
        <div className="flex gap-2">
          {resumeInfo?.has_bookmark && (
            <Button variant="outline" onClick={onResume}>
              <RotateCcw className="w-4 h-4 mr-1" />
              Resume ({resumeInfo.remaining_after_bookmark} remaining)
            </Button>
          )}
          <Button variant="primary" onClick={onStart} disabled={!dashboard?.pending_count}>
            <Play className="w-4 h-4 mr-1" />
            Start Review
          </Button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard
          icon={ClipboardCheck}
          label="Pending"
          value={dashboard?.pending_count ?? 0}
          color="blue"
        />
        <StatCard
          icon={CheckCircle2}
          label="Reviewed Today"
          value={dashboard?.today_reviewed ?? 0}
          color="green"
        />
        <StatCard
          icon={TrendingUp}
          label="Approval Rate"
          value={`${(dashboard?.approval_rate ?? 0).toFixed(1)}%`}
          color="emerald"
        />
        <StatCard
          icon={BarChart3}
          label="Avg Confidence"
          value={`${((dashboard?.avg_confidence ?? 0) * 100).toFixed(0)}%`}
          color="purple"
        />
      </div>

      {/* Number review alert */}
      {dashboard?.needs_number_review_count ? (
        <div className="flex items-center gap-3 p-4 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg">
          <AlertTriangle className="w-5 h-5 text-yellow-600 dark:text-yellow-400 flex-shrink-0" />
          <div>
            <p className="text-sm font-medium text-yellow-800 dark:text-yellow-200">
              {dashboard.needs_number_review_count} items need number verification
            </p>
            <p className="text-xs text-yellow-600 dark:text-yellow-400">
              These records have uncertain numeric values that require manual review.
            </p>
          </div>
        </div>
      ) : null}

      {/* By Source */}
      {dashboard?.by_source && dashboard.by_source.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
          <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700">
            <h3 className="text-sm font-medium text-gray-900 dark:text-gray-100">Pending by Source</h3>
          </div>
          <div className="divide-y divide-gray-100 dark:divide-gray-700">
            {dashboard.by_source.map((source) => (
              <div key={source.source_id} className="flex items-center justify-between px-4 py-3">
                <span className="text-sm text-gray-700 dark:text-gray-300">{source.source_name}</span>
                <Badge variant="info">{source.pending_count}</Badge>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recent Reviews */}
      {dashboard?.recent_reviews && dashboard.recent_reviews.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
          <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700">
            <h3 className="text-sm font-medium text-gray-900 dark:text-gray-100">Recent Reviews</h3>
          </div>
          <div className="divide-y divide-gray-100 dark:divide-gray-700">
            {dashboard.recent_reviews.slice(0, 5).map((review) => (
              <div key={review._id} className="flex items-center justify-between px-4 py-3">
                <div className="flex items-center gap-2">
                  <Badge
                    variant={
                      review.review_status === 'approved'
                        ? 'success'
                        : review.review_status === 'corrected'
                          ? 'warning'
                          : 'default'
                    }
                  >
                    {review.review_status}
                  </Badge>
                  <span className="text-xs text-gray-500 dark:text-gray-400">
                    {review.reviewed_at
                      ? new Date(review.reviewed_at).toLocaleString()
                      : ''}
                  </span>
                </div>
                <span className="text-xs text-gray-400">
                  {review.confidence_score
                    ? `${(review.confidence_score * 100).toFixed(0)}%`
                    : ''}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Keyboard shortcuts help */}
      <div className="bg-gray-50 dark:bg-gray-800/50 rounded-lg p-4 border border-gray-200 dark:border-gray-700">
        <h3 className="text-sm font-medium text-gray-900 dark:text-gray-100 mb-3">Keyboard Shortcuts</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-xs">
          {[
            { key: 'Enter', action: 'Approve row' },
            { key: 'Space', action: 'Flag row' },
            { key: 'Backspace', action: 'Reject row' },
            { key: 'Ctrl+Enter', action: 'Approve all' },
            { key: '↑ / ↓', action: 'Navigate rows' },
            { key: 'Ctrl+Z', action: 'Undo' },
            { key: 'Esc', action: 'Back to dashboard' },
          ].map(({ key, action }) => (
            <div key={key} className="flex items-center gap-2">
              <kbd className="px-1.5 py-0.5 bg-gray-200 dark:bg-gray-700 rounded text-gray-600 dark:text-gray-300 font-mono">
                {key}
              </kbd>
              <span className="text-gray-500 dark:text-gray-400">{action}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

// ============================================================
// Stat Card
// ============================================================

const colorMap: Record<string, string> = {
  blue: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400',
  green: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400',
  emerald: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-400',
  purple: 'bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400',
}

function StatCard({
  icon: Icon,
  label,
  value,
  color,
}: {
  icon: any
  label: string
  value: string | number
  color: string
}) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
      <div className="flex items-center gap-3">
        <div className={cn('p-2 rounded-lg', colorMap[color])}>
          <Icon className="w-5 h-5" />
        </div>
        <div>
          <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">{value}</p>
          <p className="text-xs text-gray-500 dark:text-gray-400">{label}</p>
        </div>
      </div>
    </div>
  )
}