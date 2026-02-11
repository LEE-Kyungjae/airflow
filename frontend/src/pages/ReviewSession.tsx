import { useEffect, useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { ArrowLeft, Monitor, Table2 } from 'lucide-react'
import { Button } from '@/components/ui/Button'
import { LoadingPage } from '@/components/ui/LoadingSpinner'
import { ReviewToolbar } from '@/components/review/ReviewToolbar'
import { SourceViewer } from '@/components/review/SourceViewer'
import { DataGrid } from '@/components/review/DataGrid'
import { RejectReasonModal } from '@/components/review/RejectReasonModal'
import { useReviewSession } from '@/hooks/useReviewSession'
import { useKeyboardReview } from '@/hooks/useKeyboardReview'
import { useToast } from '@/hooks/useToast'
import { cn } from '@/lib/utils'
import type { SourceHighlight, RejectionReason } from '@/types'

export default function ReviewSession() {
  const navigate = useNavigate()
  const toast = useToast()
  const [showRejectModal, setShowRejectModal] = useState(false)
  const [activeHighlight, setActiveHighlight] = useState<SourceHighlight | undefined>()
  const [mobileTab, setMobileTab] = useState<'source' | 'data'>('data')

  const session = useReviewSession()

  // Load initial review on mount
  useEffect(() => {
    session.loadReview('forward')
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Handle row click → highlight in source
  const handleRowClick = useCallback(
    (index: number) => {
      // Update selected row using selectRow from session
      session.selectRow(index)

      // Find highlight for this row
      if (session.sourceContent?.highlights?.[index]) {
        setActiveHighlight(session.sourceContent.highlights[index])
      } else if (session.sourceContent?.highlights && session.review?.original_data) {
        // Try to match by field name from the data
        const data = Array.isArray(session.review.original_data)
          ? session.review.original_data
          : [session.review.original_data]
        if (data[index]) {
          const fields = Object.keys(data[index])
          const highlight = session.sourceContent.highlights.find((h) =>
            fields.includes(h.field)
          )
          if (highlight) setActiveHighlight(highlight)
        }
      }
    },
    [session]
  )

  // Handle reject with reason
  const handleReject = useCallback(() => {
    setShowRejectModal(true)
  }, [])

  const handleRejectConfirm = useCallback(
    (reason: RejectionReason, notes?: string) => {
      session.rejectRow(session.selectedRowIndex)
      setShowRejectModal(false)
      toast.info(`Marked as rejected: ${reason}`)
    },
    [session, toast]
  )

  // Handle escape → confirm exit
  const handleEscape = useCallback(() => {
    if (window.confirm('Exit review session? Your progress is auto-saved.')) {
      navigate('/reviews')
    }
  }, [navigate])

  // Keyboard shortcuts
  useKeyboardReview({
    onMoveUp: session.moveUp,
    onMoveDown: session.moveDown,
    onApprove: () => session.approveRow(session.selectedRowIndex),
    onFlag: () => session.flagRow(session.selectedRowIndex),
    onReject: handleReject,
    onUndo: session.undoLast,
    onApproveAll: session.approveAll,
    onEscape: handleEscape,
    enabled: !showRejectModal && !!session.review,
  })

  // Update active highlight when selected row changes
  useEffect(() => {
    if (session.sourceContent?.highlights?.[session.selectedRowIndex]) {
      setActiveHighlight(session.sourceContent.highlights[session.selectedRowIndex])
    }
  }, [session.selectedRowIndex, session.sourceContent])

  if (session.isLoading && !session.review) {
    return <LoadingPage />
  }

  if (!session.review && !session.hasNext) {
    return (
      <div className="flex flex-col items-center justify-center h-[calc(100vh-4rem)] gap-4">
        <div className="text-6xl">🎉</div>
        <h2 className="text-xl font-semibold text-gray-900 dark:text-gray-100">All caught up!</h2>
        <p className="text-gray-500 dark:text-gray-400">No more reviews pending</p>
        <Button variant="outline" onClick={() => navigate('/reviews')}>
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to Dashboard
        </Button>
      </div>
    )
  }

  const reviewData = session.review?.original_data ?? {}

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)]">
      {/* Toolbar */}
      <ReviewToolbar
        position={session.position}
        totalPending={session.totalPending}
        sourceName={session.sourceMeta?.name ?? 'Unknown'}
        sourceType={session.sourceMeta?.type ?? 'html'}
        onPrev={() => session.loadReview('backward', session.review?._id)}
        onNext={() => session.loadReview('forward', session.review?._id)}
        onApproveAll={session.approveAll}
        onSaveAndNext={session.saveAndNext}
        isLoading={session.isLoading}
      />

      {/* Mobile tab switcher */}
      <div className="flex md:hidden border-b border-gray-200 dark:border-gray-700">
        <button
          onClick={() => setMobileTab('source')}
          className={cn(
            'flex-1 flex items-center justify-center gap-2 py-2 text-sm font-medium border-b-2 transition-colors',
            mobileTab === 'source'
              ? 'border-blue-500 text-blue-600 dark:text-blue-400'
              : 'border-transparent text-gray-500 dark:text-gray-400'
          )}
        >
          <Monitor className="w-4 h-4" />
          Source
        </button>
        <button
          onClick={() => setMobileTab('data')}
          className={cn(
            'flex-1 flex items-center justify-center gap-2 py-2 text-sm font-medium border-b-2 transition-colors',
            mobileTab === 'data'
              ? 'border-blue-500 text-blue-600 dark:text-blue-400'
              : 'border-transparent text-gray-500 dark:text-gray-400'
          )}
        >
          <Table2 className="w-4 h-4" />
          Data
        </button>
      </div>

      {/* Split View */}
      <div className="flex-1 flex overflow-hidden">
        {/* Source Viewer (left) */}
        <div
          className={cn(
            'border-r border-gray-200 dark:border-gray-700',
            'md:w-1/2 md:block',
            mobileTab === 'source' ? 'w-full' : 'hidden'
          )}
        >
          <SourceViewer
            content={session.sourceContent}
            activeHighlight={activeHighlight}
            isLoading={session.isLoading}
          />
        </div>

        {/* Data Grid (right) */}
        <div
          className={cn(
            'md:w-1/2 md:block',
            mobileTab === 'data' ? 'w-full' : 'hidden'
          )}
        >
          <DataGrid
            data={reviewData}
            selectedIndex={session.selectedRowIndex}
            rowStatuses={session.rowStatuses}
            onRowClick={handleRowClick}
            confidenceScore={session.review?.confidence_score}
          />
        </div>
      </div>

      {/* Bottom action bar */}
      <div className="flex items-center justify-between px-4 py-3 bg-white dark:bg-gray-800 border-t border-gray-200 dark:border-gray-700">
        <div className="flex items-center gap-2">
          <Button variant="ghost" size="sm" onClick={() => navigate('/reviews')}>
            <ArrowLeft className="w-4 h-4 mr-1" />
            Exit
          </Button>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => session.flagRow(session.selectedRowIndex)}
          >
            Flag
          </Button>
          <Button
            variant="destructive"
            size="sm"
            onClick={handleReject}
          >
            Reject
          </Button>
          <Button
            variant="primary"
            size="sm"
            onClick={() => session.approveRow(session.selectedRowIndex)}
          >
            Approve
          </Button>
        </div>
      </div>

      {/* Reject Reason Modal */}
      <RejectReasonModal
        isOpen={showRejectModal}
        onClose={() => setShowRejectModal(false)}
        onConfirm={handleRejectConfirm}
      />
    </div>
  )
}
