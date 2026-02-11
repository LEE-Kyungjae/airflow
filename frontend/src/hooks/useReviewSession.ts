import { useState, useCallback, useRef } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useToast } from './useToast'
import {
  getNextReview,
  getReviewSourceContent,
  updateReview,
  revertReview,
  batchApprove,
} from '@/api/reviews'
import type { ReviewItem, SourceContentData, ReviewAction, RejectionReason } from '@/types'

interface UndoEntry {
  reviewId: string
  rowIndex: number
  previousStatus: string
}

interface ReviewSessionState {
  review: ReviewItem | null
  sourceContent: SourceContentData | null
  sourceMeta: { name: string; type: string; url: string } | null
  position: number
  totalPending: number
  selectedRowIndex: number
  rowStatuses: Map<number, string>
  isLoading: boolean
  hasNext: boolean
}

export function useReviewSession() {
  const toast = useToast()
  const queryClient = useQueryClient()
  const undoStack = useRef<UndoEntry[]>([])
  const sessionStartTime = useRef<number>(Date.now())

  const [state, setState] = useState<ReviewSessionState>({
    review: null,
    sourceContent: null,
    sourceMeta: null,
    position: 0,
    totalPending: 0,
    selectedRowIndex: 0,
    rowStatuses: new Map(),
    isLoading: false,
    hasNext: true,
  })

  const loadReview = useCallback(
    async (direction: 'forward' | 'backward' = 'forward', currentId?: string) => {
      setState((prev) => ({ ...prev, isLoading: true }))
      try {
        const result = await getNextReview({
          current_id: currentId || state.review?._id,
          direction,
        })

        if (!result.has_next || !result.review) {
          setState((prev) => ({ ...prev, hasNext: false, isLoading: false }))
          toast.info('No more reviews in queue')
          return
        }

        // Load source content
        let sourceContent: SourceContentData | null = null
        try {
          sourceContent = await getReviewSourceContent(result.review._id)
        } catch {
          // Source content may not be available
        }

        // Initialize row statuses from review data
        const data = result.review.original_data
        const rowStatuses = new Map<number, string>()
        if (Array.isArray(data)) {
          data.forEach((_, i) => rowStatuses.set(i, 'pending'))
        } else {
          rowStatuses.set(0, 'pending')
        }

        setState({
          review: result.review,
          sourceContent,
          sourceMeta: result.source || null,
          position: result.position || 0,
          totalPending: result.total_pending || 0,
          selectedRowIndex: 0,
          rowStatuses,
          isLoading: false,
          hasNext: true,
        })

        undoStack.current = []
        sessionStartTime.current = Date.now()
      } catch (err) {
        setState((prev) => ({ ...prev, isLoading: false }))
        toast.error('Failed to load review')
      }
    },
    [state.review?._id, toast]
  )

  const updateRowStatus = useCallback((index: number, status: string) => {
    setState((prev) => {
      const newStatuses = new Map(prev.rowStatuses)
      const previousStatus = newStatuses.get(index) || 'pending'

      // Push to undo stack
      if (prev.review) {
        undoStack.current.push({
          reviewId: prev.review._id,
          rowIndex: index,
          previousStatus,
        })
      }

      newStatuses.set(index, status)
      return { ...prev, rowStatuses: newStatuses }
    })
  }, [])

  const approveRowMutation = useMutation({
    mutationFn: async ({ reviewId, status }: { reviewId: string; status: string }) => {
      const duration = Date.now() - sessionStartTime.current
      return updateReview(reviewId, {
        status,
        review_duration_ms: duration,
      })
    },
    onError: () => toast.error('Failed to update review'),
  })

  const approveRow = useCallback(
    (index: number) => {
      updateRowStatus(index, 'approved')
      // Move to next row
      setState((prev) => ({
        ...prev,
        selectedRowIndex: Math.min(index + 1, prev.rowStatuses.size - 1),
      }))
    },
    [updateRowStatus]
  )

  const flagRow = useCallback(
    (index: number) => {
      updateRowStatus(index, 'flagged')
      setState((prev) => ({
        ...prev,
        selectedRowIndex: Math.min(index + 1, prev.rowStatuses.size - 1),
      }))
    },
    [updateRowStatus]
  )

  const rejectRow = useCallback(
    (index: number) => {
      updateRowStatus(index, 'rejected')
      setState((prev) => ({
        ...prev,
        selectedRowIndex: Math.min(index + 1, prev.rowStatuses.size - 1),
      }))
    },
    [updateRowStatus]
  )

  const undoLast = useCallback(() => {
    const entry = undoStack.current.pop()
    if (!entry) {
      toast.info('Nothing to undo')
      return
    }

    setState((prev) => {
      const newStatuses = new Map(prev.rowStatuses)
      newStatuses.set(entry.rowIndex, entry.previousStatus)
      return { ...prev, rowStatuses: newStatuses, selectedRowIndex: entry.rowIndex }
    })

    toast.info('Undone')
  }, [toast])

  const approveAll = useCallback(() => {
    setState((prev) => {
      const newStatuses = new Map(prev.rowStatuses)
      newStatuses.forEach((status, key) => {
        if (status === 'pending') {
          newStatuses.set(key, 'approved')
        }
      })
      return { ...prev, rowStatuses: newStatuses }
    })
  }, [])

  const saveAndNext = useCallback(async () => {
    if (!state.review) return

    // Determine overall status based on row statuses
    const statuses = Array.from(state.rowStatuses.values())
    const hasRejected = statuses.includes('rejected')
    const hasFlagged = statuses.includes('flagged')
    const allApproved = statuses.every((s) => s === 'approved')

    let reviewStatus: string
    let rejectionReason: string | undefined

    if (hasRejected) {
      reviewStatus = 'rejected'
    } else if (hasFlagged) {
      reviewStatus = 'on_hold'
    } else if (allApproved) {
      reviewStatus = 'approved'
    } else {
      reviewStatus = 'on_hold'
    }

    const duration = Date.now() - sessionStartTime.current

    try {
      await updateReview(state.review._id, {
        status: reviewStatus,
        rejection_reason: rejectionReason,
        review_duration_ms: duration,
      })

      queryClient.invalidateQueries({ queryKey: ['reviewDashboard'] })
      toast.success(
        reviewStatus === 'approved'
          ? 'Approved and saved'
          : reviewStatus === 'rejected'
            ? 'Rejected'
            : 'Saved as on hold'
      )

      // Load next review
      await loadReview('forward', state.review._id)
    } catch {
      toast.error('Failed to save review')
    }
  }, [state.review, state.rowStatuses, loadReview, queryClient, toast])

  const revertLastReview = useCallback(
    async (reviewId: string) => {
      try {
        await revertReview(reviewId)
        toast.success('Review reverted to pending')
        queryClient.invalidateQueries({ queryKey: ['reviewDashboard'] })
      } catch {
        toast.error('Failed to revert review')
      }
    },
    [queryClient, toast]
  )

  const moveUp = useCallback(() => {
    setState((prev) => ({
      ...prev,
      selectedRowIndex: Math.max(0, prev.selectedRowIndex - 1),
    }))
  }, [])

  const moveDown = useCallback(() => {
    setState((prev) => ({
      ...prev,
      selectedRowIndex: Math.min(prev.rowStatuses.size - 1, prev.selectedRowIndex + 1),
    }))
  }, [])

  const selectRow = useCallback((index: number) => {
    setState((prev) => ({
      ...prev,
      selectedRowIndex: Math.max(0, Math.min(index, prev.rowStatuses.size - 1)),
    }))
  }, [])

  return {
    ...state,
    loadReview,
    approveRow,
    flagRow,
    rejectRow,
    undoLast,
    approveAll,
    saveAndNext,
    revertLastReview,
    moveUp,
    moveDown,
    selectRow,
  }
}
