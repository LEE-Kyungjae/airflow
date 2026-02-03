import { useState, useEffect, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  getNextReview,
  getSourceContent,
  updateReview,
  Review,
  FieldCorrection,
  SourceContent
} from '../services/api'
import SourceViewer from '../components/SourceViewer'
import DataPanel from '../components/DataPanel'

const REVIEWER_ID = localStorage.getItem('reviewer_id') || 'reviewer_' + Math.random().toString(36).substr(2, 9)
localStorage.setItem('reviewer_id', REVIEWER_ID)

export default function ReviewPage() {
  const { reviewId } = useParams()
  const navigate = useNavigate()
  const queryClient = useQueryClient()

  const [currentReviewId, setCurrentReviewId] = useState<string | null>(reviewId || null)
  const [corrections, setCorrections] = useState<FieldCorrection[]>([])
  const [notes, setNotes] = useState('')
  const [startTime] = useState(Date.now())
  const [showShortcuts, setShowShortcuts] = useState(true)

  // Fetch current review
  const { data: reviewData, isLoading: loadingReview } = useQuery({
    queryKey: ['review', currentReviewId],
    queryFn: () => getNextReview({ current_id: currentReviewId || undefined }),
    enabled: true
  })

  // Fetch source content
  const { data: sourceContent, isLoading: loadingSource } = useQuery({
    queryKey: ['source-content', reviewData?.review?._id],
    queryFn: () => getSourceContent(reviewData!.review!._id),
    enabled: !!reviewData?.review?._id
  })

  // Update mutation
  const updateMutation = useMutation({
    mutationFn: (params: { status: string; corrections?: FieldCorrection[]; notes?: string }) =>
      updateReview(
        reviewData!.review!._id,
        {
          status: params.status as any,
          corrections: params.corrections,
          notes: params.notes,
          review_duration_ms: Date.now() - startTime
        },
        REVIEWER_ID
      ),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['review'] })
      setCorrections([])
      setNotes('')
      goNext()
    }
  })

  const goNext = useCallback(async () => {
    const result = await getNextReview({
      current_id: reviewData?.review?._id,
      direction: 'forward'
    })
    if (result.has_next && result.review) {
      setCurrentReviewId(result.review._id)
      navigate(`/review/${result.review._id}`, { replace: true })
    }
  }, [reviewData, navigate])

  const goPrevious = useCallback(async () => {
    const result = await getNextReview({
      current_id: reviewData?.review?._id,
      direction: 'backward'
    })
    if (result.has_next && result.review) {
      setCurrentReviewId(result.review._id)
      navigate(`/review/${result.review._id}`, { replace: true })
    }
  }, [reviewData, navigate])

  const handleApprove = () => {
    updateMutation.mutate({ status: 'approved' })
  }

  const handleHold = () => {
    updateMutation.mutate({ status: 'on_hold', notes })
  }

  const handleNeedsCorrection = () => {
    updateMutation.mutate({ status: 'needs_correction', corrections, notes })
  }

  const handleCorrect = () => {
    if (corrections.length > 0) {
      updateMutation.mutate({ status: 'corrected', corrections, notes })
    }
  }

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return

      switch (e.key) {
        case 'Enter':
        case '1':
          e.preventDefault()
          handleApprove()
          break
        case '2':
          e.preventDefault()
          handleHold()
          break
        case '3':
          e.preventDefault()
          handleNeedsCorrection()
          break
        case 'ArrowLeft':
        case 'Backspace':
          e.preventDefault()
          goPrevious()
          break
        case 'ArrowRight':
          e.preventDefault()
          goNext()
          break
        case '?':
          setShowShortcuts(s => !s)
          break
      }
    }

    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [handleApprove, handleHold, handleNeedsCorrection, goPrevious, goNext])

  if (loadingReview) {
    return <div className="loading">로딩 중...</div>
  }

  if (!reviewData?.has_next || !reviewData.review) {
    return (
      <div className="review-page">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', flex: 1 }}>
          <div style={{ textAlign: 'center' }}>
            <h2>검토할 항목이 없습니다</h2>
            <p style={{ color: '#666', marginTop: '1rem' }}>모든 데이터가 검토되었습니다.</p>
            <button className="btn btn-primary" onClick={() => navigate('/')}>
              대시보드로 이동
            </button>
          </div>
        </div>
      </div>
    )
  }

  const review = reviewData.review
  const source = reviewData.source

  return (
    <div className="review-page">
      {/* Left: Source Viewer */}
      <div className="source-panel">
        <div className="panel-header">
          <h2>소스: {source?.name}</h2>
          <span className="confidence-badge medium">{source?.type.toUpperCase()}</span>
        </div>
        <div className="panel-content">
          <SourceViewer
            sourceType={source?.type || 'html'}
            sourceUrl={source?.url || ''}
            content={sourceContent}
            highlights={review.source_highlights}
            loading={loadingSource}
          />
        </div>
      </div>

      {/* Right: Data Panel */}
      <div className="data-panel">
        <div className="panel-header">
          <h2>추출된 데이터</h2>
          <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
            {review.needs_number_review && (
              <span className="confidence-badge low">숫자 검토 필요</span>
            )}
            <span className={`confidence-badge ${getConfidenceLevel(review.confidence_score)}`}>
              신뢰도: {((review.confidence_score || 0) * 100).toFixed(0)}%
            </span>
          </div>
        </div>
        <div className="panel-content">
          <DataPanel
            data={review.original_data}
            corrections={corrections}
            uncertainNumbers={review.uncertain_numbers}
            onCorrectionChange={setCorrections}
          />

          {/* Notes */}
          <div style={{ marginTop: '1rem' }}>
            <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 500 }}>메모</label>
            <textarea
              value={notes}
              onChange={e => setNotes(e.target.value)}
              placeholder="검토 관련 메모를 입력하세요..."
              style={{
                width: '100%',
                padding: '0.5rem',
                border: '1px solid #ddd',
                borderRadius: '4px',
                minHeight: '80px',
                resize: 'vertical'
              }}
            />
          </div>
        </div>

        {/* Action Bar */}
        <div className="action-bar">
          <div className="nav-buttons">
            <button className="btn btn-outline" onClick={goPrevious}>
              ← 이전
            </button>
            <span className="progress-info">
              {reviewData.position} / {reviewData.total_pending}
            </span>
            <button className="btn btn-outline" onClick={goNext}>
              다음 →
            </button>
          </div>
          <div className="action-buttons">
            <button
              className="btn btn-primary"
              onClick={handleApprove}
              disabled={updateMutation.isPending}
            >
              ✓ 검토완료 (1)
            </button>
            <button
              className="btn btn-warning"
              onClick={handleHold}
              disabled={updateMutation.isPending}
            >
              ⏸ 보류 (2)
            </button>
            <button
              className="btn btn-danger"
              onClick={corrections.length > 0 ? handleCorrect : handleNeedsCorrection}
              disabled={updateMutation.isPending}
            >
              ✎ {corrections.length > 0 ? '정정완료' : '정정필요'} (3)
            </button>
          </div>
        </div>
      </div>

      {/* Keyboard Shortcuts Help */}
      {showShortcuts && (
        <div className="shortcuts-help">
          <div><kbd>1</kbd> or <kbd>Enter</kbd> 검토완료</div>
          <div><kbd>2</kbd> 보류</div>
          <div><kbd>3</kbd> 정정</div>
          <div><kbd>←</kbd> 이전</div>
          <div><kbd>→</kbd> 다음</div>
          <div><kbd>?</kbd> 단축키 표시/숨김</div>
        </div>
      )}
    </div>
  )
}

function getConfidenceLevel(score?: number): 'high' | 'medium' | 'low' {
  if (!score) return 'low'
  if (score >= 0.8) return 'high'
  if (score >= 0.5) return 'medium'
  return 'low'
}
