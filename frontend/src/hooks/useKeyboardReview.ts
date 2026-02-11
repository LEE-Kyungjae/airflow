import { useEffect, useCallback } from 'react'

interface KeyboardReviewActions {
  onMoveUp: () => void
  onMoveDown: () => void
  onApprove: () => void
  onFlag: () => void
  onReject: () => void
  onUndo: () => void
  onApproveAll: () => void
  onEscape: () => void
  enabled: boolean
}

export function useKeyboardReview({
  onMoveUp,
  onMoveDown,
  onApprove,
  onFlag,
  onReject,
  onUndo,
  onApproveAll,
  onEscape,
  enabled,
}: KeyboardReviewActions) {
  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (!enabled) return

      // Don't capture when typing in inputs
      const target = e.target as HTMLElement
      if (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.isContentEditable) {
        return
      }

      switch (e.key) {
        case 'ArrowUp':
          e.preventDefault()
          onMoveUp()
          break
        case 'ArrowDown':
          e.preventDefault()
          onMoveDown()
          break
        case 'Enter':
          e.preventDefault()
          if (e.ctrlKey || e.metaKey) {
            onApproveAll()
          } else {
            onApprove()
          }
          break
        case ' ':
          e.preventDefault()
          onFlag()
          break
        case 'Backspace':
          e.preventDefault()
          onReject()
          break
        case 'z':
          if (e.ctrlKey || e.metaKey) {
            e.preventDefault()
            onUndo()
          }
          break
        case 'Escape':
          onEscape()
          break
      }
    },
    [enabled, onMoveUp, onMoveDown, onApprove, onFlag, onReject, onUndo, onApproveAll, onEscape]
  )

  useEffect(() => {
    if (enabled) {
      window.addEventListener('keydown', handleKeyDown)
      return () => window.removeEventListener('keydown', handleKeyDown)
    }
  }, [enabled, handleKeyDown])
}
