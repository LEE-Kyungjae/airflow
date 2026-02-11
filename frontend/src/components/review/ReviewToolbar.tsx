import { ChevronLeft, ChevronRight, CheckCheck, Keyboard } from 'lucide-react'
import { Button } from '@/components/ui/Button'
import { Badge } from '@/components/ui/Badge'

interface ReviewToolbarProps {
  position: number
  totalPending: number
  sourceName: string
  sourceType: string
  onPrev: () => void
  onNext: () => void
  onApproveAll: () => void
  onSaveAndNext: () => void
  isLoading: boolean
}

export function ReviewToolbar({
  position,
  totalPending,
  sourceName,
  sourceType,
  onPrev,
  onNext,
  onApproveAll,
  onSaveAndNext,
  isLoading,
}: ReviewToolbarProps) {
  const progress = totalPending > 0 ? ((position) / totalPending) * 100 : 0

  return (
    <div className="flex items-center justify-between px-4 py-2 bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
      {/* Left: Navigation */}
      <div className="flex items-center gap-2">
        <Button variant="ghost" size="sm" onClick={onPrev} disabled={isLoading || position <= 1}>
          <ChevronLeft className="w-4 h-4" />
        </Button>
        <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
          {position} / {totalPending}
        </span>
        <Button variant="ghost" size="sm" onClick={onNext} disabled={isLoading}>
          <ChevronRight className="w-4 h-4" />
        </Button>

        {/* Progress bar */}
        <div className="w-24 h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden ml-2">
          <div
            className="h-full bg-blue-500 rounded-full transition-all duration-300"
            style={{ width: `${Math.min(progress, 100)}%` }}
          />
        </div>
      </div>

      {/* Center: Source info */}
      <div className="flex items-center gap-2">
        <span className="text-sm font-medium text-gray-900 dark:text-gray-100 truncate max-w-48">
          {sourceName}
        </span>
        <Badge variant="default">
          {sourceType.toUpperCase()}
        </Badge>
      </div>

      {/* Right: Actions + Shortcuts hint */}
      <div className="flex items-center gap-2">
        <Button variant="outline" size="sm" onClick={onApproveAll} disabled={isLoading}>
          <CheckCheck className="w-4 h-4 mr-1" />
          Approve All
        </Button>
        <Button variant="primary" size="sm" onClick={onSaveAndNext} disabled={isLoading} loading={isLoading}>
          Save & Next
        </Button>
        <div className="hidden lg:flex items-center gap-1 ml-2 text-xs text-gray-400 dark:text-gray-500">
          <Keyboard className="w-3 h-3" />
          <span>↑↓ Enter Space Bksp</span>
        </div>
      </div>
    </div>
  )
}
