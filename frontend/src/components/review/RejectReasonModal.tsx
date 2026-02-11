import { useState, useEffect, useCallback } from 'react'
import { AlertTriangle, RefreshCw, Settings, Clock, MessageSquare } from 'lucide-react'
import { Button } from '@/components/ui/Button'
import { cn } from '@/lib/utils'
import type { RejectionReason } from '@/types'

interface RejectReasonModalProps {
  isOpen: boolean
  onClose: () => void
  onConfirm: (reason: RejectionReason, notes?: string) => void
}

const reasons: { key: RejectionReason; label: string; description: string; icon: any; shortcut: string }[] = [
  {
    key: 'data_error',
    label: 'Data Error',
    description: 'Auto re-crawl will be triggered',
    icon: RefreshCw,
    shortcut: '1',
  },
  {
    key: 'source_changed',
    label: 'Source Structure Changed',
    description: 'Crawler update needed. Previous version will be preserved.',
    icon: Settings,
    shortcut: '2',
  },
  {
    key: 'source_not_updated',
    label: 'Source Not Updated',
    description: 'Will retry on next scheduled crawl',
    icon: Clock,
    shortcut: '3',
  },
  {
    key: 'other',
    label: 'Other',
    description: 'Provide notes below',
    icon: MessageSquare,
    shortcut: '4',
  },
]

export function RejectReasonModal({ isOpen, onClose, onConfirm }: RejectReasonModalProps) {
  const [selected, setSelected] = useState<RejectionReason | null>(null)
  const [notes, setNotes] = useState('')

  // Reset on open
  useEffect(() => {
    if (isOpen) {
      setSelected(null)
      setNotes('')
    }
  }, [isOpen])

  // Keyboard shortcuts
  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (!isOpen) return

      if (e.key >= '1' && e.key <= '4') {
        e.preventDefault()
        const idx = parseInt(e.key) - 1
        setSelected(reasons[idx].key)
      } else if (e.key === 'Enter' && selected) {
        e.preventDefault()
        onConfirm(selected, notes || undefined)
      } else if (e.key === 'Escape') {
        onClose()
      }
    },
    [isOpen, selected, notes, onConfirm, onClose]
  )

  useEffect(() => {
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [handleKeyDown])

  if (!isOpen) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />

      {/* Modal */}
      <div className="relative bg-white dark:bg-gray-800 rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden">
        {/* Header */}
        <div className="flex items-center gap-3 px-6 py-4 border-b border-gray-200 dark:border-gray-700">
          <AlertTriangle className="w-5 h-5 text-red-500" />
          <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">Reject Reason</h3>
        </div>

        {/* Reason options */}
        <div className="p-4 space-y-2">
          {reasons.map((reason) => {
            const Icon = reason.icon
            const isSelected = selected === reason.key
            return (
              <button
                key={reason.key}
                onClick={() => setSelected(reason.key)}
                className={cn(
                  'w-full flex items-center gap-3 p-3 rounded-lg border-2 text-left transition-colors',
                  isSelected
                    ? 'border-red-500 bg-red-50 dark:bg-red-900/20 dark:border-red-400'
                    : 'border-gray-200 dark:border-gray-600 hover:border-gray-300 dark:hover:border-gray-500'
                )}
              >
                <Icon
                  className={cn(
                    'w-5 h-5 flex-shrink-0',
                    isSelected ? 'text-red-600 dark:text-red-400' : 'text-gray-400'
                  )}
                />
                <div className="flex-1">
                  <p className={cn(
                    'text-sm font-medium',
                    isSelected ? 'text-red-700 dark:text-red-300' : 'text-gray-900 dark:text-gray-100'
                  )}>
                    {reason.label}
                  </p>
                  <p className="text-xs text-gray-500 dark:text-gray-400">{reason.description}</p>
                </div>
                <kbd className="text-xs px-1.5 py-0.5 bg-gray-100 dark:bg-gray-700 rounded text-gray-500 dark:text-gray-400">
                  {reason.shortcut}
                </kbd>
              </button>
            )
          })}
        </div>

        {/* Notes */}
        {selected === 'other' && (
          <div className="px-4 pb-4">
            <textarea
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              placeholder="Describe the issue..."
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-red-500 resize-none"
              rows={3}
              autoFocus
            />
          </div>
        )}

        {/* Footer */}
        <div className="flex items-center justify-between px-6 py-4 bg-gray-50 dark:bg-gray-900 border-t border-gray-200 dark:border-gray-700">
          <span className="text-xs text-gray-400">Press Enter to confirm</span>
          <div className="flex gap-2">
            <Button variant="ghost" size="sm" onClick={onClose}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              size="sm"
              onClick={() => selected && onConfirm(selected, notes || undefined)}
              disabled={!selected}
            >
              Reject
            </Button>
          </div>
        </div>
      </div>
    </div>
  )
}
