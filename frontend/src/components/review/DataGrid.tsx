import { useRef, useEffect } from 'react'
import { AlertTriangle } from 'lucide-react'
import { cn } from '@/lib/utils'

interface DataGridProps {
  data: Record<string, any> | Record<string, any>[]
  selectedIndex: number
  rowStatuses: Map<number, string>
  onRowClick: (index: number) => void
  confidenceScore?: number
}

const statusColors: Record<string, string> = {
  pending: 'bg-white dark:bg-gray-800',
  approved: 'bg-green-50 dark:bg-green-900/20 border-l-4 border-l-green-500',
  flagged: 'bg-yellow-50 dark:bg-yellow-900/20 border-l-4 border-l-yellow-500',
  rejected: 'bg-red-50 dark:bg-red-900/20 border-l-4 border-l-red-500',
}

const statusLabels: Record<string, { text: string; color: string }> = {
  pending: { text: 'Pending', color: 'text-gray-400' },
  approved: { text: 'Approved', color: 'text-green-600 dark:text-green-400' },
  flagged: { text: 'Flagged', color: 'text-yellow-600 dark:text-yellow-400' },
  rejected: { text: 'Rejected', color: 'text-red-600 dark:text-red-400' },
}

export function DataGrid({ data, selectedIndex, rowStatuses, onRowClick, confidenceScore }: DataGridProps) {
  const rowRefs = useRef<Map<number, HTMLTableRowElement>>(new Map())

  // Normalize data to array
  const rows = Array.isArray(data) ? data : [data]

  // Get all unique keys from all rows
  const columns = Array.from(new Set(rows.flatMap((row) => Object.keys(row).filter((k) => !k.startsWith('_')))))

  // Auto-scroll to selected row
  useEffect(() => {
    const row = rowRefs.current.get(selectedIndex)
    if (row) {
      row.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
    }
  }, [selectedIndex])

  return (
    <div className="flex flex-col h-full">
      {/* Summary bar */}
      <div className="flex items-center justify-between px-3 py-2 bg-gray-50 dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700">
        <span className="text-xs text-gray-500 dark:text-gray-400">
          {rows.length} rows &middot; {columns.length} fields
        </span>
        <div className="flex items-center gap-3 text-xs">
          {['approved', 'flagged', 'rejected', 'pending'].map((status) => {
            const count = Array.from(rowStatuses.values()).filter((s) => s === status).length
            if (count === 0 && status === 'pending') return null
            const label = statusLabels[status]
            return (
              <span key={status} className={label.color}>
                {label.text}: {count}
              </span>
            )
          })}
        </div>
        {confidenceScore !== undefined && (
          <div className="flex items-center gap-1">
            {confidenceScore < 0.7 && <AlertTriangle className="w-3 h-3 text-yellow-500" />}
            <span
              className={cn(
                'text-xs font-medium',
                confidenceScore >= 0.8
                  ? 'text-green-600 dark:text-green-400'
                  : confidenceScore >= 0.6
                    ? 'text-yellow-600 dark:text-yellow-400'
                    : 'text-red-600 dark:text-red-400'
              )}
            >
              Confidence: {(confidenceScore * 100).toFixed(0)}%
            </span>
          </div>
        )}
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-gray-100 dark:bg-gray-800 z-10">
            <tr>
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase w-20">
                Status
              </th>
              {columns.map((col) => (
                <th
                  key={col}
                  className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase whitespace-nowrap"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
            {rows.map((row, index) => {
              const status = rowStatuses.get(index) || 'pending'
              const isSelected = index === selectedIndex
              const label = statusLabels[status]

              return (
                <tr
                  key={index}
                  ref={(el) => {
                    if (el) rowRefs.current.set(index, el)
                  }}
                  onClick={() => onRowClick(index)}
                  className={cn(
                    'cursor-pointer transition-colors',
                    statusColors[status],
                    isSelected && 'ring-2 ring-blue-500 ring-inset',
                    !isSelected && 'hover:bg-gray-50 dark:hover:bg-gray-750'
                  )}
                >
                  <td className="px-3 py-2">
                    <span className={cn('text-xs font-medium', label.color)}>
                      {label.text}
                    </span>
                  </td>
                  {columns.map((col) => (
                    <td
                      key={col}
                      className="px-3 py-2 text-gray-900 dark:text-gray-100 max-w-48 truncate"
                      title={String(row[col] ?? '')}
                    >
                      {row[col] !== null && row[col] !== undefined ? String(row[col]) : (
                        <span className="text-gray-300 dark:text-gray-600 italic">null</span>
                      )}
                    </td>
                  ))}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
