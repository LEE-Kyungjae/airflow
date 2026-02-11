import { useRef, useEffect, useState, useCallback } from 'react'
import { AlertTriangle, Pencil, Check, X } from 'lucide-react'
import { cn } from '@/lib/utils'

interface CellCorrection {
  field: string
  rowIndex: number
  original_value: any
  corrected_value: any
}

interface DataGridProps {
  data: Record<string, any> | Record<string, any>[]
  selectedIndex: number
  rowStatuses: Map<number, string>
  onRowClick: (index: number) => void
  onCellEdit?: (rowIndex: number, field: string, originalValue: any, newValue: any) => void
  corrections?: CellCorrection[]
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

export function DataGrid({
  data,
  selectedIndex,
  rowStatuses,
  onRowClick,
  onCellEdit,
  corrections = [],
  confidenceScore,
}: DataGridProps) {
  const rowRefs = useRef<Map<number, HTMLTableRowElement>>(new Map())
  const [editingCell, setEditingCell] = useState<{ row: number; col: string } | null>(null)
  const [editValue, setEditValue] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)

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

  // Focus input when editing
  useEffect(() => {
    if (editingCell && inputRef.current) {
      inputRef.current.focus()
      inputRef.current.select()
    }
  }, [editingCell])

  // Get correction for a specific cell
  const getCorrection = useCallback(
    (rowIndex: number, field: string) =>
      corrections.find((c) => c.rowIndex === rowIndex && c.field === field),
    [corrections]
  )

  // Start editing
  const startEdit = useCallback((rowIndex: number, col: string, currentValue: any) => {
    if (!onCellEdit) return
    setEditingCell({ row: rowIndex, col })
    setEditValue(currentValue != null ? String(currentValue) : '')
  }, [onCellEdit])

  // Confirm edit
  const confirmEdit = useCallback(() => {
    if (!editingCell || !onCellEdit) return
    const row = rows[editingCell.row]
    const originalValue = row[editingCell.col]
    if (String(originalValue ?? '') !== editValue) {
      onCellEdit(editingCell.row, editingCell.col, originalValue, editValue)
    }
    setEditingCell(null)
  }, [editingCell, editValue, onCellEdit, rows])

  // Cancel edit
  const cancelEdit = useCallback(() => {
    setEditingCell(null)
  }, [])

  // Handle key in edit mode
  const handleEditKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'Enter') {
        e.preventDefault()
        e.stopPropagation()
        confirmEdit()
      } else if (e.key === 'Escape') {
        e.preventDefault()
        e.stopPropagation()
        cancelEdit()
      } else if (e.key === 'Tab') {
        e.preventDefault()
        confirmEdit()
        // Move to next column
        if (editingCell) {
          const colIdx = columns.indexOf(editingCell.col)
          if (colIdx < columns.length - 1) {
            const nextCol = columns[colIdx + 1]
            const row = rows[editingCell.row]
            startEdit(editingCell.row, nextCol, row[nextCol])
          }
        }
      }
    },
    [confirmEdit, cancelEdit, editingCell, columns, rows, startEdit]
  )

  const correctionCount = corrections.length

  return (
    <div className="flex flex-col h-full">
      {/* Summary bar */}
      <div className="flex items-center justify-between px-3 py-2 bg-gray-50 dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700">
        <span className="text-xs text-gray-500 dark:text-gray-400">
          {rows.length} rows &middot; {columns.length} fields
          {correctionCount > 0 && (
            <span className="ml-2 text-blue-600 dark:text-blue-400">
              &middot; {correctionCount} edited
            </span>
          )}
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
                  {columns.map((col) => {
                    const correction = getCorrection(index, col)
                    const isEditing = editingCell?.row === index && editingCell?.col === col
                    const displayValue = correction ? correction.corrected_value : row[col]

                    return (
                      <td
                        key={col}
                        className={cn(
                          'px-3 py-2 max-w-48',
                          correction && 'bg-blue-50/50 dark:bg-blue-900/10'
                        )}
                        onDoubleClick={(e) => {
                          e.stopPropagation()
                          startEdit(index, col, displayValue)
                        }}
                      >
                        {isEditing ? (
                          <div className="flex items-center gap-1">
                            <input
                              ref={inputRef}
                              value={editValue}
                              onChange={(e) => setEditValue(e.target.value)}
                              onKeyDown={handleEditKeyDown}
                              onBlur={confirmEdit}
                              className="w-full px-1 py-0.5 text-sm border border-blue-400 rounded bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-1 focus:ring-blue-500"
                            />
                            <button onClick={confirmEdit} className="p-0.5 text-green-600 hover:text-green-700">
                              <Check className="w-3 h-3" />
                            </button>
                            <button onClick={cancelEdit} className="p-0.5 text-red-600 hover:text-red-700">
                              <X className="w-3 h-3" />
                            </button>
                          </div>
                        ) : (
                          <div className="flex items-center gap-1 group">
                            {correction ? (
                              <span className="text-blue-700 dark:text-blue-300 truncate" title={`Original: ${correction.original_value} → ${correction.corrected_value}`}>
                                {String(displayValue ?? '')}
                              </span>
                            ) : displayValue !== null && displayValue !== undefined ? (
                              <span className="text-gray-900 dark:text-gray-100 truncate" title={String(displayValue)}>
                                {String(displayValue)}
                              </span>
                            ) : (
                              <span className="text-gray-300 dark:text-gray-600 italic">null</span>
                            )}
                            {onCellEdit && isSelected && (
                              <Pencil className="w-3 h-3 text-gray-400 opacity-0 group-hover:opacity-100 flex-shrink-0" />
                            )}
                          </div>
                        )}
                      </td>
                    )
                  })}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
