import { useState } from 'react'
import { Trash2, Plus, GripVertical, Code } from 'lucide-react'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/Button'
import type { FieldDefinition } from '@/types'

interface FieldEditorProps {
  fields: FieldDefinition[]
  onChange: (fields: FieldDefinition[]) => void
  showSelectors?: boolean
}

const dataTypes: FieldDefinition['data_type'][] = ['string', 'number', 'date', 'url', 'list']

function emptyField(): FieldDefinition {
  return {
    name: '',
    selector: '',
    data_type: 'string',
    is_list: false,
  }
}

export function FieldEditor({ fields, onChange, showSelectors: initialShowSelectors }: FieldEditorProps) {
  const [showSelectors, setShowSelectors] = useState(initialShowSelectors ?? false)

  const updateField = (index: number, updates: Partial<FieldDefinition>) => {
    const next = fields.map((f, i) => (i === index ? { ...f, ...updates } : f))
    onChange(next)
  }

  const removeField = (index: number) => {
    onChange(fields.filter((_, i) => i !== index))
  }

  const addField = () => {
    onChange([...fields, emptyField()])
  }

  return (
    <div className="space-y-3">
      {/* Toggle selectors */}
      <div className="flex items-center justify-between">
        <p className="text-sm font-medium text-gray-700 dark:text-gray-300">
          Fields ({fields.length})
        </p>
        <button
          type="button"
          onClick={() => setShowSelectors(!showSelectors)}
          className={cn(
            'flex items-center gap-1.5 text-xs px-2 py-1 rounded',
            showSelectors
              ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400'
              : 'text-gray-500 hover:bg-gray-100 dark:text-gray-400 dark:hover:bg-gray-700'
          )}
        >
          <Code className="w-3 h-3" />
          {showSelectors ? 'Hide selectors' : 'Show selectors'}
        </button>
      </div>

      {/* Field list */}
      <div className="space-y-2">
        {fields.map((field, index) => (
          <div
            key={index}
            className="flex items-center gap-2 p-3 bg-gray-50 dark:bg-gray-700/50 rounded-lg group"
          >
            <GripVertical className="w-4 h-4 text-gray-300 dark:text-gray-600 flex-shrink-0" />

            {/* Field name */}
            <input
              type="text"
              value={field.name}
              onChange={(e) => updateField(index, { name: e.target.value })}
              placeholder="Field name"
              className="flex-1 min-w-0 px-3 py-1.5 text-sm border border-gray-200 dark:border-gray-600 rounded bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-1 focus:ring-blue-500"
            />

            {/* Data type */}
            <select
              value={field.data_type}
              onChange={(e) =>
                updateField(index, { data_type: e.target.value as FieldDefinition['data_type'] })
              }
              className="px-2 py-1.5 text-sm border border-gray-200 dark:border-gray-600 rounded bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-1 focus:ring-blue-500"
            >
              {dataTypes.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>

            {/* Selector (tech detail) */}
            {showSelectors && (
              <input
                type="text"
                value={field.selector}
                onChange={(e) => updateField(index, { selector: e.target.value })}
                placeholder="CSS selector"
                className="w-40 px-3 py-1.5 text-xs font-mono border border-gray-200 dark:border-gray-600 rounded bg-white dark:bg-gray-800 text-gray-600 dark:text-gray-400 focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
            )}

            {/* Delete */}
            <button
              type="button"
              onClick={() => removeField(index)}
              className="p-1.5 text-gray-400 hover:text-red-500 dark:text-gray-500 dark:hover:text-red-400 opacity-0 group-hover:opacity-100 transition-opacity"
            >
              <Trash2 className="w-4 h-4" />
            </button>
          </div>
        ))}
      </div>

      {/* Add field */}
      <Button type="button" variant="outline" size="sm" onClick={addField}>
        <Plus className="w-4 h-4" />
        Add Field
      </Button>
    </div>
  )
}
