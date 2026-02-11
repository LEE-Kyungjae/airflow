import { useState } from 'react'
import { Clock, Calendar, CalendarDays, CalendarRange, Settings2 } from 'lucide-react'
import { cn } from '@/lib/utils'

interface SchedulePickerProps {
  value: string
  onChange: (cron: string) => void
}

const presets = [
  { label: 'Every Hour', cron: '0 * * * *', icon: Clock, description: 'Runs at the start of every hour' },
  { label: 'Daily 9 AM', cron: '0 9 * * *', icon: Calendar, description: 'Runs daily at 9:00 AM' },
  { label: 'Weekly Mon', cron: '0 9 * * 1', icon: CalendarDays, description: 'Every Monday at 9:00 AM' },
  { label: 'Monthly 1st', cron: '0 9 1 * *', icon: CalendarRange, description: '1st of every month at 9:00 AM' },
]

function getNextRunDescription(cron: string): string {
  const parts = cron.split(' ')
  if (parts.length !== 5) return ''
  const [min, hour, dom, , dow] = parts

  if (min === '0' && hour === '*') return 'Next run: top of next hour'
  if (min === '0' && hour === '9' && dom === '*' && dow === '*') return 'Next run: tomorrow at 9:00 AM'
  if (min === '0' && hour === '9' && dom === '*' && dow === '1') return 'Next run: next Monday at 9:00 AM'
  if (min === '0' && hour === '9' && dom === '1' && dow === '*') return 'Next run: 1st of next month at 9:00 AM'
  return `Cron: ${cron}`
}

export function SchedulePicker({ value, onChange }: SchedulePickerProps) {
  const [isCustom, setIsCustom] = useState(
    !presets.some((p) => p.cron === value) && value !== ''
  )

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-2 gap-3">
        {presets.map((preset) => {
          const Icon = preset.icon
          const isSelected = value === preset.cron && !isCustom
          return (
            <button
              key={preset.cron}
              type="button"
              onClick={() => {
                setIsCustom(false)
                onChange(preset.cron)
              }}
              className={cn(
                'flex items-center gap-3 p-3 rounded-lg border-2 text-left transition-colors',
                isSelected
                  ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20 dark:border-blue-400'
                  : 'border-gray-200 dark:border-gray-600 hover:border-gray-300 dark:hover:border-gray-500'
              )}
            >
              <Icon
                className={cn(
                  'w-5 h-5 flex-shrink-0',
                  isSelected
                    ? 'text-blue-600 dark:text-blue-400'
                    : 'text-gray-400 dark:text-gray-500'
                )}
              />
              <div>
                <p
                  className={cn(
                    'text-sm font-medium',
                    isSelected
                      ? 'text-blue-700 dark:text-blue-300'
                      : 'text-gray-900 dark:text-gray-100'
                  )}
                >
                  {preset.label}
                </p>
                <p className="text-xs text-gray-500 dark:text-gray-400">
                  {preset.description}
                </p>
              </div>
            </button>
          )
        })}
      </div>

      {/* Custom cron toggle */}
      <button
        type="button"
        onClick={() => setIsCustom(!isCustom)}
        className="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200"
      >
        <Settings2 className="w-4 h-4" />
        {isCustom ? 'Use preset' : 'Custom cron expression'}
      </button>

      {isCustom && (
        <div className="space-y-2">
          <input
            type="text"
            value={value}
            onChange={(e) => onChange(e.target.value)}
            placeholder="0 */6 * * *"
            className="w-full px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg text-sm font-mono bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          <p className="text-xs text-gray-500 dark:text-gray-400">
            Format: minute hour day-of-month month day-of-week
          </p>
        </div>
      )}

      {/* Next run preview */}
      {value && (
        <p className="text-xs text-blue-600 dark:text-blue-400">
          {getNextRunDescription(value)}
        </p>
      )}
    </div>
  )
}
