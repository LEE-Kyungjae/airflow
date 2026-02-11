import { Check } from 'lucide-react'
import { cn } from '@/lib/utils'

interface Step {
  label: string
  description?: string
}

interface StepperProps {
  steps: Step[]
  currentStep: number
  onStepClick?: (step: number) => void
}

export function Stepper({ steps, currentStep, onStepClick }: StepperProps) {
  return (
    <nav className="flex items-center justify-between">
      {steps.map((step, index) => {
        const isCompleted = index < currentStep
        const isCurrent = index === currentStep
        const isClickable = onStepClick && index < currentStep

        return (
          <div key={index} className="flex items-center flex-1 last:flex-none">
            {/* Step circle + label */}
            <button
              type="button"
              onClick={() => isClickable && onStepClick(index)}
              disabled={!isClickable}
              className={cn(
                'flex items-center gap-3 group',
                isClickable && 'cursor-pointer'
              )}
            >
              <div
                className={cn(
                  'w-10 h-10 rounded-full flex items-center justify-center text-sm font-semibold border-2 transition-colors flex-shrink-0',
                  isCompleted
                    ? 'bg-blue-600 border-blue-600 text-white dark:bg-blue-500 dark:border-blue-500'
                    : isCurrent
                    ? 'border-blue-600 text-blue-600 bg-blue-50 dark:border-blue-400 dark:text-blue-400 dark:bg-blue-900/30'
                    : 'border-gray-300 text-gray-400 bg-white dark:border-gray-600 dark:text-gray-500 dark:bg-gray-800'
                )}
              >
                {isCompleted ? <Check className="w-5 h-5" /> : index + 1}
              </div>
              <div className="hidden sm:block">
                <p
                  className={cn(
                    'text-sm font-medium',
                    isCurrent || isCompleted
                      ? 'text-gray-900 dark:text-gray-100'
                      : 'text-gray-400 dark:text-gray-500'
                  )}
                >
                  {step.label}
                </p>
                {step.description && (
                  <p className="text-xs text-gray-500 dark:text-gray-400">
                    {step.description}
                  </p>
                )}
              </div>
            </button>

            {/* Connector line */}
            {index < steps.length - 1 && (
              <div
                className={cn(
                  'flex-1 h-0.5 mx-4',
                  index < currentStep
                    ? 'bg-blue-600 dark:bg-blue-500'
                    : 'bg-gray-200 dark:bg-gray-700'
                )}
              />
            )}
          </div>
        )
      })}
    </nav>
  )
}
