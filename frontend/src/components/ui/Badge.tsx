import { cn } from '@/lib/utils'

type BadgeVariant = 'default' | 'success' | 'warning' | 'error' | 'info'

interface BadgeProps {
  children: React.ReactNode
  variant?: BadgeVariant
  className?: string
}

const variantStyles: Record<BadgeVariant, string> = {
  default: 'bg-gray-100 text-gray-700',
  success: 'bg-green-100 text-green-700',
  warning: 'bg-yellow-100 text-yellow-700',
  error: 'bg-red-100 text-red-700',
  info: 'bg-blue-100 text-blue-700',
}

export function Badge({ children, variant = 'default', className }: BadgeProps) {
  return (
    <span
      className={cn(
        'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium',
        variantStyles[variant],
        className
      )}
    >
      {children}
    </span>
  )
}

// Status badge helper
export function StatusBadge({ status }: { status: string }) {
  const statusConfig: Record<string, { variant: BadgeVariant; label: string }> = {
    active: { variant: 'success', label: 'Active' },
    inactive: { variant: 'default', label: 'Inactive' },
    error: { variant: 'error', label: 'Error' },
    healing: { variant: 'warning', label: 'Healing' },
    pending: { variant: 'info', label: 'Pending' },
    success: { variant: 'success', label: 'Success' },
    failed: { variant: 'error', label: 'Failed' },
    running: { variant: 'info', label: 'Running' },
  }

  const config = statusConfig[status] || { variant: 'default', label: status }

  return <Badge variant={config.variant}>{config.label}</Badge>
}
