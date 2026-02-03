import { Bell, Search } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { getSystemHealth } from '@/api/dashboard'
import { cn } from '@/lib/utils'

export default function Header() {
  const { data: health } = useQuery({
    queryKey: ['systemHealth'],
    queryFn: getSystemHealth,
    refetchInterval: 30000, // Refresh every 30 seconds
  })

  const statusColors = {
    healthy: 'bg-green-500',
    degraded: 'bg-yellow-500',
    critical: 'bg-red-500',
  }

  return (
    <header className="h-16 bg-white border-b border-gray-200 flex items-center justify-between px-6">
      {/* Search */}
      <div className="flex items-center gap-4 flex-1">
        <div className="relative w-96">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            placeholder="Search sources..."
            className="w-full pl-10 pr-4 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
        </div>
      </div>

      {/* Right section */}
      <div className="flex items-center gap-4">
        {/* System Status */}
        {health && (
          <div className="flex items-center gap-2 px-3 py-1.5 bg-gray-50 rounded-lg">
            <span
              className={cn(
                'w-2 h-2 rounded-full',
                statusColors[health.status] || 'bg-gray-400'
              )}
            />
            <span className="text-sm font-medium text-gray-700">
              {health.status === 'healthy' ? 'System Healthy' :
               health.status === 'degraded' ? 'System Degraded' :
               'System Critical'}
            </span>
            <span className="text-sm text-gray-500">
              ({health.overall_score}%)
            </span>
          </div>
        )}

        {/* Notifications */}
        <button className="relative p-2 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded-lg">
          <Bell className="w-5 h-5" />
          {health && health.pending_alerts > 0 && (
            <span className="absolute top-1 right-1 w-4 h-4 bg-red-500 text-white text-xs rounded-full flex items-center justify-center">
              {health.pending_alerts}
            </span>
          )}
        </button>
      </div>
    </header>
  )
}
