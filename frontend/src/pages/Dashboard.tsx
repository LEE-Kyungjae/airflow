import { useQuery } from '@tanstack/react-query'
import {
  Database,
  Activity,
  CheckCircle,
  AlertTriangle,
  TrendingUp,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/Card'
import { LoadingPage } from '@/components/ui/LoadingSpinner'
import { getDashboardStats, getExecutionTrends, getSystemHealth } from '@/api/dashboard'
import { formatNumber, formatMs } from '@/lib/utils'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'

export default function Dashboard() {
  const { data: stats, isLoading: statsLoading } = useQuery({
    queryKey: ['dashboardStats'],
    queryFn: getDashboardStats,
    refetchInterval: 30000,
  })

  const { data: trends } = useQuery({
    queryKey: ['executionTrends'],
    queryFn: () => getExecutionTrends(7),
    refetchInterval: 60000,
  })

  const { data: health } = useQuery({
    queryKey: ['systemHealth'],
    queryFn: getSystemHealth,
    refetchInterval: 30000,
  })

  if (statsLoading) return <LoadingPage />

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          title="Active Sources"
          value={stats?.sources.active || 0}
          subtitle={`of ${stats?.sources.total || 0} total`}
          icon={Database}
          color="blue"
        />
        <StatCard
          title="Success Rate"
          value={`${stats?.recent_executions.success_rate || 0}%`}
          subtitle={`${stats?.recent_executions.success || 0}/${stats?.recent_executions.total || 0} successful`}
          icon={CheckCircle}
          color="green"
        />
        <StatCard
          title="Unresolved Errors"
          value={stats?.unresolved_errors || 0}
          icon={AlertTriangle}
          color={stats?.unresolved_errors ? 'red' : 'gray'}
        />
        <StatCard
          title="Active Crawlers"
          value={stats?.crawlers.active || 0}
          subtitle={`of ${stats?.crawlers.total || 0} total`}
          icon={Activity}
          color="purple"
        />
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Execution Trends Chart */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <TrendingUp className="w-5 h-5" />
              Execution Trends (7 Days)
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              {trends && trends.length > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={trends}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                    <XAxis dataKey="date" fontSize={12} />
                    <YAxis fontSize={12} />
                    <Tooltip />
                    <Legend />
                    <Line
                      type="monotone"
                      dataKey="success"
                      stroke="#22c55e"
                      strokeWidth={2}
                      name="Success"
                    />
                    <Line
                      type="monotone"
                      dataKey="failed"
                      stroke="#ef4444"
                      strokeWidth={2}
                      name="Failed"
                    />
                  </LineChart>
                </ResponsiveContainer>
              ) : (
                <div className="flex items-center justify-center h-full text-gray-500">
                  No data available
                </div>
              )}
            </div>
          </CardContent>
        </Card>

        {/* System Health Card */}
        <Card>
          <CardHeader>
            <CardTitle>System Health</CardTitle>
          </CardHeader>
          <CardContent>
            {health ? (
              <div className="space-y-4">
                {/* Health Score */}
                <div className="flex items-center gap-4">
                  <div className="relative w-24 h-24">
                    <svg className="w-full h-full transform -rotate-90">
                      <circle
                        cx="48"
                        cy="48"
                        r="40"
                        stroke="#e5e7eb"
                        strokeWidth="8"
                        fill="none"
                      />
                      <circle
                        cx="48"
                        cy="48"
                        r="40"
                        stroke={
                          health.status === 'healthy'
                            ? '#22c55e'
                            : health.status === 'degraded'
                            ? '#f59e0b'
                            : '#ef4444'
                        }
                        strokeWidth="8"
                        fill="none"
                        strokeDasharray={`${(health.overall_score / 100) * 251.2} 251.2`}
                      />
                    </svg>
                    <div className="absolute inset-0 flex items-center justify-center">
                      <span className="text-2xl font-bold">{health.overall_score}</span>
                    </div>
                  </div>
                  <div>
                    <p className="font-medium text-gray-900">
                      {health.status === 'healthy'
                        ? 'System Healthy'
                        : health.status === 'degraded'
                        ? 'System Degraded'
                        : 'System Critical'}
                    </p>
                    <p className="text-sm text-gray-500">
                      {health.active_sources} active, {health.failed_sources} failed
                    </p>
                  </div>
                </div>

                {/* Components Status */}
                <div className="space-y-2">
                  {Object.entries(health.components).map(([name, status]) => (
                    <div key={name} className="flex items-center justify-between">
                      <span className="text-sm text-gray-600 capitalize">{name}</span>
                      <span
                        className={`text-sm font-medium ${
                          status === 'healthy'
                            ? 'text-green-600'
                            : status === 'degraded'
                            ? 'text-yellow-600'
                            : 'text-red-600'
                        }`}
                      >
                        {status}
                      </span>
                    </div>
                  ))}
                </div>

                {/* Alerts */}
                {health.pending_alerts > 0 && (
                  <div className="p-3 bg-red-50 rounded-lg">
                    <p className="text-sm text-red-700">
                      {health.pending_alerts} pending alert(s) require attention
                    </p>
                  </div>
                )}
              </div>
            ) : (
              <div className="flex items-center justify-center h-48 text-gray-500">
                Loading health data...
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

// Stat Card Component
interface StatCardProps {
  title: string
  value: string | number
  subtitle?: string
  icon: React.ElementType
  color: 'blue' | 'green' | 'red' | 'purple' | 'gray'
}

function StatCard({ title, value, subtitle, icon: Icon, color }: StatCardProps) {
  const colorStyles = {
    blue: 'text-blue-600 bg-blue-50',
    green: 'text-green-600 bg-green-50',
    red: 'text-red-600 bg-red-50',
    purple: 'text-purple-600 bg-purple-50',
    gray: 'text-gray-600 bg-gray-50',
  }

  return (
    <Card>
      <CardContent className="flex items-center gap-4 py-6">
        <div className={`p-3 rounded-lg ${colorStyles[color]}`}>
          <Icon className="w-6 h-6" />
        </div>
        <div>
          <p className="text-sm font-medium text-gray-500">{title}</p>
          <p className="text-2xl font-bold text-gray-900">{value}</p>
          {subtitle && <p className="text-sm text-gray-500">{subtitle}</p>}
        </div>
      </CardContent>
    </Card>
  )
}
