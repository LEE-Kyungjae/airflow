import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Wifi, WifiOff, CheckCircle } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { Badge, StatusBadge } from '@/components/ui/Badge'
import { LoadingPage } from '@/components/ui/LoadingSpinner'
import { getRealtimeStatus, getHealingSessions, approveHealing } from '@/api/monitoring'
import { formatRelativeTime, formatMs } from '@/lib/utils'
import { useWebSocket } from '@/hooks/useWebSocket'
import { useToast } from '@/hooks/useToast'

export default function Monitoring() {
  const queryClient = useQueryClient()
  const toast = useToast()
  const { isConnected } = useWebSocket(
    `${import.meta.env.VITE_WS_URL || 'ws://localhost:8000'}/api/monitoring/ws/live`
  )

  const { data: status, isLoading } = useQuery({
    queryKey: ['realtimeStatus'],
    queryFn: getRealtimeStatus,
    refetchInterval: 10000,
  })

  const { data: healingData } = useQuery({
    queryKey: ['healingSessions'],
    queryFn: () => getHealingSessions({ limit: 20 }),
    refetchInterval: 10000,
  })

  const approveMutation = useMutation({
    mutationFn: ({ sessionId, attempts }: { sessionId: string; attempts: number }) =>
      approveHealing(sessionId, attempts),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['healingSessions'] })
      toast.success('Healing approved')
    },
    onError: (err: Error) => {
      toast.error(err.message || 'Failed to approve healing')
    },
  })

  if (isLoading) return <LoadingPage />

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Monitoring</h1>
        <div className="flex items-center gap-2">
          {isConnected ? (
            <Badge variant="success" className="flex items-center gap-1">
              <Wifi className="w-3 h-3" />
              Live
            </Badge>
          ) : (
            <Badge variant="error" className="flex items-center gap-1">
              <WifiOff className="w-3 h-3" />
              Disconnected
            </Badge>
          )}
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
        {status?.summary && (
          <>
            <SummaryCard label="Total" value={status.summary.total} color="gray" />
            <SummaryCard label="Active" value={status.summary.active} color="green" />
            <SummaryCard label="Failed" value={status.summary.failed} color="red" />
            <SummaryCard label="Healing" value={status.summary.healing} color="yellow" />
            <SummaryCard label="Pending" value={status.summary.pending} color="blue" />
          </>
        )}
      </div>

      {/* Pipeline Status Grid */}
      <Card>
        <CardHeader>
          <CardTitle>Pipeline Status</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {status?.pipelines.map((pipeline) => (
              <div
                key={pipeline.source_id}
                className="p-4 border border-gray-200 dark:border-gray-700 rounded-lg hover:border-gray-300 dark:hover:border-gray-600 transition-colors"
              >
                <div className="flex items-center justify-between mb-2">
                  <span className="font-medium truncate text-gray-900 dark:text-gray-100">{pipeline.source_name}</span>
                  <StatusBadge status={pipeline.status} />
                </div>
                <div className="space-y-1 text-sm text-gray-500 dark:text-gray-400">
                  <div className="flex justify-between">
                    <span>Success Rate</span>
                    <span className="font-medium text-gray-700 dark:text-gray-300">{pipeline.success_rate}%</span>
                  </div>
                  <div className="flex justify-between">
                    <span>Avg Time</span>
                    <span>{formatMs(pipeline.avg_execution_time_ms)}</span>
                  </div>
                  <div className="flex justify-between">
                    <span>Last Run</span>
                    <span>{formatRelativeTime(pipeline.last_run)}</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Healing Sessions */}
      {healingData && healingData.sessions.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Self-Healing Sessions</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {healingData.sessions.map((session) => (
                <div
                  key={session.session_id}
                  className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-700/50 rounded-lg"
                >
                  <div>
                    <p className="font-medium text-gray-900 dark:text-gray-100">{session.source_name}</p>
                    <p className="text-sm text-gray-500 dark:text-gray-400">
                      Error: {session.error_code} | Attempt: {session.current_attempt}/
                      {session.max_attempts}
                    </p>
                  </div>
                  <div className="flex items-center gap-3">
                    <StatusBadge status={session.status} />
                    {session.status === 'waiting_admin' && (
                      <Button
                        size="sm"
                        onClick={() =>
                          approveMutation.mutate({
                            sessionId: session.session_id,
                            attempts: 3,
                          })
                        }
                        loading={approveMutation.isPending}
                      >
                        <CheckCircle className="w-4 h-4" />
                        Approve
                      </Button>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}

function SummaryCard({
  label,
  value,
  color,
}: {
  label: string
  value: number
  color: 'gray' | 'green' | 'red' | 'yellow' | 'blue'
}) {
  const colorStyles = {
    gray: 'bg-gray-100 text-gray-700 dark:bg-gray-700/50 dark:text-gray-300',
    green: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400',
    red: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400',
    yellow: 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400',
    blue: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400',
  }

  return (
    <div className={`p-4 rounded-lg ${colorStyles[color]}`}>
      <p className="text-sm font-medium opacity-80">{label}</p>
      <p className="text-2xl font-bold">{value}</p>
    </div>
  )
}
