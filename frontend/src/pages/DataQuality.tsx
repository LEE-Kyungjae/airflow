import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import {
  AlertTriangle,
  CheckCircle,
  XCircle,
  TrendingUp,
  TrendingDown,
  Minus,
  Bell,
  FileWarning,
  Database,
  Eye,
  Clock,
  BarChart3,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { Badge } from '@/components/ui/Badge'
import { LoadingPage } from '@/components/ui/LoadingSpinner'
import {
  getQualityDashboard,
  getAnomalies,
  getValidationResults,
  getValidationTimeline,
  acknowledgeAnomaly,
  type QualityAnomaly,
  type ValidationResult,
} from '@/api/dataQuality'
import { formatRelativeTime } from '@/lib/utils'
import { useToast } from '@/hooks/useToast'

export default function DataQuality() {
  const queryClient = useQueryClient()
  const [selectedSource, setSelectedSource] = useState<string | null>(null)
  const [timelineDays, setTimelineDays] = useState(7)
  const [timelineInterval, setTimelineInterval] = useState<'hour' | 'day'>('day')
  const toast = useToast()

  const { data: dashboard, isLoading } = useQuery({
    queryKey: ['qualityDashboard'],
    queryFn: getQualityDashboard,
    refetchInterval: 30000,
  })

  const { data: anomalies } = useQuery({
    queryKey: ['anomalies'],
    queryFn: () => getAnomalies({ hours: 24 }),
    refetchInterval: 30000,
  })

  const { data: validationResults } = useQuery({
    queryKey: ['validationResults', selectedSource],
    queryFn: () => getValidationResults({ source_id: selectedSource || undefined, limit: 10 }),
    refetchInterval: 30000,
  })

  const { data: timeline } = useQuery({
    queryKey: ['validationTimeline', selectedSource, timelineDays, timelineInterval],
    queryFn: () =>
      getValidationTimeline({
        source_id: selectedSource || undefined,
        days: timelineDays,
        interval: timelineInterval,
      }),
    refetchInterval: 60000,
  })

  const acknowledgeMutation = useMutation({
    mutationFn: acknowledgeAnomaly,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['anomalies'] })
      queryClient.invalidateQueries({ queryKey: ['qualityDashboard'] })
      toast.success('Anomaly acknowledged')
    },
    onError: (err: Error) => {
      toast.error(err.message || 'Failed to acknowledge')
    },
  })

  if (isLoading) return <LoadingPage />

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Data Quality Monitor</h1>
        <Badge variant={dashboard?.overall_stats?.anomalies_count ? 'error' : 'success'}>
          {dashboard?.overall_stats?.anomalies_count || 0} Active Anomalies
        </Badge>
      </div>

      {/* Overview Stats */}
      <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
        <StatCard
          label="Avg Quality Score"
          value={`${dashboard?.overall_stats?.avg_quality_score?.toFixed(1) || 0}%`}
          icon={<Database className="w-5 h-5" />}
          color={getScoreColor(dashboard?.overall_stats?.avg_quality_score || 0)}
        />
        <StatCard
          label="Total Sources"
          value={dashboard?.overall_stats?.total_sources || 0}
          icon={<FileWarning className="w-5 h-5" />}
          color="blue"
        />
        <StatCard
          label="Issues (24h)"
          value={dashboard?.overall_stats?.total_issues_24h || 0}
          icon={<AlertTriangle className="w-5 h-5" />}
          color="yellow"
        />
        <StatCard
          label="Anomalies"
          value={dashboard?.overall_stats?.anomalies_count || 0}
          icon={<Bell className="w-5 h-5" />}
          color={dashboard?.overall_stats?.anomalies_count ? 'red' : 'green'}
        />
        <StatCard
          label="Below Threshold"
          value={dashboard?.overall_stats?.sources_below_threshold || 0}
          icon={<XCircle className="w-5 h-5" />}
          color={dashboard?.overall_stats?.sources_below_threshold ? 'red' : 'green'}
        />
      </div>

      {/* Timeline Charts */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle className="flex items-center gap-2">
              <Clock className="w-5 h-5" />
              데이터 수집 타임라인
            </CardTitle>
            <div className="flex items-center gap-2">
              <select
                value={timelineDays}
                onChange={(e) => setTimelineDays(Number(e.target.value))}
                className="px-3 py-1.5 text-sm border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value={1}>1일</option>
                <option value={7}>7일</option>
                <option value={14}>14일</option>
                <option value={30}>30일</option>
              </select>
              <select
                value={timelineInterval}
                onChange={(e) => setTimelineInterval(e.target.value as 'hour' | 'day')}
                className="px-3 py-1.5 text-sm border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="hour">시간별</option>
                <option value="day">일별</option>
              </select>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {timeline?.timeline && timeline.timeline.length > 0 ? (
            <div className="space-y-6">
              {/* Quality Score Trend */}
              <div>
                <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">품질 점수 추이</h4>
                <ResponsiveContainer width="100%" height={200}>
                  <AreaChart data={timeline.timeline}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                    <XAxis dataKey="label" tick={{ fontSize: 12 }} stroke="#9ca3af" />
                    <YAxis domain={[0, 100]} tick={{ fontSize: 12 }} stroke="#9ca3af" />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: 'white',
                        border: '1px solid #e5e7eb',
                        borderRadius: '8px',
                      }}
                      formatter={(value: number | undefined) => [`${(value ?? 0).toFixed(1)}%`, '품질 점수']}
                    />
                    <Area
                      type="monotone"
                      dataKey="avg_quality_score"
                      stroke="#3b82f6"
                      fill="#93c5fd"
                      fillOpacity={0.5}
                      name="평균"
                    />
                    <Area
                      type="monotone"
                      dataKey="min_quality_score"
                      stroke="#ef4444"
                      fill="#fca5a5"
                      fillOpacity={0.3}
                      name="최저"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>

              {/* Validation Count & Records */}
              <div>
                <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">수집 건수</h4>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={timeline.timeline}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                    <XAxis dataKey="label" tick={{ fontSize: 12 }} stroke="#9ca3af" />
                    <YAxis tick={{ fontSize: 12 }} stroke="#9ca3af" />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: 'white',
                        border: '1px solid #e5e7eb',
                        borderRadius: '8px',
                      }}
                    />
                    <Legend />
                    <Bar dataKey="validation_count" name="검증 횟수" fill="#6366f1" radius={[4, 4, 0, 0]} />
                    <Bar dataKey="total_records" name="레코드 수" fill="#a5b4fc" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>

              {/* Issues Trend */}
              <div>
                <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">이슈 발생 추이</h4>
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={timeline.timeline}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                    <XAxis dataKey="label" tick={{ fontSize: 12 }} stroke="#9ca3af" />
                    <YAxis tick={{ fontSize: 12 }} stroke="#9ca3af" />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: 'white',
                        border: '1px solid #e5e7eb',
                        borderRadius: '8px',
                      }}
                    />
                    <Legend />
                    <Line
                      type="monotone"
                      dataKey="critical_issues"
                      name="Critical"
                      stroke="#dc2626"
                      strokeWidth={2}
                      dot={{ r: 3 }}
                    />
                    <Line
                      type="monotone"
                      dataKey="error_issues"
                      name="Error"
                      stroke="#f97316"
                      strokeWidth={2}
                      dot={{ r: 3 }}
                    />
                    <Line
                      type="monotone"
                      dataKey="warning_issues"
                      name="Warning"
                      stroke="#eab308"
                      strokeWidth={2}
                      dot={{ r: 3 }}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          ) : (
            <div className="flex items-center justify-center h-48 text-gray-500 dark:text-gray-400">
              <div className="text-center">
                <BarChart3 className="w-12 h-12 mx-auto mb-2 text-gray-300 dark:text-gray-600" />
                <p>타임라인 데이터가 없습니다</p>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Source Quality Scores */}
        <Card>
          <CardHeader>
            <CardTitle>Source Quality Scores</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {dashboard?.source_scores?.map((source) => (
                <div
                  key={source.source_id}
                  className={`flex items-center justify-between p-3 rounded-lg border cursor-pointer transition-colors ${
                    selectedSource === source.source_id
                      ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
                      : 'border-gray-200 hover:border-gray-300 dark:border-gray-700 dark:hover:border-gray-600'
                  }`}
                  onClick={() =>
                    setSelectedSource(selectedSource === source.source_id ? null : source.source_id)
                  }
                >
                  <div className="flex items-center gap-3">
                    <div
                      className={`w-2 h-2 rounded-full ${getScoreDotColor(source.quality_score)}`}
                    />
                    <div>
                      <p className="font-medium text-gray-900 dark:text-gray-100">{source.source_name}</p>
                      <p className="text-xs text-gray-500 dark:text-gray-400">
                        Last: {formatRelativeTime(source.last_validation)}
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <span
                      className={`text-lg font-bold ${getScoreTextColor(source.quality_score)}`}
                    >
                      {source.quality_score.toFixed(1)}%
                    </span>
                    {source.trend === 'up' && <TrendingUp className="w-4 h-4 text-green-500" />}
                    {source.trend === 'down' && <TrendingDown className="w-4 h-4 text-red-500" />}
                    {source.trend === 'stable' && <Minus className="w-4 h-4 text-gray-400" />}
                  </div>
                </div>
              ))}
              {(!dashboard?.source_scores || dashboard.source_scores.length === 0) && (
                <p className="text-gray-500 dark:text-gray-400 text-center py-4">No validation data available</p>
              )}
            </div>
          </CardContent>
        </Card>

        {/* Top Issues */}
        <Card>
          <CardHeader>
            <CardTitle>Top Issues (All Sources)</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {dashboard?.top_issues?.map((issue, idx) => (
                <div
                  key={issue.rule}
                  className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-700/50 rounded-lg"
                >
                  <div className="flex items-center gap-3">
                    <span className="text-sm font-medium text-gray-400 dark:text-gray-500">#{idx + 1}</span>
                    <div>
                      <p className="font-medium text-gray-900 dark:text-gray-100">{formatRuleName(issue.rule)}</p>
                      <p className="text-xs text-gray-500 dark:text-gray-400">
                        Affects {issue.affected_sources} source(s)
                      </p>
                    </div>
                  </div>
                  <Badge variant="warning">{issue.count} issues</Badge>
                </div>
              ))}
              {(!dashboard?.top_issues || dashboard.top_issues.length === 0) && (
                <p className="text-gray-500 dark:text-gray-400 text-center py-4">No issues detected</p>
              )}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Active Anomalies */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Bell className="w-5 h-5 text-orange-500" />
            Active Anomalies (24h)
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {anomalies?.filter((a) => !a.acknowledged).map((anomaly) => (
              <AnomalyCard
                key={anomaly.id}
                anomaly={anomaly}
                onAcknowledge={() => acknowledgeMutation.mutate(anomaly.id)}
                isLoading={acknowledgeMutation.isPending}
              />
            ))}
            {(!anomalies || anomalies.filter((a) => !a.acknowledged).length === 0) && (
              <div className="flex items-center justify-center gap-2 py-8 text-gray-500 dark:text-gray-400">
                <CheckCircle className="w-5 h-5 text-green-500" />
                <span>No active anomalies</span>
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Recent Validations */}
      <Card>
        <CardHeader>
          <CardTitle>
            Recent Validations
            {selectedSource && (
              <span className="text-sm font-normal text-gray-500 dark:text-gray-400 ml-2">
                (Filtered by source)
              </span>
            )}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="text-left text-sm text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700">
                  <th className="pb-3 font-medium">Source</th>
                  <th className="pb-3 font-medium">Run ID</th>
                  <th className="pb-3 font-medium">Records</th>
                  <th className="pb-3 font-medium">Quality Score</th>
                  <th className="pb-3 font-medium">Issues</th>
                  <th className="pb-3 font-medium">Validated</th>
                  <th className="pb-3 font-medium">Actions</th>
                </tr>
              </thead>
              <tbody>
                {validationResults?.items?.map((result) => (
                  <ValidationResultRow key={result.id} result={result} />
                ))}
              </tbody>
            </table>
            {(!validationResults?.items || validationResults.items.length === 0) && (
              <p className="text-gray-500 dark:text-gray-400 text-center py-8">No validation results</p>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function StatCard({
  label,
  value,
  icon,
  color,
}: {
  label: string
  value: string | number
  icon: React.ReactNode
  color: string
}) {
  const colorStyles: Record<string, string> = {
    gray: 'bg-gray-100 text-gray-700 dark:bg-gray-700/50 dark:text-gray-300',
    green: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400',
    red: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400',
    yellow: 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400',
    blue: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400',
  }

  return (
    <div className={`p-4 rounded-lg ${colorStyles[color]}`}>
      <div className="flex items-center gap-2 mb-1 opacity-80">
        {icon}
        <span className="text-sm font-medium">{label}</span>
      </div>
      <p className="text-2xl font-bold">{value}</p>
    </div>
  )
}

function AnomalyCard({
  anomaly,
  onAcknowledge,
  isLoading,
}: {
  anomaly: QualityAnomaly
  onAcknowledge: () => void
  isLoading: boolean
}) {
  const severityColors: Record<string, string> = {
    info: 'bg-blue-50 border-blue-200 dark:bg-blue-900/20 dark:border-blue-800',
    warning: 'bg-yellow-50 border-yellow-200 dark:bg-yellow-900/20 dark:border-yellow-800',
    error: 'bg-red-50 border-red-200 dark:bg-red-900/20 dark:border-red-800',
  }

  const severityBadge: Record<string, 'info' | 'warning' | 'error'> = {
    info: 'info',
    warning: 'warning',
    error: 'error',
  }

  return (
    <div
      className={`flex items-center justify-between p-4 rounded-lg border ${
        severityColors[anomaly.severity] || 'bg-gray-50 border-gray-200 dark:bg-gray-700/50 dark:border-gray-600'
      }`}
    >
      <div className="flex items-start gap-3">
        <AlertTriangle
          className={`w-5 h-5 mt-0.5 ${
            anomaly.severity === 'error'
              ? 'text-red-500'
              : anomaly.severity === 'warning'
                ? 'text-yellow-500'
                : 'text-blue-500'
          }`}
        />
        <div>
          <div className="flex items-center gap-2 mb-1">
            <span className="font-medium">{anomaly.source_id}</span>
            <Badge variant={severityBadge[anomaly.severity] || 'info'}>
              {anomaly.anomaly_type.replace('_', ' ')}
            </Badge>
          </div>
          <p className="text-sm text-gray-600 dark:text-gray-400">{anomaly.description}</p>
          <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">
            Detected {formatRelativeTime(anomaly.detected_at)}
          </p>
        </div>
      </div>
      <Button size="sm" variant="outline" onClick={onAcknowledge} loading={isLoading}>
        <CheckCircle className="w-4 h-4" />
        Acknowledge
      </Button>
    </div>
  )
}

function ValidationResultRow({ result }: { result: ValidationResult }) {
  const [showDetails, setShowDetails] = useState(false)

  return (
    <>
      <tr className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800/50">
        <td className="py-3">{result.source_id}</td>
        <td className="py-3 font-mono text-sm">{result.run_id.slice(0, 8)}...</td>
        <td className="py-3">{result.total_records.toLocaleString()}</td>
        <td className="py-3">
          <span className={`font-bold ${getScoreTextColor(result.quality_score)}`}>
            {result.quality_score.toFixed(1)}%
          </span>
        </td>
        <td className="py-3">
          <div className="flex gap-1">
            {result.issue_summary.by_severity.critical > 0 && (
              <Badge variant="error">{result.issue_summary.by_severity.critical} critical</Badge>
            )}
            {result.issue_summary.by_severity.error > 0 && (
              <Badge variant="error">{result.issue_summary.by_severity.error} error</Badge>
            )}
            {result.issue_summary.by_severity.warning > 0 && (
              <Badge variant="warning">{result.issue_summary.by_severity.warning} warn</Badge>
            )}
            {result.issue_summary.total === 0 && <Badge variant="success">Clean</Badge>}
          </div>
        </td>
        <td className="py-3 text-sm text-gray-500 dark:text-gray-400">
          {formatRelativeTime(result.validated_at)}
        </td>
        <td className="py-3">
          <Button size="sm" variant="ghost" onClick={() => setShowDetails(!showDetails)}>
            <Eye className="w-4 h-4" />
          </Button>
        </td>
      </tr>
      {showDetails && (
        <tr>
          <td colSpan={7} className="p-4 bg-gray-50 dark:bg-gray-800/50">
            <IssueDetails result={result} />
          </td>
        </tr>
      )}
    </>
  )
}

function IssueDetails({ result }: { result: ValidationResult }) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      {/* Issues by Rule */}
      <div>
        <h4 className="font-medium text-sm text-gray-700 dark:text-gray-300 mb-2">Issues by Rule</h4>
        <div className="space-y-1">
          {Object.entries(result.issue_summary.by_rule).map(([rule, count]) => (
            <div key={rule} className="flex justify-between text-sm text-gray-700 dark:text-gray-300">
              <span>{formatRuleName(rule)}</span>
              <span className="font-medium">{count}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Issues by Field */}
      <div>
        <h4 className="font-medium text-sm text-gray-700 dark:text-gray-300 mb-2">Issues by Field</h4>
        <div className="space-y-1">
          {Object.entries(result.issue_summary.by_field)
            .slice(0, 5)
            .map(([field, count]) => (
              <div key={field} className="flex justify-between text-sm text-gray-700 dark:text-gray-300">
                <span>{field}</span>
                <span className="font-medium">{count}</span>
              </div>
            ))}
        </div>
      </div>

      {/* Sample Issues */}
      {result.issues.length > 0 && (
        <div className="md:col-span-2">
          <h4 className="font-medium text-sm text-gray-700 dark:text-gray-300 mb-2">Sample Issues (First 5)</h4>
          <div className="space-y-2">
            {result.issues.slice(0, 5).map((issue, idx) => (
              <div key={idx} className="text-sm p-2 bg-white dark:bg-gray-800 rounded border border-gray-200 dark:border-gray-700">
                <div className="flex items-center gap-2 mb-1">
                  <Badge
                    variant={
                      issue.severity === 'critical' || issue.severity === 'error'
                        ? 'error'
                        : issue.severity === 'warning'
                          ? 'warning'
                          : 'info'
                    }
                  >
                    {issue.severity}
                  </Badge>
                  <span className="font-medium">{issue.field_name}</span>
                  <span className="text-gray-400 dark:text-gray-500">({issue.rule_name})</span>
                </div>
                <p className="text-gray-600 dark:text-gray-400">{issue.message}</p>
                {issue.suggestion && (
                  <p className="text-blue-600 dark:text-blue-400 text-xs mt-1">Suggestion: {issue.suggestion}</p>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// Helper functions
function getScoreColor(score: number): string {
  if (score >= 90) return 'green'
  if (score >= 70) return 'yellow'
  return 'red'
}

function getScoreDotColor(score: number): string {
  if (score >= 90) return 'bg-green-500'
  if (score >= 70) return 'bg-yellow-500'
  return 'bg-red-500'
}

function getScoreTextColor(score: number): string {
  if (score >= 90) return 'text-green-600 dark:text-green-400'
  if (score >= 70) return 'text-yellow-600 dark:text-yellow-400'
  return 'text-red-600 dark:text-red-400'
}

function formatRuleName(rule: string): string {
  return rule
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (l) => l.toUpperCase())
}
