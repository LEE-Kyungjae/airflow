// Source types
export interface Source {
  _id: string
  name: string
  url: string
  type: 'html' | 'pdf' | 'excel' | 'csv'
  fields: FieldDefinition[]
  schedule?: string
  status: 'active' | 'inactive' | 'error'
  last_run?: string
  last_success?: string
  error_count: number
  created_at: string
  updated_at: string
}

export interface FieldDefinition {
  name: string
  selector: string
  data_type: 'string' | 'number' | 'date' | 'url' | 'list'
  is_list: boolean
  attribute?: string
  pattern?: string
}

// Crawler types
export interface Crawler {
  _id: string
  source_id: string
  version: number
  status: 'active' | 'testing' | 'deprecated'
  dag_id: string
  created_at: string
  created_by: string
  code?: string
}

// Dashboard types
export interface DashboardStats {
  sources: {
    total: number
    active: number
    error: number
  }
  crawlers: {
    total: number
    active: number
  }
  recent_executions: {
    total: number
    success: number
    failed: number
    success_rate: number
  }
  unresolved_errors: number
  timestamp: string
}

export interface ExecutionTrend {
  date: string
  total: number
  success: number
  failed: number
  success_rate: number
  avg_time_ms: number
  total_records: number
}

// Monitoring types
export interface PipelineStatus {
  source_id: string
  source_name: string
  status: 'active' | 'inactive' | 'error' | 'healing' | 'pending'
  last_run?: string
  last_success?: string
  error_count: number
  success_rate: number
  avg_execution_time_ms: number
  schedule?: string
  healing_session?: string
}

export interface HealingSession {
  session_id: string
  source_id: string
  source_name: string
  status: string
  error_code: string
  current_attempt: number
  max_attempts: number
  admin_notified: boolean
  started_at: string
  last_activity: string
}

export interface SystemHealth {
  overall_score: number
  status: 'healthy' | 'degraded' | 'critical'
  active_sources: number
  failed_sources: number
  active_healing_sessions: number
  pending_alerts: number
  components: Record<string, string>
  last_check: string
}

// Error types
export interface ErrorLog {
  _id: string
  source_id: string
  error_code: string
  error_type: string
  message: string
  auto_recoverable: boolean
  resolved: boolean
  resolved_at?: string
  created_at: string
}

export interface ErrorSummary {
  error_code: string
  error_type: string
  count: number
  unresolved_count: number
  auto_recoverable: boolean
  last_occurred?: string
  affected_sources_count: number
}

// API Response types
export interface ApiResponse<T> {
  data: T
  message?: string
}

export interface PaginatedResponse<T> {
  items: T[]
  total: number
  page: number
  limit: number
}
