import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { getDashboard, getReviewQueue } from '../services/api'

export default function DashboardPage() {
  const navigate = useNavigate()

  const { data: dashboard, isLoading } = useQuery({
    queryKey: ['dashboard'],
    queryFn: getDashboard,
    refetchInterval: 30000 // Refresh every 30 seconds
  })

  const { data: queue } = useQuery({
    queryKey: ['queue-preview'],
    queryFn: () => getReviewQueue({ limit: 5, priority_numbers: true })
  })

  if (isLoading) {
    return <div className="loading">로딩 중...</div>
  }

  return (
    <div className="dashboard">
      <h2 style={{ marginBottom: '2rem' }}>검토 대시보드</h2>

      {/* Stats Grid */}
      <div className="stats-grid">
        <div className="stat-card">
          <h3>검토 대기</h3>
          <div className={`value ${dashboard?.pending_count && dashboard.pending_count > 50 ? 'warning' : ''}`}>
            {dashboard?.pending_count || 0}
          </div>
        </div>

        <div className="stat-card">
          <h3>오늘 검토 완료</h3>
          <div className="value success">
            {dashboard?.today_reviewed || 0}
          </div>
        </div>

        <div className="stat-card">
          <h3>승인율</h3>
          <div className={`value ${dashboard?.approval_rate && dashboard.approval_rate >= 90 ? 'success' : dashboard?.approval_rate && dashboard.approval_rate >= 70 ? 'warning' : 'danger'}`}>
            {(dashboard?.approval_rate || 0).toFixed(1)}%
          </div>
        </div>

        <div className="stat-card">
          <h3>평균 신뢰도</h3>
          <div className={`value ${dashboard?.avg_confidence && dashboard.avg_confidence >= 0.8 ? 'success' : dashboard?.avg_confidence && dashboard.avg_confidence >= 0.5 ? 'warning' : 'danger'}`}>
            {((dashboard?.avg_confidence || 0) * 100).toFixed(0)}%
          </div>
        </div>

        <div className="stat-card">
          <h3>숫자 검토 필요</h3>
          <div className={`value ${dashboard?.needs_number_review_count ? 'warning' : ''}`}>
            {dashboard?.needs_number_review_count || 0}
          </div>
        </div>
      </div>

      {/* Start Review Button */}
      <div style={{ marginBottom: '2rem' }}>
        <button
          className="btn btn-primary"
          onClick={() => navigate('/review')}
          style={{ padding: '1rem 2rem', fontSize: '1.1rem' }}
        >
          검토 시작하기 →
        </button>
      </div>

      {/* By Source */}
      {dashboard?.by_source && dashboard.by_source.length > 0 && (
        <div style={{ marginBottom: '2rem' }}>
          <h3 style={{ marginBottom: '1rem' }}>소스별 대기 현황</h3>
          <div style={{
            background: 'white',
            borderRadius: '8px',
            overflow: 'hidden',
            boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
          }}>
            {dashboard.by_source.map((item, i) => (
              <div
                key={item.source_id}
                style={{
                  padding: '1rem',
                  borderBottom: i < dashboard.by_source.length - 1 ? '1px solid #eee' : 'none',
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center'
                }}
              >
                <span>{item.source_name}</span>
                <div>
                  <span style={{
                    background: '#ffc107',
                    color: '#333',
                    padding: '0.25rem 0.75rem',
                    borderRadius: '12px',
                    fontSize: '0.85rem',
                    fontWeight: 500
                  }}>
                    {item.pending_count} 대기
                  </span>
                  <button
                    className="btn btn-outline"
                    onClick={() => navigate(`/review?source=${item.source_id}`)}
                    style={{ marginLeft: '0.5rem', padding: '0.25rem 0.75rem' }}
                  >
                    검토
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Queue Preview */}
      {queue && queue.length > 0 && (
        <div>
          <h3 style={{ marginBottom: '1rem' }}>검토 대기열 미리보기</h3>
          <div style={{
            background: 'white',
            borderRadius: '8px',
            overflow: 'hidden',
            boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
          }}>
            <table className="data-table" style={{ margin: 0 }}>
              <thead>
                <tr>
                  <th>소스</th>
                  <th>데이터 미리보기</th>
                  <th>신뢰도</th>
                  <th>상태</th>
                </tr>
              </thead>
              <tbody>
                {queue.map(item => (
                  <tr
                    key={item.review._id}
                    style={{ cursor: 'pointer' }}
                    onClick={() => navigate(`/review/${item.review._id}`)}
                  >
                    <td>
                      <div style={{ fontWeight: 500 }}>{item.source_name}</div>
                      <div style={{ fontSize: '0.8rem', color: '#666' }}>{item.source_type}</div>
                    </td>
                    <td style={{ maxWidth: '300px' }}>
                      <div style={{
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap'
                      }}>
                        {getDataPreview(item.review.original_data)}
                      </div>
                    </td>
                    <td>
                      <span className={`confidence-badge ${getConfidenceLevel(item.review.confidence_score)}`}>
                        {((item.review.confidence_score || 0) * 100).toFixed(0)}%
                      </span>
                    </td>
                    <td>
                      {item.review.needs_number_review && (
                        <span className="confidence-badge low">숫자 검토</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Recent Reviews */}
      {dashboard?.recent_reviews && dashboard.recent_reviews.length > 0 && (
        <div style={{ marginTop: '2rem' }}>
          <h3 style={{ marginBottom: '1rem' }}>최근 검토 내역</h3>
          <div style={{
            background: 'white',
            borderRadius: '8px',
            padding: '1rem',
            boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
          }}>
            {dashboard.recent_reviews.slice(0, 5).map(review => (
              <div
                key={review._id}
                style={{
                  padding: '0.5rem 0',
                  borderBottom: '1px solid #eee',
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center'
                }}
              >
                <span style={{ color: '#666', fontSize: '0.85rem' }}>
                  {review.reviewed_at ? new Date(review.reviewed_at).toLocaleString('ko-KR') : '-'}
                </span>
                <span className={`confidence-badge ${getStatusColor(review.review_status)}`}>
                  {getStatusLabel(review.review_status)}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function getDataPreview(data: Record<string, any>): string {
  const values = Object.entries(data)
    .filter(([k]) => !k.startsWith('_'))
    .slice(0, 3)
    .map(([k, v]) => `${k}: ${String(v).slice(0, 30)}`)
  return values.join(' | ')
}

function getConfidenceLevel(score?: number): 'high' | 'medium' | 'low' {
  if (!score) return 'low'
  if (score >= 0.8) return 'high'
  if (score >= 0.5) return 'medium'
  return 'low'
}

function getStatusColor(status: string): 'high' | 'medium' | 'low' {
  switch (status) {
    case 'approved':
    case 'corrected':
      return 'high'
    case 'on_hold':
      return 'medium'
    case 'needs_correction':
      return 'low'
    default:
      return 'medium'
  }
}

function getStatusLabel(status: string): string {
  switch (status) {
    case 'approved': return '승인'
    case 'on_hold': return '보류'
    case 'needs_correction': return '정정필요'
    case 'corrected': return '정정완료'
    default: return status
  }
}
