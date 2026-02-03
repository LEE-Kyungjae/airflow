import { useState } from 'react'
import { FieldCorrection, UncertainNumber } from '../services/api'

interface Props {
  data: Record<string, any>
  corrections: FieldCorrection[]
  uncertainNumbers: UncertainNumber[]
  onCorrectionChange: (corrections: FieldCorrection[]) => void
}

export default function DataPanel({ data, corrections, uncertainNumbers, onCorrectionChange }: Props) {
  const [editingField, setEditingField] = useState<string | null>(null)
  const [editValue, setEditValue] = useState('')
  const [editReason, setEditReason] = useState('')

  // Get uncertain number fields
  const uncertainFields = new Set(
    uncertainNumbers.filter(n => n.needs_review).map(n => {
      // Try to find which field contains this number
      for (const [key, value] of Object.entries(data)) {
        if (String(value).includes(n.value)) {
          return key
        }
      }
      return null
    }).filter(Boolean)
  )

  const handleStartEdit = (field: string) => {
    const existing = corrections.find(c => c.field === field)
    setEditingField(field)
    setEditValue(existing?.corrected_value ?? data[field] ?? '')
    setEditReason(existing?.reason ?? '')
  }

  const handleSaveEdit = () => {
    if (!editingField) return

    const newCorrection: FieldCorrection = {
      field: editingField,
      original_value: data[editingField],
      corrected_value: editValue,
      reason: editReason || undefined
    }

    const updated = corrections.filter(c => c.field !== editingField)
    if (editValue !== String(data[editingField])) {
      updated.push(newCorrection)
    }

    onCorrectionChange(updated)
    setEditingField(null)
    setEditValue('')
    setEditReason('')
  }

  const handleCancelEdit = () => {
    setEditingField(null)
    setEditValue('')
    setEditReason('')
  }

  const getFieldCorrection = (field: string) => {
    return corrections.find(c => c.field === field)
  }

  // Filter out internal fields
  const displayFields = Object.entries(data).filter(([key]) =>
    !key.startsWith('_') && !['confidence', 'ocr_confidence', 'ai_confidence'].includes(key)
  )

  return (
    <div className="data-panel-content">
      <table className="data-table">
        <thead>
          <tr>
            <th>필드</th>
            <th>값</th>
            <th>액션</th>
          </tr>
        </thead>
        <tbody>
          {displayFields.map(([field, value]) => {
            const correction = getFieldCorrection(field)
            const isUncertain = uncertainFields.has(field)
            const isEditing = editingField === field

            return (
              <tr
                key={field}
                className={`
                  ${isUncertain ? 'uncertain' : ''}
                  ${correction ? 'corrected' : ''}
                `}
              >
                <th>
                  {field}
                  {isUncertain && (
                    <span
                      style={{
                        marginLeft: '0.5rem',
                        color: '#dc3545',
                        fontSize: '0.8rem'
                      }}
                      title="숫자 검토 필요"
                    >
                      ⚠
                    </span>
                  )}
                </th>
                <td className="value">
                  {isEditing ? (
                    <div className="field-correction">
                      <input
                        type="text"
                        value={editValue}
                        onChange={e => setEditValue(e.target.value)}
                        autoFocus
                        onKeyDown={e => {
                          if (e.key === 'Enter') handleSaveEdit()
                          if (e.key === 'Escape') handleCancelEdit()
                        }}
                      />
                    </div>
                  ) : correction ? (
                    <div>
                      <div style={{ textDecoration: 'line-through', color: '#999' }}>
                        {formatValue(value)}
                      </div>
                      <div style={{ color: '#28a745', fontWeight: 500 }}>
                        → {formatValue(correction.corrected_value)}
                      </div>
                      {correction.reason && (
                        <div style={{ fontSize: '0.8rem', color: '#666', marginTop: '0.25rem' }}>
                          사유: {correction.reason}
                        </div>
                      )}
                    </div>
                  ) : (
                    formatValue(value)
                  )}
                </td>
                <td>
                  {isEditing ? (
                    <div style={{ display: 'flex', gap: '0.25rem' }}>
                      <button className="btn btn-primary" onClick={handleSaveEdit} style={{ padding: '0.25rem 0.5rem' }}>
                        저장
                      </button>
                      <button className="btn btn-outline" onClick={handleCancelEdit} style={{ padding: '0.25rem 0.5rem' }}>
                        취소
                      </button>
                    </div>
                  ) : (
                    <button
                      className="btn btn-outline"
                      onClick={() => handleStartEdit(field)}
                      style={{ padding: '0.25rem 0.5rem' }}
                    >
                      {correction ? '재수정' : '수정'}
                    </button>
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>

      {/* Uncertain Numbers Detail */}
      {uncertainNumbers.length > 0 && (
        <div style={{ marginTop: '1.5rem' }}>
          <h4 style={{ marginBottom: '0.5rem' }}>숫자 검토 항목</h4>
          <div style={{
            background: '#fff3cd',
            padding: '1rem',
            borderRadius: '4px',
            fontSize: '0.9rem'
          }}>
            {uncertainNumbers.filter(n => n.needs_review).map((n, i) => (
              <div key={i} style={{ marginBottom: '0.5rem' }}>
                <strong>{n.value}</strong>
                <span style={{ marginLeft: '0.5rem', color: '#666' }}>
                  (유형: {n.type}, 신뢰도: {(n.confidence * 100).toFixed(0)}%)
                </span>
              </div>
            ))}
            {uncertainNumbers.filter(n => n.needs_review).length === 0 && (
              <span style={{ color: '#666' }}>검토가 필요한 숫자가 없습니다.</span>
            )}
          </div>
        </div>
      )}

      {/* Corrections Summary */}
      {corrections.length > 0 && (
        <div style={{ marginTop: '1.5rem' }}>
          <h4 style={{ marginBottom: '0.5rem' }}>
            수정 내역 ({corrections.length}건)
            <button
              className="btn btn-outline"
              onClick={() => onCorrectionChange([])}
              style={{ marginLeft: '1rem', padding: '0.25rem 0.5rem', fontSize: '0.8rem' }}
            >
              전체 취소
            </button>
          </h4>
          <div style={{
            background: '#d4edda',
            padding: '1rem',
            borderRadius: '4px',
            fontSize: '0.9rem'
          }}>
            {corrections.map((c, i) => (
              <div key={i} style={{ marginBottom: '0.5rem' }}>
                <strong>{c.field}</strong>:
                <span style={{ textDecoration: 'line-through', marginLeft: '0.5rem', color: '#666' }}>
                  {formatValue(c.original_value)}
                </span>
                <span style={{ marginLeft: '0.5rem' }}>→</span>
                <span style={{ marginLeft: '0.5rem', color: '#155724', fontWeight: 500 }}>
                  {formatValue(c.corrected_value)}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Edit Reason Input (shown when editing) */}
      {editingField && (
        <div style={{ marginTop: '1rem', padding: '1rem', background: '#f5f5f5', borderRadius: '4px' }}>
          <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 500 }}>
            수정 사유 (선택사항)
          </label>
          <input
            type="text"
            value={editReason}
            onChange={e => setEditReason(e.target.value)}
            placeholder="수정 사유를 입력하세요..."
            style={{ width: '100%', padding: '0.5rem', border: '1px solid #ddd', borderRadius: '4px' }}
          />
        </div>
      )}
    </div>
  )
}

function formatValue(value: any): string {
  if (value === null || value === undefined) return '(없음)'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}
