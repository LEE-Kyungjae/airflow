import { useState, useEffect, useRef } from 'react'
import { SourceContent, SourceHighlight } from '../services/api'

interface Props {
  sourceType: string
  sourceUrl: string
  content?: SourceContent
  highlights: SourceHighlight[]
  loading: boolean
}

export default function SourceViewer({ sourceType, sourceUrl, content, highlights, loading }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [activeHighlight, setActiveHighlight] = useState<string | null>(null)

  if (loading) {
    return <div className="loading">소스 로딩 중...</div>
  }

  // Render based on source type
  switch (sourceType) {
    case 'html':
      return (
        <HtmlViewer
          url={sourceUrl}
          htmlSnapshot={content?.html_snapshot}
          highlights={highlights}
          activeHighlight={activeHighlight}
          onHighlightClick={setActiveHighlight}
        />
      )
    case 'pdf':
      return (
        <PdfViewer
          url={sourceUrl}
          highlights={highlights}
          activeHighlight={activeHighlight}
        />
      )
    case 'json':
      return (
        <JsonViewer
          data={content?.raw_data}
          highlights={highlights}
          activeHighlight={activeHighlight}
          onHighlightClick={setActiveHighlight}
        />
      )
    case 'csv':
    case 'excel':
      return (
        <TableViewer
          data={content?.raw_data}
          highlights={highlights}
          activeHighlight={activeHighlight}
        />
      )
    default:
      return (
        <div className="source-viewer">
          <div className="text-preview">
            {JSON.stringify(content?.raw_data, null, 2)}
          </div>
        </div>
      )
  }
}

// HTML Viewer with highlights
function HtmlViewer({
  url,
  htmlSnapshot,
  highlights,
  activeHighlight,
  onHighlightClick
}: {
  url: string
  htmlSnapshot?: string
  highlights: SourceHighlight[]
  activeHighlight: string | null
  onHighlightClick: (field: string | null) => void
}) {
  const iframeRef = useRef<HTMLIFrameElement>(null)
  const [useSnapshot, setUseSnapshot] = useState(true)

  useEffect(() => {
    if (!iframeRef.current || !htmlSnapshot || !useSnapshot) return

    const doc = iframeRef.current.contentDocument
    if (!doc) return

    // Write HTML snapshot
    doc.open()
    doc.write(htmlSnapshot)
    doc.close()

    // Add highlight styles
    const style = doc.createElement('style')
    style.textContent = `
      .review-highlight {
        outline: 3px solid #ff4444 !important;
        outline-offset: 2px;
        background: rgba(255, 68, 68, 0.1) !important;
        position: relative;
      }
      .review-highlight::before {
        content: attr(data-field);
        position: absolute;
        top: -20px;
        left: 0;
        background: #ff4444;
        color: white;
        font-size: 11px;
        padding: 2px 6px;
        border-radius: 2px;
        z-index: 1000;
      }
      .review-highlight.active {
        outline-color: #007bff !important;
        background: rgba(0, 123, 255, 0.1) !important;
      }
    `
    doc.head.appendChild(style)

    // Apply highlights based on selectors
    highlights.forEach(h => {
      if (h.selector) {
        try {
          const elements = doc.querySelectorAll(h.selector)
          elements.forEach(el => {
            el.classList.add('review-highlight')
            el.setAttribute('data-field', h.field)
            if (h.field === activeHighlight) {
              el.classList.add('active')
            }
          })
        } catch (e) {
          console.warn('Invalid selector:', h.selector)
        }
      }
    })
  }, [htmlSnapshot, highlights, activeHighlight, useSnapshot])

  if (!useSnapshot) {
    return (
      <div className="source-viewer">
        <div style={{ padding: '1rem', background: '#fff3cd', marginBottom: '1rem' }}>
          <span>외부 URL 로드됨 - 하이라이트가 표시되지 않을 수 있습니다.</span>
          <button
            onClick={() => setUseSnapshot(true)}
            style={{ marginLeft: '1rem' }}
            className="btn btn-outline"
          >
            스냅샷으로 보기
          </button>
        </div>
        <iframe
          src={url}
          style={{ width: '100%', height: 'calc(100% - 60px)', border: 'none' }}
        />
      </div>
    )
  }

  return (
    <div className="source-viewer" style={{ position: 'relative', height: '100%' }}>
      {htmlSnapshot ? (
        <>
          <div style={{ padding: '0.5rem', background: '#e8f5e9', fontSize: '0.85rem' }}>
            <span>스냅샷 미리보기</span>
            <button
              onClick={() => setUseSnapshot(false)}
              style={{ marginLeft: '1rem' }}
              className="btn btn-outline"
            >
              원본 URL 로드
            </button>
          </div>
          <iframe
            ref={iframeRef}
            style={{ width: '100%', height: 'calc(100% - 40px)', border: 'none' }}
            sandbox="allow-same-origin"
          />
        </>
      ) : (
        <div style={{ padding: '2rem', textAlign: 'center', color: '#666' }}>
          <p>HTML 스냅샷이 없습니다.</p>
          <button onClick={() => setUseSnapshot(false)} className="btn btn-primary">
            원본 URL 로드
          </button>
        </div>
      )}
    </div>
  )
}

// PDF Viewer with bbox highlights
function PdfViewer({
  url,
  highlights,
  activeHighlight
}: {
  url: string
  highlights: SourceHighlight[]
  activeHighlight: string | null
}) {
  const [scale, setScale] = useState(1)

  return (
    <div className="source-viewer" style={{ position: 'relative' }}>
      <div style={{ padding: '0.5rem', background: '#f5f5f5', display: 'flex', gap: '0.5rem' }}>
        <button onClick={() => setScale(s => Math.max(0.5, s - 0.1))} className="btn btn-outline">
          -
        </button>
        <span>{(scale * 100).toFixed(0)}%</span>
        <button onClick={() => setScale(s => Math.min(2, s + 0.1))} className="btn btn-outline">
          +
        </button>
      </div>

      <div style={{ position: 'relative', overflow: 'auto', height: 'calc(100% - 40px)' }}>
        <iframe
          src={`${url}#toolbar=0`}
          style={{
            width: `${100 * scale}%`,
            height: `${100 * scale}%`,
            minHeight: '800px',
            border: 'none'
          }}
        />

        {/* Bbox highlights overlay */}
        {highlights.filter(h => h.bbox).map((h, i) => (
          <div
            key={i}
            className={`highlight-overlay ${h.field === activeHighlight ? 'active' : ''}`}
            style={{
              left: `${(h.bbox!.x) * scale}px`,
              top: `${(h.bbox!.y) * scale}px`,
              width: `${(h.bbox!.width) * scale}px`,
              height: `${(h.bbox!.height) * scale}px`
            }}
          >
            <span className="highlight-label">{h.field}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

// JSON Viewer with path highlights
function JsonViewer({
  data,
  highlights,
  activeHighlight,
  onHighlightClick
}: {
  data?: any
  highlights: SourceHighlight[]
  activeHighlight: string | null
  onHighlightClick: (field: string | null) => void
}) {
  const highlightPaths = highlights.map(h => h.field)

  const renderJson = (obj: any, path: string = '', depth: number = 0): JSX.Element => {
    if (obj === null) return <span className="json-null">null</span>
    if (typeof obj === 'boolean') return <span className="json-boolean">{obj.toString()}</span>
    if (typeof obj === 'number') return <span className="json-number">{obj}</span>
    if (typeof obj === 'string') return <span className="json-string">"{obj}"</span>

    if (Array.isArray(obj)) {
      return (
        <span>
          {'[\n'}
          {obj.map((item, i) => (
            <span key={i} style={{ marginLeft: `${(depth + 1) * 20}px` }}>
              {renderJson(item, `${path}[${i}]`, depth + 1)}
              {i < obj.length - 1 ? ',\n' : '\n'}
            </span>
          ))}
          <span style={{ marginLeft: `${depth * 20}px` }}>]</span>
        </span>
      )
    }

    if (typeof obj === 'object') {
      const entries = Object.entries(obj)
      return (
        <span>
          {'{\n'}
          {entries.map(([key, value], i) => {
            const currentPath = path ? `${path}.${key}` : key
            const isHighlighted = highlightPaths.includes(key) || highlightPaths.includes(currentPath)
            const isActive = key === activeHighlight || currentPath === activeHighlight

            return (
              <span
                key={key}
                style={{ marginLeft: `${(depth + 1) * 20}px` }}
                className={isHighlighted ? (isActive ? 'json-highlight active' : 'json-highlight') : ''}
                onClick={() => isHighlighted && onHighlightClick(key)}
              >
                <span className="json-key">"{key}"</span>: {renderJson(value, currentPath, depth + 1)}
                {i < entries.length - 1 ? ',\n' : '\n'}
              </span>
            )
          })}
          <span style={{ marginLeft: `${depth * 20}px` }}>{'}'}</span>
        </span>
      )
    }

    return <span>{String(obj)}</span>
  }

  return (
    <div className="source-viewer">
      <pre className="json-preview">
        <style>{`
          .json-key { color: #9cdcfe; }
          .json-string { color: #ce9178; }
          .json-number { color: #b5cea8; }
          .json-boolean { color: #569cd6; }
          .json-null { color: #569cd6; }
          .json-highlight {
            background: rgba(255, 68, 68, 0.2);
            outline: 1px solid #ff4444;
            padding: 2px 4px;
            border-radius: 2px;
            cursor: pointer;
          }
          .json-highlight.active {
            background: rgba(0, 123, 255, 0.2);
            outline-color: #007bff;
          }
        `}</style>
        {renderJson(data)}
      </pre>
    </div>
  )
}

// Table Viewer for CSV/Excel
function TableViewer({
  data,
  highlights,
  activeHighlight
}: {
  data?: any
  highlights: SourceHighlight[]
  activeHighlight: string | null
}) {
  if (!data || !Array.isArray(data) || data.length === 0) {
    return <div className="text-preview">데이터가 없습니다</div>
  }

  const headers = Object.keys(data[0])
  const highlightFields = highlights.map(h => h.field)

  return (
    <div className="source-viewer" style={{ overflow: 'auto' }}>
      <table className="data-table">
        <thead>
          <tr>
            <th>#</th>
            {headers.map(h => (
              <th
                key={h}
                className={highlightFields.includes(h) ? 'highlighted-header' : ''}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.slice(0, 100).map((row: any, i: number) => (
            <tr key={i}>
              <td>{i + 1}</td>
              {headers.map(h => (
                <td
                  key={h}
                  className={highlightFields.includes(h) ? 'highlighted-cell' : ''}
                  style={{
                    outline: highlightFields.includes(h) ? '2px solid #ff4444' : undefined,
                    background: highlightFields.includes(h) ? 'rgba(255,68,68,0.1)' : undefined
                  }}
                >
                  {String(row[h] ?? '')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {data.length > 100 && (
        <div style={{ padding: '1rem', textAlign: 'center', color: '#666' }}>
          ... 그 외 {data.length - 100}개 행
        </div>
      )}
    </div>
  )
}
