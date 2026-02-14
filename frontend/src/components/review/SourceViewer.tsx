import { useRef, useEffect, useState } from 'react'
import { ExternalLink, Eye, EyeOff } from 'lucide-react'
import { Button } from '@/components/ui/Button'
import { cn } from '@/lib/utils'
import type { SourceContentData, SourceHighlight } from '@/types'

interface SourceViewerProps {
  content: SourceContentData | null
  activeHighlight?: SourceHighlight
  isLoading: boolean
}

export function SourceViewer({ content, activeHighlight, isLoading }: SourceViewerProps) {
  const iframeRef = useRef<HTMLIFrameElement>(null)
  const [showHighlights, setShowHighlights] = useState(true)

  // Handle HTML highlight injection via iframe
  useEffect(() => {
    if (!activeHighlight || !iframeRef.current || !content) return

    const iframe = iframeRef.current

    if (content.source_type === 'html' && activeHighlight.selector) {
      try {
        const iframeDoc = iframe.contentDocument || iframe.contentWindow?.document
        if (iframeDoc) {
          // Remove previous highlights
          iframeDoc.querySelectorAll('.review-highlight').forEach((el) => {
            ;(el as HTMLElement).style.outline = ''
            ;(el as HTMLElement).style.backgroundColor = ''
            el.classList.remove('review-highlight')
          })

          // Add new highlight
          if (showHighlights) {
            const target = iframeDoc.querySelector(activeHighlight.selector)
            if (target) {
              ;(target as HTMLElement).style.outline = '3px solid #3b82f6'
              ;(target as HTMLElement).style.backgroundColor = 'rgba(59, 130, 246, 0.1)'
              target.classList.add('review-highlight')
              target.scrollIntoView({ behavior: 'smooth', block: 'center' })
            }
          }
        }
      } catch {
        // Cross-origin iframe - can't access content
      }
    }
  }, [activeHighlight, content, showHighlights])

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-50 dark:bg-gray-900">
        <div className="animate-spin w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full" />
      </div>
    )
  }

  if (!content) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-50 dark:bg-gray-900 text-gray-400">
        No source content available
      </div>
    )
  }

  const renderContent = () => {
    switch (content.source_type) {
      case 'html':
        if (content.html_snapshot) {
          return (
            <iframe
              ref={iframeRef}
              srcDoc={content.html_snapshot}
              className="w-full h-full border-0"
              sandbox="allow-same-origin"
              title="Source preview"
            />
          )
        }
        return (
          <iframe
            ref={iframeRef}
            src={content.source_url}
            className="w-full h-full border-0"
            sandbox="allow-same-origin allow-scripts"
            title="Source preview"
          />
        )

      case 'pdf':
        return (
          <iframe
            src={`${content.source_url}#toolbar=1`}
            className="w-full h-full border-0"
            title="PDF preview"
          />
        )

      case 'csv':
      case 'excel':
        if (content.raw_data && Array.isArray(content.raw_data)) {
          const columns = Object.keys(content.raw_data[0] || {})
          return (
            <div className="overflow-auto h-full">
              <table className="w-full text-xs">
                <thead className="sticky top-0 bg-gray-100 dark:bg-gray-800">
                  <tr>
                    {columns.map((col) => (
                      <th key={col} className="px-2 py-1 text-left font-medium text-gray-500 dark:text-gray-400">
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
                  {content.raw_data.map((row: Record<string, any>, i: number) => (
                    <tr key={i} className={cn(
                      activeHighlight && activeHighlight.bbox?.page === i
                        ? 'bg-blue-50 dark:bg-blue-900/20'
                        : ''
                    )}>
                      {columns.map((col) => (
                        <td key={col} className="px-2 py-1 text-gray-700 dark:text-gray-300">
                          {row[col] != null ? String(row[col]) : ''}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )
        }
        return <div className="p-4 text-gray-400">No raw data available</div>

      default:
        return (
          <div className="flex flex-col items-center justify-center h-full text-gray-400 gap-2">
            <p>Unsupported source type: {content.source_type}</p>
            <a
              href={content.source_url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-500 hover:underline flex items-center gap-1"
            >
              Open in browser <ExternalLink className="w-4 h-4" />
            </a>
          </div>
        )
    }
  }

  return (
    <div className="flex flex-col h-full">
      {/* Source header */}
      <div className="flex items-center justify-between px-3 py-2 bg-gray-50 dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700">
        <span className="text-xs text-gray-500 dark:text-gray-400 truncate">
          {content.source_url}
        </span>
        <div className="flex items-center gap-1">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setShowHighlights(!showHighlights)}
            title={showHighlights ? 'Hide highlights' : 'Show highlights'}
          >
            {showHighlights ? <Eye className="w-3 h-3" /> : <EyeOff className="w-3 h-3" />}
          </Button>
          <a
            href={content.source_url}
            target="_blank"
            rel="noopener noreferrer"
            className="p-1 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
          >
            <ExternalLink className="w-3 h-3" />
          </a>
        </div>
      </div>

      {/* Content area */}
      <div className="flex-1 overflow-hidden">
        {renderContent()}
      </div>
    </div>
  )
}
