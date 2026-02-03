import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { Search, CheckCircle, AlertCircle, Loader2 } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { analyzeUrl, quickAddSource } from '@/api/sources'

export default function QuickAdd() {
  const navigate = useNavigate()
  const [url, setUrl] = useState('')
  const [name, setName] = useState('')
  const [analysisResult, setAnalysisResult] = useState<any>(null)

  const analyzeMutation = useMutation({
    mutationFn: analyzeUrl,
    onSuccess: (data) => {
      setAnalysisResult(data)
      if (data.recommended_name) {
        setName(data.recommended_name)
      }
    },
  })

  const addMutation = useMutation({
    mutationFn: quickAddSource,
    onSuccess: () => {
      navigate('/sources')
    },
  })

  const handleAnalyze = () => {
    if (!url) return
    setAnalysisResult(null)
    analyzeMutation.mutate(url)
  }

  const handleAdd = () => {
    if (!url || !name) return
    addMutation.mutate({
      url,
      name,
      auto_start: true,
    })
  }

  return (
    <div className="max-w-2xl mx-auto space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Quick Add Source</h1>
      <p className="text-gray-600">
        Enter a URL and we'll automatically analyze the page structure and create a crawler for you.
      </p>

      {/* URL Input */}
      <Card>
        <CardHeader>
          <CardTitle>Step 1: Enter URL</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              URL to crawl
            </label>
            <div className="flex gap-2">
              <input
                type="url"
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                placeholder="https://example.com/data"
                className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
              <Button
                onClick={handleAnalyze}
                loading={analyzeMutation.isPending}
                disabled={!url}
              >
                <Search className="w-4 h-4" />
                Analyze
              </Button>
            </div>
          </div>

          {analyzeMutation.isError && (
            <div className="p-4 bg-red-50 rounded-lg flex items-start gap-3">
              <AlertCircle className="w-5 h-5 text-red-500 mt-0.5" />
              <div>
                <p className="font-medium text-red-700">Analysis Failed</p>
                <p className="text-sm text-red-600">
                  {(analyzeMutation.error as Error)?.message || 'Could not analyze URL'}
                </p>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Analysis Result */}
      {analysisResult && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <CheckCircle className="w-5 h-5 text-green-500" />
              Step 2: Review Analysis
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <p className="text-sm font-medium text-gray-500">Page Type</p>
                <p className="font-medium">{analysisResult.page_type || 'Unknown'}</p>
              </div>
              <div>
                <p className="text-sm font-medium text-gray-500">Recommended Strategy</p>
                <p className="font-medium">{analysisResult.crawl_strategy || 'HTML'}</p>
              </div>
              <div>
                <p className="text-sm font-medium text-gray-500">Requires JavaScript</p>
                <p className="font-medium">
                  {analysisResult.requires_javascript ? 'Yes' : 'No'}
                </p>
              </div>
              <div>
                <p className="text-sm font-medium text-gray-500">Recommended Schedule</p>
                <p className="font-medium">{analysisResult.recommended_schedule || 'Daily'}</p>
              </div>
            </div>

            {/* Detected Fields */}
            {analysisResult.detected_fields?.length > 0 && (
              <div>
                <p className="text-sm font-medium text-gray-500 mb-2">
                  Detected Fields ({analysisResult.detected_fields.length})
                </p>
                <div className="space-y-2">
                  {analysisResult.detected_fields.slice(0, 5).map((field: any, i: number) => (
                    <div
                      key={i}
                      className="flex items-center justify-between p-2 bg-gray-50 rounded"
                    >
                      <span className="font-medium">{field.name}</span>
                      <span className="text-sm text-gray-500">{field.data_type}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Name & Submit */}
      {analysisResult && (
        <Card>
          <CardHeader>
            <CardTitle>Step 3: Name & Create</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Source Name
              </label>
              <input
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="My Data Source"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>

            <Button
              onClick={handleAdd}
              loading={addMutation.isPending}
              disabled={!name}
              className="w-full"
            >
              Create Source & Start Crawling
            </Button>

            {addMutation.isError && (
              <div className="p-4 bg-red-50 rounded-lg">
                <p className="text-sm text-red-600">
                  {(addMutation.error as Error)?.message || 'Failed to create source'}
                </p>
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}
