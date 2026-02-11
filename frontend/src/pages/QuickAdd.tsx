import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  Search,
  CheckCircle,
  AlertCircle,
  Globe,
  FileCode,
  Zap,
  ArrowRight,
  ArrowLeft,
  Rocket,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { Badge } from '@/components/ui/Badge'
import { Stepper } from '@/components/ui/Stepper'
import { FieldEditor } from '@/components/source/FieldEditor'
import { SchedulePicker } from '@/components/source/SchedulePicker'
import { useToast } from '@/hooks/useToast'
import { analyzeUrl, quickAddSource } from '@/api/sources'
import type { FieldDefinition } from '@/types'

const steps = [
  { label: 'URL Input', description: 'Analyze page' },
  { label: 'Fields', description: 'Configure data' },
  { label: 'Schedule', description: 'Set timing' },
  { label: 'Confirm', description: 'Review & create' },
]

interface AnalysisResult {
  page_type?: string
  crawl_strategy?: string
  requires_javascript?: boolean
  recommended_schedule?: string
  recommended_name?: string
  detected_fields?: FieldDefinition[]
}

export default function QuickAdd() {
  const navigate = useNavigate()
  const toast = useToast()
  const [currentStep, setCurrentStep] = useState(0)
  const [url, setUrl] = useState('')
  const [analysisResult, setAnalysisResult] = useState<AnalysisResult | null>(null)
  const [fields, setFields] = useState<FieldDefinition[]>([])
  const [schedule, setSchedule] = useState('0 9 * * *')
  const [name, setName] = useState('')
  const [hint, setHint] = useState('')
  const [autoStart, setAutoStart] = useState(true)

  const analyzeMutation = useMutation({
    mutationFn: analyzeUrl,
    onSuccess: (data: AnalysisResult) => {
      setAnalysisResult(data)
      setFields(data.detected_fields || [])
      if (data.recommended_name) setName(data.recommended_name)
      if (data.recommended_schedule) setSchedule(data.recommended_schedule)
      setCurrentStep(1)
      toast.success('Page analysis complete!')
    },
    onError: (err: Error) => {
      toast.error(err.message || 'Failed to analyze URL')
    },
  })

  const addMutation = useMutation({
    mutationFn: quickAddSource,
    onSuccess: () => {
      toast.success('Source created successfully!')
      navigate('/sources')
    },
    onError: (err: Error) => {
      toast.error(err.message || 'Failed to create source')
    },
  })

  const handleAnalyze = () => {
    if (!url) return
    setAnalysisResult(null)
    analyzeMutation.mutate(url)
  }

  const handleCreate = () => {
    addMutation.mutate({
      url,
      name: name || undefined,
      hint: hint || undefined,
      auto_start: autoStart,
    })
  }

  const canProceed = () => {
    switch (currentStep) {
      case 0: return !!analysisResult
      case 1: return fields.length > 0
      case 2: return !!schedule
      case 3: return true
      default: return false
    }
  }

  return (
    <div className="max-w-3xl mx-auto space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">Add New Source</h1>
        <p className="text-gray-600 dark:text-gray-400 mt-1">
          Enter a URL and we'll automatically analyze the page structure.
        </p>
      </div>

      {/* Stepper */}
      <Stepper
        steps={steps}
        currentStep={currentStep}
        onStepClick={(step) => {
          if (step < currentStep) setCurrentStep(step)
        }}
      />

      {/* Step Content */}
      <Card>
        <CardContent className="p-6">
          {/* Step 0: URL Input & Analysis */}
          {currentStep === 0 && (
            <div className="space-y-6">
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  URL to crawl
                </label>
                <div className="flex gap-2">
                  <input
                    type="url"
                    value={url}
                    onChange={(e) => setUrl(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && handleAnalyze()}
                    placeholder="https://example.com/data"
                    className="flex-1 px-4 py-2.5 border border-gray-300 dark:border-gray-600 rounded-lg text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
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

              {/* Loading skeleton */}
              {analyzeMutation.isPending && (
                <div className="space-y-3 animate-pulse">
                  <div className="h-4 bg-gray-200 dark:bg-gray-700 rounded w-1/3" />
                  <div className="grid grid-cols-2 gap-4">
                    <div className="h-20 bg-gray-200 dark:bg-gray-700 rounded-lg" />
                    <div className="h-20 bg-gray-200 dark:bg-gray-700 rounded-lg" />
                    <div className="h-20 bg-gray-200 dark:bg-gray-700 rounded-lg" />
                    <div className="h-20 bg-gray-200 dark:bg-gray-700 rounded-lg" />
                  </div>
                </div>
              )}

              {/* Error */}
              {analyzeMutation.isError && (
                <div className="p-4 bg-red-50 dark:bg-red-900/20 rounded-lg flex items-start gap-3">
                  <AlertCircle className="w-5 h-5 text-red-500 dark:text-red-400 mt-0.5" />
                  <div>
                    <p className="font-medium text-red-700 dark:text-red-300">Analysis Failed</p>
                    <p className="text-sm text-red-600 dark:text-red-400">
                      {(analyzeMutation.error as Error)?.message || 'Could not analyze URL'}
                    </p>
                  </div>
                </div>
              )}

              {/* Analysis result summary */}
              {analysisResult && (
                <div className="space-y-4">
                  <div className="flex items-center gap-2 text-green-600 dark:text-green-400">
                    <CheckCircle className="w-5 h-5" />
                    <span className="font-medium">Analysis Complete</span>
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    <div className="p-3 bg-gray-50 dark:bg-gray-700/50 rounded-lg">
                      <div className="flex items-center gap-2 mb-1">
                        <Globe className="w-4 h-4 text-gray-400" />
                        <span className="text-xs text-gray-500 dark:text-gray-400">Page Type</span>
                      </div>
                      <p className="font-medium text-gray-900 dark:text-gray-100">
                        {analysisResult.page_type || 'Unknown'}
                      </p>
                    </div>
                    <div className="p-3 bg-gray-50 dark:bg-gray-700/50 rounded-lg">
                      <div className="flex items-center gap-2 mb-1">
                        <FileCode className="w-4 h-4 text-gray-400" />
                        <span className="text-xs text-gray-500 dark:text-gray-400">Strategy</span>
                      </div>
                      <p className="font-medium text-gray-900 dark:text-gray-100">
                        {analysisResult.crawl_strategy || 'HTML'}
                      </p>
                    </div>
                    <div className="p-3 bg-gray-50 dark:bg-gray-700/50 rounded-lg">
                      <div className="flex items-center gap-2 mb-1">
                        <Zap className="w-4 h-4 text-gray-400" />
                        <span className="text-xs text-gray-500 dark:text-gray-400">JavaScript</span>
                      </div>
                      <Badge variant={analysisResult.requires_javascript ? 'warning' : 'success'}>
                        {analysisResult.requires_javascript ? 'Required' : 'Not Required'}
                      </Badge>
                    </div>
                    <div className="p-3 bg-gray-50 dark:bg-gray-700/50 rounded-lg">
                      <div className="flex items-center gap-2 mb-1">
                        <CheckCircle className="w-4 h-4 text-gray-400" />
                        <span className="text-xs text-gray-500 dark:text-gray-400">Fields Found</span>
                      </div>
                      <p className="font-medium text-gray-900 dark:text-gray-100">
                        {analysisResult.detected_fields?.length || 0} fields
                      </p>
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Step 1: Field Configuration */}
          {currentStep === 1 && (
            <div className="space-y-4">
              <div>
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
                  Configure Fields
                </h3>
                <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                  Review and edit the detected fields. You can rename, change types, or add new fields.
                </p>
              </div>
              <FieldEditor fields={fields} onChange={setFields} />
            </div>
          )}

          {/* Step 2: Schedule & Settings */}
          {currentStep === 2 && (
            <div className="space-y-6">
              <div>
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
                  Schedule & Settings
                </h3>
                <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                  Configure when and how the crawler runs.
                </p>
              </div>

              {/* Source name */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Source Name
                </label>
                <input
                  type="text"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="My Data Source"
                  className="w-full px-4 py-2.5 border border-gray-300 dark:border-gray-600 rounded-lg text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              {/* Schedule */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Crawl Schedule
                </label>
                <SchedulePicker value={schedule} onChange={setSchedule} />
              </div>

              {/* Hint / tags */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Hint / Category (optional)
                </label>
                <input
                  type="text"
                  value={hint}
                  onChange={(e) => setHint(e.target.value)}
                  placeholder="e.g. news, finance, products"
                  className="w-full px-4 py-2.5 border border-gray-300 dark:border-gray-600 rounded-lg text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              {/* Auto-start toggle */}
              <label className="flex items-center gap-3 cursor-pointer">
                <div
                  className={`relative w-11 h-6 rounded-full transition-colors ${
                    autoStart ? 'bg-blue-600 dark:bg-blue-500' : 'bg-gray-300 dark:bg-gray-600'
                  }`}
                  onClick={() => setAutoStart(!autoStart)}
                >
                  <div
                    className={`absolute top-0.5 w-5 h-5 bg-white rounded-full shadow transition-transform ${
                      autoStart ? 'translate-x-5' : 'translate-x-0.5'
                    }`}
                  />
                </div>
                <span className="text-sm text-gray-700 dark:text-gray-300">
                  Start crawling immediately after creation
                </span>
              </label>
            </div>
          )}

          {/* Step 3: Review & Confirm */}
          {currentStep === 3 && (
            <div className="space-y-6">
              <div>
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
                  Review & Create
                </h3>
                <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                  Review your configuration before creating the source.
                </p>
              </div>

              <div className="space-y-4">
                {/* Summary card */}
                <div className="p-4 bg-gray-50 dark:bg-gray-700/50 rounded-lg space-y-3">
                  <div className="flex justify-between items-center">
                    <span className="text-sm text-gray-500 dark:text-gray-400">URL</span>
                    <span className="text-sm font-medium text-gray-900 dark:text-gray-100 truncate max-w-xs">
                      {url}
                    </span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-sm text-gray-500 dark:text-gray-400">Name</span>
                    <span className="text-sm font-medium text-gray-900 dark:text-gray-100">
                      {name || '(auto)'}
                    </span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-sm text-gray-500 dark:text-gray-400">Fields</span>
                    <Badge variant="info">{fields.length} fields</Badge>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-sm text-gray-500 dark:text-gray-400">Schedule</span>
                    <span className="text-sm font-mono text-gray-900 dark:text-gray-100">
                      {schedule}
                    </span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-sm text-gray-500 dark:text-gray-400">Auto Start</span>
                    <Badge variant={autoStart ? 'success' : 'default'}>
                      {autoStart ? 'Yes' : 'No'}
                    </Badge>
                  </div>
                  {analysisResult?.crawl_strategy && (
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-gray-500 dark:text-gray-400">Strategy</span>
                      <span className="text-sm font-medium text-gray-900 dark:text-gray-100">
                        {analysisResult.crawl_strategy}
                      </span>
                    </div>
                  )}
                </div>

                {/* Field preview */}
                <div>
                  <p className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Fields to extract:
                  </p>
                  <div className="flex flex-wrap gap-2">
                    {fields.map((f, i) => (
                      <Badge key={i} variant="info">
                        {f.name} ({f.data_type})
                      </Badge>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Navigation buttons */}
      <div className="flex justify-between">
        <Button
          variant="outline"
          onClick={() => setCurrentStep(Math.max(0, currentStep - 1))}
          disabled={currentStep === 0}
        >
          <ArrowLeft className="w-4 h-4" />
          Back
        </Button>

        {currentStep < 3 ? (
          <Button
            onClick={() => setCurrentStep(currentStep + 1)}
            disabled={!canProceed()}
          >
            Next
            <ArrowRight className="w-4 h-4" />
          </Button>
        ) : (
          <Button
            onClick={handleCreate}
            loading={addMutation.isPending}
          >
            <Rocket className="w-4 h-4" />
            Create Source & Start Crawling
          </Button>
        )}
      </div>
    </div>
  )
}
