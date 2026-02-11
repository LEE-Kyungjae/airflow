import { useState, useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { Bell, Search, X } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { getSystemHealth } from '@/api/dashboard'
import { getSources } from '@/api/sources'
import { cn } from '@/lib/utils'
// Layout context available via useSidebar if needed
import { StatusBadge } from '@/components/ui/Badge'
import type { Source } from '@/types'

export default function Header() {
  const navigate = useNavigate()
  const [searchQuery, setSearchQuery] = useState('')
  const [debouncedQuery, setDebouncedQuery] = useState('')
  const [showResults, setShowResults] = useState(false)
  const [showMobileSearch, setShowMobileSearch] = useState(false)
  const searchRef = useRef<HTMLDivElement>(null)

  const { data: health } = useQuery({
    queryKey: ['systemHealth'],
    queryFn: getSystemHealth,
    refetchInterval: 30000,
  })

  const { data: searchResults } = useQuery({
    queryKey: ['sourceSearch', debouncedQuery],
    queryFn: () => getSources(),
    enabled: debouncedQuery.length >= 2,
  })

  // Debounce search input
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedQuery(searchQuery)
    }, 300)
    return () => clearTimeout(timer)
  }, [searchQuery])

  // Filter results client-side
  const filteredResults = searchResults
    ?.filter(
      (s: Source) =>
        s.name.toLowerCase().includes(debouncedQuery.toLowerCase()) ||
        s.url.toLowerCase().includes(debouncedQuery.toLowerCase())
    )
    .slice(0, 5)

  // Close dropdown on outside click
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) {
        setShowResults(false)
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  const handleSelect = (source: Source) => {
    setSearchQuery('')
    setShowResults(false)
    setShowMobileSearch(false)
    navigate(`/sources/${source._id}`)
  }

  const statusColors: Record<string, string> = {
    healthy: 'bg-green-500',
    degraded: 'bg-yellow-500',
    critical: 'bg-red-500',
  }

  return (
    <header className="h-16 bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 flex items-center justify-between px-4 md:px-6">
      {/* Left section */}
      <div className="flex items-center gap-3 flex-1">
        {/* Spacer for mobile hamburger */}
        <div className="w-10 md:hidden" />

        {/* Desktop search */}
        <div ref={searchRef} className="relative hidden md:block w-96">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500" />
          <input
            type="text"
            placeholder="Search sources..."
            value={searchQuery}
            onChange={(e) => {
              setSearchQuery(e.target.value)
              setShowResults(true)
            }}
            onFocus={() => searchQuery.length >= 2 && setShowResults(true)}
            className="w-full pl-10 pr-4 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />

          {/* Search results dropdown */}
          {showResults && debouncedQuery.length >= 2 && (
            <div className="absolute top-full left-0 right-0 mt-1 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg z-50 overflow-hidden">
              {filteredResults && filteredResults.length > 0 ? (
                filteredResults.map((source: Source) => (
                  <button
                    key={source._id}
                    onClick={() => handleSelect(source)}
                    className="w-full px-4 py-3 text-left hover:bg-gray-50 dark:hover:bg-gray-700 flex items-center justify-between gap-3 border-b last:border-b-0 border-gray-100 dark:border-gray-700"
                  >
                    <div className="min-w-0">
                      <p className="text-sm font-medium text-gray-900 dark:text-gray-100 truncate">
                        {source.name}
                      </p>
                      <p className="text-xs text-gray-500 dark:text-gray-400 truncate">
                        {source.url}
                      </p>
                    </div>
                    <StatusBadge status={source.status} />
                  </button>
                ))
              ) : (
                <div className="px-4 py-3 text-sm text-gray-500 dark:text-gray-400">
                  No sources found
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Right section */}
      <div className="flex items-center gap-2 md:gap-4">
        {/* Mobile search toggle */}
        <button
          onClick={() => setShowMobileSearch(!showMobileSearch)}
          className="md:hidden p-2 text-gray-500 hover:text-gray-700 hover:bg-gray-100 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:bg-gray-700 rounded-lg"
        >
          {showMobileSearch ? <X className="w-5 h-5" /> : <Search className="w-5 h-5" />}
        </button>

        {/* System Status */}
        {health && (
          <div className="hidden sm:flex items-center gap-2 px-3 py-1.5 bg-gray-50 dark:bg-gray-700 rounded-lg">
            <span
              className={cn(
                'w-2 h-2 rounded-full',
                statusColors[health.status] || 'bg-gray-400'
              )}
            />
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
              {health.status === 'healthy'
                ? 'System Healthy'
                : health.status === 'degraded'
                ? 'System Degraded'
                : 'System Critical'}
            </span>
            <span className="text-sm text-gray-500 dark:text-gray-400">
              ({health.overall_score}%)
            </span>
          </div>
        )}

        {/* Notifications */}
        <button className="relative p-2 text-gray-500 hover:text-gray-700 hover:bg-gray-100 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:bg-gray-700 rounded-lg">
          <Bell className="w-5 h-5" />
          {health && health.pending_alerts > 0 && (
            <span className="absolute top-1 right-1 w-4 h-4 bg-red-500 text-white text-xs rounded-full flex items-center justify-center">
              {health.pending_alerts}
            </span>
          )}
        </button>
      </div>

      {/* Mobile search bar - full width overlay */}
      {showMobileSearch && (
        <div className="absolute top-16 left-0 right-0 bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 p-4 z-50 md:hidden">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500" />
            <input
              type="text"
              placeholder="Search sources..."
              value={searchQuery}
              onChange={(e) => {
                setSearchQuery(e.target.value)
                setShowResults(true)
              }}
              autoFocus
              className="w-full pl-10 pr-4 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>

          {/* Mobile search results */}
          {debouncedQuery.length >= 2 && (
            <div className="mt-2 border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
              {filteredResults && filteredResults.length > 0 ? (
                filteredResults.map((source: Source) => (
                  <button
                    key={source._id}
                    onClick={() => handleSelect(source)}
                    className="w-full px-4 py-3 text-left hover:bg-gray-50 dark:hover:bg-gray-700 flex items-center justify-between gap-3 border-b last:border-b-0 border-gray-100 dark:border-gray-700"
                  >
                    <div className="min-w-0">
                      <p className="text-sm font-medium text-gray-900 dark:text-gray-100 truncate">
                        {source.name}
                      </p>
                      <p className="text-xs text-gray-500 dark:text-gray-400 truncate">
                        {source.url}
                      </p>
                    </div>
                    <StatusBadge status={source.status} />
                  </button>
                ))
              ) : (
                <div className="px-4 py-3 text-sm text-gray-500 dark:text-gray-400">
                  No sources found
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </header>
  )
}
