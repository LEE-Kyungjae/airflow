import { NavLink } from 'react-router-dom'
import {
  LayoutDashboard,
  Database,
  PlusCircle,
  Activity,
  AlertTriangle,
  ShieldCheck,
  Moon,
  Sun,
  Menu,
  ChevronsLeft,
  ChevronsRight,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { useTheme } from '@/hooks/useTheme'
import { useSidebar } from './Layout'

const navigation = [
  { name: 'Dashboard', href: '/', icon: LayoutDashboard },
  { name: 'Sources', href: '/sources', icon: Database },
  { name: 'Quick Add', href: '/quick-add', icon: PlusCircle },
  { name: 'Monitoring', href: '/monitoring', icon: Activity },
  { name: 'Data Quality', href: '/data-quality', icon: ShieldCheck },
  { name: 'Errors', href: '/errors', icon: AlertTriangle },
]

export default function Sidebar() {
  const { isDark, toggleTheme } = useTheme()
  const { isOpen, isCollapsed, setIsOpen, toggleSidebar } = useSidebar()

  return (
    <>
      {/* Mobile hamburger button - shown only on small screens */}
      <button
        onClick={() => setIsOpen(true)}
        className="fixed top-4 left-4 z-20 p-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-sm md:hidden"
        aria-label="Open menu"
      >
        <Menu className="w-5 h-5 text-gray-700 dark:text-gray-300" />
      </button>

      {/* Sidebar */}
      <aside
        className={cn(
          'bg-white dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700 flex flex-col transition-all duration-300 flex-shrink-0',
          // Mobile: fixed overlay, hidden by default
          'fixed inset-y-0 left-0 z-40 md:relative md:z-auto',
          isOpen ? 'translate-x-0' : '-translate-x-full md:translate-x-0',
          // Desktop: collapsed or expanded
          isCollapsed ? 'md:w-16' : 'md:w-64',
          'w-64'
        )}
      >
        {/* Logo */}
        <div className="h-16 flex items-center px-4 border-b border-gray-200 dark:border-gray-700 justify-between">
          <div className="flex items-center gap-2 overflow-hidden">
            <div className="w-8 h-8 bg-blue-600 dark:bg-blue-500 rounded-lg flex items-center justify-center flex-shrink-0">
              <Activity className="w-5 h-5 text-white" />
            </div>
            {!isCollapsed && (
              <span className="font-semibold text-gray-900 dark:text-gray-100 whitespace-nowrap">
                Crawler System
              </span>
            )}
          </div>
          {/* Collapse toggle - desktop only */}
          <button
            onClick={toggleSidebar}
            className="hidden md:flex p-1 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-500 dark:text-gray-400"
            aria-label={isCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          >
            {isCollapsed ? (
              <ChevronsRight className="w-4 h-4" />
            ) : (
              <ChevronsLeft className="w-4 h-4" />
            )}
          </button>
        </div>

        {/* Navigation */}
        <nav className="flex-1 px-2 py-4 space-y-1">
          {navigation.map((item) => (
            <NavLink
              key={item.name}
              to={item.href}
              onClick={() => setIsOpen(false)}
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors',
                  isActive
                    ? 'bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400'
                    : 'text-gray-700 hover:bg-gray-100 dark:text-gray-300 dark:hover:bg-gray-700',
                  isCollapsed && 'md:justify-center md:px-2'
                )
              }
              title={isCollapsed ? item.name : undefined}
            >
              <item.icon className="w-5 h-5 flex-shrink-0" />
              <span className={cn(isCollapsed && 'md:hidden')}>
                {item.name}
              </span>
            </NavLink>
          ))}
        </nav>

        {/* Footer - Theme Toggle */}
        <div className="p-3 border-t border-gray-200 dark:border-gray-700">
          <button
            onClick={toggleTheme}
            className={cn(
              'flex items-center gap-3 px-3 py-2 w-full rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-100 dark:text-gray-300 dark:hover:bg-gray-700 transition-colors',
              isCollapsed && 'md:justify-center md:px-2'
            )}
            title={isCollapsed ? (isDark ? 'Light Mode' : 'Dark Mode') : undefined}
          >
            {isDark ? <Sun className="w-5 h-5 flex-shrink-0" /> : <Moon className="w-5 h-5 flex-shrink-0" />}
            <span className={cn(isCollapsed && 'md:hidden')}>
              {isDark ? 'Light Mode' : 'Dark Mode'}
            </span>
          </button>
        </div>
      </aside>
    </>
  )
}
