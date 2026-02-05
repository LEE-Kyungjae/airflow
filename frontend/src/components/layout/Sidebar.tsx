import { NavLink } from 'react-router-dom'
import {
  LayoutDashboard,
  Database,
  PlusCircle,
  Activity,
  AlertTriangle,
  Settings,
  ShieldCheck,
} from 'lucide-react'
import { cn } from '@/lib/utils'

const navigation = [
  { name: 'Dashboard', href: '/', icon: LayoutDashboard },
  { name: 'Sources', href: '/sources', icon: Database },
  { name: 'Quick Add', href: '/quick-add', icon: PlusCircle },
  { name: 'Monitoring', href: '/monitoring', icon: Activity },
  { name: 'Data Quality', href: '/data-quality', icon: ShieldCheck },
  { name: 'Errors', href: '/errors', icon: AlertTriangle },
]

export default function Sidebar() {
  return (
    <aside className="w-64 bg-white border-r border-gray-200 flex flex-col">
      {/* Logo */}
      <div className="h-16 flex items-center px-6 border-b border-gray-200">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 bg-blue-600 rounded-lg flex items-center justify-center">
            <Activity className="w-5 h-5 text-white" />
          </div>
          <span className="font-semibold text-gray-900">Crawler System</span>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-3 py-4 space-y-1">
        {navigation.map((item) => (
          <NavLink
            key={item.name}
            to={item.href}
            className={({ isActive }) =>
              cn(
                'flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors',
                isActive
                  ? 'bg-blue-50 text-blue-700'
                  : 'text-gray-700 hover:bg-gray-100'
              )
            }
          >
            <item.icon className="w-5 h-5" />
            {item.name}
          </NavLink>
        ))}
      </nav>

      {/* Footer */}
      <div className="p-4 border-t border-gray-200">
        <button className="flex items-center gap-3 px-3 py-2 w-full rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-100 transition-colors">
          <Settings className="w-5 h-5" />
          Settings
        </button>
      </div>
    </aside>
  )
}
