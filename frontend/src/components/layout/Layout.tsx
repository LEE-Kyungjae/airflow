import { useState, createContext, useContext } from 'react'
import { Outlet } from 'react-router-dom'
import Sidebar from './Sidebar'
import Header from './Header'

interface SidebarContextType {
  isOpen: boolean
  isCollapsed: boolean
  setIsOpen: (open: boolean) => void
  setIsCollapsed: (collapsed: boolean) => void
  toggleSidebar: () => void
}

const SidebarContext = createContext<SidebarContextType>({
  isOpen: false,
  isCollapsed: false,
  setIsOpen: () => {},
  setIsCollapsed: () => {},
  toggleSidebar: () => {},
})

export const useSidebar = () => useContext(SidebarContext)

export default function Layout() {
  const [isOpen, setIsOpen] = useState(false)
  const [isCollapsed, setIsCollapsed] = useState(false)

  const toggleSidebar = () => {
    // On mobile: toggle open/close overlay
    // On desktop: toggle collapsed/expanded
    if (window.innerWidth < 768) {
      setIsOpen(!isOpen)
    } else {
      setIsCollapsed(!isCollapsed)
    }
  }

  return (
    <SidebarContext.Provider
      value={{ isOpen, isCollapsed, setIsOpen, setIsCollapsed, toggleSidebar }}
    >
      <div className="flex h-screen bg-gray-50 dark:bg-gray-900">
        {/* Mobile overlay */}
        {isOpen && (
          <div
            className="fixed inset-0 bg-black/50 z-30 md:hidden"
            onClick={() => setIsOpen(false)}
          />
        )}

        <Sidebar />

        <div className="flex-1 flex flex-col overflow-hidden min-w-0">
          <Header />
          <main className="flex-1 overflow-auto p-4 md:p-6">
            <Outlet />
          </main>
        </div>
      </div>
    </SidebarContext.Provider>
  )
}
