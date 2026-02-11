import { Routes, Route } from 'react-router-dom'
import Layout from './components/layout/Layout'
import Dashboard from './pages/Dashboard'
import Sources from './pages/Sources'
import QuickAdd from './pages/QuickAdd'
import Monitoring from './pages/Monitoring'
import DataQuality from './pages/DataQuality'
import Errors from './pages/Errors'
import SourceDetail from './pages/SourceDetail'
import { ToastProvider } from './hooks/useToast'
import { ToastContainer } from './components/ui/Toast'

function App() {
  return (
    <ToastProvider>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Dashboard />} />
          <Route path="sources" element={<Sources />} />
          <Route path="sources/:id" element={<SourceDetail />} />
          <Route path="quick-add" element={<QuickAdd />} />
          <Route path="monitoring" element={<Monitoring />} />
          <Route path="data-quality" element={<DataQuality />} />
          <Route path="errors" element={<Errors />} />
        </Route>
      </Routes>
      <ToastContainer />
    </ToastProvider>
  )
}

export default App
