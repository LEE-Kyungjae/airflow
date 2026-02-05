import { Routes, Route } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import Layout from './components/layout/Layout'
import Dashboard from './pages/Dashboard'
import Sources from './pages/Sources'
import QuickAdd from './pages/QuickAdd'
import Monitoring from './pages/Monitoring'
import DataQuality from './pages/DataQuality'
import Errors from './pages/Errors'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5000,
      retry: 1,
    },
  },
})

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Dashboard />} />
          <Route path="sources" element={<Sources />} />
          <Route path="quick-add" element={<QuickAdd />} />
          <Route path="monitoring" element={<Monitoring />} />
          <Route path="data-quality" element={<DataQuality />} />
          <Route path="errors" element={<Errors />} />
        </Route>
      </Routes>
    </QueryClientProvider>
  )
}

export default App
