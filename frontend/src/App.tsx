import { Routes, Route } from 'react-router-dom'
import ReviewPage from './pages/ReviewPage'
import DashboardPage from './pages/DashboardPage'

function App() {
  return (
    <div className="app">
      <header className="app-header">
        <h1>데이터 검토 시스템</h1>
        <nav>
          <a href="/">대시보드</a>
          <a href="/review">검토하기</a>
        </nav>
      </header>
      <main>
        <Routes>
          <Route path="/" element={<DashboardPage />} />
          <Route path="/review" element={<ReviewPage />} />
          <Route path="/review/:reviewId" element={<ReviewPage />} />
        </Routes>
      </main>
    </div>
  )
}

export default App
