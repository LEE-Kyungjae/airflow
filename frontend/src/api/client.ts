import axios from 'axios'

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

export const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Add correlation ID to all requests
apiClient.interceptors.request.use((config) => {
  const correlationId = crypto.randomUUID()
  config.headers['X-Correlation-ID'] = correlationId
  return config
})

// Handle errors globally
apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 429) {
      console.error('Rate limit exceeded')
    }
    return Promise.reject(error)
  }
)

export default apiClient
