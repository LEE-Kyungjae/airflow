import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { ToastContainer } from './Toast'
import { ToastProvider, useToast } from '@/hooks/useToast'

function AddToastButton({ type, message }: { type: 'success' | 'error' | 'warning' | 'info'; message: string }) {
  const { addToast } = useToast()
  return <button onClick={() => addToast(type, message, 0)}>{`Add ${type}`}</button>
}

function renderWithProvider(ui: React.ReactElement) {
  return render(
    <ToastProvider>
      {ui}
      <ToastContainer />
    </ToastProvider>
  )
}

describe('ToastContainer', () => {
  it('renders nothing when no toasts', () => {
    const { container } = renderWithProvider(<div />)
    expect(container.querySelector('.fixed')).toBeNull()
  })

  it('shows a success toast', () => {
    renderWithProvider(<AddToastButton type="success" message="Saved!" />)
    fireEvent.click(screen.getByText('Add success'))
    expect(screen.getByText('Saved!')).toBeInTheDocument()
  })

  it('shows an error toast', () => {
    renderWithProvider(<AddToastButton type="error" message="Failed!" />)
    fireEvent.click(screen.getByText('Add error'))
    expect(screen.getByText('Failed!')).toBeInTheDocument()
  })

  it('removes toast when close button clicked', () => {
    renderWithProvider(<AddToastButton type="info" message="Info msg" />)
    fireEvent.click(screen.getByText('Add info'))
    expect(screen.getByText('Info msg')).toBeInTheDocument()

    const closeButtons = screen.getAllByRole('button').filter(
      btn => btn !== screen.getByText('Add info')
    )
    fireEvent.click(closeButtons[0])
    expect(screen.queryByText('Info msg')).toBeNull()
  })

  it('shows warning toast with correct styles', () => {
    renderWithProvider(<AddToastButton type="warning" message="Warning!" />)
    fireEvent.click(screen.getByText('Add warning'))
    expect(screen.getByText('Warning!')).toBeInTheDocument()
  })
})
