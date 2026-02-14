import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { ConfirmModal } from './ConfirmModal'

const defaultProps = {
  open: true,
  title: 'Delete Item',
  message: 'Are you sure you want to delete this item?',
  onConfirm: vi.fn(),
  onCancel: vi.fn(),
}

describe('ConfirmModal', () => {
  it('renders nothing when closed', () => {
    const { container } = render(<ConfirmModal {...defaultProps} open={false} />)
    expect(container.innerHTML).toBe('')
  })

  it('renders title and message when open', () => {
    render(<ConfirmModal {...defaultProps} />)
    expect(screen.getByText('Delete Item')).toBeInTheDocument()
    expect(screen.getByText('Are you sure you want to delete this item?')).toBeInTheDocument()
  })

  it('shows default button labels', () => {
    render(<ConfirmModal {...defaultProps} />)
    expect(screen.getByText('Confirm')).toBeInTheDocument()
    expect(screen.getByText('Cancel')).toBeInTheDocument()
  })

  it('shows custom button labels', () => {
    render(<ConfirmModal {...defaultProps} confirmLabel="Yes, delete" cancelLabel="No, keep" />)
    expect(screen.getByText('Yes, delete')).toBeInTheDocument()
    expect(screen.getByText('No, keep')).toBeInTheDocument()
  })

  it('calls onConfirm when confirm clicked', () => {
    const onConfirm = vi.fn()
    render(<ConfirmModal {...defaultProps} onConfirm={onConfirm} />)
    fireEvent.click(screen.getByText('Confirm'))
    expect(onConfirm).toHaveBeenCalledTimes(1)
  })

  it('calls onCancel when cancel clicked', () => {
    const onCancel = vi.fn()
    render(<ConfirmModal {...defaultProps} onCancel={onCancel} />)
    fireEvent.click(screen.getByText('Cancel'))
    expect(onCancel).toHaveBeenCalledTimes(1)
  })

  it('calls onCancel on Escape key', () => {
    const onCancel = vi.fn()
    render(<ConfirmModal {...defaultProps} onCancel={onCancel} />)
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(onCancel).toHaveBeenCalledTimes(1)
  })

  it('calls onCancel when backdrop clicked', () => {
    const onCancel = vi.fn()
    render(<ConfirmModal {...defaultProps} onCancel={onCancel} />)
    // Backdrop is the first child with bg-black/50
    const backdrop = document.querySelector('.bg-black\\/50')
    if (backdrop) fireEvent.click(backdrop)
    expect(onCancel).toHaveBeenCalled()
  })
})
