import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { Stepper } from './Stepper'

const steps = [
  { label: 'URL', description: 'Enter URL' },
  { label: 'Fields', description: 'Configure fields' },
  { label: 'Review', description: 'Review & submit' },
]

describe('Stepper', () => {
  it('renders all step labels', () => {
    render(<Stepper steps={steps} currentStep={0} />)
    expect(screen.getByText('URL')).toBeInTheDocument()
    expect(screen.getByText('Fields')).toBeInTheDocument()
    expect(screen.getByText('Review')).toBeInTheDocument()
  })

  it('renders step descriptions', () => {
    render(<Stepper steps={steps} currentStep={0} />)
    expect(screen.getByText('Enter URL')).toBeInTheDocument()
    expect(screen.getByText('Configure fields')).toBeInTheDocument()
    expect(screen.getByText('Review & submit')).toBeInTheDocument()
  })

  it('shows step numbers for uncompleted steps', () => {
    render(<Stepper steps={steps} currentStep={0} />)
    expect(screen.getByText('1')).toBeInTheDocument()
    expect(screen.getByText('2')).toBeInTheDocument()
    expect(screen.getByText('3')).toBeInTheDocument()
  })

  it('calls onStepClick for completed steps', () => {
    const onStepClick = vi.fn()
    render(<Stepper steps={steps} currentStep={2} onStepClick={onStepClick} />)

    // Click on first step (completed, should be clickable)
    fireEvent.click(screen.getByText('URL'))
    expect(onStepClick).toHaveBeenCalledWith(0)
  })

  it('does not call onStepClick for current or future steps', () => {
    const onStepClick = vi.fn()
    render(<Stepper steps={steps} currentStep={1} onStepClick={onStepClick} />)

    // Click on current step (Fields) - should not fire
    fireEvent.click(screen.getByText('Fields'))
    // Click on future step (Review) - should not fire
    fireEvent.click(screen.getByText('Review'))
    expect(onStepClick).not.toHaveBeenCalled()
  })

  it('works without onStepClick', () => {
    render(<Stepper steps={steps} currentStep={1} />)
    expect(screen.getByText('URL')).toBeInTheDocument()
  })
})
