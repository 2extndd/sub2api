import { mount } from '@vue/test-utils'
import { describe, expect, it } from 'vitest'
import MonitorMetricPair from '@/components/user/monitor/MonitorMetricPair.vue'

describe('MonitorMetricPair', () => {
  it('renders dialog latency, average TTFT, and endpoint ping as separate metrics', () => {
    const wrapper = mount(MonitorMetricPair, {
      props: {
        primaryIcon: 'bolt',
        primaryLabel: 'Dialog Latency',
        primaryValue: '3413',
        primaryUnit: 'ms',
        secondaryIcon: 'clock',
        secondaryLabel: 'Avg TTFT (7d)',
        secondaryValue: '812',
        secondaryUnit: 'ms',
        tertiaryIcon: 'globe',
        tertiaryLabel: 'Endpoint PING',
        tertiaryValue: '26',
        tertiaryUnit: 'ms',
      },
      global: {
        stubs: {
          Icon: true,
        },
      },
    })

    expect(wrapper.findAll('[data-monitor-metric]')).toHaveLength(3)
    expect(wrapper.text()).toContain('Dialog Latency')
    expect(wrapper.text()).toContain('3413ms')
    expect(wrapper.text()).toContain('Avg TTFT (7d)')
    expect(wrapper.text()).toContain('812ms')
    expect(wrapper.text()).toContain('Endpoint PING')
    expect(wrapper.text()).toContain('26ms')
  })

  it('keeps the TTFT slot visible when the metric is unavailable', () => {
    const wrapper = mount(MonitorMetricPair, {
      props: {
        primaryIcon: 'bolt',
        primaryLabel: 'Dialog Latency',
        primaryValue: '3413',
        primaryUnit: 'ms',
        secondaryIcon: 'clock',
        secondaryLabel: 'Avg TTFT (7d)',
        secondaryValue: '-',
        secondaryUnit: 'ms',
        tertiaryIcon: 'globe',
        tertiaryLabel: 'Endpoint PING',
        tertiaryValue: '26',
        tertiaryUnit: 'ms',
      },
      global: {
        stubs: {
          Icon: true,
        },
      },
    })

    expect(wrapper.findAll('[data-monitor-metric]')).toHaveLength(3)
    expect(wrapper.text()).toContain('-')
    expect(wrapper.text()).not.toContain('-ms')
  })
})
