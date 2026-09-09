<template>
  <div
    class="mt-5 overflow-hidden rounded-md border border-gray-200/80 bg-gray-50/70 dark:border-dark-700/70 dark:bg-dark-900/35"
  >
    <div
      class="grid divide-x divide-gray-200/80 dark:divide-dark-700/70"
      :class="tertiaryLabel ? 'grid-cols-3' : 'grid-cols-2'"
    >
      <div data-monitor-metric class="min-w-0 px-2.5 py-3">
        <div
          class="flex min-h-8 items-start gap-1 text-[10px] font-semibold uppercase leading-4 text-gray-500 dark:text-gray-400"
        >
          <Icon :name="primaryIcon" size="xs" class="mt-0.5 shrink-0" />
          <span class="break-words">{{ primaryLabel }}</span>
        </div>
        <div class="mt-1 whitespace-nowrap font-mono text-lg font-bold tabular-nums text-gray-900 dark:text-gray-100">
          {{ primaryValue }}<span v-if="primaryValue !== '-'" class="ml-0.5 text-xs font-normal text-gray-400">{{ primaryUnit }}</span>
        </div>
      </div>

      <div data-monitor-metric class="min-w-0 px-2.5 py-3">
        <div
          class="flex min-h-8 items-start gap-1 text-[10px] font-semibold uppercase leading-4 text-gray-500 dark:text-gray-400"
        >
          <Icon :name="secondaryIcon" size="xs" class="mt-0.5 shrink-0" />
          <span class="break-words">{{ secondaryLabel }}</span>
        </div>
        <div class="mt-1 whitespace-nowrap font-mono text-lg font-bold tabular-nums text-gray-900 dark:text-gray-100">
          {{ secondaryValue }}<span v-if="secondaryValue !== '-'" class="ml-0.5 text-xs font-normal text-gray-400">{{ secondaryUnit }}</span>
        </div>
      </div>

      <div v-if="tertiaryLabel" data-monitor-metric class="min-w-0 px-2.5 py-3">
        <div
          class="flex min-h-8 items-start gap-1 text-[10px] font-semibold uppercase leading-4 text-gray-500 dark:text-gray-400"
        >
          <Icon :name="tertiaryIcon ?? 'clock'" size="xs" class="mt-0.5 shrink-0" />
          <span class="break-words">{{ tertiaryLabel }}</span>
        </div>
        <div class="mt-1 whitespace-nowrap font-mono text-lg font-bold tabular-nums text-gray-900 dark:text-gray-100">
          {{ tertiaryValue }}<span v-if="tertiaryValue !== '-'" class="ml-0.5 text-xs font-normal text-gray-400">{{ tertiaryUnit }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import Icon from '@/components/icons/Icon.vue'

type MetricIcon = 'bolt' | 'globe' | 'clock' | 'link'

defineProps<{
  primaryLabel: string
  primaryValue: string
  primaryUnit: string
  primaryIcon: MetricIcon
  secondaryLabel: string
  secondaryValue: string
  secondaryUnit: string
  secondaryIcon: MetricIcon
  tertiaryLabel?: string
  tertiaryValue?: string
  tertiaryUnit?: string
  tertiaryIcon?: MetricIcon
}>()
</script>
