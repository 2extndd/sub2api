<template>
  <BaseDialog :show="show" :title="t('admin.users.accountDenialsTitle')" width="wide" @close="$emit('close')">
    <div v-if="user" class="space-y-5">
      <div class="rounded-2xl border border-primary-100 bg-primary-50/70 p-4 dark:border-primary-900/40 dark:bg-primary-900/20">
        <p class="font-semibold text-gray-900 dark:text-white">{{ user.email }}</p>
        <p class="mt-1 text-sm leading-6 text-gray-600 dark:text-gray-300">
          {{ t('admin.users.accountDenialsHint') }}
        </p>
      </div>

      <div class="flex flex-col gap-3 sm:flex-row sm:items-center">
        <div class="relative flex-1">
          <Icon name="search" size="sm" class="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            v-model="search"
            type="search"
            :placeholder="t('admin.users.accountDenialsSearch')"
            class="w-full rounded-xl border border-gray-200 bg-white py-2.5 pl-10 pr-4 text-sm text-gray-900 outline-none transition focus:border-primary-500 focus:ring-2 focus:ring-primary-500/20 dark:border-dark-600 dark:bg-dark-800 dark:text-white"
          />
        </div>
        <label class="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-300">
          <input v-model="selectedOnly" type="checkbox" class="h-4 w-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500" />
          {{ t('admin.users.accountDenialsSelectedOnly') }}
        </label>
      </div>

      <div class="flex items-center justify-between rounded-xl bg-gray-50 px-4 py-3 dark:bg-dark-800">
        <div>
          <p class="text-sm font-semibold text-gray-900 dark:text-white">
            {{ t('admin.users.accountDenialsCount', { count: selectedIds.size }) }}
          </p>
          <p class="mt-0.5 text-xs text-gray-500 dark:text-gray-400">
            {{ selectedIds.size === 0 ? t('admin.users.accountDenialsDefaultAllow') : t('admin.users.accountDenialsApplied') }}
          </p>
        </div>
        <button v-if="selectedIds.size" type="button" class="text-sm font-medium text-primary-600 hover:text-primary-700 dark:text-primary-400" @click="clearSelection">
          {{ t('common.clear') }}
        </button>
      </div>

      <div v-if="loading" class="flex h-64 items-center justify-center">
        <div class="h-8 w-8 animate-spin rounded-full border-2 border-primary-500 border-t-transparent"></div>
      </div>
      <div v-else-if="filteredAccounts.length === 0" class="flex h-48 flex-col items-center justify-center rounded-2xl border border-dashed border-gray-300 text-center dark:border-dark-600">
        <Icon name="search" size="lg" class="text-gray-300 dark:text-gray-600" />
        <p class="mt-3 text-sm text-gray-500 dark:text-gray-400">{{ t('admin.users.accountDenialsEmpty') }}</p>
      </div>
      <div v-else class="max-h-[430px] space-y-2 overflow-y-auto pr-1">
        <label
          v-for="account in filteredAccounts"
          :key="account.id"
          class="flex cursor-pointer items-center gap-3 rounded-xl border p-3 transition"
          :class="selectedIds.has(account.id)
            ? 'border-red-200 bg-red-50/70 dark:border-red-900/50 dark:bg-red-900/15'
            : 'border-gray-200 bg-white hover:border-gray-300 dark:border-dark-600 dark:bg-dark-800 dark:hover:border-dark-500'"
        >
          <input
            type="checkbox"
            :checked="selectedIds.has(account.id)"
            class="h-4 w-4 rounded border-gray-300 text-red-600 focus:ring-red-500"
            @change="toggleAccount(account.id)"
          />
          <div class="min-w-0 flex-1">
            <div class="flex flex-wrap items-center gap-2">
              <span class="font-mono text-xs text-gray-400">#{{ account.id }}</span>
              <span class="truncate text-sm font-semibold text-gray-900 dark:text-white">{{ account.name }}</span>
              <span class="rounded-full bg-gray-100 px-2 py-0.5 text-[11px] font-medium uppercase text-gray-600 dark:bg-dark-700 dark:text-gray-300">{{ account.platform }}</span>
            </div>
            <div class="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-xs text-gray-500 dark:text-gray-400">
              <span>{{ account.type }}</span>
              <span>{{ t('admin.users.accountDenialsPriority', { value: account.priority }) }}</span>
              <span :class="account.status === 'active' && account.schedulable ? 'text-green-600 dark:text-green-400' : 'text-orange-600 dark:text-orange-400'">
                {{ account.status }}{{ account.schedulable ? '' : ' · paused' }}
              </span>
            </div>
          </div>
          <span v-if="selectedIds.has(account.id)" class="rounded-full bg-red-100 px-2.5 py-1 text-xs font-semibold text-red-700 dark:bg-red-900/40 dark:text-red-300">
            {{ t('admin.users.accountDenialsExcluded') }}
          </span>
        </label>
      </div>
    </div>

    <template #footer>
      <div class="flex justify-end gap-3">
        <button type="button" class="rounded-xl border border-gray-300 px-4 py-2.5 text-sm font-medium text-gray-700 hover:bg-gray-50 dark:border-dark-600 dark:text-gray-300 dark:hover:bg-dark-700" @click="$emit('close')">
          {{ t('common.cancel') }}
        </button>
        <button type="button" :disabled="submitting || loading" class="rounded-xl bg-primary-600 px-5 py-2.5 text-sm font-semibold text-white shadow-sm transition hover:bg-primary-700 disabled:cursor-not-allowed disabled:opacity-50" @click="save">
          {{ submitting ? t('common.saving') : t('common.save') }}
        </button>
      </div>
    </template>
  </BaseDialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import BaseDialog from '@/components/common/BaseDialog.vue'
import Icon from '@/components/icons/Icon.vue'
import { accountsAPI, usersAPI } from '@/api/admin'
import { useAppStore } from '@/stores/app'
import type { Account, AdminUser } from '@/types'

const props = defineProps<{ show: boolean; user: AdminUser | null }>()
const emit = defineEmits(['close', 'success'])
const { t } = useI18n()
const appStore = useAppStore()
const accounts = ref<Account[]>([])
const selectedIds = ref(new Set<number>())
const revision = ref(0)
const search = ref('')
const selectedOnly = ref(false)
const loading = ref(false)
const submitting = ref(false)

const filteredAccounts = computed(() => {
  const query = search.value.trim().toLowerCase()
  return accounts.value.filter((account) => {
    if (selectedOnly.value && !selectedIds.value.has(account.id)) return false
    if (!query) return true
    return String(account.id).includes(query)
      || account.name.toLowerCase().includes(query)
      || account.platform.toLowerCase().includes(query)
      || account.type.toLowerCase().includes(query)
  })
})

watch(() => props.show, (visible) => {
  if (visible && props.user) void load()
  if (!visible) {
    search.value = ''
    selectedOnly.value = false
  }
})

async function load() {
  if (!props.user) return
  loading.value = true
  try {
    const [policy, firstPage] = await Promise.all([
      usersAPI.getAccountDenials(props.user.id),
      accountsAPI.list(1, 100, { sort_by: 'id', sort_order: 'asc', lite: 'true' })
    ])
    const loaded = [...firstPage.items]
    for (let page = 2; page <= firstPage.pages; page += 1) {
      const next = await accountsAPI.list(page, 100, { sort_by: 'id', sort_order: 'asc', lite: 'true' })
      loaded.push(...next.items)
    }
    accounts.value = loaded
    selectedIds.value = new Set(policy.account_ids)
    revision.value = policy.revision
  } catch (error) {
    console.error('Failed to load account deny-list:', error)
    appStore.showError(t('admin.users.accountDenialsLoadError'))
  } finally {
    loading.value = false
  }
}

function toggleAccount(accountId: number) {
  const next = new Set(selectedIds.value)
  if (next.has(accountId)) next.delete(accountId)
  else next.add(accountId)
  selectedIds.value = next
}

function clearSelection() {
  selectedIds.value = new Set()
}

async function save() {
  if (!props.user) return
  submitting.value = true
  try {
    const policy = await usersAPI.replaceAccountDenials(
      props.user.id,
      revision.value,
      [...selectedIds.value].sort((a, b) => a - b)
    )
    revision.value = policy.revision
    appStore.showSuccess(t('admin.users.accountDenialsSaved'))
    emit('success')
    emit('close')
  } catch (error) {
    console.error('Failed to save account deny-list:', error)
    const status = (error as { response?: { status?: number } })?.response?.status
    if (status === 409) {
      appStore.showError(t('admin.users.accountDenialsConflict'))
      await load()
    } else {
      appStore.showError(t('admin.users.accountDenialsSaveError'))
    }
  } finally {
    submitting.value = false
  }
}
</script>
