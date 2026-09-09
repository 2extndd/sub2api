<template>
  <BaseDialog :show="show" :title="t('admin.accounts.denyingUsersTitle')" width="narrow" @close="$emit('close')">
    <div v-if="account" class="space-y-4">
      <div class="rounded-xl border border-gray-200 bg-gray-50 p-4 dark:border-dark-700 dark:bg-dark-800/70">
        <div class="flex items-center gap-2">
          <span class="font-mono text-xs text-gray-400">#{{ account.id }}</span>
          <span class="font-semibold text-gray-900 dark:text-white">{{ account.name }}</span>
        </div>
        <p class="mt-1 text-sm text-gray-600 dark:text-gray-300">
          {{ t('admin.accounts.denyingUsersHint') }}
        </p>
      </div>

      <div v-if="loading" class="flex h-40 items-center justify-center">
        <div class="h-8 w-8 animate-spin rounded-full border-2 border-primary-500 border-t-transparent"></div>
      </div>
      <div v-else-if="users.length === 0" class="rounded-xl border border-dashed border-gray-300 p-8 text-center text-sm text-gray-500 dark:border-dark-600 dark:text-gray-400">
        {{ t('admin.accounts.denyingUsersEmpty') }}
      </div>
      <div v-else class="max-h-96 space-y-2 overflow-y-auto">
        <div
          v-for="user in users"
          :key="user.user_id"
          class="flex items-center justify-between rounded-xl border border-gray-200 px-4 py-3 dark:border-dark-700"
        >
          <span class="truncate text-sm font-medium text-gray-900 dark:text-white">{{ user.email }}</span>
          <span class="ml-3 font-mono text-xs text-gray-400">#{{ user.user_id }}</span>
        </div>
      </div>
    </div>

    <template #footer>
      <button type="button" class="btn btn-secondary" @click="$emit('close')">
        {{ t('common.close') }}
      </button>
    </template>
  </BaseDialog>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import BaseDialog from '@/components/common/BaseDialog.vue'
import { accountsAPI, type AccountDenyingUser } from '@/api/admin/accounts'
import { useAppStore } from '@/stores/app'
import type { Account } from '@/types'

const props = defineProps<{ show: boolean; account: Account | null }>()
defineEmits(['close'])
const { t } = useI18n()
const appStore = useAppStore()
const loading = ref(false)
const users = ref<AccountDenyingUser[]>([])

watch(() => props.show, (visible) => {
  if (visible && props.account) void load()
  if (!visible) users.value = []
})

async function load() {
  if (!props.account) return
  loading.value = true
  try {
    const result = await accountsAPI.getDenyingUsers(props.account.id)
    users.value = result.users
  } catch (error) {
    console.error('Failed to load users denying account:', error)
    appStore.showError(t('admin.accounts.denyingUsersLoadError'))
  } finally {
    loading.value = false
  }
}
</script>
