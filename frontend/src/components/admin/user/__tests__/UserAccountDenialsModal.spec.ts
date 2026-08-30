import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import UserAccountDenialsModal from '../UserAccountDenialsModal.vue'

const apiMocks = vi.hoisted(() => ({
  getAccountDenials: vi.fn(),
  replaceAccountDenials: vi.fn(),
  listAccounts: vi.fn(),
}))

vi.mock('@/api/admin', () => ({
  usersAPI: {
    getAccountDenials: apiMocks.getAccountDenials,
    replaceAccountDenials: apiMocks.replaceAccountDenials,
  },
  accountsAPI: { list: apiMocks.listAccounts },
}))

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({ showError: vi.fn(), showSuccess: vi.fn() }),
}))

vi.mock('vue-i18n', () => ({
  useI18n: () => ({ t: (key: string) => key }),
}))

const user = { id: 7, email: 'user@example.test' }
const account = {
  id: 85,
  name: 'API2CN Claude 01',
  platform: 'openai',
  type: 'apikey',
  status: 'active',
  schedulable: true,
  priority: 1,
}

function mountModal() {
  return mount(UserAccountDenialsModal, {
    props: { show: true, user: user as never },
    global: {
      stubs: {
        BaseDialog: { template: '<div><slot/><slot name="footer"/></div>' },
        Icon: true,
      },
    },
  })
}

describe('UserAccountDenialsModal', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
    apiMocks.getAccountDenials.mockResolvedValue({ user_id: 7, revision: 3, account_ids: [] })
    apiMocks.listAccounts.mockResolvedValue({ items: [account], total: 1, page: 1, page_size: 100, pages: 1 })
    apiMocks.replaceAccountDenials.mockResolvedValue({ user_id: 7, revision: 4, account_ids: [85] })
  })

  it('defaults to all accounts and saves only explicit exclusions', async () => {
    const wrapper = mountModal()
    await flushPromises()

    expect(wrapper.text()).toContain('admin.users.accountDenialsDefaultAllow')
    const checkboxes = wrapper.findAll('input[type="checkbox"]')
    await checkboxes[1].setValue(true)
    await wrapper.findAll('button').at(-1)!.trigger('click')
    await flushPromises()

    expect(apiMocks.replaceAccountDenials).toHaveBeenCalledWith(7, 3, [85])
  })
})
