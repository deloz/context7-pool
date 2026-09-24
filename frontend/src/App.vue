<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import {
  CopyDocument,
  Delete,
  Edit,
  Key,
  Lock,
  Plus,
  RefreshRight,
  Search,
  Setting,
  SwitchButton,
  Warning,
} from '@element-plus/icons-vue'
import { ElMessage, ElMessageBox } from 'element-plus'

import {
  changePassword,
  clearAdminToken,
  createKey,
  createRelayToken,
  deleteKey,
  deleteRelayToken,
  fetchAuthStatus,
  fetchContext7MinuteStats,
  fetchContext7RequestLogs,
  fetchContext7Settings,
  fetchContext7StatsSummary,
  fetchKey,
  fetchKeys,
  fetchMeta,
  fetchRelayTokens,
  loginAdmin,
  logoutAdmin,
  rotateRelayToken,
  resetKeyHealth,
  setAdminToken,
  setUnauthorizedHandler,
  setupAdmin,
  updateContext7Settings,
  updateKey,
  updateRelayToken,
} from './api'
import type {
  Context7MinuteStat,
  Context7RequestLog,
  Context7Settings,
  Context7StatsSummary,
  KeyItem,
  RelayTokenItem,
  RelayTokenPage,
  RuntimeMeta,
} from './types'

type AppState = 'checking' | 'setup' | 'login' | 'dashboard'

const appState = ref<AppState>('checking')
const authLoading = ref(false)
const authSubmitting = ref(false)
const loading = ref(false)
const saving = ref(false)
const settingsSaving = ref(false)
const passwordSaving = ref(false)
const relaySaving = ref(false)
const relayLoading = ref(false)
const statsLoading = ref(false)

const dialogVisible = ref(false)
const settingsDialogVisible = ref(false)
const passwordDialogVisible = ref(false)
const relayDialogVisible = ref(false)
const relayRevealVisible = ref(false)
const editingId = ref<number | null>(null)
const relayEditingId = ref<number | null>(null)

const keys = ref<KeyItem[]>([])
const meta = ref<RuntimeMeta | null>(null)
const context7Settings = ref<Context7Settings | null>(null)
const relayTokens = ref<RelayTokenItem[]>([])
const relayTokenTotal = ref(0)
const relayTokenPage = ref(1)
const relayTokenPageSize = ref(10)
const relayTokenPlain = ref('')
const revealedRelayTokenName = ref('')
const statsSummary = ref<Context7StatsSummary | null>(null)
const statsMinutes = ref<Context7MinuteStat[]>([])
const statsLogs = ref<Context7RequestLog[]>([])
const statsLogTotal = ref(0)
const statsLogPage = ref(1)
const statsLogPageSize = ref(20)

const setupForm = reactive({
  username: '',
  password: '',
})

const loginForm = reactive({
  username: '',
  password: '',
})

const passwordForm = reactive({
  oldPassword: '',
  newPassword: '',
  confirmPassword: '',
})

const form = reactive({
  name: '',
  apiKey: '',
  enabled: true,
})

const settingsForm = reactive({
  apiBaseURL: '',
})

const relayForm = reactive({
  name: 'context7-relay',
})

const statsFilters = reactive({
  apiKeyID: '',
  success: '',
  statusCode: '',
})

const isEditing = computed(() => editingId.value !== null)
const isRelayEditing = computed(() => relayEditingId.value !== null)
const relayDialogTitle = computed(() => (isRelayEditing.value ? '编辑 Relay Token' : '新建 Relay Token'))
const relayMCPArgument = computed(() => (relayTokenPlain.value ? `--api-key ${relayTokenPlain.value}` : ''))
const statsSuccessPercent = computed(() => formatPercent(statsSummary.value?.success_rate ?? 0))

let unauthorizedNoticeOpen = false

setUnauthorizedHandler(() => {
  if (appState.value === 'setup') return
  clearAdminToken()
  appState.value = 'login'
  if (!unauthorizedNoticeOpen) {
    unauthorizedNoticeOpen = true
    ElMessage.warning('登录已失效')
    window.setTimeout(() => {
      unauthorizedNoticeOpen = false
    }, 600)
  }
})

const healthTagType = (status: string) => {
  switch (status) {
    case 'healthy':
      return 'success'
    case 'cooling':
      return 'danger'
    case 'degraded':
      return 'warning'
    default:
      return 'info'
  }
}

const formatTime = (value?: string | null) => {
  if (!value) return '—'
  return new Date(value).toLocaleString()
}

const formatPercent = (value: number) => `${(value * 100).toFixed(1)}%`

const formatLatency = (value?: number | null) => `${Math.round(value ?? 0)} ms`

const requestPath = (row: Context7RequestLog) => (row.query ? `${row.path}?${row.query}` : row.path)

const statusTagType = (statusCode: number, success: boolean) => {
  if (statusCode === 0) return 'danger'
  if (success) return 'success'
  if (statusCode >= 500) return 'danger'
  if (statusCode >= 400) return 'warning'
  return 'info'
}

const isCancel = (error: unknown) => error === 'cancel' || error === 'close'

const copyText = async (value?: string) => {
  if (!value) {
    ElMessage.warning('无可复制内容')
    return
  }
  if (!navigator.clipboard) {
    ElMessage.error('当前浏览器不支持剪贴板')
    return
  }

  try {
    await navigator.clipboard.writeText(value)
    ElMessage.success('已复制')
  } catch (error) {
    ElMessage.error((error as Error).message)
  }
}

const bootstrap = async () => {
  authLoading.value = true
  try {
    const status = await fetchAuthStatus()
    if (status.setup_required) {
      clearAdminToken()
      appState.value = 'setup'
      return
    }
    if (!status.authenticated) {
      clearAdminToken()
      appState.value = 'login'
      return
    }

    appState.value = 'dashboard'
    await loadData()
  } catch (error) {
    clearAdminToken()
    appState.value = 'login'
    ElMessage.error((error as Error).message)
  } finally {
    authLoading.value = false
  }
}

const loadData = async () => {
  loading.value = true
  try {
    const [metaResponse, keysResponse, settingsResponse, relayResponse, summaryResponse, minuteResponse, logResponse] = await Promise.all([
      fetchMeta(),
      fetchKeys(),
      fetchContext7Settings(),
      fetchRelayTokens(relayQueryParams()),
      fetchContext7StatsSummary(),
      fetchContext7MinuteStats(minuteQueryParams()),
      fetchContext7RequestLogs(logQueryParams()),
    ])
    meta.value = metaResponse
    keys.value = keysResponse
    context7Settings.value = settingsResponse
    applyRelayTokenPage(relayResponse)
    statsSummary.value = summaryResponse
    statsMinutes.value = minuteResponse.items
    statsLogs.value = logResponse.items
    statsLogTotal.value = logResponse.total
    statsLogPage.value = logResponse.page
    statsLogPageSize.value = logResponse.page_size
  } catch (error) {
    ElMessage.error((error as Error).message)
  } finally {
    loading.value = false
  }
}

const relayQueryParams = () => ({
  page: relayTokenPage.value,
  page_size: relayTokenPageSize.value,
})

const applyRelayTokenPage = (response: RelayTokenPage) => {
  relayTokens.value = response.items
  relayTokenTotal.value = response.total
  relayTokenPage.value = response.page
  relayTokenPageSize.value = response.page_size
}

const loadRelayTokens = async () => {
  relayLoading.value = true
  try {
    let response = await fetchRelayTokens(relayQueryParams())
    if (response.items.length === 0 && response.total > 0 && response.page > 1) {
      relayTokenPage.value = response.page - 1
      response = await fetchRelayTokens(relayQueryParams())
    }
    applyRelayTokenPage(response)
  } catch (error) {
    ElMessage.error((error as Error).message)
  } finally {
    relayLoading.value = false
  }
}

const loadStatsData = async () => {
  statsLoading.value = true
  try {
    const [summaryResponse, minuteResponse, logResponse] = await Promise.all([
      fetchContext7StatsSummary(),
      fetchContext7MinuteStats(minuteQueryParams()),
      fetchContext7RequestLogs(logQueryParams()),
    ])
    statsSummary.value = summaryResponse
    statsMinutes.value = minuteResponse.items
    statsLogs.value = logResponse.items
    statsLogTotal.value = logResponse.total
    statsLogPage.value = logResponse.page
    statsLogPageSize.value = logResponse.page_size
  } catch (error) {
    ElMessage.error((error as Error).message)
  } finally {
    statsLoading.value = false
  }
}

const minuteQueryParams = () => {
  const to = new Date()
  const from = new Date(to.getTime() - 60 * 60 * 1000)
  return {
    from: from.toISOString(),
    to: to.toISOString(),
    api_key_id: parsePositiveNumber(statsFilters.apiKeyID),
  }
}

const logQueryParams = () => ({
  page: statsLogPage.value,
  page_size: statsLogPageSize.value,
  api_key_id: parsePositiveNumber(statsFilters.apiKeyID),
  success: parseSuccessFilter(),
  status_code: parseStatusCode(),
})

const parsePositiveNumber = (value: string) => {
  const parsed = Number(value)
  return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined
}

const parseStatusCode = () => {
  const value = statsFilters.statusCode.trim()
  if (!value) return undefined
  const parsed = Number(value)
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : undefined
}

const parseSuccessFilter = () => {
  if (statsFilters.success === '') return undefined
  return statsFilters.success === 'true'
}

const submitSetup = async () => {
  if (!setupForm.username.trim()) {
    ElMessage.error('用户名不能为空')
    return
  }
  if (setupForm.password.length < 8) {
    ElMessage.error('密码至少 8 个字符')
    return
  }

  authSubmitting.value = true
  try {
    const token = await setupAdmin({
      username: setupForm.username,
      password: setupForm.password,
    })
    setAdminToken(token.token)
    appState.value = 'dashboard'
    ElMessage.success('管理员已创建')
    await loadData()
  } catch (error) {
    ElMessage.error((error as Error).message)
  } finally {
    authSubmitting.value = false
  }
}

const submitLogin = async () => {
  if (!loginForm.username.trim() || !loginForm.password) {
    ElMessage.error('请输入用户名和密码')
    return
  }

  authSubmitting.value = true
  try {
    const token = await loginAdmin({
      username: loginForm.username,
      password: loginForm.password,
    })
    setAdminToken(token.token)
    loginForm.password = ''
    appState.value = 'dashboard'
    await loadData()
  } catch (error) {
    ElMessage.error((error as Error).message)
  } finally {
    authSubmitting.value = false
  }
}

const handleLogout = async () => {
  try {
    await logoutAdmin()
  } catch {
    // The local token is cleared even if the server-side session is already gone.
  }
  clearAdminToken()
  appState.value = 'login'
}

const openPasswordDialog = () => {
  passwordForm.oldPassword = ''
  passwordForm.newPassword = ''
  passwordForm.confirmPassword = ''
  passwordDialogVisible.value = true
}

const submitPasswordChange = async () => {
  if (passwordForm.newPassword.length < 8) {
    ElMessage.error('新密码至少 8 个字符')
    return
  }
  if (passwordForm.newPassword !== passwordForm.confirmPassword) {
    ElMessage.error('两次新密码不一致')
    return
  }

  passwordSaving.value = true
  try {
    await changePassword({
      old_password: passwordForm.oldPassword,
      new_password: passwordForm.newPassword,
    })
    passwordDialogVisible.value = false
    clearAdminToken()
    appState.value = 'login'
    ElMessage.success('密码已修改，请重新登录')
  } catch (error) {
    ElMessage.error((error as Error).message)
  } finally {
    passwordSaving.value = false
  }
}

const openCreateDialog = () => {
  editingId.value = null
  form.name = ''
  form.apiKey = ''
  form.enabled = true
  dialogVisible.value = true
}

const openSettingsDialog = () => {
  settingsForm.apiBaseURL = context7Settings.value?.api_base_url ?? ''
  settingsDialogVisible.value = true
}

const openEditDialog = async (row: KeyItem) => {
  try {
    const detail = await fetchKey(row.id)
    editingId.value = detail.id
    form.name = detail.name
    form.apiKey = detail.api_key
    form.enabled = detail.enabled
    dialogVisible.value = true
  } catch (error) {
    ElMessage.error((error as Error).message)
  }
}

const submitForm = async () => {
  saving.value = true
  try {
    if (isEditing.value && editingId.value !== null) {
      await updateKey(editingId.value, {
        name: form.name,
        api_key: form.apiKey,
        enabled: form.enabled,
      })
      ElMessage.success('Key updated')
    } else {
      await createKey({
        name: form.name,
        api_key: form.apiKey,
        enabled: form.enabled,
      })
      ElMessage.success('Key created')
    }
    dialogVisible.value = false
    await loadData()
  } catch (error) {
    ElMessage.error((error as Error).message)
  } finally {
    saving.value = false
  }
}

const submitSettingsForm = async () => {
  settingsSaving.value = true
  try {
    const updated = await updateContext7Settings({
      api_base_url: settingsForm.apiBaseURL,
    })
    context7Settings.value = updated
    settingsDialogVisible.value = false
    ElMessage.success('Context7 settings updated')
    await loadData()
  } catch (error) {
    ElMessage.error((error as Error).message)
  } finally {
    settingsSaving.value = false
  }
}

const openCreateRelayDialog = () => {
  relayEditingId.value = null
  relayForm.name = 'context7-relay'
  relayDialogVisible.value = true
}

const openEditRelayDialog = (row: RelayTokenItem) => {
  relayEditingId.value = row.id
  relayForm.name = row.name
  relayDialogVisible.value = true
}

const submitRelayToken = async () => {
  if (!relayForm.name.trim()) {
    ElMessage.error('名称不能为空')
    return
  }

  relaySaving.value = true
  try {
    if (isRelayEditing.value && relayEditingId.value !== null) {
      await updateRelayToken(relayEditingId.value, { name: relayForm.name })
      relayDialogVisible.value = false
      ElMessage.success('Relay token 已更新')
      await loadRelayTokens()
      return
    }

    const created = await createRelayToken({ name: relayForm.name })
    showRelayToken(created.name, created.token)
    relayDialogVisible.value = false
    ElMessage.success('Relay token 已创建')
    await loadRelayTokens()
  } catch (error) {
    ElMessage.error((error as Error).message)
  } finally {
    relaySaving.value = false
  }
}

const showRelayToken = (name: string, token: string) => {
  revealedRelayTokenName.value = name
  relayTokenPlain.value = token
  relayRevealVisible.value = true
}

const copyRelayToken = async (row: RelayTokenItem) => {
  await copyText(row.token ?? undefined)
}

const copyRevealedRelayToken = async () => {
  await copyText(relayTokenPlain.value)
}

const copyAPIKey = async (row: KeyItem) => {
  await copyText(row.api_key)
}

const handleRotateRelayToken = async (row: RelayTokenItem) => {
  try {
    await ElMessageBox.confirm(`轮换 Relay Token "${row.name}"？旧 token 将立即失效。`, 'Confirm', {
      type: 'warning',
    })
    const rotated = await rotateRelayToken(row.id)
    showRelayToken(rotated.name, rotated.token)
    ElMessage.success('Relay token 已轮换')
    await loadRelayTokens()
  } catch (error) {
    if (!isCancel(error)) {
      ElMessage.error((error as Error).message)
    }
  }
}

const handleDeleteRelayToken = async (row: RelayTokenItem) => {
  try {
    await ElMessageBox.confirm(`删除 Relay Token "${row.name}"？删除后该 token 将无法继续访问。`, 'Confirm', {
      type: 'warning',
    })
    await deleteRelayToken(row.id)
    ElMessage.success('Relay token 已删除')
    await loadRelayTokens()
  } catch (error) {
    if (!isCancel(error)) {
      ElMessage.error((error as Error).message)
    }
  }
}

const handleDelete = async (row: KeyItem) => {
  try {
    await ElMessageBox.confirm(`Delete key "${row.name}"?`, 'Confirm', {
      type: 'warning',
    })
    await deleteKey(row.id)
    ElMessage.success('Key deleted')
    await loadData()
  } catch (error) {
    if (!isCancel(error)) {
      ElMessage.error((error as Error).message)
    }
  }
}

const handleResetHealth = async (row: KeyItem) => {
  try {
    await resetKeyHealth(row.id)
    ElMessage.success('Health reset')
    await loadData()
  } catch (error) {
    ElMessage.error((error as Error).message)
  }
}

const handleEnabledChange = async (row: KeyItem, enabled: boolean | string | number) => {
  const nextValue = Boolean(enabled)
  try {
    await updateKey(row.id, { enabled: nextValue })
    await loadData()
  } catch (error) {
    row.enabled = !nextValue
    ElMessage.error((error as Error).message)
  }
}

const bindEnabledChange = (row: KeyItem) => (value: boolean | string | number) => {
  void handleEnabledChange(row, value)
}

const handleStatsSearch = async () => {
  statsLogPage.value = 1
  await loadStatsData()
}

const handleStatsReset = async () => {
  statsFilters.apiKeyID = ''
  statsFilters.success = ''
  statsFilters.statusCode = ''
  statsLogPage.value = 1
  statsLogPageSize.value = 20
  await loadStatsData()
}

const handleStatsPageChange = async (page: number) => {
  statsLogPage.value = page
  await loadStatsData()
}

const handleStatsPageSizeChange = async (size: number) => {
  statsLogPageSize.value = size
  statsLogPage.value = 1
  await loadStatsData()
}

const handleRelayPageChange = async (page: number) => {
  relayTokenPage.value = page
  await loadRelayTokens()
}

const handleRelayPageSizeChange = async (size: number) => {
  relayTokenPageSize.value = size
  relayTokenPage.value = 1
  await loadRelayTokens()
}

onMounted(() => {
  void bootstrap()
})
</script>

<template>
  <div class="page-shell">
    <section v-if="appState === 'checking'" class="auth-screen">
      <el-card shadow="never" class="auth-card">
        <el-skeleton :rows="5" animated />
      </el-card>
    </section>

    <section v-else-if="appState === 'setup'" class="auth-screen">
      <el-card shadow="never" class="auth-card">
        <div class="auth-heading">
          <p class="eyebrow">ContextPool</p>
          <h1>初始化管理员</h1>
        </div>
        <el-form label-position="top" @submit.prevent>
          <el-form-item label="用户名">
            <el-input v-model="setupForm.username" autocomplete="username" placeholder="admin" />
          </el-form-item>
          <el-form-item label="密码">
            <el-input
              v-model="setupForm.password"
              type="password"
              autocomplete="new-password"
              show-password
              @keyup.enter="submitSetup"
            />
          </el-form-item>
          <el-button type="primary" :loading="authSubmitting || authLoading" @click="submitSetup">
            创建管理员
          </el-button>
        </el-form>
      </el-card>
    </section>

    <section v-else-if="appState === 'login'" class="auth-screen">
      <el-card shadow="never" class="auth-card">
        <div class="auth-heading">
          <p class="eyebrow">ContextPool</p>
          <h1>管理员登录</h1>
        </div>
        <el-form label-position="top" @submit.prevent>
          <el-form-item label="用户名">
            <el-input v-model="loginForm.username" autocomplete="username" />
          </el-form-item>
          <el-form-item label="密码">
            <el-input
              v-model="loginForm.password"
              type="password"
              autocomplete="current-password"
              show-password
              @keyup.enter="submitLogin"
            />
          </el-form-item>
          <el-button type="primary" :loading="authSubmitting || authLoading" @click="submitLogin">登录</el-button>
        </el-form>
      </el-card>
    </section>

    <template v-else>
      <section class="hero">
        <div>
          <p class="eyebrow">ContextPool</p>
          <h1>Context7 Pool Admin</h1>
          <p class="hero-copy">单实例内存轮询、异步状态落库、透明代理热路径不查库。</p>
        </div>
        <div class="hero-actions">
          <el-button :icon="RefreshRight" :loading="loading" plain @click="loadData">刷新</el-button>
          <el-button :icon="Setting" plain @click="openSettingsDialog">Context7 设置</el-button>
          <el-button :icon="Lock" plain @click="openPasswordDialog">修改密码</el-button>
          <el-button :icon="SwitchButton" plain @click="handleLogout">退出登录</el-button>
          <el-button type="primary" :icon="Plus" @click="openCreateDialog">新增 Key</el-button>
        </div>
      </section>

      <section class="summary-grid">
        <el-card shadow="hover" class="summary-card">
          <p class="summary-label">总 Key</p>
          <p class="summary-value">{{ meta?.total_key_count ?? 0 }}</p>
        </el-card>
        <el-card shadow="hover" class="summary-card">
          <p class="summary-label">可用 Key</p>
          <p class="summary-value">{{ meta?.available_key_count ?? 0 }}</p>
        </el-card>
        <el-card shadow="hover" class="summary-card">
          <p class="summary-label">冷却中</p>
          <p class="summary-value">{{ meta?.cooling_key_count ?? 0 }}</p>
        </el-card>
        <el-card shadow="hover" class="summary-card">
          <p class="summary-label">快照版本</p>
          <p class="summary-value">#{{ meta?.snapshot_version ?? 0 }}</p>
        </el-card>
      </section>

      <el-card shadow="never" class="panel-card settings-card">
        <template #header>
          <div class="panel-header">
            <div>
              <h2>Context7 Relay</h2>
              <p>数据库配置为运行时真值，修改后立即生效，无需重启。</p>
            </div>
            <el-button plain @click="openSettingsDialog">编辑 URL</el-button>
          </div>
        </template>

        <div class="settings-row">
          <div>
            <p class="settings-label">Context7 API Base URL</p>
            <p class="settings-value">{{ context7Settings?.api_base_url ?? '—' }}</p>
          </div>
          <el-tag :type="meta?.upstream_configured ? 'success' : 'warning'">
            {{ meta?.upstream_configured ? '已配置' : '未配置' }}
          </el-tag>
        </div>
      </el-card>

      <el-card shadow="never" class="panel-card relay-card">
        <template #header>
          <div class="panel-header">
            <div>
              <h2>Relay Token</h2>
              <p>多个 MCP 客户端可分别使用自己的 token，删除或轮换只影响对应条目。</p>
            </div>
            <div class="panel-actions">
              <el-button type="primary" :icon="Key" @click="openCreateRelayDialog">新建 Token</el-button>
            </div>
          </div>
        </template>

        <el-table v-loading="loading || relayLoading" :data="relayTokens" stripe empty-text="暂无 Relay Token">
          <el-table-column prop="name" label="名称" min-width="180" />
          <el-table-column label="Token" min-width="240">
            <template #default="{ row }">
              <div class="copy-value-row key-cell">
                <span>{{ row.masked_token }}</span>
                <el-button
                  :icon="CopyDocument"
                  circle
                  plain
                  size="small"
                  :disabled="!row.token"
                  title="复制完整 Token"
                  @click="copyRelayToken(row)"
                />
              </div>
            </template>
          </el-table-column>
          <el-table-column label="创建时间" min-width="180">
            <template #default="{ row }">{{ formatTime(row.created_at) }}</template>
          </el-table-column>
          <el-table-column label="最近使用" min-width="180">
            <template #default="{ row }">{{ formatTime(row.last_used_at) }}</template>
          </el-table-column>
          <el-table-column label="操作" fixed="right" width="260">
            <template #default="{ row }">
              <div class="row-actions">
                <el-button link type="primary" :icon="Edit" @click="openEditRelayDialog(row)">编辑</el-button>
                <el-button link type="warning" :icon="RefreshRight" @click="handleRotateRelayToken(row)">轮换</el-button>
                <el-button link type="danger" :icon="Delete" @click="handleDeleteRelayToken(row)">删除</el-button>
              </div>
            </template>
          </el-table-column>
        </el-table>

        <div class="pagination-row">
          <el-pagination
            :current-page="relayTokenPage"
            :page-size="relayTokenPageSize"
            :page-sizes="[10, 20, 50, 100]"
            layout="total, sizes, prev, pager, next, jumper"
            :total="relayTokenTotal"
            @update:current-page="handleRelayPageChange"
            @update:page-size="handleRelayPageSizeChange"
          />
        </div>
      </el-card>

      <el-alert
        v-if="meta && !meta.upstream_configured"
        class="state-alert"
        title="Context7 relay 当前不可用，请检查 Context7 API Base URL。"
        type="warning"
        :icon="Warning"
        show-icon
        :closable="false"
      />

      <el-card shadow="never" class="panel-card">
        <template #header>
          <div class="panel-header">
            <div>
              <h2>Context7 Statistics</h2>
              <p>只统计 /relay/context7/*，永久保留请求日志和分钟聚合。</p>
            </div>
            <el-button :icon="RefreshRight" :loading="statsLoading" plain @click="loadStatsData">刷新统计</el-button>
          </div>
        </template>

        <section class="stats-summary-grid">
          <el-card shadow="hover" class="summary-card">
            <p class="summary-label">总请求</p>
            <p class="summary-value">{{ statsSummary?.total_requests ?? 0 }}</p>
          </el-card>
          <el-card shadow="hover" class="summary-card">
            <p class="summary-label">成功率</p>
            <p class="summary-value">{{ statsSuccessPercent }}</p>
          </el-card>
          <el-card shadow="hover" class="summary-card">
            <p class="summary-label">失败数</p>
            <p class="summary-value">{{ statsSummary?.failed_requests ?? 0 }}</p>
          </el-card>
          <el-card shadow="hover" class="summary-card">
            <p class="summary-label">平均耗时</p>
            <p class="summary-value stats-latency">{{ formatLatency(statsSummary?.average_latency_ms) }}</p>
          </el-card>
          <el-card shadow="hover" class="summary-card">
            <p class="summary-label">最近请求</p>
            <p class="summary-value stats-secondary">{{ formatTime(statsSummary?.last_request_at) }}</p>
          </el-card>
          <el-card shadow="hover" class="summary-card">
            <p class="summary-label">最近状态</p>
            <p class="summary-value">{{ statsSummary?.last_status_code ?? 0 }}</p>
          </el-card>
        </section>

        <div class="stats-mini-grid">
          <el-card shadow="never" class="mini-stat-card">
            <p class="summary-label">2xx / 4xx / 5xx</p>
            <p class="mini-stat-value">
              {{ statsSummary?.status_2xx ?? 0 }} / {{ statsSummary?.status_4xx ?? 0 }} / {{ statsSummary?.status_5xx ?? 0 }}
            </p>
          </el-card>
          <el-card shadow="never" class="mini-stat-card">
            <p class="summary-label">网络错误</p>
            <p class="mini-stat-value">{{ statsSummary?.network_errors ?? 0 }}</p>
          </el-card>
          <el-card shadow="never" class="mini-stat-card">
            <p class="summary-label">最大耗时</p>
            <p class="mini-stat-value">{{ formatLatency(statsSummary?.max_latency_ms) }}</p>
          </el-card>
          <el-card shadow="never" class="mini-stat-card">
            <p class="summary-label">最近错误</p>
            <p class="mini-stat-value mini-stat-text">{{ statsSummary?.last_error ?? '—' }}</p>
          </el-card>
        </div>
      </el-card>

      <el-card shadow="never" class="panel-card">
        <template #header>
          <div class="panel-header">
            <div>
              <h2>Minute Trend</h2>
              <p>默认展示最近 60 分钟的 key 分钟聚合。</p>
            </div>
            <span>数据源：minute buckets</span>
          </div>
        </template>

        <el-table v-loading="statsLoading" :data="statsMinutes" stripe empty-text="暂无统计数据">
          <el-table-column label="分钟" min-width="180">
            <template #default="{ row }">{{ formatTime(row.minute_at) }}</template>
          </el-table-column>
          <el-table-column prop="api_key_name" label="Key" min-width="140" />
          <el-table-column prop="total_requests" label="总请求" width="100" />
          <el-table-column prop="success_requests" label="成功" width="90" />
          <el-table-column prop="failed_requests" label="失败" width="90" />
          <el-table-column label="成功率" width="110">
            <template #default="{ row }">{{ formatPercent(row.success_rate) }}</template>
          </el-table-column>
          <el-table-column label="平均耗时" width="110">
            <template #default="{ row }">{{ formatLatency(row.average_latency_ms) }}</template>
          </el-table-column>
          <el-table-column label="最大耗时" width="110">
            <template #default="{ row }">{{ formatLatency(row.max_latency_ms) }}</template>
          </el-table-column>
          <el-table-column label="错误" min-width="220">
            <template #default="{ row }">
              <span class="error-text">{{ row.last_error || '—' }}</span>
            </template>
          </el-table-column>
        </el-table>
      </el-card>

      <el-card shadow="never" class="panel-card">
        <template #header>
          <div class="panel-header">
            <div>
              <h2>Request Logs</h2>
              <p>日志不保存 request/response body，也不保存 Authorization 或上游 API key。</p>
            </div>
            <span>分页 {{ statsLogPage }} / {{ Math.max(1, Math.ceil(statsLogTotal / statsLogPageSize)) }}</span>
          </div>
        </template>

        <div class="stats-filter-row">
          <el-select v-model="statsFilters.apiKeyID" clearable placeholder="全部 Key" class="stats-filter-item">
            <el-option label="全部 Key" value="" />
            <el-option v-for="item in keys" :key="item.id" :label="item.name" :value="String(item.id)" />
          </el-select>
          <el-select v-model="statsFilters.success" clearable placeholder="结果" class="stats-filter-item">
            <el-option label="全部结果" value="" />
            <el-option label="成功" value="true" />
            <el-option label="失败" value="false" />
          </el-select>
          <el-input
            v-model="statsFilters.statusCode"
            clearable
            placeholder="状态码"
            class="stats-filter-item"
            @keyup.enter="handleStatsSearch"
          />
          <div class="stats-filter-actions">
            <el-button :icon="Search" type="primary" :loading="statsLoading" @click="handleStatsSearch">筛选</el-button>
            <el-button plain @click="handleStatsReset">重置</el-button>
          </div>
        </div>

        <el-table v-loading="statsLoading" :data="statsLogs" stripe empty-text="暂无请求日志">
          <el-table-column label="时间" min-width="180">
            <template #default="{ row }">{{ formatTime(row.started_at) }}</template>
          </el-table-column>
          <el-table-column prop="api_key_name" label="Key" min-width="120" />
          <el-table-column prop="method" label="方法" width="90" />
          <el-table-column label="路径" min-width="260">
            <template #default="{ row }">{{ requestPath(row) }}</template>
          </el-table-column>
          <el-table-column label="状态码" width="100">
            <template #default="{ row }">
              <el-tag :type="statusTagType(row.status_code, row.success)">
                {{ row.status_code || 'NET' }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column label="结果" width="90">
            <template #default="{ row }">
              <el-tag :type="row.success ? 'success' : 'danger'">{{ row.success ? '成功' : '失败' }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="耗时" width="100">
            <template #default="{ row }">{{ formatLatency(row.latency_ms) }}</template>
          </el-table-column>
          <el-table-column label="客户端 IP" min-width="140">
            <template #default="{ row }">{{ row.client_ip || '—' }}</template>
          </el-table-column>
          <el-table-column label="来源" min-width="120">
            <template #default="{ row }">{{ row.client_source || '—' }}</template>
          </el-table-column>
          <el-table-column label="错误" min-width="240">
            <template #default="{ row }">
              <span class="error-text">{{ row.error || '—' }}</span>
            </template>
          </el-table-column>
        </el-table>

        <div class="pagination-row">
          <el-pagination
            :current-page="statsLogPage"
            :page-size="statsLogPageSize"
            :page-sizes="[10, 20, 50, 100]"
            layout="total, sizes, prev, pager, next, jumper"
            :total="statsLogTotal"
            @update:current-page="handleStatsPageChange"
            @update:page-size="handleStatsPageSizeChange"
          />
        </div>
      </el-card>

      <el-card shadow="never" class="panel-card">
        <template #header>
          <div class="panel-header">
            <div>
              <h2>API Keys</h2>
              <p>冷却秒数 {{ meta?.cooldown_seconds ?? 0 }}，失败阈值 {{ meta?.failure_threshold ?? 0 }}</p>
            </div>
            <span>快照更新时间：{{ formatTime(meta?.snapshot_updated_at) }}</span>
          </div>
        </template>

        <el-table v-loading="loading" :data="keys" stripe>
          <el-table-column prop="name" label="名称" min-width="160" />
          <el-table-column label="Key" min-width="220">
            <template #default="{ row }">
              <div class="copy-value-row key-cell">
                <span>{{ row.masked_api_key }}</span>
                <el-button
                  :icon="CopyDocument"
                  circle
                  plain
                  size="small"
                  title="复制完整 API Key"
                  @click="copyAPIKey(row)"
                />
              </div>
            </template>
          </el-table-column>
          <el-table-column label="启用" width="110">
            <template #default="{ row }">
              <el-switch :model-value="row.enabled" @change="bindEnabledChange(row)" />
            </template>
          </el-table-column>
          <el-table-column label="健康状态" width="130">
            <template #default="{ row }">
              <el-tag :type="healthTagType(row.health_status)">{{ row.health_status }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="failure_streak" label="失败次数" width="110" />
          <el-table-column label="冷却到期" min-width="180">
            <template #default="{ row }">{{ formatTime(row.cooldown_until) }}</template>
          </el-table-column>
          <el-table-column label="最近成功" min-width="180">
            <template #default="{ row }">{{ formatTime(row.last_success_at) }}</template>
          </el-table-column>
          <el-table-column label="最近错误" min-width="240">
            <template #default="{ row }">
              <span class="error-text">{{ row.last_error || '—' }}</span>
            </template>
          </el-table-column>
          <el-table-column label="操作" fixed="right" width="250">
            <template #default="{ row }">
              <div class="row-actions">
                <el-button link type="primary" @click="openEditDialog(row)">编辑</el-button>
                <el-button link type="warning" @click="handleResetHealth(row)">重置健康</el-button>
                <el-button link type="danger" :icon="Delete" @click="handleDelete(row)">删除</el-button>
              </div>
            </template>
          </el-table-column>
        </el-table>
      </el-card>

      <el-dialog v-model="dialogVisible" :title="isEditing ? '编辑 Key' : '新增 Key'" width="520px">
        <el-form label-position="top">
          <el-form-item label="名称">
            <el-input v-model="form.name" maxlength="64" placeholder="例如：primary-key-01" />
          </el-form-item>
          <el-form-item label="API Key">
            <el-input v-model="form.apiKey" type="textarea" :rows="4" placeholder="输入完整的上游 API Key" />
          </el-form-item>
          <el-form-item label="启用状态">
            <el-switch v-model="form.enabled" />
          </el-form-item>
        </el-form>

        <template #footer>
          <el-button @click="dialogVisible = false">取消</el-button>
          <el-button type="primary" :loading="saving" @click="submitForm">保存</el-button>
        </template>
      </el-dialog>

      <el-dialog v-model="settingsDialogVisible" title="编辑 Context7 API Base URL" width="560px">
        <el-form label-position="top">
          <el-form-item label="API Base URL">
            <el-input v-model="settingsForm.apiBaseURL" placeholder="例如：https://context7.com/api" />
          </el-form-item>
        </el-form>

        <template #footer>
          <el-button @click="settingsDialogVisible = false">取消</el-button>
          <el-button type="primary" :loading="settingsSaving" @click="submitSettingsForm">保存</el-button>
        </template>
      </el-dialog>

      <el-dialog v-model="passwordDialogVisible" title="修改密码" width="520px">
        <el-form label-position="top">
          <el-form-item label="旧密码">
            <el-input v-model="passwordForm.oldPassword" type="password" autocomplete="current-password" show-password />
          </el-form-item>
          <el-form-item label="新密码">
            <el-input v-model="passwordForm.newPassword" type="password" autocomplete="new-password" show-password />
          </el-form-item>
          <el-form-item label="确认新密码">
            <el-input
              v-model="passwordForm.confirmPassword"
              type="password"
              autocomplete="new-password"
              show-password
              @keyup.enter="submitPasswordChange"
            />
          </el-form-item>
        </el-form>

        <template #footer>
          <el-button @click="passwordDialogVisible = false">取消</el-button>
          <el-button type="primary" :loading="passwordSaving" @click="submitPasswordChange">保存</el-button>
        </template>
      </el-dialog>

      <el-dialog v-model="relayDialogVisible" :title="relayDialogTitle" width="520px">
        <el-form label-position="top">
          <el-form-item label="名称">
            <el-input v-model="relayForm.name" maxlength="64" placeholder="context7-relay" />
          </el-form-item>
        </el-form>

        <template #footer>
          <el-button @click="relayDialogVisible = false">取消</el-button>
          <el-button type="primary" :loading="relaySaving" @click="submitRelayToken">
            {{ isRelayEditing ? '保存' : '新建' }}
          </el-button>
        </template>
      </el-dialog>

      <el-dialog v-model="relayRevealVisible" title="Relay Token" width="640px" @closed="relayTokenPlain = ''">
        <div class="token-reveal">
          <p>{{ revealedRelayTokenName }} 的完整 token 已保存，可在 Relay Token 表格继续复制。</p>
          <el-input :model-value="relayTokenPlain" readonly>
            <template #append>
              <el-button :icon="CopyDocument" @click="copyRevealedRelayToken">复制</el-button>
            </template>
          </el-input>
          <p class="mcp-arg">{{ relayMCPArgument }}</p>
        </div>

        <template #footer>
          <el-button type="primary" @click="relayRevealVisible = false">关闭</el-button>
        </template>
      </el-dialog>
    </template>
  </div>
</template>
