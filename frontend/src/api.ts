import type {
  AuthStatus,
  AuthTokenResponse,
  Context7MinuteStatsResponse,
  Context7RequestLogPage,
  Context7Settings,
  Context7StatsSummary,
  KeyDetail,
  KeyListResponse,
  RelayTokenItem,
  RelayTokenPage,
  RelayTokenResponse,
  RelayTokenView,
  RuntimeMeta,
} from './types'

const adminTokenKey = 'contextpool_admin_token'

let unauthorizedHandler: (() => void) | null = null

export function getAdminToken() {
  return sessionStorage.getItem(adminTokenKey) ?? ''
}

export function setAdminToken(token: string) {
  sessionStorage.setItem(adminTokenKey, token)
}

export function clearAdminToken() {
  sessionStorage.removeItem(adminTokenKey)
}

export function setUnauthorizedHandler(handler: (() => void) | null) {
  unauthorizedHandler = handler
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const token = getAdminToken()
  const response = await fetch(url, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(init?.headers ?? {}),
    },
  })

  if (!response.ok) {
    const payload = (await response.json().catch(() => null)) as { error?: string } | null
    if (response.status === 401) {
      clearAdminToken()
      unauthorizedHandler?.()
    }
    throw new Error(payload?.error ?? `Request failed: ${response.status}`)
  }

  if (response.status === 204) {
    return undefined as T
  }

  return (await response.json()) as T
}

export function fetchAuthStatus() {
  return request<AuthStatus>('/api/admin/auth/status')
}

export function setupAdmin(payload: { username: string; password: string }) {
  return request<AuthTokenResponse>('/api/admin/auth/setup', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export function loginAdmin(payload: { username: string; password: string }) {
  return request<AuthTokenResponse>('/api/admin/auth/login', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export function logoutAdmin() {
  return request<void>('/api/admin/auth/logout', {
    method: 'POST',
  })
}

export function changePassword(payload: { old_password: string; new_password: string }) {
  return request<void>('/api/admin/auth/change-password', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export function fetchMeta() {
  return request<RuntimeMeta>('/api/admin/meta')
}

export function fetchContext7StatsSummary() {
  return request<Context7StatsSummary>('/api/admin/stats/context7/summary')
}

export function fetchContext7MinuteStats(params: {
  from?: string
  to?: string
  api_key_id?: number
}) {
  const query = new URLSearchParams()
  if (params.from) query.set('from', params.from)
  if (params.to) query.set('to', params.to)
  if (params.api_key_id) query.set('api_key_id', String(params.api_key_id))
  const suffix = query.toString()
  return request<Context7MinuteStatsResponse>(`/api/admin/stats/context7/minutes${suffix ? `?${suffix}` : ''}`)
}

export function fetchContext7RequestLogs(params: {
  page: number
  page_size: number
  api_key_id?: number
  success?: boolean
  status_code?: number
}) {
  const query = new URLSearchParams()
  query.set('page', String(params.page))
  query.set('page_size', String(params.page_size))
  if (params.api_key_id) query.set('api_key_id', String(params.api_key_id))
  if (params.success !== undefined) query.set('success', String(params.success))
  if (params.status_code !== undefined) query.set('status_code', String(params.status_code))
  return request<Context7RequestLogPage>(`/api/admin/stats/context7/logs?${query.toString()}`)
}

export function fetchContext7Settings() {
  return request<Context7Settings>('/api/admin/settings/context7')
}

export async function fetchKeys() {
  const response = await request<KeyListResponse>('/api/admin/keys')
  return response.items
}

export function fetchKey(id: number) {
  return request<KeyDetail>(`/api/admin/keys/${id}`)
}

export function createKey(payload: { name: string; api_key: string; enabled: boolean }) {
  return request<KeyDetail>('/api/admin/keys', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export function updateKey(id: number, payload: Partial<{ name: string; api_key: string; enabled: boolean }>) {
  return request<KeyDetail>(`/api/admin/keys/${id}`, {
    method: 'PATCH',
    body: JSON.stringify(payload),
  })
}

export function deleteKey(id: number) {
  return request<void>(`/api/admin/keys/${id}`, {
    method: 'DELETE',
  })
}

export function resetKeyHealth(id: number) {
  return request<KeyDetail>(`/api/admin/keys/${id}/reset-health`, {
    method: 'POST',
  })
}

export function updateContext7Settings(payload: Context7Settings) {
  return request<Context7Settings>('/api/admin/settings/context7', {
    method: 'PATCH',
    body: JSON.stringify(payload),
  })
}

export function fetchRelayToken() {
  return request<RelayTokenView>('/api/admin/relay-token')
}

export function fetchRelayTokens(params: { page: number; page_size: number }) {
  const query = new URLSearchParams()
  query.set('page', String(params.page))
  query.set('page_size', String(params.page_size))
  return request<RelayTokenPage>(`/api/admin/relay-tokens?${query.toString()}`)
}

export function createRelayToken(payload: { name: string }) {
  return request<RelayTokenResponse>('/api/admin/relay-tokens', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export function updateRelayToken(id: number, payload: { name: string }) {
  return request<RelayTokenItem>(`/api/admin/relay-tokens/${id}`, {
    method: 'PATCH',
    body: JSON.stringify(payload),
  })
}

export function rotateRelayToken(id: number) {
  return request<RelayTokenResponse>(`/api/admin/relay-tokens/${id}/rotate`, {
    method: 'POST',
  })
}

export function deleteRelayToken(id: number) {
  return request<void>(`/api/admin/relay-tokens/${id}`, {
    method: 'DELETE',
  })
}

export function generateRelayToken(payload?: { name?: string }) {
  return request<RelayTokenResponse>('/api/admin/relay-token', {
    method: 'POST',
    body: JSON.stringify(payload ?? {}),
  })
}

export function revokeRelayToken() {
  return request<void>('/api/admin/relay-token', {
    method: 'DELETE',
  })
}
