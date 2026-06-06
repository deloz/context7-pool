export interface AuthStatus {
  setup_required: boolean
  authenticated: boolean
}

export interface AuthTokenResponse {
  token: string
  expires_at: string
}

export interface RuntimeMeta {
  total_key_count: number
  available_key_count: number
  cooling_key_count: number
  snapshot_updated_at: string
  failure_threshold: number
  cooldown_seconds: number
  snapshot_version: number
  upstream_configured: boolean
}

export interface Context7Settings {
  api_base_url: string
}

export interface KeyItem {
  id: number
  name: string
  api_key: string
  masked_api_key: string
  enabled: boolean
  health_status: string
  failure_streak: number
  cooldown_until: string | null
  last_error: string | null
  last_status_code: number | null
  last_success_at: string | null
  created_at: string
  updated_at: string
}

export interface KeyDetail extends KeyItem {
  api_key: string
}

export interface KeyListResponse {
  items: KeyItem[]
}

export interface RelayTokenView {
  configured: boolean
  name?: string
  token?: string
  masked_token?: string
  created_at?: string
  last_used_at?: string | null
}

export interface RelayTokenResponse {
  token: string
  masked_token: string
  created_at: string
}

export interface Context7StatsSummary {
  total_requests: number
  success_requests: number
  failed_requests: number
  success_rate: number
  average_latency_ms: number
  last_request_at: string | null
  last_status_code: number
  last_error: string | null
  network_errors: number
  status_2xx: number
  status_4xx: number
  status_5xx: number
  total_latency_ms: number
  max_latency_ms: number
}

export interface Context7MinuteStat {
  api_key_id: number
  api_key_name: string
  minute_at: string
  total_requests: number
  success_requests: number
  failed_requests: number
  status_2xx: number
  status_4xx: number
  status_5xx: number
  network_errors: number
  total_latency_ms: number
  max_latency_ms: number
  last_status_code: number
  last_error: string | null
  updated_at: string
  success_rate: number
  average_latency_ms: number
}

export interface Context7MinuteStatsResponse {
  items: Context7MinuteStat[]
}

export interface Context7RequestLog {
  id: number
  api_key_id: number
  api_key_name: string
  method: string
  path: string
  query: string
  status_code: number
  success: boolean
  latency_ms: number
  error: string | null
  client_ip: string | null
  user_agent: string | null
  client_source: string | null
  client_ide: string | null
  client_version: string | null
  transport: string | null
  started_at: string
  finished_at: string
}

export interface Context7RequestLogPage {
  items: Context7RequestLog[]
  total: number
  page: number
  page_size: number
}
