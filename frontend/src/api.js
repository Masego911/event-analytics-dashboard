export class ApiError extends Error {
  constructor(message, status) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      Accept: 'application/json',
      ...options.headers,
    },
    ...options,
  })

  const contentType = response.headers.get('content-type') || ''
  const payload = contentType.includes('application/json')
    ? await response.json()
    : await response.text()

  if (!response.ok) {
    const message = typeof payload === 'object' && payload?.error
      ? payload.error
      : `Request failed with status ${response.status}`
    throw new ApiError(message, response.status)
  }
  return payload
}

export const api = {
  dashboard: (signal) => request('/api/dashboard', { signal }),
  show: (id, signal) => request(`/api/show?i=${encodeURIComponent(id)}`, { signal }),
  category: (category, signal) => request(`/api/category?cat=${encodeURIComponent(category)}`, { signal }),
  insights: (signal) => request('/api/insights', { signal }),
  report: (signal) => request('/api/report', { signal }),
  settings: (signal) => request('/api/settings', { signal }),
  saveSettings: (kind, values) => request(`/api/settings?kind=${encodeURIComponent(kind)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' },
    body: values instanceof URLSearchParams ? values.toString() : new URLSearchParams(values).toString(),
  }),
  upload: (file) => {
    const body = new FormData()
    body.append('csvFile', file)
    return request('/api/upload', { method: 'POST', body })
  },
}
