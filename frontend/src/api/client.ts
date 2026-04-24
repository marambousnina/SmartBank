import axios from 'axios'

const api = axios.create({ baseURL: '/api', timeout: 15000 })

function dateParams(dateFrom?: string, dateTo?: string) {
  if (dateFrom && dateTo) return { params: { date_from: dateFrom, date_to: dateTo } }
  return {}
}

// ── Date range ───────────────────────────────────────────────────────────
export const getDateRange         = () => api.get('/date-range').then(r => r.data)

// ── Dashboard ────────────────────────────────────────────────────────────
export const getDashboardKpis    = (df?: string, dt?: string) => api.get('/dashboard/kpis', dateParams(df, dt)).then(r => r.data)
export const getProjectStatus    = () => api.get('/dashboard/charts/project-status').then(r => r.data)
export const getTicketsTrend     = (df?: string, dt?: string) => api.get('/dashboard/charts/tickets-trend', dateParams(df, dt)).then(r => r.data)
export const getDoraMetrics      = (df?: string, dt?: string) => api.get('/dashboard/charts/dora-metrics', dateParams(df, dt)).then(r => r.data)
export const getOnTimeGauge      = (df?: string, dt?: string) => api.get('/dashboard/charts/on-time-gauge', dateParams(df, dt)).then(r => r.data)
export const getBugsSeverity     = () => api.get('/dashboard/charts/bugs-severity').then(r => r.data)
export const getDashboardAlerts  = () => api.get('/dashboard/alerts').then(r => r.data)

// ── Teams ────────────────────────────────────────────────────────────────
export const getTeams            = () => api.get('/teams').then(r => r.data)
export const getTeamKpis         = (team: string, df?: string, dt?: string) => api.get(`/teams/${encodeURIComponent(team)}/kpis`, dateParams(df, dt)).then(r => r.data)
export const getTeamRadar        = (team: string, df?: string, dt?: string) => api.get(`/teams/${encodeURIComponent(team)}/charts/radar`, dateParams(df, dt)).then(r => r.data)
export const getTeamMemberLoad   = (team: string, df?: string, dt?: string) => api.get(`/teams/${encodeURIComponent(team)}/charts/member-load`, dateParams(df, dt)).then(r => r.data)
export const getTeamVelocity     = (team: string, df?: string, dt?: string) => api.get(`/teams/${encodeURIComponent(team)}/charts/velocity`, dateParams(df, dt)).then(r => r.data)
export const getTeamBurndown     = (team: string) => api.get(`/teams/${encodeURIComponent(team)}/charts/burndown`).then(r => r.data)

// ── Projects ─────────────────────────────────────────────────────────────
export const getProjects         = () => api.get('/projects').then(r => r.data)
export const getProjectKpis      = (code: string, df?: string, dt?: string) => api.get(`/projects/${code}/kpis`, dateParams(df, dt)).then(r => r.data)
export const getProjectBugs      = (code: string) => api.get(`/projects/${code}/charts/bugs`).then(r => r.data)
export const getProjectBudget    = (code: string) => api.get(`/projects/${code}/charts/budget`).then(r => r.data)

// ── Personnel ────────────────────────────────────────────────────────────
export const getPersonnel        = () => api.get('/personnel').then(r => r.data)
export const getPersonKpis       = (id: number, df?: string, dt?: string) => api.get(`/personnel/${id}/kpis`, dateParams(df, dt)).then(r => r.data)
export const getPersonTasks      = (id: number) => api.get(`/personnel/${id}/charts/tasks`).then(r => r.data)
<<<<<<< Updated upstream
export const getPersonTrend      = (id: number, df?: string, dt?: string) => api.get(`/personnel/${id}/charts/trend`, dateParams(df, dt)).then(r => r.data)
export const getPersonComparison = (id: number, df?: string, dt?: string) => api.get(`/personnel/${id}/charts/comparison`, dateParams(df, dt)).then(r => r.data)
=======
export const getPersonTrend      = (id: number) => api.get(`/personnel/${id}/charts/trend`).then(r => r.data)
export const getPersonComparison = (id: number) => api.get(`/personnel/${id}/charts/comparison`).then(r => r.data)

// ── Predictions ML ───────────────────────────────────────────────────────
export const getPredictAllProjects  = () => api.get('/predict/projects').then(r => r.data)
export const getPredictProject      = (key: string) => api.get(`/predict/projects/${key}`).then(r => r.data)
export const predictTicket          = (payload: Record<string, unknown>) =>
  api.post('/predict/ticket', payload).then(r => r.data)
>>>>>>> Stashed changes
