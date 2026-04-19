import axios from 'axios'

const api = axios.create({ baseURL: '/api', timeout: 15000 })

// ── Dashboard ────────────────────────────────────────────────────────────
export const getDashboardKpis    = () => api.get('/dashboard/kpis').then(r => r.data)
export const getProjectStatus    = () => api.get('/dashboard/charts/project-status').then(r => r.data)
export const getTicketsTrend     = () => api.get('/dashboard/charts/tickets-trend').then(r => r.data)
export const getDoraMetrics      = () => api.get('/dashboard/charts/dora-metrics').then(r => r.data)
export const getOnTimeGauge      = () => api.get('/dashboard/charts/on-time-gauge').then(r => r.data)
export const getBugsSeverity     = () => api.get('/dashboard/charts/bugs-severity').then(r => r.data)
export const getDashboardAlerts  = () => api.get('/dashboard/alerts').then(r => r.data)

// ── Teams ────────────────────────────────────────────────────────────────
export const getTeams            = () => api.get('/teams').then(r => r.data)
export const getTeamKpis         = (team: string) => api.get(`/teams/${encodeURIComponent(team)}/kpis`).then(r => r.data)
export const getTeamRadar        = (team: string) => api.get(`/teams/${encodeURIComponent(team)}/charts/radar`).then(r => r.data)
export const getTeamMemberLoad   = (team: string) => api.get(`/teams/${encodeURIComponent(team)}/charts/member-load`).then(r => r.data)
export const getTeamVelocity     = (team: string) => api.get(`/teams/${encodeURIComponent(team)}/charts/velocity`).then(r => r.data)
export const getTeamBurndown     = (team: string) => api.get(`/teams/${encodeURIComponent(team)}/charts/burndown`).then(r => r.data)

// ── Projects ─────────────────────────────────────────────────────────────
export const getProjects         = () => api.get('/projects').then(r => r.data)
export const getProjectKpis      = (code: string) => api.get(`/projects/${code}/kpis`).then(r => r.data)
export const getProjectBugs      = (code: string) => api.get(`/projects/${code}/charts/bugs`).then(r => r.data)
export const getProjectBudget    = (code: string) => api.get(`/projects/${code}/charts/budget`).then(r => r.data)

// ── Personnel ────────────────────────────────────────────────────────────
export const getPersonnel        = () => api.get('/personnel').then(r => r.data)
export const getPersonKpis       = (id: number) => api.get(`/personnel/${id}/kpis`).then(r => r.data)
export const getPersonTasks      = (id: number) => api.get(`/personnel/${id}/charts/tasks`).then(r => r.data)
export const getPersonTrend      = (id: number) => api.get(`/personnel/${id}/charts/trend`).then(r => r.data)
export const getPersonComparison = (id: number) => api.get(`/personnel/${id}/charts/comparison`).then(r => r.data)
