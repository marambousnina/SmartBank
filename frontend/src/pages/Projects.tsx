import { useState, useCallback, useMemo } from 'react'
import {
  TrendingUp, Calendar, Shield, DollarSign, AlertCircle,
  Loader2, Search, Filter, Clock, CheckCircle, GitCommit, Target,
  ChevronDown, Rocket, Activity, RefreshCw, Zap,
} from 'lucide-react'
import KpiCard from '../components/KpiCard'
import HBar from '../components/charts/HBar'
import VBar from '../components/charts/VBar'
import MultiLine from '../components/charts/MultiLine'
import PeriodSelector, { Period } from '../components/PeriodSelector'
import { useFetch } from '../hooks/useFetch'
import {
  getProjects, getProjectKpis, getProjectBugs, getProjectBudget,
  getProjectTrend, getProjectDora, updateProjectStatus,
} from '../api/client'

interface Project {
  project_code: string; project_name: string; domain: string
  status: string
  total_tickets: number; done_tickets: number; progress_pct: number
  risk_score: number; nb_bugs: number; nb_delayed: number
}

interface DoraData {
  data_source: string; available: boolean
  lead_time_h: number | null; lead_time_days: number | null
  deploy_freq_per_week: number | null
  cfr_pct: number | null
  mttr_hours: number | null; mttr_days: number | null
  nb_deployed: number; delay_rate_pct: number | null
}

// ── Status helpers ────────────────────────────────────────────────────────────
const STATUS_OPTIONS = ['Actif', 'Terminé', 'En pause', 'En attente'] as const

const STATUS_COLORS: Record<string, { bg: string; text: string; border: string; dot: string }> = {
  'Terminé':    { bg: 'bg-emerald-500/10', text: 'text-emerald-400', border: 'border-emerald-500/20', dot: '●' },
  'Actif':      { bg: 'bg-indigo-500/10',  text: 'text-indigo-400',  border: 'border-indigo-500/20',  dot: '●' },
  'En pause':   { bg: 'bg-amber-500/10',   text: 'text-amber-400',   border: 'border-amber-500/20',   dot: '●' },
  'En attente': { bg: 'bg-slate-500/10',   text: 'text-slate-400',   border: 'border-slate-500/20',   dot: '○' },
}

function StatusBadge({ status }: { status: string }) {
  const c = STATUS_COLORS[status] ?? STATUS_COLORS['En pause']
  return <span className={`badge ${c.bg} ${c.text} border ${c.border}`}>{c.dot} {status}</span>
}

function RiskBadge({ score }: { score: number }) {
  const cls = score >= 70 ? 'bg-rose-500/20 text-rose-300'
    : score >= 40 ? 'bg-amber-500/20 text-amber-300'
    : 'bg-emerald-500/20 text-emerald-300'
  return <span className={`badge ${cls} font-bold`}>{score}</span>
}

function StatusDropdown({ current, onChange }: { current: string; onChange: (s: string) => void }) {
  const [open, setOpen] = useState(false)
  const c = STATUS_COLORS[current] ?? STATUS_COLORS['En pause']
  return (
    <div className="relative inline-block">
      <button
        onClick={() => setOpen(o => !o)}
        className={`flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-full border ${c.bg} ${c.text} ${c.border} hover:opacity-80 transition-opacity`}
      >
        {c.dot} {current} <ChevronDown size={12} />
      </button>
      {open && (
        <div className="absolute z-50 top-full left-0 mt-1 w-36 rounded-lg border border-slate-700 bg-slate-800 shadow-xl py-1"
          onMouseLeave={() => setOpen(false)}>
          {STATUS_OPTIONS.map(opt => {
            const sc = STATUS_COLORS[opt]
            return (
              <button key={opt} onClick={() => { onChange(opt); setOpen(false) }}
                className={`w-full text-left px-3 py-1.5 text-xs ${sc.text} hover:bg-slate-700/60 transition-colors`}>
                {sc.dot} {opt}
              </button>
            )
          })}
        </div>
      )}
    </div>
  )
}

// ── DORA metric card ──────────────────────────────────────────────────────────
function DoraCard({
  icon, label, value, unit, sublabel, available, color,
}: {
  icon: React.ReactNode; label: string
  value: string | number | null; unit?: string
  sublabel?: string; available: boolean
  color: 'indigo' | 'emerald' | 'amber' | 'rose' | 'cyan' | 'violet'
}) {
  const colorMap = {
    indigo:  { bg: 'bg-indigo-500/10',  icon: 'text-indigo-400',  border: 'border-indigo-500/20',  badge: 'bg-indigo-500/20 text-indigo-300' },
    emerald: { bg: 'bg-emerald-500/10', icon: 'text-emerald-400', border: 'border-emerald-500/20', badge: 'bg-emerald-500/20 text-emerald-300' },
    amber:   { bg: 'bg-amber-500/10',   icon: 'text-amber-400',   border: 'border-amber-500/20',   badge: 'bg-amber-500/20 text-amber-300' },
    rose:    { bg: 'bg-rose-500/10',    icon: 'text-rose-400',    border: 'border-rose-500/20',    badge: 'bg-rose-500/20 text-rose-300' },
    cyan:    { bg: 'bg-cyan-500/10',    icon: 'text-cyan-400',    border: 'border-cyan-500/20',    badge: 'bg-cyan-500/20 text-cyan-300' },
    violet:  { bg: 'bg-violet-500/10',  icon: 'text-violet-400',  border: 'border-violet-500/20',  badge: 'bg-violet-500/20 text-violet-300' },
  }
  const c = colorMap[color]
  return (
    <div className={`card border ${c.border} flex flex-col gap-2`}>
      <div className="flex items-center justify-between">
        <div className={`w-8 h-8 rounded-lg ${c.bg} flex items-center justify-center`}>
          <span className={`text-base ${c.icon}`}>{icon}</span>
        </div>
        {!available && (
          <span className="text-[10px] bg-slate-700/60 text-slate-400 px-1.5 py-0.5 rounded">
            Pas de Git
          </span>
        )}
      </div>
      <p className="text-xs text-slate-400">{label}</p>
      {value !== null && available ? (
        <div className="flex items-baseline gap-1">
          <span className="text-2xl font-bold text-white">{value}</span>
          {unit && <span className="text-sm text-slate-400">{unit}</span>}
        </div>
      ) : (
        <span className="text-xl font-bold text-slate-600">N/A</span>
      )}
      {sublabel && available && value !== null && (
        <p className="text-[11px] text-slate-500">{sublabel}</p>
      )}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
export default function Projects() {
  const { data: rawProjects, loading: projLoading } = useFetch<Project[]>(getProjects)

  const [selected, setSelected]             = useState<string | null>(null)
  const [period, setPeriod]                 = useState<Period | null>(null)
  const [search, setSearch]                 = useState('')
  const [domainFilter, setDomainFilter]     = useState('Tous')
  const [statusFilter, setStatusFilter]     = useState('Tous')
  const [statusUpdating, setStatusUpdating] = useState(false)
  const [localStatuses, setLocalStatuses]   = useState<Record<string, string>>({})

  const onPeriodChange = useCallback((p: Period) => setPeriod(p), [])
  const df = period?.dateFrom
  const dt = period?.dateTo

  const kpis   = useFetch(() => selected ? getProjectKpis(selected, df, dt)  : Promise.resolve(null), [selected, df, dt])
  const dora   = useFetch(() => selected ? getProjectDora(selected)           : Promise.resolve(null), [selected])
  const bugs   = useFetch(() => selected ? getProjectBugs(selected)           : Promise.resolve([]),   [selected])
  const budget = useFetch(() => selected ? getProjectBudget(selected)         : Promise.resolve([]),   [selected])
  const trend  = useFetch(() => selected ? getProjectTrend(selected, df, dt)  : Promise.resolve([]),   [selected, df, dt])

  const k = kpis.data as Record<string, unknown> | null
  const d = dora.data as DoraData | null

  const bugsData   = (bugs.data   ?? []).map((r: Record<string, unknown>) => ({ name: String(r.severity ?? ''), value: Number(r.count ?? 0) }))
  const budgetData = (budget.data ?? []).map((r: Record<string, unknown>) => ({ category: String(r.category ?? ''), value: Number(r.value ?? 0) }))
  const trendData  = (trend.data  ?? []).map((r: Record<string, unknown>) => ({
    month: String(r.month ?? ''), created: Number(r.created ?? 0),
    resolved: Number(r.resolved ?? 0), on_time_pct: Number(r.on_time_pct ?? 0),
  }))

  const domains = useMemo(() => {
    const set = new Set((rawProjects ?? []).map(p => p.domain))
    return ['Tous', ...Array.from(set).sort()]
  }, [rawProjects])

  const statuses = useMemo(() => {
    const set = new Set((rawProjects ?? []).map(p => p.status ?? 'Actif'))
    return ['Tous', ...Array.from(set).sort()]
  }, [rawProjects])

  const projects = useMemo(() => {
    return (rawProjects ?? []).filter(p => {
      const matchSearch = !search
        || p.project_name.toLowerCase().includes(search.toLowerCase())
        || p.project_code.toLowerCase().includes(search.toLowerCase())
      const matchDomain = domainFilter === 'Tous' || p.domain === domainFilter
      const matchStatus = statusFilter === 'Tous' || (p.status ?? 'Actif') === statusFilter
      return matchSearch && matchDomain && matchStatus
    })
  }, [rawProjects, search, domainFilter, statusFilter])

  const getStatus = (p: Project) => localStatuses[p.project_code] ?? p.status ?? 'Actif'

  const selectedProject = projects.find(p => p.project_code === selected)
    ?? (rawProjects ?? []).find(p => p.project_code === selected)

  async function handleStatusChange(code: string, newStatus: string) {
    setStatusUpdating(true)
    try {
      await updateProjectStatus(code, newStatus)
      setLocalStatuses(prev => ({ ...prev, [code]: newStatus }))
    } catch (e) { console.error(e) }
    finally { setStatusUpdating(false) }
  }

  const doraAvailable = d?.available ?? false

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-white">Portefeuille Projets</h1>
        <p className="text-slate-400 text-sm mt-0.5">Vue d'ensemble et analyse détaillée</p>
      </div>

      {/* Period Selector */}
      <div className="card py-3 px-4">
        <PeriodSelector onChange={onPeriodChange} />
      </div>

      {/* ── Filtres ─────────────────────────────────────────────────────── */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="relative flex-1 min-w-48">
          <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
          <input type="text" placeholder="Rechercher un projet…" value={search}
            onChange={e => setSearch(e.target.value)}
            className="w-full bg-slate-800 border border-slate-700 rounded-lg pl-8 pr-3 py-2 text-sm text-white placeholder:text-slate-500 focus:outline-none focus:border-indigo-500 transition-colors" />
        </div>
        <div className="flex items-center gap-2">
          <Filter size={14} className="text-slate-400" />
          <select value={domainFilter} onChange={e => setDomainFilter(e.target.value)}
            className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-indigo-500 transition-colors">
            {domains.map(d => <option key={d} value={d}>{d}</option>)}
          </select>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-slate-400 text-sm">Statut</span>
          <select value={statusFilter} onChange={e => setStatusFilter(e.target.value)}
            className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-indigo-500 transition-colors">
            {statuses.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>
        <span className="text-xs text-slate-500 ml-auto">
          {projects.length} projet{projects.length !== 1 ? 's' : ''}
        </span>
      </div>

      {/* ── Tableau projets (sans colonne Progression) ───────────────── */}
      <div className="card p-0 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-700">
                <th className="text-left text-xs text-slate-400 uppercase tracking-wide px-5 py-3">Projet</th>
                <th className="text-left text-xs text-slate-400 uppercase tracking-wide px-3 py-3">Statut</th>
                <th className="text-left text-xs text-slate-400 uppercase tracking-wide px-3 py-3">Risque</th>
                <th className="text-left text-xs text-slate-400 uppercase tracking-wide px-3 py-3">Bugs</th>
                <th className="text-left text-xs text-slate-400 uppercase tracking-wide px-3 py-3">En retard</th>
                <th className="text-left text-xs text-slate-400 uppercase tracking-wide px-3 py-3">Tickets</th>
              </tr>
            </thead>
            <tbody>
              {projLoading && (
                <tr><td colSpan={6} className="text-center py-8 text-slate-500">
                  <Loader2 className="animate-spin inline mr-2" size={16} />Chargement…
                </td></tr>
              )}
              {!projLoading && projects.length === 0 && (
                <tr><td colSpan={6} className="text-center py-8 text-slate-500">Aucun projet trouvé</td></tr>
              )}
              {projects.map(p => (
                <tr key={p.project_code}
                  onClick={() => setSelected(p.project_code === selected ? null : p.project_code)}
                  className={`border-b border-slate-700/40 cursor-pointer transition-colors ${
                    selected === p.project_code
                      ? 'bg-indigo-500/10 border-l-2 border-l-indigo-500'
                      : 'hover:bg-slate-700/30'
                  }`}>
                  <td className="px-5 py-3">
                    <p className="font-semibold text-white">{p.project_name}</p>
                    <p className="text-[11px] text-slate-500">{p.project_code} · {p.domain}</p>
                  </td>
                  <td className="px-3 py-3"><StatusBadge status={getStatus(p)} /></td>
                  <td className="px-3 py-3"><RiskBadge score={Number(p.risk_score ?? 0)} /></td>
                  <td className="px-3 py-3 text-slate-300">{p.nb_bugs}</td>
                  <td className="px-3 py-3">
                    <span className={p.nb_delayed > 0 ? 'text-amber-400 font-medium' : 'text-slate-400'}>
                      {p.nb_delayed}
                    </span>
                  </td>
                  <td className="px-3 py-3 text-slate-400">{p.total_tickets}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* ── Détail projet ────────────────────────────────────────────────── */}
      {selected && (
        <div className="space-y-6 animate-fadeIn">

          {/* En-tête détail */}
          <div className="flex items-center gap-3 flex-wrap">
            <span className="text-sm text-slate-400">Détail</span>
            <span className="text-white font-semibold text-lg">{String(k?.project_name ?? selected)}</span>
            <div className="ml-auto flex items-center gap-2">
              {statusUpdating && <Loader2 size={14} className="animate-spin text-slate-400" />}
              {selectedProject && (
                <StatusDropdown current={getStatus(selectedProject)}
                  onChange={s => handleStatusChange(selected, s)} />
              )}
            </div>
          </div>

          {/* ── Section 1 : KPIs projet ── */}
          <div>
            <p className="text-xs text-slate-500 uppercase tracking-widest mb-3 font-medium">
              Indicateurs projet
            </p>
            {kpis.loading ? (
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                {Array(4).fill(0).map((_, i) => <KpiCard key={i} icon="" label="…" value="—" loading />)}
              </div>
            ) : (
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                <KpiCard icon={<Shield size={18} />}      label="Score Risque"     value={Number(k?.risk_score ?? 0).toFixed(0)}             color={Number(k?.risk_score) >= 70 ? 'rose' : 'amber'} />
                <KpiCard icon={<Clock size={18} />}       label="Lead Time moyen"  value={Number(k?.avg_lead_days ?? 0).toFixed(1)} unit="j"  color="violet" />
                <KpiCard icon={<CheckCircle size={18} />} label="Livraison à temps" value={Number(k?.on_time_rate ?? 0).toFixed(1)}  unit="%" color="emerald" />
                <KpiCard icon={<AlertCircle size={18} />} label="Jobs en échec"    value={Number(k?.critical_incidents ?? 0)}                color="rose" />
                <KpiCard icon={<Target size={18} />}      label="Tickets total"    value={Number(k?.total_tickets ?? 0)}                     color="cyan" />
                <KpiCard icon={<CheckCircle size={18} />} label="Tickets terminés" value={Number(k?.done_tickets ?? 0)}                      color="emerald" />
                <KpiCard icon={<GitCommit size={18} />}   label="Commits"          value={Number(k?.nb_commits ?? 0)}                        color="indigo" />
                <KpiCard icon={<DollarSign size={18} />}  label="Variance budget"  value={Number(k?.budget_variance_pct ?? 0).toFixed(1)} unit="%" color="amber" />
              </div>
            )}
          </div>

          {/* ── Section 2 : DORA Metrics ── */}
          <div>
            <div className="flex items-center gap-3 mb-3">
              <p className="text-xs text-slate-500 uppercase tracking-widest font-medium">
                Métriques DORA
              </p>
              {d && (
                <span className={`text-[10px] px-2 py-0.5 rounded-full font-medium ${
                  doraAvailable
                    ? 'bg-emerald-500/20 text-emerald-300'
                    : 'bg-slate-700 text-slate-400'
                }`}>
                  {doraAvailable ? 'Git + Jira' : 'Jira uniquement — Deploy Freq / CFR / MTTR non disponibles'}
                </span>
              )}
            </div>

            {dora.loading ? (
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                {Array(4).fill(0).map((_, i) => <KpiCard key={i} icon="" label="…" value="—" loading />)}
              </div>
            ) : (
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                {/* Lead Time — toujours disponible */}
                <DoraCard
                  icon={<Clock size={16} />}
                  label="Lead Time moyen"
                  value={d?.lead_time_days != null ? d.lead_time_days : null}
                  unit="j"
                  sublabel={d?.lead_time_h != null ? `${d.lead_time_h} heures` : undefined}
                  available={true}
                  color="cyan"
                />
                {/* Deploy Frequency */}
                <DoraCard
                  icon={<Rocket size={16} />}
                  label="Deploy Frequency"
                  value={d?.deploy_freq_per_week != null ? d.deploy_freq_per_week : null}
                  unit="/sem"
                  sublabel={d?.nb_deployed != null ? `${d.nb_deployed} déploiements` : undefined}
                  available={doraAvailable}
                  color="indigo"
                />
                {/* CFR */}
                <DoraCard
                  icon={<Activity size={16} />}
                  label="Change Failure Rate"
                  value={d?.cfr_pct != null ? d.cfr_pct : null}
                  unit="%"
                  sublabel={d?.cfr_pct != null
                    ? d.cfr_pct < 15 ? 'Elite (< 15%)' : d.cfr_pct < 30 ? 'High' : 'A améliorer'
                    : undefined}
                  available={doraAvailable}
                  color={d?.cfr_pct != null && d.cfr_pct >= 15 ? 'rose' : 'emerald'}
                />
                {/* MTTR */}
                <DoraCard
                  icon={<RefreshCw size={16} />}
                  label="MTTR"
                  value={d?.mttr_days != null ? d.mttr_days : null}
                  unit="j"
                  sublabel={d?.mttr_hours != null ? `${d.mttr_hours} heures` : undefined}
                  available={doraAvailable}
                  color={d?.mttr_hours != null && d.mttr_hours > 48 ? 'rose' : 'amber'}
                />
              </div>
            )}

            {/* Taux de retard */}
            {d && d.delay_rate_pct != null && (
              <div className="mt-3 flex items-center gap-2 text-sm">
                <Zap size={14} className="text-amber-400" />
                <span className="text-slate-400">Taux de retard :</span>
                <span className={`font-semibold ${d.delay_rate_pct > 50 ? 'text-rose-400' : d.delay_rate_pct > 20 ? 'text-amber-400' : 'text-emerald-400'}`}>
                  {d.delay_rate_pct}%
                </span>
                <span className="text-slate-500 text-xs">des tickets livrés en retard</span>
              </div>
            )}
          </div>

          {/* ── Section 3 : Graphiques ── */}
          <div>
            <p className="text-xs text-slate-500 uppercase tracking-widest mb-3 font-medium">Graphiques</p>
            <div className="chart-grid-2">
              <VBar
                title="Budget Planifié vs Réel (person-jours)"
                data={budgetData}
                xKey="category"
                bars={[{ key: 'value', label: 'Jours', color: '#6366f1' }]}
              />
              <HBar
                title="Jobs en échec (CI/CD)"
                data={bugsData}
                xKey="value"
                yKey="name"
                colorByIndex
              />
            </div>
          </div>

          {/* ── Section 4 : Évolution mensuelle ── */}
          <div>
            <p className="text-xs text-slate-500 uppercase tracking-widest mb-3 font-medium">Évolution mensuelle</p>
            {trend.loading ? (
              <div className="card animate-pulse h-64" />
            ) : (
              <MultiLine
                title="Tickets créés / résolus & Taux de livraison"
                data={trendData}
                xKey="month"
                series={[
                  { key: 'created',     label: 'Créés',               color: '#6366f1' },
                  { key: 'resolved',    label: 'Résolus',             color: '#10b981' },
                  { key: 'on_time_pct', label: 'Livraison à temps (%)', color: '#f59e0b' },
                ]}
              />
            )}
          </div>

        </div>
      )}
    </div>
  )
}
