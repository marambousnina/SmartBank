import { useState, useCallback } from 'react'
import { TrendingUp, Calendar, Shield, DollarSign, AlertCircle, Loader2 } from 'lucide-react'
import KpiCard from '../components/KpiCard'
import HBar from '../components/charts/HBar'
import VBar from '../components/charts/VBar'
import PeriodSelector, { Period } from '../components/PeriodSelector'
import { useFetch } from '../hooks/useFetch'
import { getProjects, getProjectKpis, getProjectBugs, getProjectBudget } from '../api/client'

interface Project {
  project_code: string; project_name: string; domain: string
  total_tickets: number; done_tickets: number; progress_pct: number
  risk_score: number; nb_bugs: number; nb_delayed: number
}

function StatusBadge({ pct }: { pct: number }) {
  if (pct >= 75) return <span className="badge bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">● Terminé</span>
  if (pct >= 30) return <span className="badge bg-indigo-500/10 text-indigo-400 border border-indigo-500/20">● Actif</span>
  return <span className="badge bg-amber-500/10 text-amber-400 border border-amber-500/20">● En pause</span>
}

function RiskBadge({ score }: { score: number }) {
  const cls = score >= 70 ? 'bg-rose-500/20 text-rose-300' : score >= 40 ? 'bg-amber-500/20 text-amber-300' : 'bg-emerald-500/20 text-emerald-300'
  return <span className={`badge ${cls} font-bold`}>{score}</span>
}

function ProgressBar({ pct }: { pct: number }) {
  const color = pct >= 75 ? 'bg-emerald-500' : pct >= 40 ? 'bg-indigo-500' : 'bg-amber-500'
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-1.5 bg-slate-700 rounded-full overflow-hidden">
        <div className={`h-full ${color} rounded-full transition-all`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs text-slate-400 w-8 text-right">{pct}%</span>
    </div>
  )
}

export default function Projects() {
  const { data: projects, loading: projLoading } = useFetch<Project[]>(getProjects)
  const [selected, setSelected] = useState<string | null>(null)
  const [period, setPeriod] = useState<Period | null>(null)
  const onPeriodChange = useCallback((p: Period) => setPeriod(p), [])

  const df = period?.dateFrom
  const dt = period?.dateTo

  const kpis   = useFetch(() => selected ? getProjectKpis(selected, df, dt) : Promise.resolve(null), [selected, df, dt])
  const bugs   = useFetch(() => selected ? getProjectBugs(selected)          : Promise.resolve([]),   [selected])
  const budget = useFetch(() => selected ? getProjectBudget(selected)        : Promise.resolve([]),   [selected])

  const k = kpis.data as Record<string, unknown> | null

  const bugsData = (bugs.data ?? []).map((r: Record<string,unknown>) => ({
    name: String(r.severity ?? ''), value: Number(r.count ?? 0)
  }))

  const budgetData = (budget.data ?? []).map((r: Record<string,unknown>) => ({
    category: String(r.category ?? ''),
    value: Number(r.value ?? 0),
  }))

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

      {/* Portfolio Table */}
      <div className="card p-0 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-700">
                <th className="text-left text-xs text-slate-400 uppercase tracking-wide px-5 py-3">Projet</th>
                <th className="text-left text-xs text-slate-400 uppercase tracking-wide px-3 py-3">Statut</th>
                <th className="text-left text-xs text-slate-400 uppercase tracking-wide px-3 py-3 w-40">Progression</th>
                <th className="text-left text-xs text-slate-400 uppercase tracking-wide px-3 py-3">Risque</th>
                <th className="text-left text-xs text-slate-400 uppercase tracking-wide px-3 py-3">Bugs</th>
                <th className="text-left text-xs text-slate-400 uppercase tracking-wide px-3 py-3">En retard</th>
              </tr>
            </thead>
            <tbody>
              {projLoading && (
                <tr><td colSpan={6} className="text-center py-8 text-slate-500"><Loader2 className="animate-spin inline mr-2" size={16} />Chargement…</td></tr>
              )}
              {(projects ?? []).map(p => (
                <tr
                  key={p.project_code}
                  onClick={() => setSelected(p.project_code === selected ? null : p.project_code)}
                  className={`border-b border-slate-700/40 cursor-pointer transition-colors ${
                    selected === p.project_code
                      ? 'bg-indigo-500/10 border-l-2 border-l-indigo-500'
                      : 'hover:bg-slate-700/30'
                  }`}
                >
                  <td className="px-5 py-3">
                    <p className="font-semibold text-white">{p.project_name}</p>
                    <p className="text-[11px] text-slate-500">{p.project_code} · {p.domain}</p>
                  </td>
                  <td className="px-3 py-3"><StatusBadge pct={p.progress_pct} /></td>
                  <td className="px-3 py-3 w-40"><ProgressBar pct={Number(p.progress_pct ?? 0)} /></td>
                  <td className="px-3 py-3"><RiskBadge score={Number(p.risk_score ?? 0)} /></td>
                  <td className="px-3 py-3 text-slate-300">{p.nb_bugs}</td>
                  <td className="px-3 py-3">
                    <span className={p.nb_delayed > 0 ? 'text-amber-400 font-medium' : 'text-slate-400'}>
                      {p.nb_delayed}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Project Detail */}
      {selected && (
        <div className="space-y-4 animate-fadeIn">
          <div className="flex items-center gap-2 text-sm text-slate-400">
            <span>Détail →</span>
            <span className="text-white font-semibold">{String(k?.project_name ?? selected)}</span>
          </div>

          {/* 5 KPI Cards */}
          {kpis.loading ? (
            <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
              {Array(5).fill(0).map((_, i) => <KpiCard key={i} icon="" label="…" value="—" loading />)}
            </div>
          ) : (
            <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
              <KpiCard icon={<TrendingUp size={18} />}  label="Progression"     value={Number(k?.progress_pct ?? 0).toFixed(0)} unit="%"  color="indigo"  />
              <KpiCard icon={<Calendar size={18} />}    label="Deadline"        value={String(k?.deadline ?? '—')}              color="cyan"    />
              <KpiCard icon={<Shield size={18} />}      label="Risque"          value={Number(k?.risk_score ?? 0).toFixed(0)}              color={Number(k?.risk_score) >= 70 ? 'rose' : 'amber'} />
              <KpiCard icon={<DollarSign size={18} />}  label="Variance budget" value={Number(k?.budget_variance_pct ?? 0).toFixed(1)} unit="%" color="emerald" />
              <KpiCard icon={<AlertCircle size={18} />} label="Jobs en échec"   value={Number(k?.critical_incidents ?? 0)}              color="rose"    />
            </div>
          )}

          {/* 2 Charts */}
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
      )}
    </div>
  )
}
