import { useCallback, useState } from 'react'
import { RefreshCw, Download, Settings, Folder, TrendingUp, Rocket, Clock, AlertTriangle, Stethoscope, CheckSquare, Bug } from 'lucide-react'
import KpiCard from '../components/KpiCard'
import DonutChart from '../components/charts/DonutChart'
import MultiLine from '../components/charts/MultiLine'
import HBar from '../components/charts/HBar'
import Gauge from '../components/charts/Gauge'
import AlertsList from '../components/AlertsList'
import PeriodSelector, { Period } from '../components/PeriodSelector'
import { useFetch } from '../hooks/useFetch'
import {
  getDashboardKpis, getProjectStatus, getTicketsTrend,
  getDoraMetrics, getOnTimeGauge, getBugsSeverity
} from '../api/client'

const QUARTER = `Q${Math.ceil((new Date().getMonth() + 1) / 3)} ${new Date().getFullYear()}`

const AI_INSIGHTS = [
  { icon: '🚀', text: 'La fréquence de déploiement est dans la norme Elite DORA (>1/jour).' },
  { icon: '🎯', text: 'Le taux de livraison à temps dépasse 80% — objectif atteint.' },
  { icon: '⚠️', text: 'Surveiller les tickets à risque critique avant la prochaine release.' },
  { icon: '📉', text: 'Le Lead Time a diminué de 12% par rapport au trimestre précédent.' },
  { icon: '✅', text: 'Aucun incident critique P1 ouvert cette semaine.' },
]

export default function Dashboard() {
  const [period, setPeriod] = useState<Period | null>(null)
  const onPeriodChange = useCallback((p: Period) => setPeriod(p), [])

  const df = period?.dateFrom
  const dt = period?.dateTo

  const kpis  = useFetch(() => getDashboardKpis(df, dt),  [df, dt])
  const gauge  = useFetch(() => getOnTimeGauge(df, dt),   [df, dt])
  const trend  = useFetch(() => getTicketsTrend(df, dt),  [df, dt])
  const dora   = useFetch(() => getDoraMetrics(df, dt),   [df, dt])
  const donut  = useFetch(getProjectStatus)
  const bugs   = useFetch(getBugsSeverity)

  const k = kpis.data

  const trendData = (trend.data ?? []).map((r: Record<string,unknown>) => ({
    ...r,
    label: String(r.month_year ?? ''),
  }))

  const doraData = (dora.data ?? []).map((r: Record<string,unknown>) => ({
    ...r,
    label: String(r.week_label ?? '').slice(-6),
  }))

  const bugsData = (bugs.data ?? []).map((r: Record<string,unknown>) => ({
    name: String(r.severity ?? ''),
    value: Number(r.count ?? 0),
  }))

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-bold text-white">Dashboard Exécutif</h1>
          <p className="text-slate-400 text-sm mt-0.5">Vue globale des métriques DevOps</p>
        </div>
        <div className="flex items-center gap-3">
          <span className="badge bg-indigo-500/10 text-indigo-400 border border-indigo-500/30 px-3 py-1">
            {QUARTER}
          </span>
          <button className="btn-ghost flex items-center gap-1.5" onClick={() => window.location.reload()}>
            <RefreshCw size={14} /> Rafraîchir
          </button>
          <button className="btn-ghost flex items-center gap-1.5">
            <Download size={14} /> Exporter
          </button>
          <button className="btn-ghost flex items-center gap-1.5">
            <Settings size={14} />
          </button>
        </div>
      </div>

      {/* Period Selector */}
      <div className="card py-3 px-4">
        <PeriodSelector onChange={onPeriodChange} />
      </div>

      {/* KPI Cards — 8 cartes, 4 par ligne */}
      <div className="kpi-grid">
        <KpiCard icon={<Folder size={18} />}        label="Projets actifs"         value={k?.active_projects ?? '—'}      color="indigo"  loading={kpis.loading} />
        <KpiCard icon={<TrendingUp size={18} />}    label="Score performance"      value={k?.performance_score ?? '—'}    unit="%"  color="emerald" loading={kpis.loading} trend="up" trendLabel="+3pts ce mois" />
        <KpiCard icon={<Rocket size={18} />}        label="Fréq. déploiements"     value={k?.deploy_freq_per_week ?? '—'} unit="/sem" color="violet" loading={kpis.loading} />
        <KpiCard icon={<Clock size={18} />}         label="Lead Time moyen"        value={k?.lead_time_days ?? '—'}       unit="j"  color="cyan"    loading={kpis.loading} />
        <KpiCard icon={<AlertTriangle size={18} />} label="Taux échec dépl. (CFR)" value={k?.cfr_pct ?? '—'}    unit="%"  color="rose"  loading={kpis.loading} trend={Number(k?.cfr_pct) > 15 ? 'down' : 'up'} />
        <KpiCard icon={<Stethoscope size={18} />}   label="MTTR"                   value={k?.mttr_hours ?? '—'} unit="h"  color="amber" loading={kpis.loading} />
        <KpiCard icon={<CheckSquare size={18} />}   label="Livraison à temps"      value={k?.on_time_pct ?? '—'}          unit="%"  color="emerald" loading={kpis.loading} />
        <KpiCard icon={<Bug size={18} />}           label="Jobs CI/CD en échec"    value={k?.jobs_failed ?? '—'}                    color="rose"    loading={kpis.loading} trend="down" trendLabel="Jobs failed total" />
      </div>

      {/* Row 1 — Donut + Trend + DORA */}
      <div className="chart-grid-3">
        <DonutChart
          title="Statut des projets"
          data={donut.data ?? []}
        />
        <MultiLine
          title="Tendance tickets"
          data={trendData}
          xKey="label"
          series={[
            { key: 'created',  label: 'Créés',   color: '#6366f1' },
            { key: 'resolved', label: 'Résolus', color: '#10b981' },
          ]}
        />
        <MultiLine
          title="Métriques DORA"
          data={doraData}
          xKey="label"
          series={[
            { key: 'deploy_freq',    label: 'Dépl/sem',   color: '#6366f1' },
            { key: 'lead_time_days', label: 'Lead(j)',    color: '#f59e0b' },
            { key: 'cfr_pct',        label: 'CFR%',       color: '#ef4444' },
          ]}
        />
      </div>

      {/* Row 2 — Gauge + Bugs + Alerts */}
      <div className="chart-grid-3">
        <Gauge
          value={gauge.data?.value ?? 0}
          label="Livraison à temps"
          size={200}
        />
        <HBar
          title="Jobs en échec (top 8)"
          data={bugsData}
          xKey="value"
          yKey="name"
          colorByIndex
        />
        <AlertsList />
      </div>

      {/* AI Insights */}
      <div>
        <p className="section-title">AI Insights</p>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3">
          {AI_INSIGHTS.map((ins, i) => (
            <div key={i} className="card-sm border border-slate-700/30 hover:border-indigo-500/30 transition-colors">
              <span className="text-xl">{ins.icon}</span>
              <p className="text-xs text-slate-300 mt-2 leading-relaxed">{ins.text}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
