import { useState, useEffect, useCallback } from 'react'
import { Users, Zap, Activity, Clock, AlertTriangle, Stethoscope, Bug, CheckSquare, ChevronDown, Loader2 } from 'lucide-react'
import KpiCard from '../components/KpiCard'
import RadarComp from '../components/charts/RadarComp'
import HBar from '../components/charts/HBar'
import VBar from '../components/charts/VBar'
import Burndown from '../components/charts/Burndown'
import PeriodSelector, { Period } from '../components/PeriodSelector'
import { useFetch } from '../hooks/useFetch'
import { getTeams, getTeamKpis, getTeamRadar, getTeamMemberLoad, getTeamVelocity, getTeamBurndown } from '../api/client'

export default function Teams() {
  const { data: teams, loading: teamsLoading } = useFetch(getTeams)
  const [selected, setSelected] = useState<string>('')
  const [period, setPeriod] = useState<Period | null>(null)
  const onPeriodChange = useCallback((p: Period) => setPeriod(p), [])

  useEffect(() => {
    if (teams && teams.length > 0 && !selected) setSelected(teams[0])
  }, [teams, selected])

  const df = period?.dateFrom
  const dt = period?.dateTo

  const kpis     = useFetch(() => selected ? getTeamKpis(selected, df, dt)        : Promise.resolve(null), [selected, df, dt])
  const radar    = useFetch(() => selected ? getTeamRadar(selected, df, dt)        : Promise.resolve([]),   [selected, df, dt])
  const members  = useFetch(() => selected ? getTeamMemberLoad(selected, df, dt)   : Promise.resolve([]),   [selected, df, dt])
  const velocity = useFetch(() => selected ? getTeamVelocity(selected, df, dt)     : Promise.resolve([]),   [selected, df, dt])
  const burndown = useFetch(() => selected ? getTeamBurndown(selected)             : Promise.resolve(null), [selected])

  const k = kpis.data

  const memberData = (members.data ?? []).map((r: Record<string,unknown>) => ({
    member:   String(r.member ?? ''),
    load_pct: Number(r.load_pct ?? 0),
  }))

  const velocityData = (velocity.data ?? []).map((r: Record<string,unknown>) => ({
    sprint_name: String(r.sprint_name ?? '').slice(-12),
    actual: Number(r.actual ?? 0),
    target: Number(r.target ?? 0),
  }))

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-bold text-white">Analyse des Équipes</h1>
          <p className="text-slate-400 text-sm mt-0.5">Performance et métriques par équipe</p>
        </div>

        {/* Team selector */}
        <div className="relative">
          <select
            value={selected}
            onChange={e => setSelected(e.target.value)}
            className="appearance-none bg-slate-800 border border-slate-600 text-slate-200 text-sm rounded-lg pl-4 pr-10 py-2.5 focus:outline-none focus:border-indigo-500 cursor-pointer"
          >
            {teamsLoading && <option>Chargement…</option>}
            {(teams ?? []).map((t: string) => <option key={t} value={t}>{t}</option>)}
          </select>
          <ChevronDown size={14} className="absolute right-3 top-3.5 text-slate-400 pointer-events-none" />
        </div>
      </div>

      {/* Period Selector */}
      <div className="card py-3 px-4">
        <PeriodSelector onChange={onPeriodChange} />
      </div>

      {/* KPI Cards */}
      {kpis.loading ? (
        <div className="kpi-grid">
          {Array(8).fill(0).map((_, i) => <KpiCard key={i} icon="" label="…" value="—" loading />)}
        </div>
      ) : (
        <div className="kpi-grid">
          <KpiCard icon={<Zap size={18} />}           label="Vélocité moyenne"   value={k?.avg_velocity ?? '—'}    unit="tick/membre" color="violet" />
          <KpiCard icon={<Activity size={18} />}      label="Charge équipe"      value={k?.team_load_pct ?? '—'}   unit="%"           color="indigo" />
          <KpiCard icon={<Users size={18} />}         label="Fréq. déploiements" value={k?.deploy_freq ?? '—'}     unit="/sem"        color="emerald" />
          <KpiCard icon={<Clock size={18} />}         label="Lead Time"          value={k?.lead_time_days ?? '—'}  unit="j"           color="cyan" />
          <KpiCard icon={<AlertTriangle size={18} />} label="CFR"                value={k?.cfr_pct ?? '—'}         unit="%"           color="rose" />
          <KpiCard icon={<Stethoscope size={18} />}   label="MTTR"               value={k?.mttr_hours ?? '—'}      unit="h"           color="amber" />
          <KpiCard icon={<Bug size={18} />}           label="Jobs en échec"      value={k?.critical_bugs ?? '—'}                      color="rose" />
          <KpiCard icon={<CheckSquare size={18} />}   label="Livraison à temps"  value={k?.on_time_pct ?? '—'}     unit="%"           color="emerald" />
        </div>
      )}

      {/* Charts 2x2 */}
      {selected ? (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <RadarComp
            title="Radar performance"
            data={radar.data ?? []}
          />
          <Burndown
            title="Burndown dernier sprint"
            sprintName={(burndown.data as {sprint_name?: string})?.sprint_name}
            data={(burndown.data as {data?: {day:string;ideal:number;actual?:number}[]})?.data ?? []}
          />
          <HBar
            title="Charge des membres"
            data={memberData}
            xKey="load_pct"
            yKey="member"
          />
          <VBar
            title="Vélocité par sprint"
            data={velocityData}
            xKey="sprint_name"
            bars={[
              { key: 'actual', label: 'Réel',  color: '#6366f1' },
              { key: 'target', label: 'Cible', color: '#475569' },
            ]}
          />
        </div>
      ) : (
        <div className="flex items-center justify-center h-40 text-slate-500">
          <Loader2 size={24} className="animate-spin mr-2" /> Sélectionnez une équipe…
        </div>
      )}
    </div>
  )
}
