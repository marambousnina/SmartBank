import { useState, useEffect } from 'react'
import { CheckSquare, Clock, Activity, BarChart2, Bug, ChevronDown, Loader2 } from 'lucide-react'
import KpiCard from '../components/KpiCard'
import DonutChart from '../components/charts/DonutChart'
import MultiLine from '../components/charts/MultiLine'
import VBar from '../components/charts/VBar'
import { useFetch } from '../hooks/useFetch'
import {
  getPersonnel, getPersonKpis, getPersonTasks,
  getPersonTrend, getPersonComparison
} from '../api/client'

interface Person { id: number; name: string; team: string; source: string; departement: string; email: string }

function Avatar({ name }: { name: string }) {
  const initials = name.split(' ').slice(0, 2).map(w => w[0]).join('').toUpperCase()
  const colors = ['bg-indigo-600', 'bg-violet-600', 'bg-emerald-600', 'bg-cyan-600', 'bg-rose-600', 'bg-amber-600']
  const color  = colors[name.charCodeAt(0) % colors.length]
  return (
    <div className={`w-14 h-14 rounded-full ${color} flex items-center justify-center text-white text-xl font-bold shrink-0`}>
      {initials}
    </div>
  )
}

const SOURCE_LABELS: Record<string, string> = {
  'git+jira': 'Développeur', 'git': 'Contributeur Git', 'jira': 'Gestionnaire Jira'
}

export default function Personnel() {
  const { data: staff, loading: staffLoading } = useFetch<Person[]>(getPersonnel)
  const [selectedId, setSelectedId] = useState<number | null>(null)

  useEffect(() => {
    if (staff && staff.length > 0 && !selectedId) setSelectedId(staff[0].id)
  }, [staff, selectedId])

  const selectedPerson = (staff ?? []).find((p: Person) => p.id === selectedId)

  const kpis       = useFetch(() => selectedId ? getPersonKpis(selectedId) : Promise.resolve(null), [selectedId])
  const tasks      = useFetch(() => selectedId ? getPersonTasks(selectedId) : Promise.resolve([]),   [selectedId])
  const trend      = useFetch(() => selectedId ? getPersonTrend(selectedId) : Promise.resolve([]),   [selectedId])
  const comparison = useFetch(() => selectedId ? getPersonComparison(selectedId) : Promise.resolve([]), [selectedId])

  const k = kpis.data as Record<string, unknown> | null

  const tasksData = (tasks.data ?? []).map((r: Record<string,unknown>) => ({
    name: String(r.type ?? ''), value: Number(r.count ?? 0)
  }))

  const trendData = (trend.data ?? []).map((r: Record<string,unknown>) => ({
    label: String(r.month_year ?? ''),
    on_time_pct: Number(r.on_time_pct ?? 0),
    tickets_done: Number(r.tickets_done ?? 0),
  }))

  const compData = (comparison.data ?? []).map((r: Record<string,unknown>) => ({
    name: String(r.name ?? '').split(' ')[0],
    score: Number(r.score ?? 0),
    isCurrent: Boolean(r.is_current),
  }))

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Analyse Individuelle</h1>
          <p className="text-slate-400 text-sm mt-0.5">Performance et contributions par membre</p>
        </div>

        {/* Person selector */}
        <div className="relative">
          <select
            value={selectedId ?? ''}
            onChange={e => setSelectedId(Number(e.target.value))}
            className="appearance-none bg-slate-800 border border-slate-600 text-slate-200 text-sm rounded-lg pl-4 pr-10 py-2.5 focus:outline-none focus:border-indigo-500 cursor-pointer max-w-64"
          >
            {staffLoading && <option>Chargement…</option>}
            {(staff ?? []).map((p: Person) => (
              <option key={p.id} value={p.id}>
                {p.name} — {SOURCE_LABELS[p.source] ?? p.source}
              </option>
            ))}
          </select>
          <ChevronDown size={14} className="absolute right-3 top-3.5 text-slate-400 pointer-events-none" />
        </div>
      </div>

      {selectedId && (
        <>
          {/* Identity Card */}
          <div className="card flex items-start gap-5">
            <Avatar name={selectedPerson?.name ?? '?'} />
            <div className="flex-1">
              <h2 className="text-xl font-bold text-white">{selectedPerson?.name}</h2>
              <p className="text-slate-400 text-sm">{SOURCE_LABELS[selectedPerson?.source ?? ''] ?? 'Développeur'}</p>
              <div className="flex flex-wrap gap-2 mt-3">
                {selectedPerson?.team && (
                  <span className="badge bg-indigo-500/10 text-indigo-400 border border-indigo-500/20">
                    🏢 {selectedPerson.team}
                  </span>
                )}
                {selectedPerson?.departement && (
                  <span className="badge bg-slate-700/60 text-slate-300 border border-slate-600/40">
                    {selectedPerson.departement}
                  </span>
                )}
                {selectedPerson?.email && (
                  <span className="badge bg-slate-700/60 text-slate-400 border border-slate-600/40 text-[10px]">
                    {selectedPerson.email}
                  </span>
                )}
                {selectedPerson?.source && (
                  <span className="badge bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                    {selectedPerson.source}
                  </span>
                )}
              </div>
            </div>
          </div>

          {/* 6 KPI Cards */}
          {kpis.loading ? (
            <div className="grid grid-cols-2 lg:grid-cols-6 gap-4">
              {Array(6).fill(0).map((_, i) => <KpiCard key={i} icon="" label="…" value="—" loading />)}
            </div>
          ) : (
            <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4">
              <KpiCard icon="📋"                            label="Tâches assignées"    value={Number(k?.nb_assigned  ?? 0)}     color="indigo"  />
              <KpiCard icon={<CheckSquare size={18} />}     label="Tâches terminées"    value={Number(k?.nb_done      ?? 0)}     color="emerald" />
              <KpiCard icon={<Clock size={18} />}           label="Livraison à temps"   value={Number(k?.on_time_pct  ?? 0)} unit="%" color="cyan" />
              <KpiCard icon={<Activity size={18} />}        label="Charge de travail"   value={Number(k?.load_pct     ?? 0)} unit="%" color="violet" />
              <KpiCard icon={<BarChart2 size={18} />}       label="Score performance"   value={Number(k?.perf_score   ?? 0)}     color="indigo"  />
              <KpiCard icon={<Bug size={18} />}             label="Jobs en échec"       value={Number(k?.nb_bugs      ?? 0)}     color="rose"    />
            </div>
          )}

          {/* 3 Charts */}
          {tasks.loading || trend.loading || comparison.loading ? (
            <div className="flex items-center justify-center h-40 text-slate-500">
              <Loader2 size={20} className="animate-spin mr-2" />Chargement des graphiques…
            </div>
          ) : (
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              <DonutChart
                title="Répartition des tâches"
                data={tasksData.length > 0 ? tasksData : [{ name: 'Aucune donnée', value: 1 }]}
              />
              <MultiLine
                title="Tendance performance (6 mois)"
                data={trendData}
                xKey="label"
                series={[
                  { key: 'on_time_pct',  label: 'On-Time %',     color: '#6366f1' },
                  { key: 'tickets_done', label: 'Tickets livrés', color: '#10b981' },
                ]}
              />
              <VBar
                title="Comparaison équipe"
                data={compData}
                xKey="name"
                bars={[{ key: 'score', label: 'Score', color: '#6366f1' }]}
              />
            </div>
          )}
        </>
      )}

      {!selectedId && !staffLoading && (
        <div className="flex items-center justify-center h-40 text-slate-500">
          <Loader2 size={24} className="animate-spin mr-2" /> Sélectionnez un membre…
        </div>
      )}
    </div>
  )
}
