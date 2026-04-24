import { useState } from 'react'
import { BrainCircuit, AlertTriangle, CheckCircle2, TrendingUp, Loader2, RefreshCw, FlaskConical } from 'lucide-react'
import { useFetch } from '../hooks/useFetch'
import { getPredictAllProjects, predictTicket } from '../api/client'

// ── Types ─────────────────────────────────────────────────────────────────────
interface ProjectPrediction {
  ProjectKey: string
  nb_tickets: number
  delay_rate_actual_pct: number
  delay_rate_predicted_pct: number
  avg_delay_proba: number
  max_delay_proba: number
  risk_level: string
  project_will_delay: number
}

interface TicketPredResult {
  delay_probability_pct: number
  will_be_delayed: boolean
  risk_level: string
  verdict: string
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function riskColor(level: string) {
  switch (level) {
    case 'Critique': return { bg: 'bg-rose-500/15', text: 'text-rose-400', border: 'border-rose-500/30', bar: 'bg-rose-500' }
    case 'Élevé':
    case 'Eleve':    return { bg: 'bg-orange-500/15', text: 'text-orange-400', border: 'border-orange-500/30', bar: 'bg-orange-500' }
    case 'Modéré':
    case 'Modere':   return { bg: 'bg-amber-500/15', text: 'text-amber-400', border: 'border-amber-500/30', bar: 'bg-amber-500' }
    default:         return { bg: 'bg-emerald-500/15', text: 'text-emerald-400', border: 'border-emerald-500/30', bar: 'bg-emerald-500' }
  }
}

function ProbaBar({ value, level }: { value: number; level: string }) {
  const c = riskColor(level)
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-2 bg-slate-700 rounded-full overflow-hidden">
        <div className={`h-full rounded-full transition-all duration-500 ${c.bar}`} style={{ width: `${Math.min(value, 100)}%` }} />
      </div>
      <span className="text-xs font-bold w-10 text-right text-slate-300">{value.toFixed(0)}%</span>
    </div>
  )
}

// ── Composant carte projet ────────────────────────────────────────────────────
function ProjectCard({ p }: { p: ProjectPrediction }) {
  const c = riskColor(p.risk_level)
  const delayed = p.project_will_delay === 1

  return (
    <div className={`rounded-xl border ${c.border} ${c.bg} p-4 space-y-3`}>
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <p className="text-base font-bold text-white">{p.ProjectKey}</p>
          <p className="text-xs text-slate-400 mt-0.5">{p.nb_tickets} tickets</p>
        </div>
        <span className={`flex items-center gap-1.5 text-xs font-semibold px-2.5 py-1 rounded-full border ${c.border} ${c.text}`}>
          {delayed
            ? <><AlertTriangle size={11} /> {p.risk_level}</>
            : <><CheckCircle2 size={11} /> {p.risk_level}</>
          }
        </span>
      </div>

      {/* Verdict */}
      <div className={`flex items-center gap-2 text-sm font-semibold ${delayed ? 'text-rose-300' : 'text-emerald-300'}`}>
        {delayed
          ? <AlertTriangle size={14} className="shrink-0" />
          : <CheckCircle2 size={14} className="shrink-0" />
        }
        {delayed ? 'Probablement EN RETARD' : 'Livraison À TEMPS'}
      </div>

      {/* Probabilité moyenne */}
      <div className="space-y-1">
        <div className="flex justify-between text-xs text-slate-400">
          <span>Probabilité de retard (moy.)</span>
        </div>
        <ProbaBar value={p.avg_delay_proba} level={p.risk_level} />
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 gap-2 pt-1">
        <div className="bg-slate-800/60 rounded-lg p-2 text-center">
          <p className="text-[10px] text-slate-500 uppercase tracking-wide">Retard réel</p>
          <p className="text-sm font-bold text-slate-200">{p.delay_rate_actual_pct}%</p>
        </div>
        <div className="bg-slate-800/60 rounded-lg p-2 text-center">
          <p className="text-[10px] text-slate-500 uppercase tracking-wide">Prédit</p>
          <p className="text-sm font-bold text-slate-200">{p.delay_rate_predicted_pct}%</p>
        </div>
      </div>
    </div>
  )
}

// ── Formulaire prédiction ticket ──────────────────────────────────────────────
const DEFAULT_FORM = {
  nb_status_changes: 3,
  time_blocked_hours: 0,
  time_in_review_hours: 10,
  time_in_qa_hours: 5,
  time_correction_review_hours: 0,
  time_correction_qa_hours: 0,
  nb_commits: 0,
  nb_mrs: 0,
  was_blocked: 0,
  is_bug: 0,
  nb_tickets_total: 100,
  nb_tickets_done: 20,
  completion_rate: 20,
  issue_type: 'Story',
}

function TicketSimulator() {
  const [form, setForm]         = useState(DEFAULT_FORM)
  const [result, setResult]     = useState<TicketPredResult | null>(null)
  const [loading, setLoading]   = useState(false)
  const [error, setError]       = useState<string | null>(null)

  const set = (key: string, val: string | number) => setForm(f => ({ ...f, [key]: val }))

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setError(null)
    try {
      const res = await predictTicket(form as Record<string, unknown>)
      setResult(res)
    } catch {
      setError('Erreur lors de la prédiction. Vérifiez que l\'API est démarrée.')
    } finally {
      setLoading(false)
    }
  }

  const c = result ? riskColor(result.risk_level) : null

  return (
    <div className="bg-slate-900 border border-slate-700/50 rounded-xl p-6 space-y-5">
      <div className="flex items-center gap-2">
        <FlaskConical size={18} className="text-indigo-400" />
        <h2 className="text-base font-bold text-white">Simulateur de ticket</h2>
        <span className="text-xs text-slate-500">— prédiction en temps réel</span>
      </div>

      <form onSubmit={handleSubmit} className="space-y-4">
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
          {[
            { key: 'nb_status_changes',            label: 'Changements statut',  type: 'number', min: 0  },
            { key: 'time_blocked_hours',            label: 'Heures bloqué',       type: 'number', min: 0  },
            { key: 'time_in_review_hours',          label: 'Heures en review',    type: 'number', min: 0  },
            { key: 'time_in_qa_hours',              label: 'Heures en QA',        type: 'number', min: 0  },
            { key: 'time_correction_review_hours',  label: 'Correct. review (h)', type: 'number', min: 0  },
            { key: 'time_correction_qa_hours',      label: 'Correct. QA (h)',     type: 'number', min: 0  },
            { key: 'nb_commits',                    label: 'Commits liés',        type: 'number', min: 0  },
            { key: 'nb_mrs',                        label: 'MRs liées',           type: 'number', min: 0  },
            { key: 'nb_tickets_total',              label: 'Total tickets projet', type: 'number', min: 1 },
            { key: 'nb_tickets_done',               label: 'Tickets terminés',    type: 'number', min: 0  },
            { key: 'completion_rate',               label: 'Completion projet %', type: 'number', min: 0  },
          ].map(({ key, label, type, min }) => (
            <div key={key} className="flex flex-col gap-1">
              <label className="text-[11px] text-slate-400">{label}</label>
              <input
                type={type}
                min={min}
                value={(form as Record<string, unknown>)[key] as number}
                onChange={e => set(key, parseFloat(e.target.value) || 0)}
                className="bg-slate-800 border border-slate-600 rounded-lg px-3 py-1.5 text-sm text-white
                           focus:outline-none focus:border-indigo-500 transition-colors"
              />
            </div>
          ))}

          {/* Type ticket */}
          <div className="flex flex-col gap-1">
            <label className="text-[11px] text-slate-400">Type de ticket</label>
            <select
              value={form.issue_type}
              onChange={e => set('issue_type', e.target.value)}
              className="bg-slate-800 border border-slate-600 rounded-lg px-3 py-1.5 text-sm text-white
                         focus:outline-none focus:border-indigo-500 transition-colors"
            >
              {['Story', 'Task', 'Epic', 'Tache', 'Sous-tache'].map(t => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>
          </div>

          {/* Checkboxes */}
          {[
            { key: 'was_blocked', label: 'Ticket bloqué' },
            { key: 'is_bug',      label: 'Est un bug'   },
          ].map(({ key, label }) => (
            <div key={key} className="flex flex-col gap-1 justify-end">
              <label className="flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={(form as Record<string, unknown>)[key] === 1}
                  onChange={e => set(key, e.target.checked ? 1 : 0)}
                  className="w-4 h-4 accent-indigo-500"
                />
                <span className="text-sm text-slate-300">{label}</span>
              </label>
            </div>
          ))}
        </div>

        <button
          type="submit"
          disabled={loading}
          className="flex items-center gap-2 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50
                     text-white text-sm font-semibold px-5 py-2.5 rounded-lg transition-colors"
        >
          {loading ? <Loader2 size={15} className="animate-spin" /> : <BrainCircuit size={15} />}
          {loading ? 'Analyse...' : 'Prédire le retard'}
        </button>
      </form>

      {error && (
        <p className="text-sm text-rose-400 bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-2">
          {error}
        </p>
      )}

      {result && c && (
        <div className={`rounded-xl border ${c.border} ${c.bg} p-5 space-y-3`}>
          <div className="flex items-center gap-2">
            {result.will_be_delayed
              ? <AlertTriangle size={20} className="text-rose-400" />
              : <CheckCircle2 size={20} className="text-emerald-400" />
            }
            <p className={`text-base font-bold ${c.text}`}>{result.verdict}</p>
          </div>
          <div className="space-y-1">
            <p className="text-xs text-slate-400">Probabilité de retard</p>
            <ProbaBar value={result.delay_probability_pct} level={result.risk_level} />
          </div>
          <div className="flex gap-3">
            <span className={`text-xs font-semibold px-3 py-1 rounded-full border ${c.border} ${c.text}`}>
              Risque {result.risk_level}
            </span>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Page principale ───────────────────────────────────────────────────────────
export default function Predictions() {
  const { data, loading, error, reload } = useFetch<{ total: number; model: string; data: ProjectPrediction[] }>(getPredictAllProjects)

  const projects = data?.data ?? []
  const nbCritique = projects.filter(p => p.risk_level === 'Critique').length
  const nbDelayed  = projects.filter(p => p.project_will_delay === 1).length
  const avgProba   = projects.length ? (projects.reduce((s, p) => s + p.avg_delay_proba, 0) / projects.length).toFixed(0) : '—'

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-indigo-600/20 border border-indigo-500/30 flex items-center justify-center">
            <BrainCircuit size={20} className="text-indigo-400" />
          </div>
          <div>
            <h1 className="text-xl font-bold text-white">Prédictions de retard</h1>
            <p className="text-sm text-slate-400">Modèle Random Forest — 92% accuracy, ROC-AUC 0.977</p>
          </div>
        </div>
        <button onClick={reload} className="flex items-center gap-1.5 text-slate-400 hover:text-white text-sm transition-colors">
          <RefreshCw size={14} /> Actualiser
        </button>
      </div>

      {/* KPI résumé */}
      {!loading && !error && (
        <div className="grid grid-cols-3 gap-4">
          <div className="bg-slate-900 border border-slate-700/50 rounded-xl p-4 flex items-center gap-3">
            <TrendingUp size={22} className="text-indigo-400 shrink-0" />
            <div>
              <p className="text-xs text-slate-400">Projets analysés</p>
              <p className="text-xl font-bold text-white">{projects.length}</p>
            </div>
          </div>
          <div className="bg-slate-900 border border-rose-500/20 rounded-xl p-4 flex items-center gap-3 bg-rose-500/5">
            <AlertTriangle size={22} className="text-rose-400 shrink-0" />
            <div>
              <p className="text-xs text-slate-400">Projets en retard prédit</p>
              <p className="text-xl font-bold text-rose-300">{nbDelayed} <span className="text-sm text-slate-400">dont {nbCritique} critiques</span></p>
            </div>
          </div>
          <div className="bg-slate-900 border border-amber-500/20 rounded-xl p-4 flex items-center gap-3 bg-amber-500/5">
            <BrainCircuit size={22} className="text-amber-400 shrink-0" />
            <div>
              <p className="text-xs text-slate-400">Probabilité moy. retard</p>
              <p className="text-xl font-bold text-amber-300">{avgProba}%</p>
            </div>
          </div>
        </div>
      )}

      {/* Grille projets */}
      {loading && (
        <div className="flex items-center justify-center h-48">
          <Loader2 size={28} className="text-indigo-400 animate-spin" />
        </div>
      )}

      {error && (
        <div className="bg-rose-500/10 border border-rose-500/20 rounded-xl p-4 text-rose-400 text-sm">
          {error} — Vérifiez que l'API est démarrée (<code className="text-xs">uvicorn api.main:app --reload</code>)
        </div>
      )}

      {!loading && !error && (
        <div>
          <p className="text-xs text-slate-500 mb-3 uppercase tracking-widest font-semibold">Résultats par projet</p>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
            {projects
              .slice()
              .sort((a, b) => b.avg_delay_proba - a.avg_delay_proba)
              .map(p => <ProjectCard key={p.ProjectKey} p={p} />)
            }
          </div>
        </div>
      )}

      {/* Simulateur ticket */}
      <TicketSimulator />
    </div>
  )
}
