import { useState, useEffect, useMemo } from 'react'
import { Calendar } from 'lucide-react'
import { useFetch } from '../hooks/useFetch'

export interface Period {
  dateFrom: string   // YYYY-MM-DD (start of selected period)
  dateTo:   string   // YYYY-MM-DD (end of selected period)
}

interface Option {
  label:    string
  dateFrom: string
  dateTo:   string
}

type Granularity = 'month' | 'week'

function toIso(d: Date): string {
  return d.toISOString().slice(0, 10)
}

function buildMonthOptions(minDate: string, maxDate: string): Option[] {
  const options: Option[] = []
  const end = new Date(maxDate)
  let cur = new Date(minDate.slice(0, 7) + '-01')

  while (cur <= end) {
    const y = cur.getFullYear()
    const m = cur.getMonth()
    const dateFrom = `${y}-${String(m + 1).padStart(2, '0')}-01`
    const lastDay  = new Date(y, m + 1, 0).getDate()
    const dateTo   = `${y}-${String(m + 1).padStart(2, '0')}-${lastDay}`
    const label    = cur.toLocaleDateString('fr-FR', { month: 'short', year: 'numeric' })
    options.push({ label, dateFrom, dateTo })
    cur = new Date(y, m + 1, 1)
  }
  return options.reverse()
}

function buildWeekOptions(minDate: string, maxDate: string): Option[] {
  const options: Option[] = []
  const end = new Date(maxDate)

  // Start from Monday of the week containing minDate
  let monday = new Date(minDate)
  const dow = monday.getDay()
  monday.setDate(monday.getDate() - (dow === 0 ? 6 : dow - 1))

  while (monday <= end) {
    const sunday = new Date(monday)
    sunday.setDate(sunday.getDate() + 6)

    // ISO week number
    const tmp = new Date(monday.getFullYear(), 0, 4)
    const weekNum = Math.ceil(((monday.getTime() - tmp.getTime()) / 86400000 + tmp.getDay() + 1) / 7)

    options.push({
      label:    `Sem ${weekNum} ${monday.getFullYear()} (${toIso(monday)})`,
      dateFrom: toIso(monday),
      dateTo:   toIso(sunday),
    })
    monday = new Date(monday)
    monday.setDate(monday.getDate() + 7)
  }
  return options.reverse()
}

// Default to last 6 months
function defaultPeriod(): Period {
  const now = new Date()
  const dateTo   = toIso(new Date(now.getFullYear(), now.getMonth() + 1, 0))
  const dateFrom = toIso(new Date(now.getFullYear(), now.getMonth() - 5, 1))
  return { dateFrom, dateTo }
}

interface Props {
  onChange: (period: Period) => void
}

export default function PeriodSelector({ onChange }: Props) {
  const [granularity, setGranularity] = useState<Granularity>('month')
  const { data: range } = useFetch<{ min_date: string; max_date: string }>(
    () => fetch('/api/date-range').then(r => r.json())
  )

  const options = useMemo<Option[]>(() => {
    if (!range) return []
    return granularity === 'month'
      ? buildMonthOptions(range.min_date, range.max_date)
      : buildWeekOptions(range.min_date, range.max_date)
  }, [range, granularity])

  // Indexes into `options` (options are newest-first, index 0 = most recent)
  const [fromIdx, setFromIdx] = useState<number>(-1)
  const [toIdx,   setToIdx]   = useState<number>(-1)

  // When options are ready, default to last 6 months / last 6 weeks
  useEffect(() => {
    if (options.length === 0) return
    const def = defaultPeriod()
    // Find closest matching start option
    const fIdx = options.findIndex(o => o.dateFrom <= def.dateFrom)
    const tIdx = 0  // most recent
    const resolvedFrom = fIdx >= 0 ? fIdx : options.length - 1
    setFromIdx(resolvedFrom)
    setToIdx(tIdx)
  }, [options])

  // Emit whenever selection changes and is valid
  useEffect(() => {
    if (fromIdx < 0 || toIdx < 0 || options.length === 0) return
    const from = options[fromIdx]
    const to   = options[toIdx]
    if (!from || !to) return

    // Ensure dateFrom <= dateTo (options are reversed so fromIdx >= toIdx for chronological)
    const chronoFrom = from.dateFrom < to.dateFrom ? from : to
    const chronoTo   = from.dateFrom < to.dateFrom ? to   : from

    // Minimum 7 days
    const diff = (new Date(chronoTo.dateTo).getTime() - new Date(chronoFrom.dateFrom).getTime()) / 86400000
    if (diff >= 6) {
      onChange({ dateFrom: chronoFrom.dateFrom, dateTo: chronoTo.dateTo })
    }
  }, [fromIdx, toIdx, options, onChange])

  // When granularity changes, reset to defaults
  useEffect(() => {
    setFromIdx(-1)
    setToIdx(-1)
  }, [granularity])

  const isInvalid = () => {
    if (fromIdx < 0 || toIdx < 0 || !options[fromIdx] || !options[toIdx]) return false
    const f = options[Math.max(fromIdx, toIdx)]
    const t = options[Math.min(fromIdx, toIdx)]
    const diff = (new Date(t.dateTo).getTime() - new Date(f.dateFrom).getTime()) / 86400000
    return diff < 6
  }

  return (
    <div className="flex items-center gap-3 flex-wrap">
      <span className="flex items-center gap-1.5 text-slate-400 text-sm">
        <Calendar size={14} />
        Période
      </span>

      {/* Granularity toggle */}
      <div className="flex rounded-lg overflow-hidden border border-slate-700">
        {(['month', 'week'] as Granularity[]).map(g => (
          <button
            key={g}
            onClick={() => setGranularity(g)}
            className={`px-3 py-1.5 text-xs font-medium transition-colors ${
              granularity === g
                ? 'bg-indigo-600 text-white'
                : 'bg-slate-800 text-slate-400 hover:text-slate-200'
            }`}
          >
            {g === 'month' ? 'Mois' : 'Semaines'}
          </button>
        ))}
      </div>

      {/* From selector */}
      <div className="flex items-center gap-2">
        <span className="text-xs text-slate-500">De</span>
        <select
          value={fromIdx}
          onChange={e => setFromIdx(Number(e.target.value))}
          className="appearance-none bg-slate-800 border border-slate-600 text-slate-200 text-xs rounded-lg px-3 py-1.5 focus:outline-none focus:border-indigo-500 cursor-pointer"
        >
          {options.map((o, i) => (
            <option key={i} value={i}>{o.label}</option>
          ))}
        </select>
      </div>

      {/* To selector */}
      <div className="flex items-center gap-2">
        <span className="text-xs text-slate-500">À</span>
        <select
          value={toIdx}
          onChange={e => setToIdx(Number(e.target.value))}
          className="appearance-none bg-slate-800 border border-slate-600 text-slate-200 text-xs rounded-lg px-3 py-1.5 focus:outline-none focus:border-indigo-500 cursor-pointer"
        >
          {options.map((o, i) => (
            <option key={i} value={i}>{o.label}</option>
          ))}
        </select>
      </div>

      {isInvalid() && (
        <span className="text-xs text-rose-400 font-medium">
          Période minimale : 1 semaine
        </span>
      )}
    </div>
  )
}
