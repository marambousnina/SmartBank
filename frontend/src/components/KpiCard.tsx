import { ReactNode } from 'react'
import { TrendingUp, TrendingDown, Minus } from 'lucide-react'

interface Props {
  icon: string | ReactNode
  label: string
  value: string | number
  unit?: string
  trend?: 'up' | 'down' | 'neutral'
  trendLabel?: string
  color?: 'indigo' | 'emerald' | 'amber' | 'rose' | 'cyan' | 'violet'
  loading?: boolean
}

const colorMap = {
  indigo:  { bg: 'bg-indigo-500/10',  icon: 'text-indigo-400',  border: 'border-indigo-500/20' },
  emerald: { bg: 'bg-emerald-500/10', icon: 'text-emerald-400', border: 'border-emerald-500/20' },
  amber:   { bg: 'bg-amber-500/10',   icon: 'text-amber-400',   border: 'border-amber-500/20' },
  rose:    { bg: 'bg-rose-500/10',    icon: 'text-rose-400',    border: 'border-rose-500/20' },
  cyan:    { bg: 'bg-cyan-500/10',    icon: 'text-cyan-400',    border: 'border-cyan-500/20' },
  violet:  { bg: 'bg-violet-500/10',  icon: 'text-violet-400',  border: 'border-violet-500/20' },
}

export default function KpiCard({ icon, label, value, unit, trend, trendLabel, color = 'indigo', loading }: Props) {
  const c = colorMap[color]

  if (loading) return (
    <div className="card animate-pulse">
      <div className="h-8 w-8 rounded-lg bg-slate-700 mb-3" />
      <div className="h-3 w-24 bg-slate-700 rounded mb-2" />
      <div className="h-7 w-16 bg-slate-700 rounded" />
    </div>
  )

  const TrendIcon = trend === 'up' ? TrendingUp : trend === 'down' ? TrendingDown : Minus
  const trendColor = trend === 'up' ? 'text-emerald-400' : trend === 'down' ? 'text-rose-400' : 'text-slate-400'

  return (
    <div className={`card border ${c.border} hover:border-opacity-60 transition-all hover:-translate-y-0.5`}>
      <div className={`w-9 h-9 rounded-lg ${c.bg} flex items-center justify-center mb-3`}>
        <span className={`text-lg ${c.icon}`}>{icon}</span>
      </div>
      <p className="text-xs text-slate-400 mb-1">{label}</p>
      <div className="flex items-baseline gap-1">
        <span className="text-2xl font-bold text-white">{value}</span>
        {unit && <span className="text-sm text-slate-400">{unit}</span>}
      </div>
      {trendLabel && (
        <div className={`flex items-center gap-1 mt-1.5 text-xs ${trendColor}`}>
          <TrendIcon size={12} />
          <span>{trendLabel}</span>
        </div>
      )}
    </div>
  )
}
