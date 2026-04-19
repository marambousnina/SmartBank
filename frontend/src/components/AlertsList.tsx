import { AlertTriangle, CheckCircle, XCircle, Info } from 'lucide-react'
import { useFetch } from '../hooks/useFetch'
import { getDashboardAlerts } from '../api/client'

interface Alert { level: 'danger' | 'warning' | 'success' | 'info'; message: string }

const cfg = {
  danger:  { icon: XCircle,        color: 'text-rose-400',    bg: 'bg-rose-500/10',    border: 'border-rose-500/30' },
  warning: { icon: AlertTriangle,  color: 'text-amber-400',   bg: 'bg-amber-500/10',   border: 'border-amber-500/30' },
  success: { icon: CheckCircle,    color: 'text-emerald-400', bg: 'bg-emerald-500/10', border: 'border-emerald-500/30' },
  info:    { icon: Info,           color: 'text-cyan-400',    bg: 'bg-cyan-500/10',    border: 'border-cyan-500/30' },
}

export default function AlertsList() {
  const { data, loading } = useFetch<Alert[]>(getDashboardAlerts)

  return (
    <div className="card h-full">
      <p className="section-title">Alertes actives</p>
      {loading && (
        <div className="space-y-2">
          {[1,2,3].map(i => <div key={i} className="h-10 bg-slate-700 rounded-lg animate-pulse" />)}
        </div>
      )}
      <div className="space-y-2 max-h-52 overflow-y-auto pr-1">
        {data?.map((a, i) => {
          const { icon: Icon, color, bg, border } = cfg[a.level]
          return (
            <div key={i} className={`flex items-start gap-2.5 p-3 rounded-lg border ${bg} ${border}`}>
              <Icon size={15} className={`${color} mt-0.5 shrink-0`} />
              <p className={`text-xs ${color}`}>{a.message}</p>
            </div>
          )
        })}
      </div>
    </div>
  )
}
