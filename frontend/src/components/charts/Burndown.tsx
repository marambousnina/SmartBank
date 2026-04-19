import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts'

interface BurndownPoint { day: string; ideal: number; actual?: number }
interface Props { data: BurndownPoint[]; sprintName?: string; title?: string }

export default function Burndown({ data, sprintName, title }: Props) {
  return (
    <div className="card h-full">
      <div className="flex items-center justify-between mb-3">
        {title && <p className="section-title mb-0">{title}</p>}
        {sprintName && (
          <span className="badge bg-indigo-500/10 text-indigo-400 border border-indigo-500/20">
            {sprintName}
          </span>
        )}
      </div>
      <ResponsiveContainer width="100%" height={210}>
        <LineChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: -10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(100,116,139,0.2)" />
          <XAxis dataKey="day" tick={{ fill: '#64748b', fontSize: 11 }} tickLine={false} axisLine={false} />
          <YAxis tick={{ fill: '#64748b', fontSize: 11 }} tickLine={false} axisLine={false} />
          <Tooltip
            contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
            labelStyle={{ color: '#f1f5f9', fontWeight: 600 }}
            itemStyle={{ color: '#cbd5e1' }}
          />
          <Legend formatter={(v) => <span style={{ color: '#94a3b8', fontSize: 12 }}>{v}</span>} />
          <Line type="monotone" dataKey="ideal"  name="Idéal"  stroke="#475569" strokeWidth={2} strokeDasharray="5 5" dot={false} />
          <Line type="monotone" dataKey="actual" name="Réel"   stroke="#6366f1" strokeWidth={2} dot={{ r: 3, fill: '#6366f1' }} activeDot={{ r: 5 }} connectNulls={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
