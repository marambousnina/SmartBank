import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts'

interface BarSeries { key: string; label: string; color: string }
interface Props {
  data: Record<string, unknown>[]
  bars: BarSeries[]
  xKey: string
  title?: string
}

export default function VBar({ data, bars, xKey, title }: Props) {
  return (
    <div className="card h-full">
      {title && <p className="section-title">{title}</p>}
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: -10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(100,116,139,0.2)" />
          <XAxis dataKey={xKey} tick={{ fill: '#64748b', fontSize: 10 }} tickLine={false} axisLine={false} />
          <YAxis tick={{ fill: '#64748b', fontSize: 11 }} tickLine={false} axisLine={false} />
          <Tooltip
            contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
            labelStyle={{ color: '#f1f5f9', fontWeight: 600 }}
            itemStyle={{ color: '#cbd5e1' }}
            cursor={{ fill: 'rgba(99,102,241,0.08)' }}
          />
          <Legend formatter={(v) => <span style={{ color: '#94a3b8', fontSize: 12 }}>{v}</span>} />
          {bars.map(b => (
            <Bar key={b.key} dataKey={b.key} name={b.label} fill={b.color} radius={[4,4,0,0]} maxBarSize={32} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
