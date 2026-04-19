import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts'

interface Series { key: string; label: string; color: string }
interface Props { data: Record<string, unknown>[]; series: Series[]; xKey: string; title?: string; yLabel?: string }

export default function MultiLine({ data, series, xKey, title, yLabel }: Props) {
  return (
    <div className="card h-full">
      {title && <p className="section-title">{title}</p>}
      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: -10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(100,116,139,0.2)" />
          <XAxis dataKey={xKey} tick={{ fill: '#64748b', fontSize: 11 }} tickLine={false} axisLine={false} />
          <YAxis tick={{ fill: '#64748b', fontSize: 11 }} tickLine={false} axisLine={false} label={yLabel ? { value: yLabel, angle: -90, position: 'insideLeft', fill: '#475569', fontSize: 10 } : undefined} />
          <Tooltip
            contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
            labelStyle={{ color: '#f1f5f9', fontWeight: 600, marginBottom: 4 }}
            itemStyle={{ color: '#cbd5e1' }}
          />
          <Legend formatter={(v) => <span style={{ color: '#94a3b8', fontSize: 12 }}>{v}</span>} />
          {series.map(s => (
            <Line
              key={s.key}
              type="monotone"
              dataKey={s.key}
              name={s.label}
              stroke={s.color}
              strokeWidth={2}
              dot={{ r: 3, fill: s.color }}
              activeDot={{ r: 5 }}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
