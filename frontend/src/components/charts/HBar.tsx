import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell
} from 'recharts'

const COLORS = ['#ef4444', '#f97316', '#f59e0b', '#6366f1', '#10b981', '#06b6d4']

interface Props {
  data: { name?: string; severity?: string; member?: string; value?: number; count?: number; load_pct?: number; actual?: number; target?: number; score?: number }[]
  xKey: string
  yKey: string
  title?: string
  colorByIndex?: boolean
  grouped?: boolean
}

export default function HBar({ data, xKey, yKey, title, colorByIndex = false }: Props) {
  return (
    <div className="card h-full">
      {title && <p className="section-title">{title}</p>}
      <ResponsiveContainer width="100%" height={Math.max(180, data.length * 36)}>
        <BarChart data={data} layout="vertical" margin={{ top: 4, right: 20, bottom: 0, left: 8 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(100,116,139,0.2)" horizontal={false} />
          <XAxis type="number" tick={{ fill: '#64748b', fontSize: 11 }} tickLine={false} axisLine={false} />
          <YAxis type="category" dataKey={yKey} width={110} tick={{ fill: '#94a3b8', fontSize: 11 }} tickLine={false} axisLine={false} />
          <Tooltip
            contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
            labelStyle={{ color: '#f1f5f9' }}
            itemStyle={{ color: '#cbd5e1' }}
            cursor={{ fill: 'rgba(99,102,241,0.08)' }}
          />
          <Bar dataKey={xKey} radius={[0, 4, 4, 0]} maxBarSize={24}>
            {data.map((_, i) => (
              <Cell key={i} fill={colorByIndex ? COLORS[i % COLORS.length] : '#6366f1'} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
