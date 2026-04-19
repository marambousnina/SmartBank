import { PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer } from 'recharts'

const COLORS = ['#6366f1', '#10b981', '#f59e0b', '#ef4444', '#06b6d4', '#8b5cf6']

interface Props { data: { name: string; value: number }[]; title?: string }

export default function DonutChart({ data, title }: Props) {
  const total = data.reduce((s, d) => s + d.value, 0)
  return (
    <div className="card h-full">
      {title && <p className="section-title">{title}</p>}
      <ResponsiveContainer width="100%" height={220}>
        <PieChart>
          <Pie
            data={data}
            cx="50%"
            cy="50%"
            innerRadius={55}
            outerRadius={80}
            paddingAngle={3}
            dataKey="value"
          >
            {data.map((_, i) => (
              <Cell key={i} fill={COLORS[i % COLORS.length]} stroke="transparent" />
            ))}
          </Pie>
          <Tooltip
            formatter={(v: number) => [`${v} (${((v/total)*100).toFixed(0)}%)`, '']}
            contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
            labelStyle={{ color: '#f1f5f9' }}
            itemStyle={{ color: '#cbd5e1' }}
          />
          <Legend
            iconType="circle"
            iconSize={8}
            formatter={(v) => <span style={{ color: '#94a3b8', fontSize: 12 }}>{v}</span>}
          />
        </PieChart>
      </ResponsiveContainer>
    </div>
  )
}
