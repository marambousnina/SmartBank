import {
  RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  ResponsiveContainer, Tooltip
} from 'recharts'

interface Props { data: { axis: string; value: number }[]; title?: string }

export default function RadarComp({ data, title }: Props) {
  return (
    <div className="card h-full">
      {title && <p className="section-title">{title}</p>}
      <ResponsiveContainer width="100%" height={240}>
        <RadarChart data={data} margin={{ top: 10, right: 20, bottom: 10, left: 20 }}>
          <PolarGrid stroke="rgba(100,116,139,0.3)" />
          <PolarAngleAxis dataKey="axis" tick={{ fill: '#94a3b8', fontSize: 11 }} />
          <PolarRadiusAxis angle={30} domain={[0, 100]} tick={{ fill: '#64748b', fontSize: 9 }} tickCount={4} />
          <Radar
            dataKey="value"
            stroke="#6366f1"
            fill="#6366f1"
            fillOpacity={0.25}
            strokeWidth={2}
          />
          <Tooltip
            contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
            labelStyle={{ color: '#f1f5f9' }}
            itemStyle={{ color: '#6366f1' }}
            formatter={(v: number) => [`${v.toFixed(1)}%`, 'Score']}
          />
        </RadarChart>
      </ResponsiveContainer>
    </div>
  )
}
