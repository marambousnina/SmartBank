import { PieChart, Pie, Cell } from 'recharts'

interface Props { value: number; label?: string; size?: number }

export default function Gauge({ value, label = 'On-Time Rate', size = 200 }: Props) {
  const clamped = Math.min(100, Math.max(0, value))
  const data = [
    { value: clamped },
    { value: 100 - clamped },
  ]
  const color = clamped >= 75 ? '#10b981' : clamped >= 50 ? '#f59e0b' : '#ef4444'
  const cx = size / 2
  const cy = size * 0.6

  return (
    <div className="card h-full flex flex-col items-center justify-center">
      <p className="section-title text-center">{label}</p>
      <div style={{ position: 'relative', width: size, height: size * 0.65 }}>
        <PieChart width={size} height={size * 0.65}>
          <Pie
            data={data}
            cx={cx}
            cy={cy}
            startAngle={180}
            endAngle={0}
            innerRadius={size * 0.28}
            outerRadius={size * 0.4}
            paddingAngle={0}
            dataKey="value"
            strokeWidth={0}
          >
            <Cell fill={color} />
            <Cell fill="#1e293b" />
          </Pie>
        </PieChart>
        {/* Needle value overlay */}
        <div
          style={{
            position: 'absolute',
            bottom: size * 0.08,
            left: 0,
            right: 0,
            textAlign: 'center',
          }}
        >
          <span style={{ fontSize: size * 0.18, fontWeight: 700, color: '#f1f5f9', lineHeight: 1 }}>
            {clamped.toFixed(1)}
          </span>
          <span style={{ fontSize: size * 0.08, color: '#94a3b8' }}>%</span>
        </div>
      </div>
      <div className="flex gap-6 mt-2 text-xs">
        <span className="text-rose-400">0%</span>
        <span className="text-amber-400">50%</span>
        <span className="text-emerald-400">100%</span>
      </div>
    </div>
  )
}
