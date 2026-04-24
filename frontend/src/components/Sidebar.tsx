import { NavLink } from 'react-router-dom'
import { LayoutDashboard, Users, FolderKanban, UserCircle, BarChart3, BrainCircuit } from 'lucide-react'

const nav = [
  { to: '/dashboard',    label: 'Dashboard',    icon: LayoutDashboard },
  { to: '/equipes',      label: 'Equipes',      icon: Users },
  { to: '/projets',      label: 'Projets',      icon: FolderKanban },
  { to: '/personne',     label: 'Personnel',    icon: UserCircle },
  { to: '/predictions',  label: 'Prédictions',  icon: BrainCircuit },
]

export default function Sidebar() {
  return (
    <aside className="fixed inset-y-0 left-0 w-56 bg-slate-900 border-r border-slate-700/50 flex flex-col z-20">
      {/* Logo */}
      <div className="flex items-center gap-3 px-5 py-5 border-b border-slate-700/50">
        <div className="w-8 h-8 rounded-lg bg-indigo-600 flex items-center justify-center">
          <BarChart3 size={18} className="text-white" />
        </div>
        <div>
          <p className="text-sm font-bold text-white">SmartBank</p>
          <p className="text-[10px] text-slate-400">Metrics & DevOps</p>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-3 py-4 space-y-1">
        <p className="px-2 mb-2 text-[10px] font-semibold text-slate-500 uppercase tracking-widest">Vues</p>
        {nav.map(({ to, label, icon: Icon }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all ${
                isActive
                  ? 'bg-indigo-600/20 text-indigo-400 border border-indigo-500/30'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800'
              }`
            }
          >
            <Icon size={16} />
            {label}
          </NavLink>
        ))}
      </nav>

      {/* Footer */}
      <div className="px-5 py-4 border-t border-slate-700/50">
        <p className="text-[10px] text-slate-600">SmartBank v1.0 — DWH</p>
      </div>
    </aside>
  )
}
