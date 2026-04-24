import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import Teams from './pages/Teams'
import Projects from './pages/Projects'
import Personnel from './pages/Personnel'
import Predictions from './pages/Predictions'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard"   element={<Dashboard />} />
          <Route path="/equipes"     element={<Teams />} />
          <Route path="/projets"     element={<Projects />} />
          <Route path="/personne"    element={<Personnel />} />
          <Route path="/predictions" element={<Predictions />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
