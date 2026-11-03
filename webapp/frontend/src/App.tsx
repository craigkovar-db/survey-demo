import { useEffect, useState } from 'react'
import { Navigate, Route, Routes } from 'react-router-dom'
import ChatDrawer from './components/ChatDrawer'
import TopBar from './components/TopBar'
import DashboardPage from './pages/Dashboard'
import ExplorePage from './pages/Explore'

export interface AppConfig {
  appName: string
  dashboardUrl: string
}

export default function App() {
  const [config, setConfig] = useState<AppConfig | null>(null)
  const [configError, setConfigError] = useState<string | null>(null)

  useEffect(() => {
    fetch('/api/config')
      .then((r) => {
        if (!r.ok) throw new Error(`/api/config returned ${r.status}`)
        return r.json()
      })
      .then((j: AppConfig) => setConfig(j))
      .catch((e: Error) => setConfigError(e.message))
  }, [])

  return (
    <div className="dc-root">
      <TopBar appName={config?.appName ?? 'DataCruises Customer Insight'} />

      <main className="dc-main">
        {configError && (
          <div className="dc-banner dc-banner-error">
            Couldn't load /api/config: {configError}
          </div>
        )}

        <Routes>
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route
            path="/dashboard"
            element={<DashboardPage dashboardUrl={config?.dashboardUrl} />}
          />
          <Route path="/explore" element={<ExplorePage />} />
          <Route path="*" element={<Navigate to="/dashboard" replace />} />
        </Routes>
      </main>

      {/* Chat drawer lives in the global layout so it's available on every page. */}
      <ChatDrawer />
    </div>
  )
}
