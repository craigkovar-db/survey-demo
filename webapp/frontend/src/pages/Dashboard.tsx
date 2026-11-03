interface Props {
  dashboardUrl?: string
}

export default function DashboardPage({ dashboardUrl }: Props) {
  if (!dashboardUrl) {
    return (
      <div className="dc-page">
        <div className="dc-loading">Loading dashboard configuration…</div>
      </div>
    )
  }

  return (
    <div className="dc-page dc-page-dashboard">
      <div className="dc-page-head">
        <div className="dc-page-head-row">
          <div>
            <h1 className="dc-page-title">Customer Insight Dashboard</h1>
            <p className="dc-page-sub">
              Live AI/BI dashboard — survey KPIs, monthly trends, and amenity sentiment.
              Use the Explore tab for self-serve questions.
            </p>
          </div>
          <a
            className="dc-btn"
            href={dashboardUrl}
            target="_blank"
            rel="noopener noreferrer"
            title="Open the dashboard in a new tab"
          >
            Open full dashboard ↗
          </a>
        </div>
      </div>

      <div className="dc-iframe-wrap">
        <iframe
          title="DataCruises AI/BI Dashboard"
          src={dashboardUrl}
          className="dc-iframe"
          frameBorder={0}
          allow="clipboard-read; clipboard-write"
        />
      </div>
    </div>
  )
}
