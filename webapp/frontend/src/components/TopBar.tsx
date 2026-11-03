import { NavLink } from 'react-router-dom'

interface Props {
  appName: string
}

export default function TopBar({ appName }: Props) {
  return (
    <header className="dc-topbar">
      <div className="dc-brand">
        <img src="/logo.svg" alt="DataCruises" className="dc-logo" />
        <span className="dc-title">{appName}</span>
      </div>
      <nav className="dc-nav">
        <NavLink
          to="/dashboard"
          className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}
        >
          Dashboard
        </NavLink>
        <NavLink
          to="/explore"
          className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}
        >
          Explore
        </NavLink>
      </nav>
    </header>
  )
}
