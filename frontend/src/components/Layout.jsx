import { Link, useNavigation } from '../navigation.jsx'

const navItems = [
  { to: '/', label: 'Dashboard', match: (path) => path === '/' },
  { to: '/insights', label: 'Insights', match: (path) => path === '/insights' || path === '/dataInsights' },
  { to: '/reports', label: 'Reports', match: (path) => path === '/reports' || path === '/report' },
  { to: '/settings', label: 'Settings', match: (path) => ['/settings', '/categories', '/exclusions'].includes(path) },
]

export default function Layout({ children }) {
  const { location } = useNavigation()
  return (
    <div className="app-shell">
      <a className="skip-link" href="#main-content">Skip to dashboard content</a>
      <header className="topbar">
        <Link className="brand" to="/" aria-label="EventPulse AI dashboard">
          <span className="brand-mark" aria-hidden="true">EP</span>
          <span>
            <strong>EventPulse AI</strong>
            <small>Audience intelligence</small>
          </span>
        </Link>
        <nav className="primary-nav" aria-label="Primary navigation">
          {navItems.map((item) => (
            <Link
              key={item.to}
              to={item.to}
              className={item.match(location.pathname) ? 'active' : undefined}
              aria-current={item.match(location.pathname) ? 'page' : undefined}
            >
              {item.label}
            </Link>
          ))}
        </nav>
      </header>
      <main id="main-content" className="page-shell">{children}</main>
      <footer className="footer">
        <span>EventPulse AI</span>
        <span>Audience operations console</span>
      </footer>
    </div>
  )
}

export function PageHeader({ eyebrow, title, description, actions }) {
  return (
    <div className="page-header">
      <div>
        {eyebrow && <div className="eyebrow">{eyebrow}</div>}
        <h1>{title}</h1>
        {description && <p>{description}</p>}
      </div>
      {actions && <div className="page-actions">{actions}</div>}
    </div>
  )
}

export function LoadingState({ label = 'Loading audience data' }) {
  return (
    <div className="state-card" role="status">
      <span className="spinner" aria-hidden="true" />
      <strong>{label}</strong>
      <p>The Java analytics engine is preparing the latest results.</p>
    </div>
  )
}

export function ErrorState({ error, onRetry }) {
  return (
    <div className="state-card error-state" role="alert">
      <span className="state-icon" aria-hidden="true">!</span>
      <strong>We could not load this view</strong>
      <p>{error?.message || 'An unexpected request error occurred.'}</p>
      {onRetry && <button className="button secondary" type="button" onClick={onRetry}>Try again</button>}
    </div>
  )
}

export function StatCard({ label, value, detail, tone = 'default' }) {
  return (
    <div className={`stat-card ${tone === 'gold' ? 'gold' : ''}`}>
      <span>{label}</span>
      <strong>{value}</strong>
      {detail && <small>{detail}</small>}
    </div>
  )
}

export function Card({ title, eyebrow, meta, children, className = '' }) {
  return (
    <section className={`card ${className}`.trim()}>
      {(title || eyebrow || meta) && (
        <header className="card-header">
          <div>
            {eyebrow && <div className="eyebrow">{eyebrow}</div>}
            {title && <h2>{title}</h2>}
          </div>
          {meta && <span>{meta}</span>}
        </header>
      )}
      {children}
    </section>
  )
}
