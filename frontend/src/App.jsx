import { useEffect } from 'react'
import Layout from './components/Layout.jsx'
import Dashboard from './pages/Dashboard.jsx'
import Insights from './pages/Insights.jsx'
import Reports from './pages/Reports.jsx'
import Settings from './pages/Settings.jsx'
import ContactList from './pages/ContactList.jsx'
import { Link, NavigationProvider, useNavigation } from './navigation.jsx'

const pageTitles = {
  '/': 'Audience Dashboard',
  '/insights': 'Data Insights',
  '/dataInsights': 'Data Insights',
  '/reports': 'Reports',
  '/report': 'Reports',
  '/settings': 'Settings',
  '/categories': 'Show Categories',
  '/exclusions': 'Excluded Emails',
}

function NotFound() {
  return (
    <div className="state-card">
      <span className="state-icon" aria-hidden="true">404</span>
      <strong>That page does not exist</strong>
      <p>Return to the audience dashboard to continue.</p>
      <Link className="button primary" to="/">Open dashboard</Link>
    </div>
  )
}

function AppRoutes() {
  const { location } = useNavigation()
  const path = location.pathname.replace(/\/$/, '') || '/'

  useEffect(() => {
    const title = pageTitles[path]
      || (path.startsWith('/show') || path.startsWith('/category') || path.startsWith('/contacts') ? 'Contact List' : 'The One Room')
    document.title = `${title} · The One Room`
  }, [path])

  let page
  if (path === '/') page = <Dashboard />
  else if (path === '/insights' || path === '/dataInsights') page = <Insights />
  else if (path === '/reports' || path === '/report') page = <Reports />
  else if (path === '/settings' || path === '/categories' || path === '/exclusions') page = <Settings initialPath={path} />
  else if (['/showEmails', '/showPhones', '/categoryContacts', '/categoryPhones', '/contacts/show', '/contacts/category'].includes(path)) page = <ContactList path={path} />
  else page = <NotFound />

  return <Layout>{page}</Layout>
}

export default function App() {
  return (
    <NavigationProvider>
      <AppRoutes />
    </NavigationProvider>
  )
}
