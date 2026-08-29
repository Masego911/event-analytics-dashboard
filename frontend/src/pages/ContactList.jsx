import { useCallback, useMemo, useState } from 'react'
import { api } from '../api.js'
import { useApiData } from '../hooks.js'
import { Card, ErrorState, LoadingState, PageHeader } from '../components/Layout.jsx'
import { Link, useNavigation } from '../navigation.jsx'

const numberFormat = new Intl.NumberFormat('en-ZA')

function routeDetails(path, search) {
  const query = new URLSearchParams(search)
  const showRoute = path === '/showEmails' || path === '/showPhones' || path === '/contacts/show'
  const kindFromPath = path === '/showPhones' || path === '/categoryPhones' ? 'phones' : null
  const kind = kindFromPath || (query.get('kind') === 'phones' ? 'phones' : 'emails')
  return {
    type: showRoute ? 'show' : 'category',
    kind,
    id: query.get('i'),
    category: query.get('cat'),
  }
}

export default function ContactList({ path }) {
  const { location } = useNavigation()
  const details = routeDetails(path, location.search)
  const loader = useCallback((signal) => details.type === 'show'
    ? api.show(details.id, signal)
    : api.category(details.category, signal), [details.category, details.id, details.type])
  const { data, error, loading, reload } = useApiData(loader)
  const [search, setSearch] = useState('')

  const rows = useMemo(() => {
    if (!data) return []
    const source = details.kind === 'phones'
      ? data.phones
      : details.type === 'show'
        ? data.emails.map((email) => ({ email }))
        : data.contacts
    const term = search.trim().toLocaleLowerCase()
    if (!term) return source
    return source.filter((row) => Object.values(row).flat().join(' ').toLocaleLowerCase().includes(term))
  }, [data, details.kind, details.type, search])

  if (loading && !data) return <LoadingState label="Loading contact list" />
  if (error && !data) return <ErrorState error={error} onRetry={reload} />

  const title = details.type === 'show' ? data.show : data.category
  const count = details.kind === 'phones' ? data.phones.length : details.type === 'show' ? data.emails.length : data.contacts.length
  const queryKey = details.type === 'show' ? `i=${encodeURIComponent(details.id)}` : `cat=${encodeURIComponent(data.category)}`
  const emailPath = details.type === 'show' ? `/contacts/show?${queryKey}&kind=emails` : `/contacts/category?${queryKey}&kind=emails`
  const phonePath = details.type === 'show' ? `/contacts/show?${queryKey}&kind=phones` : `/contacts/category?${queryKey}&kind=phones`
  const downloadKind = details.type === 'show'
    ? (details.kind === 'phones' ? 'showPhones' : 'showEmails')
    : (details.kind === 'phones' ? 'categoryPhones' : 'categoryContacts')

  return (
    <>
      <div className="back-row"><Link to="/">← Back to dashboard</Link></div>
      <PageHeader
        eyebrow={`${details.type === 'show' ? 'Show' : 'Category'} contacts`}
        title={title}
        description={`${numberFormat.format(count)} unique ${details.kind === 'phones' ? 'phone numbers' : 'email contacts'} in this list.`}
        actions={<a className="button primary" href={`/download?kind=${downloadKind}&${queryKey}`}>Download CSV</a>}
      />
      <Card title="Contact list" meta={`${numberFormat.format(rows.length)} visible`}>
        <div className="contact-toolbar">
          <div className="view-tabs" role="tablist" aria-label="Contact type">
            <Link className={details.kind === 'emails' ? 'active' : ''} role="tab" aria-selected={details.kind === 'emails'} to={emailPath}>Emails</Link>
            <Link className={details.kind === 'phones' ? 'active' : ''} role="tab" aria-selected={details.kind === 'phones'} to={phonePath}>Numbers</Link>
          </div>
          <label className="field contact-search"><span>Search list</span><input type="search" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Email, number, or show" /></label>
        </div>
        <div className="table-scroll">
          <table>
            {details.kind === 'phones' ? (
              <>
                <thead><tr><th>#</th><th>Name</th><th>Number</th></tr></thead>
                <tbody>{rows.map((row, index) => <tr key={`${row.number}-${index}`}><td className="row-number">{index + 1}</td><td>{row.name}</td><td><a href={`tel:${row.number}`}>{row.number}</a></td></tr>)}</tbody>
              </>
            ) : details.type === 'category' ? (
              <>
                <thead><tr><th>#</th><th>Email</th><th>Shows in category</th></tr></thead>
                <tbody>{rows.map((row, index) => <tr key={`${row.email}-${index}`}><td className="row-number">{index + 1}</td><td><a href={`mailto:${row.email}`}>{row.email}</a></td><td>{row.shows.join(' · ')}</td></tr>)}</tbody>
              </>
            ) : (
              <>
                <thead><tr><th>#</th><th>Email</th></tr></thead>
                <tbody>{rows.map((row, index) => <tr key={`${row.email}-${index}`}><td className="row-number">{index + 1}</td><td><a href={`mailto:${row.email}`}>{row.email}</a></td></tr>)}</tbody>
              </>
            )}
          </table>
          {!rows.length && <div className="empty-state"><strong>No matching contacts</strong><p>Try a different search.</p></div>}
        </div>
      </Card>
    </>
  )
}
