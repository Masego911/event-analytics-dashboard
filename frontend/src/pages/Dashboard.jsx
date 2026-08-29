import { useMemo, useState } from 'react'
import { api } from '../api.js'
import { useApiData } from '../hooks.js'
import { Card, ErrorState, LoadingState, PageHeader, StatCard } from '../components/Layout.jsx'
import { Link } from '../navigation.jsx'

const numberFormat = new Intl.NumberFormat('en-ZA')
const pageSize = 12

function SortButton({ field, current, direction, onSort, children }) {
  const active = current === field
  return (
    <button className="table-sort" type="button" onClick={() => onSort(field)}>
      {children}<span aria-hidden="true">{active ? (direction === 'asc' ? '↑' : '↓') : '↕'}</span>
    </button>
  )
}

export default function Dashboard() {
  const { data, error, loading, reload } = useApiData(api.dashboard)
  const [search, setSearch] = useState('')
  const [category, setCategory] = useState('')
  const [sort, setSort] = useState({ field: 'name', direction: 'asc' })
  const [page, setPage] = useState(1)
  const [file, setFile] = useState(null)
  const [upload, setUpload] = useState({ state: 'idle', message: '' })

  const shows = data?.shows || []
  const categories = data?.categories || []
  const filtered = useMemo(() => {
    const term = search.trim().toLocaleLowerCase()
    const result = shows.filter((show) => {
      const matchesSearch = !term || show.name.toLocaleLowerCase().includes(term) || show.category.toLocaleLowerCase().includes(term)
      return matchesSearch && (!category || show.category === category)
    })
    result.sort((left, right) => {
      const first = left[sort.field]
      const second = right[sort.field]
      const comparison = typeof first === 'number'
        ? first - second
        : String(first).localeCompare(String(second), undefined, { sensitivity: 'base' })
      return sort.direction === 'asc' ? comparison : -comparison
    })
    return result
  }, [category, search, shows, sort])

  const pages = Math.max(1, Math.ceil(filtered.length / pageSize))
  const safePage = Math.min(page, pages)
  const currentShows = filtered.slice((safePage - 1) * pageSize, safePage * pageSize)

  function updateFilter(setter, value) {
    setter(value)
    setPage(1)
  }

  function changeSort(field) {
    setSort((current) => current.field === field
      ? { field, direction: current.direction === 'asc' ? 'desc' : 'asc' }
      : { field, direction: 'asc' })
    setPage(1)
  }

  async function uploadCsv(event) {
    event.preventDefault()
    if (!file) return
    setUpload({ state: 'loading', message: 'Uploading and rebuilding audience data…' })
    try {
      const result = await api.upload(file)
      setUpload({ state: 'success', message: `${result.fileName} was uploaded successfully.` })
      setFile(null)
      event.currentTarget.reset()
      reload()
    } catch (uploadError) {
      setUpload({ state: 'error', message: uploadError.message })
    }
  }

  if (loading && !data) return <LoadingState />
  if (error && !data) return <ErrorState error={error} onRetry={reload} />

  return (
    <>
      {upload.message && <div className={`toast ${upload.state}`} role="status">{upload.message}</div>}
      <PageHeader
        eyebrow="Audience operations"
        title="Audience Dashboard"
        description="Search every show, compare genre reach, and prepare your next audience campaign."
        actions={(
          <>
            <Link className="button primary" to="/insights">Open insights</Link>
            <Link className="button secondary" to="/reports">Generate report</Link>
          </>
        )}
      />

      <div className="stat-grid four">
        <StatCard label="Shows loaded" value={numberFormat.format(data.summary.totalShows)} detail="CSV event files" />
        <StatCard label="Unique contacts" value={numberFormat.format(data.summary.totalContacts)} detail="Deduplicated audience" tone="gold" />
        <StatCard label="Total tickets" value={numberFormat.format(data.summary.totalTickets)} detail="Across all shows" />
        <StatCard label="Returning audience" value={numberFormat.format(data.summary.returningAudience)} detail="Attended 2+ shows" />
      </div>

      <div className="dashboard-grid">
        <div>
          <Card title="Shows" eyebrow="Database" meta={`${numberFormat.format(shows.length)} loaded`}>
            <div className="table-tools">
              <label className="field grow">
                <span>Search shows</span>
                <input
                  type="search"
                  value={search}
                  onChange={(event) => updateFilter(setSearch, event.target.value)}
                  placeholder="Artist, event, or genre"
                />
              </label>
              <label className="field">
                <span>Category</span>
                <select value={category} onChange={(event) => updateFilter(setCategory, event.target.value)}>
                  <option value="">All categories</option>
                  {categories.map((item) => <option key={item.name} value={item.name}>{item.name}</option>)}
                </select>
              </label>
              <span className="result-count" aria-live="polite">{filtered.length} of {shows.length} shows</span>
            </div>
            {shows.length === 0 ? (
              <div className="empty-state"><strong>No shows loaded yet</strong><p>Upload a Webtickets CSV to build the dashboard.</p></div>
            ) : (
              <>
                <div className="table-scroll">
                  <table>
                    <caption className="sr-only">Shows with category and audience contact totals</caption>
                    <thead><tr>
                      <th>#</th>
                      <th><SortButton field="name" current={sort.field} direction={sort.direction} onSort={changeSort}>Show</SortButton></th>
                      <th><SortButton field="category" current={sort.field} direction={sort.direction} onSort={changeSort}>Category</SortButton></th>
                      <th><SortButton field="emailCount" current={sort.field} direction={sort.direction} onSort={changeSort}>Emails</SortButton></th>
                      <th><SortButton field="phoneCount" current={sort.field} direction={sort.direction} onSort={changeSort}>Numbers</SortButton></th>
                      <th>View</th>
                    </tr></thead>
                    <tbody>
                      {currentShows.map((show, index) => (
                        <tr key={show.id}>
                          <td className="row-number">{(safePage - 1) * pageSize + index + 1}</td>
                          <td className="show-name">{show.name}</td>
                          <td><span className="badge">{show.category}</span></td>
                          <td>{numberFormat.format(show.emailCount)}</td>
                          <td>{numberFormat.format(show.phoneCount)}</td>
                          <td className="table-actions">
                            <Link to={`/contacts/show?i=${show.id}&kind=emails`}>Emails</Link>
                            {show.phoneCount > 0 && <Link to={`/contacts/show?i=${show.id}&kind=phones`}>Numbers</Link>}
                          </td>
                        </tr>
                      ))}
                      {!currentShows.length && <tr><td colSpan="6"><div className="empty-state compact">No matching shows.</div></td></tr>}
                    </tbody>
                  </table>
                </div>
                <div className="pagination">
                  <button className="button tertiary small" type="button" disabled={safePage <= 1} onClick={() => setPage((value) => value - 1)}>Previous</button>
                  <span>Page {safePage} of {pages}</span>
                  <button className="button tertiary small" type="button" disabled={safePage >= pages} onClick={() => setPage((value) => value + 1)}>Next</button>
                </div>
              </>
            )}
          </Card>

          <Card title="By category" meta="Reach per genre lane">
            <div className="table-scroll">
              <table>
                <thead><tr><th>Category</th><th>Emails</th><th>Numbers</th><th>View</th></tr></thead>
                <tbody>{categories.map((item) => (
                  <tr key={item.name}>
                    <td><span className="badge">{item.name}</span></td>
                    <td>{numberFormat.format(item.emailCount)}</td>
                    <td>{numberFormat.format(item.phoneCount)}</td>
                    <td className="table-actions">
                      <Link to={`/contacts/category?cat=${encodeURIComponent(item.name)}&kind=emails`}>Emails</Link>
                      {item.phoneCount > 0 && <Link to={`/contacts/category?cat=${encodeURIComponent(item.name)}&kind=phones`}>Numbers</Link>}
                    </td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          </Card>
        </div>

        <aside className="dashboard-sidebar" aria-label="Dashboard tools">
          <Card title="All contacts" className="compact-card">
            <dl className="sidebar-stats">
              <div><dt>Unique contacts</dt><dd>{numberFormat.format(data.summary.totalContacts)}</dd></div>
              <div><dt>Returning</dt><dd>{numberFormat.format(data.summary.returningAudience)}</dd></div>
              <div><dt>Full export</dt><dd>contacts_with_shows.csv</dd></div>
            </dl>
          </Card>

          <Card title="Upload CSV" className="compact-card">
            <form className="upload-form" onSubmit={uploadCsv}>
              <label className="upload-drop">
                <span>Choose a Webtickets CSV</span>
                <small>{file?.name || 'No file selected'}</small>
                <input type="file" accept=".csv,text/csv" required onChange={(event) => setFile(event.target.files?.[0] || null)} />
              </label>
              <button className="button primary full" type="submit" disabled={!file || upload.state === 'loading'}>
                {upload.state === 'loading' ? 'Uploading…' : 'Upload and refresh'}
              </button>
            </form>
          </Card>

          <Card title="Settings" className="compact-card">
            <div className="stacked-actions">
              <Link className="button secondary full" to="/settings?tab=categories">Edit show categories</Link>
              <Link className="button secondary full" to="/settings?tab=exclusions">Manage excluded emails</Link>
            </div>
          </Card>
        </aside>
      </div>
    </>
  )
}
