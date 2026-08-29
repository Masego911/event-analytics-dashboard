import { useEffect, useMemo, useState } from 'react'
import { api } from '../api.js'
import { useApiData } from '../hooks.js'
import { Card, ErrorState, LoadingState, PageHeader } from '../components/Layout.jsx'
import { useNavigation } from '../navigation.jsx'

function tabFromLocation(path, search) {
  if (path === '/categories') return 'categories'
  if (path === '/exclusions') return 'exclusions'
  const requested = new URLSearchParams(search).get('tab')
  return requested === 'categories' ? 'categories' : 'exclusions'
}

export default function Settings({ initialPath }) {
  const { location, navigate } = useNavigation()
  const { data, error, loading, reload } = useApiData(api.settings)
  const [tab, setTab] = useState(() => tabFromLocation(initialPath, location.search))
  const [excludedText, setExcludedText] = useState('')
  const [overrides, setOverrides] = useState({})
  const [search, setSearch] = useState('')
  const [status, setStatus] = useState({ state: 'idle', message: '' })

  useEffect(() => setTab(tabFromLocation(initialPath, location.search)), [initialPath, location.search])
  useEffect(() => {
    if (!data) return
    setExcludedText(data.excludedEmails.join('\n'))
    setOverrides(Object.fromEntries(data.shows.map((show) => [show.name, show.override])))
  }, [data])

  const visibleShows = useMemo(() => {
    const term = search.trim().toLocaleLowerCase()
    return (data?.shows || []).filter((show) => !term || show.name.toLocaleLowerCase().includes(term) || show.currentCategory.toLocaleLowerCase().includes(term))
  }, [data, search])

  if (loading && !data) return <LoadingState label="Loading settings" />
  if (error && !data) return <ErrorState error={error} onRetry={reload} />

  function switchTab(nextTab) {
    setStatus({ state: 'idle', message: '' })
    setTab(nextTab)
    navigate(`/settings?tab=${nextTab}`, { replace: true })
  }

  async function saveExclusions(event) {
    event.preventDefault()
    setStatus({ state: 'loading', message: 'Saving excluded emails…' })
    try {
      await api.saveSettings('exclusions', { emailsText: excludedText })
      setStatus({ state: 'success', message: 'Excluded emails saved. Dashboard data has been refreshed.' })
      reload()
    } catch (saveError) {
      setStatus({ state: 'error', message: saveError.message })
    }
  }

  async function saveCategories(event) {
    event.preventDefault()
    const values = new URLSearchParams()
    values.set('totalShows', String(data.shows.length))
    data.shows.forEach((show, index) => {
      values.set(`show_${index}`, show.name)
      values.set(`cat_${index}`, overrides[show.name] || '')
    })
    setStatus({ state: 'loading', message: 'Saving category overrides…' })
    try {
      await api.saveSettings('categories', values)
      setStatus({ state: 'success', message: 'Show categories saved. Insights now use the updated categories.' })
      reload()
    } catch (saveError) {
      setStatus({ state: 'error', message: saveError.message })
    }
  }

  return (
    <>
      <PageHeader
        eyebrow="Workspace settings"
        title="Settings"
        description="Control contact exclusions and correct automatic show categories."
      />
      <div className="settings-tabs" role="tablist" aria-label="Settings sections">
        <button className={tab === 'exclusions' ? 'active' : ''} type="button" role="tab" aria-selected={tab === 'exclusions'} onClick={() => switchTab('exclusions')}>Excluded emails</button>
        <button className={tab === 'categories' ? 'active' : ''} type="button" role="tab" aria-selected={tab === 'categories'} onClick={() => switchTab('categories')}>Show categories</button>
      </div>

      {tab === 'exclusions' ? (
        <Card title="Excluded emails" meta={`${data.excludedEmails.length} currently excluded`}>
          <form className="settings-form" onSubmit={saveExclusions}>
            <div className="form-intro">
              <h3>Remove internal or test accounts from the audience database</h3>
              <p>Add one email address per line. Matching records are omitted the next time CSV data is processed.</p>
            </div>
            <label className="field">
              <span>Email addresses</span>
              <textarea value={excludedText} onChange={(event) => setExcludedText(event.target.value)} spellCheck="false" placeholder="name@example.com" />
            </label>
            <div className="form-footer">
              <button className="button primary" type="submit" disabled={status.state === 'loading'}>{status.state === 'loading' ? 'Saving…' : 'Save exclusions'}</button>
              <small>Stored locally in excluded_emails.txt</small>
            </div>
          </form>
        </Card>
      ) : (
        <Card title="Show categories" meta={`${data.shows.length} shows`}>
          <form onSubmit={saveCategories}>
            <div className="category-toolbar">
              <label className="field grow"><span>Search shows</span><input type="search" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Artist, show, or category" /></label>
              <span>{visibleShows.length} visible</span>
            </div>
            <div className="table-scroll settings-table">
              <table>
                <thead><tr><th>Show</th><th>Automatic</th><th>Current</th><th>Override</th></tr></thead>
                <tbody>{visibleShows.map((show) => {
                  const selected = overrides[show.name] || ''
                  return (
                    <tr key={show.name}>
                      <td className="show-name">{show.name}</td>
                      <td>{show.automaticCategory}</td>
                      <td><span className="badge">{selected || show.automaticCategory}</span></td>
                      <td>
                        <select value={selected} onChange={(event) => setOverrides((current) => ({ ...current, [show.name]: event.target.value }))} aria-label={`Category override for ${show.name}`}>
                          <option value="">Auto ({show.automaticCategory})</option>
                          {data.categories.map((category) => <option value={category} key={category}>{category}</option>)}
                        </select>
                      </td>
                    </tr>
                  )
                })}</tbody>
              </table>
            </div>
            <div className="form-footer category-save">
              <button className="button primary" type="submit" disabled={status.state === 'loading'}>{status.state === 'loading' ? 'Saving…' : 'Save categories'}</button>
              <small>Leave a show on Auto to use its name-based category.</small>
            </div>
          </form>
        </Card>
      )}
      {status.message && <p className={`form-message floating ${status.state}`} role="status">{status.message}</p>}
    </>
  )
}
