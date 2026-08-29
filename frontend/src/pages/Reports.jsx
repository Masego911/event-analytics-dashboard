import { useMemo, useState } from 'react'
import { api } from '../api.js'
import { useApiData } from '../hooks.js'
import { Card, ErrorState, LoadingState, PageHeader, StatCard } from '../components/Layout.jsx'

const numberFormat = new Intl.NumberFormat('en-ZA')

const definitions = [
  { id: 'contacts', label: 'All contacts', description: 'Email, phone, names, and attended shows', sheet: 'All Contacts' },
  { id: 'shows', label: 'Shows', description: 'Category, tickets, and unique reach', sheet: 'Shows' },
  { id: 'categories', label: 'Categories', description: 'Email and phone reach by genre', sheet: 'Categories' },
  { id: 'revenue', label: 'Revenue', description: 'Annual and category revenue', sheet: 'Revenue' },
  { id: 'retention', label: 'Audience retention', description: 'New and returning contacts', sheet: 'Retention' },
  { id: 'bookingTimes', label: 'Booking times', description: 'Purchases by time window', sheet: 'Booking Times' },
  { id: 'groupSizes', label: 'Group sizes', description: 'Orders by tickets purchased', sheet: 'Group Sizes' },
  { id: 'seasonality', label: 'Seasonality', description: 'Monthly and seasonal revenue', sheet: 'Seasonality' },
  { id: 'overlap', label: 'Audience overlap', description: 'Strongest shared-show audiences', sheet: 'Audience Overlap' },
  { id: 'outliers', label: 'Show outliers', description: 'Ticket-volume z-scores', sheet: 'Show Outliers' },
  { id: 'reach', label: 'Reach optimisation', description: 'Greedy set-cover results', sheet: 'Reach Optimisation' },
  { id: 'schedule', label: 'Schedule plan', description: 'Recommended shows and similar pairs', sheet: 'Schedule Plan' },
  { id: 'gender', label: 'Gender estimate', description: 'Aggregate name-based estimate', sheet: 'Gender Estimate' },
  { id: 'background', label: 'Cultural background', description: 'Aggregate surname-based estimate', sheet: 'Cultural Background' },
]

async function loadReportWorkspace(signal) {
  const [report, insights] = await Promise.all([api.report(signal), api.insights(signal)])
  return { report, insights }
}

function tableFor(id, report, insights) {
  switch (id) {
    case 'contacts':
      return {
        headers: ['Email', 'Phone', 'Shows Attended', 'First Name', 'Surname'],
        rows: report.contacts.map((contact) => [contact.email, contact.phone, contact.shows.join(' | '), contact.firstName, contact.surname]),
      }
    case 'shows':
      return { headers: ['Show', 'Category', 'Tickets Sold', 'Unique Contacts'], rows: report.shows.map((show) => [show.name, show.category, show.tickets, show.uniqueContacts]) }
    case 'categories':
      return { headers: ['Category', 'Unique Emails', 'Unique Numbers'], rows: report.categories.map((category) => [category.name, category.uniqueEmails, category.uniquePhones]) }
    case 'revenue':
      return {
        headers: ['Breakdown', 'Period / Category', 'Revenue (ZAR)'],
        rows: [
          ...insights.revenue.years.map((item) => ['Year', item.label, item.value]),
          ...insights.revenue.categories.map((item) => ['Category', item.label, item.value]),
        ],
      }
    case 'retention':
      return { headers: ['Segment', 'Contacts'], rows: insights.audience.retention.map((item) => [item.label, item.value]) }
    case 'bookingTimes':
      return { headers: ['Time Window', 'Bookings'], rows: insights.audience.bookingTimes.map((item) => [item.label, item.value]) }
    case 'groupSizes':
      return { headers: ['Group Size', 'Orders'], rows: insights.audience.groupSizes.map((item) => [item.label, item.value]) }
    case 'seasonality':
      return {
        headers: ['Breakdown', 'Period', 'Revenue (ZAR)'],
        rows: [
          ...insights.revenue.months.map((item) => ['Month', item.label, item.value]),
          ...insights.revenue.seasons.map((item) => ['Season', item.label, item.value]),
        ],
      }
    case 'overlap':
      return { headers: ['Show A', 'Show B', 'Jaccard Similarity', 'Shared Audience'], rows: insights.network.overlaps.map((item) => [item.showA, item.showB, item.score, item.sharedAudience]) }
    case 'outliers':
      return { headers: ['Show', 'Tickets', 'Z-Score', 'Flag'], rows: insights.patterns.outliers.map((item) => [item.show, item.tickets, item.zScore, item.isOutlier ? 'Outlier' : 'Normal']) }
    case 'reach':
      return { headers: ['Step', 'Show', 'New Contacts', 'Cumulative Contacts'], rows: insights.reach.showOrder.map((item) => [item.step, item.show, item.newContacts, item.cumulativeContacts]) }
    case 'schedule':
      return {
        headers: ['Record Type', 'Show A', 'Show B / Status', 'Shared Audience Depth'],
        rows: [
          ...insights.schedule.shows.map((show) => ['Recommended show', show, 'Included', '']),
          ...insights.schedule.similarPairs.map((pair) => ['Similar pair', pair.showA, pair.showB, pair.sharedAudienceDepth]),
        ],
      }
    case 'gender':
      return { headers: ['Gender', 'Contacts'], rows: insights.demographics.gender.map((item) => [item.label, item.value]) }
    case 'background':
      return { headers: ['Cultural Background', 'Contacts'], rows: insights.demographics.background.map((item) => [item.label, item.value]) }
    default:
      return { headers: [], rows: [] }
  }
}

export default function Reports() {
  const { data, error, loading, reload } = useApiData(loadReportWorkspace)
  const [selected, setSelected] = useState(() => Object.fromEntries(definitions.map((section) => [section.id, true])))
  const [generation, setGeneration] = useState({ state: 'idle', message: '' })
  const selectedCount = useMemo(() => definitions.filter((section) => selected[section.id]).length, [selected])

  if (loading && !data) return <LoadingState label="Preparing report data" />
  if (error && !data) return <ErrorState error={error} onRetry={reload} />

  async function generateWorkbook() {
    if (!selectedCount) {
      setGeneration({ state: 'error', message: 'Select at least one report section.' })
      return
    }
    setGeneration({ state: 'loading', message: 'Building the selected Excel worksheets…' })
    try {
      const ExcelModule = await import('exceljs')
      const ExcelJS = ExcelModule.default || ExcelModule
      const workbook = new ExcelJS.Workbook()
      workbook.creator = 'EventPulse AI Audience Intelligence'
      workbook.created = new Date()
      workbook.properties.date1904 = false

      definitions.filter((section) => selected[section.id]).forEach((section) => {
        const table = tableFor(section.id, data.report, data.insights)
        const worksheet = workbook.addWorksheet(section.sheet)
        worksheet.addRow(table.headers)
        table.rows.forEach((row) => worksheet.addRow(row))
        worksheet.views = [{ state: 'frozen', ySplit: 1 }]
        worksheet.autoFilter = { from: { row: 1, column: 1 }, to: { row: Math.max(1, worksheet.rowCount), column: table.headers.length } }
        const header = worksheet.getRow(1)
        header.font = { bold: true, color: { argb: 'FF111111' } }
        header.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF2C46B' } }
        header.alignment = { vertical: 'middle' }
        header.height = 24
        worksheet.columns.forEach((column, index) => {
          const headerLength = table.headers[index]?.length || 10
          const longest = table.rows.slice(0, 500).reduce((max, row) => Math.max(max, String(row[index] ?? '').length), headerLength)
          column.width = Math.min(48, Math.max(12, longest + 2))
        })
      })

      const buffer = await workbook.xlsx.writeBuffer()
      const blob = new Blob([buffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' })
      const url = URL.createObjectURL(blob)
      const anchor = document.createElement('a')
      anchor.href = url
      anchor.download = `EventPulseAI_Report_${new Date().toISOString().slice(0, 10)}.xlsx`
      document.body.appendChild(anchor)
      anchor.click()
      anchor.remove()
      URL.revokeObjectURL(url)
      setGeneration({ state: 'success', message: `Report ready with ${selectedCount} worksheet${selectedCount === 1 ? '' : 's'}.` })
    } catch (generationError) {
      setGeneration({ state: 'error', message: generationError.message || 'The report could not be generated.' })
    }
  }

  function selectEvery(value) {
    setSelected(Object.fromEntries(definitions.map((section) => [section.id, value])))
  }

  return (
    <>
      <PageHeader
        eyebrow="Reporting workspace"
        title="Generate Report"
        description="Choose the sections you need. Each selected section becomes a formatted worksheet in one Excel file."
      />
      <div className="stat-grid three">
        <StatCard label="Total contacts" value={numberFormat.format(data.report.summary.contacts)} tone="gold" />
        <StatCard label="Shows" value={numberFormat.format(data.report.summary.shows)} />
        <StatCard label="Total tickets" value={numberFormat.format(data.report.summary.tickets)} />
      </div>

      <div className="report-layout">
        <Card title="Select sections" meta={`${selectedCount} of ${definitions.length} selected`}>
          <div className="report-options">
            {definitions.map((section) => (
              <label className={`report-option ${selected[section.id] ? 'selected' : ''}`} key={section.id}>
                <input
                  type="checkbox"
                  checked={Boolean(selected[section.id])}
                  onChange={(event) => setSelected((current) => ({ ...current, [section.id]: event.target.checked }))}
                />
                <span><strong>{section.label}</strong><small>{section.description}</small></span>
              </label>
            ))}
          </div>
          <div className="report-controls">
            <button className="button primary" type="button" onClick={generateWorkbook} disabled={generation.state === 'loading'}>{generation.state === 'loading' ? 'Generating…' : 'Generate Excel report'}</button>
            <button className="button tertiary" type="button" onClick={() => selectEvery(true)}>Select all</button>
            <button className="button tertiary" type="button" onClick={() => selectEvery(false)}>Clear all</button>
          </div>
          {generation.message && <p className={`form-message ${generation.state}`} role="status">{generation.message}</p>}
        </Card>

        <aside>
          <Card title="Report preview" className="compact-card sticky-card">
            <dl className="sidebar-stats">
              <div><dt>Worksheets</dt><dd>{selectedCount}</dd></div>
              <div><dt>Format</dt><dd>.xlsx</dd></div>
              <div><dt>Source</dt><dd>Live dashboard data</dd></div>
            </dl>
            <p className="sidebar-copy">The workbook is generated in your browser. No audience data is sent to an external report service.</p>
          </Card>
        </aside>
      </div>
    </>
  )
}
