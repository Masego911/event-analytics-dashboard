import { api } from '../api.js'
import { useApiData } from '../hooks.js'
import { BarChart, DonutChart, LineChart } from '../components/Charts.jsx'
import { Card, ErrorState, LoadingState, PageHeader, StatCard } from '../components/Layout.jsx'
import { Link } from '../navigation.jsx'

const numberFormat = new Intl.NumberFormat('en-ZA')
const currencyFormat = new Intl.NumberFormat('en-ZA', { style: 'currency', currency: 'ZAR', maximumFractionDigits: 0 })
const decimalFormat = new Intl.NumberFormat('en-ZA', { maximumFractionDigits: 2 })
const percent = (value) => `${decimalFormat.format(value || 0)}%`

function InsightCard({ title, description, insight, children, className = '' }) {
  return (
    <Card title={title} className={`insight-card ${className}`}>
      <div className="insight-card-body">
        {description && <p className="chart-description">{description}</p>}
        {children}
        {insight && <p className="insight-note"><span>Insight</span>{insight}</p>}
      </div>
    </Card>
  )
}

function leadingItem(items) {
  return [...(items || [])].sort((a, b) => b.value - a.value)[0]
}

export default function Insights() {
  const { data, error, loading, reload } = useApiData(api.insights)
  if (loading && !data) return <LoadingState label="Running audience analytics" />
  if (error && !data) return <ErrorState error={error} onRetry={reload} />

  const topCategory = leadingItem(data.revenue.categories)
  const topBookingTime = leadingItem(data.audience.bookingTimes)
  const topSeason = leadingItem(data.revenue.seasons)
  const topGroup = leadingItem(data.audience.groupSizes)
  const outliers = data.patterns.outliers.filter((item) => item.isOutlier)

  return (
    <>
      <PageHeader
        eyebrow="Audience intelligence"
        title="Data Insights"
        description="A complete picture of revenue, audience behaviour, connections, and programming opportunities."
        actions={<Link className="button secondary" to="/reports">Build a report</Link>}
      />

      <div className="stat-grid five">
        <StatCard label="Total revenue" value={currencyFormat.format(data.summary.totalRevenue)} tone="gold" />
        <StatCard label="Returning audience" value={percent(data.summary.returningRate)} detail={`${numberFormat.format(data.summary.returningAudience)} people`} />
        <StatCard label="New audience" value={numberFormat.format(data.summary.newAudience)} detail="First-time attendees" />
        <StatCard label="Attendance rate" value={percent(data.summary.attendanceRate)} detail="Recorded check-ins" />
        <StatCard label="Shows per person" value={decimalFormat.format(data.summary.averageShowsPerPerson)} detail="Average attendance" />
      </div>

      <nav className="section-nav" aria-label="Insights sections">
        <a href="#revenue">Revenue &amp; audience</a>
        <a href="#network">Connections</a>
        <a href="#patterns">Patterns</a>
        <a href="#reach">Reach planning</a>
        <a href="#schedule">Schedule</a>
        <a href="#demographics">Demographics</a>
      </nav>

      <section className="insights-section" id="revenue">
        <div className="section-heading"><span>01</span><div><h2>Revenue &amp; audience</h2><p>Financial performance and the customer behaviour behind it.</p></div></div>
        <div className="insights-grid two">
          <InsightCard
            title="Revenue over time"
            description="Annual ticket revenue from the Webtickets exports."
            insight={data.revenue.years.length ? `The strongest year currently on record is ${leadingItem(data.revenue.years).label}.` : null}
          >
            <LineChart data={data.revenue.years} valueFormatter={currencyFormat.format} />
          </InsightCard>
          <InsightCard
            title="Revenue by genre"
            description="Compare the commercial contribution of each programming lane."
            insight={topCategory ? `${topCategory.label} leads with ${currencyFormat.format(topCategory.value)} in recorded revenue.` : null}
          >
            <BarChart data={data.revenue.categories} valueFormatter={currencyFormat.format} />
          </InsightCard>
          <InsightCard
            title="New vs returning audience"
            description="People attending more than one show are counted as returning."
            insight={`${percent(data.summary.returningRate)} of known contacts have attended at least two shows.`}
          >
            <DonutChart data={data.audience.retention} valueFormatter={numberFormat.format} />
          </InsightCard>
          <InsightCard
            title="When people book"
            description="Booking time windows reveal when campaigns are most likely to meet buying intent."
            insight={topBookingTime ? `${topBookingTime.label} is the busiest recorded booking window.` : null}
          >
            <DonutChart data={data.audience.bookingTimes} valueFormatter={numberFormat.format} />
          </InsightCard>
          <InsightCard
            title="Tickets per order"
            description="Order size is a useful signal for couples, groups, and word-of-mouth attendance."
            insight={topGroup ? `${topGroup.label} ticket${topGroup.label === '1' ? '' : 's'} is the most common order size.` : null}
          >
            <BarChart data={data.audience.groupSizes} valueFormatter={numberFormat.format} />
          </InsightCard>
          <InsightCard
            title="Revenue by season"
            description="South African seasons: Summer (Dec–Feb), Autumn (Mar–May), Winter (Jun–Aug), Spring (Sep–Nov)."
            insight={topSeason ? `${topSeason.label} is the strongest revenue season in the current dataset.` : null}
          >
            <BarChart data={data.revenue.seasons} valueFormatter={currencyFormat.format} />
          </InsightCard>
        </div>
      </section>

      <section className="insights-section" id="network">
        <div className="section-heading"><span>02</span><div><h2>Audience connections</h2><p>How shows and audience communities overlap.</p></div></div>
        <div className="mini-stat-grid">
          <StatCard label="Audience clusters" value={numberFormat.format(data.network.clusters)} />
          <StatCard label="Cross-genre fans" value={numberFormat.format(data.network.crossGenreFans)} tone="gold" />
          <StatCard label="Strongest bridge show" value={data.network.bridgeShow || 'Not enough data'} />
        </div>
        <div className="insights-grid two">
          <InsightCard
            title="Unique audience by show"
            description="The broadest shows bring the most different people into the database."
            insight="Use high-reach shows to acquire contacts, then use more focused shows to retain them."
          >
            <BarChart data={data.network.shows.map((item) => ({ label: item.name, value: item.uniqueAudience }))} valueFormatter={numberFormat.format} sort limit={10} />
          </InsightCard>
          <InsightCard
            title="Shows that bridge communities"
            description="Bridgeness grows when a show shares repeat attendees with multiple other shows."
            insight="Bridge shows are useful places to introduce an audience to a different genre."
          >
            <BarChart data={data.network.shows.map((item) => ({ label: item.name, value: item.bridgeness }))} valueFormatter={numberFormat.format} sort limit={10} />
          </InsightCard>
          <InsightCard title="Shows with the most shared fans" description="Jaccard overlap compares shared contacts with the combined audience of each show pair." className="wide">
            <div className="table-scroll">
              <table>
                <thead><tr><th>Show A</th><th>Show B</th><th>Overlap</th><th>Shared audience</th></tr></thead>
                <tbody>{data.network.overlaps.map((pair) => (
                  <tr key={`${pair.showA}-${pair.showB}`}><td>{pair.showA}</td><td>{pair.showB}</td><td className="gold-text">{decimalFormat.format(pair.score)}</td><td>{numberFormat.format(pair.sharedAudience)}</td></tr>
                ))}</tbody>
              </table>
            </div>
          </InsightCard>
        </div>
      </section>

      <section className="insights-section" id="patterns">
        <div className="section-heading"><span>03</span><div><h2>Patterns &amp; trends</h2><p>Statistical signals from ticket volume and booking behaviour.</p></div></div>
        <div className="insights-grid two">
          <InsightCard
            title="Ticket volume vs unique reach"
            description="Pearson correlation checks whether higher ticket sales also mean a broader unique audience."
            insight={data.patterns.pearsonCorrelation == null ? 'At least three shows are needed for this comparison.' : `The relationship is ${data.patterns.pearsonInterpretation}.`}
          >
            <div className="metric-callout"><strong>{data.patterns.pearsonCorrelation == null ? '—' : decimalFormat.format(data.patterns.pearsonCorrelation)}</strong><span>Correlation score (−1 to +1)</span></div>
          </InsightCard>
          <InsightCard
            title="Booking-time significance"
            description="A chi-squared test compares actual booking windows with an even distribution."
            insight={data.patterns.chiSquaredPValue < 0.05 ? 'Booking time is meaningfully patterned; campaign timing is likely useful.' : 'The current data does not show a statistically strong time-of-day pattern.'}
          >
            <dl className="key-values"><div><dt>Chi-squared</dt><dd>{decimalFormat.format(data.patterns.chiSquared)}</dd></div><div><dt>Approx. p-value</dt><dd>{decimalFormat.format(data.patterns.chiSquaredPValue)}</dd></div><div><dt>Cohort retention</dt><dd>{percent(data.patterns.cohortRetentionRate)}</dd></div></dl>
          </InsightCard>
          <InsightCard title="Three-year revenue moving average" description="A rolling average smooths short-term spikes to reveal direction." className="wide">
            <LineChart data={data.patterns.movingAverage} valueFormatter={currencyFormat.format} />
          </InsightCard>
          <InsightCard title="Show-volume outliers" description="A show is flagged when its ticket volume is more than two standard deviations from the mean." className="wide">
            {outliers.length ? (
              <div className="table-scroll"><table><thead><tr><th>Show</th><th>Tickets</th><th>Z-score</th></tr></thead><tbody>{outliers.map((item) => <tr key={item.show}><td>{item.show}</td><td>{numberFormat.format(item.tickets)}</td><td className="gold-text">{decimalFormat.format(item.zScore)}</td></tr>)}</tbody></table></div>
            ) : <p className="empty-copy">No show currently crosses the ±2 z-score outlier threshold.</p>}
          </InsightCard>
        </div>
      </section>

      <section className="insights-section" id="reach">
        <div className="section-heading"><span>04</span><div><h2>Reach optimisation</h2><p>A greedy set-cover plan for reaching the widest audience with fewer lists.</p></div></div>
        <div className="stat-grid three">
          <StatCard label="Target reach" value={percent(data.reach.targetPercentage)} />
          <StatCard label="Achieved reach" value={percent(data.reach.coveragePercentage)} tone="gold" />
          <StatCard label="Contacts covered" value={`${numberFormat.format(data.reach.coveredContacts)} / ${numberFormat.format(data.reach.totalContacts)}`} />
        </div>
        <div className="insights-grid two">
          <InsightCard title="Recommended show-list order" description="Each step chooses the list with the greatest number of previously unreached contacts.">
            <BarChart data={data.reach.showOrder.map((item) => ({ label: item.show, value: item.newContacts }))} valueFormatter={numberFormat.format} />
          </InsightCard>
          <InsightCard title="Recommended category order" description="The same coverage logic, applied to entire genre lanes.">
            <ol className="rank-list">{data.reach.categoryOrder.map((item) => <li key={item.category}><span>{item.step}</span><strong>{item.category}</strong><small>+{numberFormat.format(item.newContacts)} new people</small></li>)}</ol>
          </InsightCard>
        </div>
      </section>

      <section className="insights-section" id="schedule">
        <div className="section-heading"><span>05</span><div><h2>Schedule planning</h2><p>A half-season slot model that prioritises unique audience reach.</p></div></div>
        <InsightCard
          title={`Recommended programme for ${numberFormat.format(data.schedule.slotBudget)} slots`}
          description="The dynamic-programming model selects shows that maximise combined unique audience. Artistic and community value should still guide the final programme."
          insight={`The selected programme has an expected aggregate reach score of ${numberFormat.format(data.schedule.expectedAudience)}.`}
        >
          <div className="chip-list">{data.schedule.shows.map((show) => <span className="chip" key={show}>{show}</span>)}</div>
        </InsightCard>
        <InsightCard title="Pairs with the deepest shared audience" description="High similarity suggests spacing the shows apart rather than marketing them back-to-back.">
          <div className="table-scroll"><table><thead><tr><th>Show A</th><th>Show B</th><th>Shared depth</th></tr></thead><tbody>{data.schedule.similarPairs.map((pair) => <tr key={`${pair.showA}-${pair.showB}`}><td>{pair.showA}</td><td>{pair.showB}</td><td className="gold-text">{numberFormat.format(pair.sharedAudienceDepth)}</td></tr>)}</tbody></table></div>
        </InsightCard>
      </section>

      <section className="insights-section" id="demographics">
        <div className="section-heading"><span>06</span><div><h2>Audience demographics</h2><p>Broad name-based estimates—not a definitive census.</p></div></div>
        <div className="notice">Gender and cultural background are inferred from first names and surnames. Use these aggregate signals carefully; age is not available in the source data.</div>
        <div className="insights-grid two">
          <InsightCard title="Estimated gender split" description="Based on South African naming patterns in ticket records.">
            <DonutChart data={data.demographics.gender} valueFormatter={numberFormat.format} />
          </InsightCard>
          <InsightCard title="Estimated cultural background" description="A directional view based on surname patterns.">
            <DonutChart data={data.demographics.background} valueFormatter={numberFormat.format} />
          </InsightCard>
        </div>
      </section>
    </>
  )
}
