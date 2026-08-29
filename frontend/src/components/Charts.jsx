const palette = ['#f2c46b', '#68c7b8', '#9f7aea', '#ed7995', '#6ba8e5', '#87cf74']

export function BarChart({ data = [], valueFormatter = (value) => value, limit, sort = false }) {
  const prepared = [...data]
  if (sort) prepared.sort((a, b) => b.value - a.value)
  const displayed = typeof limit === 'number' ? prepared.slice(0, limit) : prepared
  const max = Math.max(1, ...displayed.map((item) => Number(item.value) || 0))

  if (!displayed.length) return <p className="empty-copy">Not enough data to draw this chart yet.</p>
  return (
    <div className="bar-chart" role="img" aria-label="Bar chart">
      {displayed.map((item, index) => (
        <div className="bar-row" key={`${item.label}-${index}`}>
          <div className="bar-label" title={item.label}>{item.label}</div>
          <div className="bar-track">
            <span
              className="bar-fill"
              style={{ width: `${Math.max(item.value > 0 ? 2 : 0, (item.value / max) * 100)}%`, background: palette[index % palette.length] }}
            />
          </div>
          <strong>{valueFormatter(item.value)}</strong>
        </div>
      ))}
    </div>
  )
}

export function DonutChart({ data = [], valueFormatter = (value) => value }) {
  const total = data.reduce((sum, item) => sum + (Number(item.value) || 0), 0)
  let position = 0
  const segments = data.map((item, index) => {
    const start = position
    position += total ? (item.value / total) * 100 : 0
    return `${palette[index % palette.length]} ${start}% ${position}%`
  })
  const background = total ? `conic-gradient(${segments.join(',')})` : 'rgba(255,255,255,.08)'

  return (
    <div className="donut-layout">
      <div className="donut" style={{ background }} role="img" aria-label={`Donut chart, total ${valueFormatter(total)}`}>
        <div><strong>{valueFormatter(total)}</strong><span>Total</span></div>
      </div>
      <div className="chart-legend">
        {data.map((item, index) => (
          <div key={`${item.label}-${index}`}>
            <span style={{ background: palette[index % palette.length] }} />
            <small>{item.label}</small>
            <strong>{valueFormatter(item.value)}</strong>
          </div>
        ))}
      </div>
    </div>
  )
}

export function LineChart({ data = [], valueFormatter = (value) => value }) {
  if (!data.length) return <p className="empty-copy">Not enough historical data to draw a trend yet.</p>
  const width = 720
  const height = 230
  const padding = 26
  const values = data.map((item) => Number(item.value) || 0)
  const min = Math.min(0, ...values)
  const max = Math.max(1, ...values)
  const range = Math.max(1, max - min)
  const step = data.length > 1 ? (width - padding * 2) / (data.length - 1) : 0
  const points = values.map((value, index) => ({
    x: data.length > 1 ? padding + index * step : width / 2,
    y: height - padding - ((value - min) / range) * (height - padding * 2),
  }))
  const path = points.map((point) => `${point.x},${point.y}`).join(' ')

  return (
    <div className="line-chart">
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Line chart">
        <line x1={padding} y1={height - padding} x2={width - padding} y2={height - padding} className="chart-axis" />
        <polyline points={path} className="chart-area-line" />
        {points.map((point, index) => (
          <g key={`${data[index].label}-${index}`}>
            <circle cx={point.x} cy={point.y} r="5" />
            <title>{`${data[index].label}: ${valueFormatter(values[index])}`}</title>
          </g>
        ))}
      </svg>
      <div className="line-labels">
        {data.map((item) => <span key={item.label}>{item.label}</span>)}
      </div>
    </div>
  )
}
