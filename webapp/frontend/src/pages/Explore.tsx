import { FormEvent, useState } from 'react'

interface SurveyHit {
  survey_id: string
  ship_name?: string
  ship_class?: string
  itinerary_name?: string
  sailing_date?: string
  overall_experience?: number
  likelihood_to_recommend?: number
  amenities_satisfaction?: number
  amenity_sentiment?: string
  amenity_names?: string[]
  amenities_comment?: string
  score: number
}

interface StageEvent {
  step: string // 'embedding' | 'lakebase' | 'enriching' | 'complete' | 'error'
  status?: 'started' | 'done'
  label?: string
  duration_ms?: number
  dims?: number
  hits?: number
  returned?: number
  message?: string
  results?: SurveyHit[]
}

interface Stage {
  step: string
  label: string
  status: 'running' | 'done' | 'failed'
  duration_ms?: number
  extra?: string
}

const STEP_LABELS: Record<string, string> = {
  embedding: 'Embedding query',
  lakebase: 'Hybrid search in Lakebase',
  lakebase_cred: '  · Minting Lakebase OAuth credential',
  lakebase_conn: '  · Connecting to Postgres',
  lakebase_query: '  · Running vector + BM25 query',
  enriching: 'Fetching survey details',
}

const EXAMPLES = [
  'spa aquavit terrace broken',
  'rude staff',
  'excellent food',
  'cold stateroom',
  'library quiet reading',
]

/**
 * Parse an SSE text stream into `data:` payloads.
 * SSE events end with a blank line; we buffer across chunks so a partial
 * event at the end of a chunk doesn't get dropped.
 */
async function* sseLines(resp: Response): AsyncGenerator<any> {
  if (!resp.body) throw new Error('response has no body')
  const reader = resp.body.getReader()
  const decoder = new TextDecoder()
  let buf = ''

  while (true) {
    const { value, done } = await reader.read()
    if (done) break
    buf += decoder.decode(value, { stream: true })
    const chunks = buf.split('\n\n')
    buf = chunks.pop() ?? ''
    for (const chunk of chunks) {
      const line = chunk.split('\n').find((l) => l.startsWith('data: '))
      if (!line) continue
      try {
        yield JSON.parse(line.slice(6))
      } catch {
        // ignore malformed events — rare, but don't kill the stream
      }
    }
  }
}

export default function ExplorePage() {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<SurveyHit[]>([])
  const [stages, setStages] = useState<Stage[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [executed, setExecuted] = useState(false)

  function reduceStageEvent(prev: Stage[], ev: StageEvent): Stage[] {
    if (!ev.step || ev.step === 'complete' || ev.step === 'error') return prev
    const label = ev.label ?? STEP_LABELS[ev.step] ?? ev.step
    const extraBits: string[] = []
    if (ev.dims) extraBits.push(`${ev.dims} dims`)
    if (ev.hits !== undefined) extraBits.push(`${ev.hits} hits`)
    if (ev.returned !== undefined) extraBits.push(`${ev.returned} surveys`)

    const idx = prev.findIndex((s) => s.step === ev.step)
    const next: Stage = {
      step: ev.step,
      label,
      status: ev.status === 'done' ? 'done' : 'running',
      duration_ms: ev.duration_ms,
      extra: extraBits.join(', ') || undefined,
    }
    if (idx >= 0) {
      const copy = [...prev]
      copy[idx] = { ...prev[idx], ...next }
      return copy
    }
    return [...prev, next]
  }

  async function doSearch(text?: string) {
    const q = (text ?? query).trim()
    if (!q) return
    setQuery(q)
    setStages([])
    setResults([])
    setError(null)
    setExecuted(true)
    setLoading(true)
    try {
      const resp = await fetch(
        `/api/search-stream?q=${encodeURIComponent(q)}&limit=20`,
      )
      if (!resp.ok) {
        throw new Error(`stream request failed: ${resp.status}`)
      }
      for await (const ev of sseLines(resp) as AsyncGenerator<StageEvent>) {
        if (ev.step === 'complete') {
          setResults(ev.results ?? [])
          continue
        }
        if (ev.step === 'error') {
          setError(ev.message ?? 'Search failed.')
          // Flip any still-running stage to "failed" so the UI doesn't spin forever.
          setStages((prev) =>
            prev.map((s) =>
              s.status === 'running' ? { ...s, status: 'failed' } : s,
            ),
          )
          continue
        }
        setStages((prev) => reduceStageEvent(prev, ev))
      }
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setLoading(false)
    }
  }

  function onSubmit(e: FormEvent) {
    e.preventDefault()
    doSearch()
  }

  const stageList = stages // already in insertion order
  const anyStageRunning =
    loading && stageList.some((s) => s.status === 'running')

  return (
    <div className="dc-page">
      <div className="dc-page-head">
        <h1 className="dc-page-title">Explore</h1>
        <p className="dc-page-sub">
          Search survey amenity comments across every sailing. Hybrid search in Lakebase
          combines vector similarity (gte-large-en) and BM25, fused with RRF — then full
          survey details come back from Unity Catalog. Watch the pipeline run below.
        </p>
      </div>

      <form className="dc-search-form" onSubmit={onSubmit}>
        <input
          type="text"
          className="dc-search-input"
          placeholder="Search survey comments…"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          autoFocus
        />
        <button type="submit" className="dc-btn" disabled={loading}>
          {loading ? 'Searching…' : 'Search'}
        </button>
      </form>

      <div className="dc-examples">
        <span className="dc-examples-label">Try:</span>
        {EXAMPLES.map((ex) => (
          <button
            key={ex}
            type="button"
            className="dc-chip"
            onClick={() => doSearch(ex)}
            disabled={loading}
          >
            {ex}
          </button>
        ))}
      </div>

      {(stageList.length > 0 || anyStageRunning) && (
        <ol className="dc-timeline">
          {stageList.map((s) => (
            <li
              key={s.step}
              className={`dc-timeline-item dc-timeline-${s.status}`}
            >
              <span className="dc-timeline-icon" aria-hidden>
                {s.status === 'done' ? '✓' : s.status === 'failed' ? '✗' : '⟳'}
              </span>
              <span className="dc-timeline-label">{s.label}</span>
              <span className="dc-timeline-meta">
                {s.extra && <span className="dc-muted">{s.extra}</span>}
                {s.duration_ms !== undefined && (
                  <span className="dc-timeline-ms">{s.duration_ms} ms</span>
                )}
              </span>
            </li>
          ))}
        </ol>
      )}

      {error && <div className="dc-banner dc-banner-error">{error}</div>}

      {!loading && executed && !error && results.length === 0 && (
        <div className="dc-placeholder">No matches for that query.</div>
      )}

      {results.length > 0 && (
        <div className="dc-results">
          {results.map((r, i) => (
            <article key={r.survey_id} className="dc-result-card">
              <header className="dc-result-head">
                <span className="dc-result-rank">#{i + 1}</span>
                <span className="dc-result-ship">
                  {r.ship_name ?? '—'}{' '}
                  {r.ship_class && <span className="dc-muted">· {r.ship_class}</span>}
                </span>
                <span className="dc-muted">{r.itinerary_name}</span>
                <span className="dc-muted">{r.sailing_date}</span>
                <span className="dc-result-score" title="Fused RRF score">
                  score {r.score.toFixed(3)}
                </span>
              </header>

              <div className="dc-result-meta">
                <span>
                  Overall <strong>{r.overall_experience ?? '—'}</strong>/10
                </span>
                <span>·</span>
                <span>
                  Recommend <strong>{r.likelihood_to_recommend ?? '—'}</strong>/10
                </span>
                <span>·</span>
                <span>
                  Amenities <strong>{r.amenities_satisfaction ?? '—'}</strong>/10
                </span>
                {r.amenity_sentiment && (
                  <>
                    <span>·</span>
                    <span
                      className={`dc-sentiment dc-sentiment-${r.amenity_sentiment.toLowerCase()}`}
                    >
                      {r.amenity_sentiment}
                    </span>
                  </>
                )}
              </div>

              {r.amenity_names && r.amenity_names.length > 0 && (
                <div className="dc-result-amenities">
                  {r.amenity_names.map((a) => (
                    <span key={a} className="dc-tag">
                      {a}
                    </span>
                  ))}
                </div>
              )}

              {r.amenities_comment && (
                <p className="dc-result-comment">"{r.amenities_comment}"</p>
              )}

              <div className="dc-muted dc-small">survey_id: {r.survey_id}</div>
            </article>
          ))}
        </div>
      )}
    </div>
  )
}
