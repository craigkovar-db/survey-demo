import { FormEvent, useEffect, useRef, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'

interface ToolCall {
  name: string
  sql?: string
  arguments?: string
}

interface GenieAnswer {
  status: string
  conversation_id?: string | null
  text?: string
  tool_calls?: ToolCall[]
}

interface ProgressStep {
  kind: 'thought' | 'sql' | 'inspect' | 'tool'
  text?: string
  sql?: string
}

interface ChatMessage {
  role: 'user' | 'bot' | 'thinking' | 'error'
  text?: string
  sql?: string[]
  progress?: ProgressStep[] // populated while role === 'thinking'
}

const KIND_LABEL: Record<ProgressStep['kind'], string> = {
  thought: 'Thinking',
  inspect: 'Inspecting',
  sql: 'Running SQL',
  tool: 'Tool',
}

/** Parse SSE text stream into successive JSON event payloads. */
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
      const line = chunk.split('\n').find((l) => l.startsWith('data:'))
      if (!line) continue
      try {
        yield JSON.parse(line.slice(5).trim())
      } catch {
        /* ignore malformed */
      }
    }
  }
}

export default function ChatDrawer() {
  const [open, setOpen] = useState(false)
  const [input, setInput] = useState('')
  const [history, setHistory] = useState<ChatMessage[]>([])
  const [conversationId, setConversationId] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const bodyRef = useRef<HTMLDivElement>(null)

  // Auto-scroll to bottom when history grows.
  useEffect(() => {
    if (bodyRef.current) {
      bodyRef.current.scrollTop = bodyRef.current.scrollHeight
    }
  }, [history])

  async function send(e: FormEvent) {
    e.preventDefault()
    const q = input.trim()
    if (!q || busy) return
    setInput('')
    setBusy(true)
    setHistory((prev) => [
      ...prev,
      { role: 'user', text: q },
      { role: 'thinking', progress: [] },
    ])

    try {
      const resp = await fetch('/api/genie-one/ask-stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question: q,
          conversation_id: conversationId,
        }),
      })
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({ detail: resp.statusText }))
        throw new Error(body.detail || `request failed: ${resp.status}`)
      }

      for await (const ev of sseLines(resp)) {
        if (ev.type === 'progress') {
          const step: ProgressStep = {
            kind: ev.kind,
            text: ev.text,
            sql: ev.sql,
          }
          setHistory((prev) => {
            const next = [...prev]
            const last = next[next.length - 1]
            if (last && last.role === 'thinking') {
              next[next.length - 1] = {
                ...last,
                progress: [...(last.progress ?? []), step],
              }
            }
            return next
          })
        } else if (ev.type === 'complete') {
          const ans = ev.answer as GenieAnswer
          if (ans.conversation_id) setConversationId(ans.conversation_id)
          const sqls = (ans.tool_calls ?? [])
            .filter((t) => t.name === 'execute_sql_query' && t.sql)
            .map((t) => t.sql!)
          setHistory((prev) => {
            const next = prev.slice(0, -1) // drop "thinking"
            if (ans.status !== 'completed') {
              next.push({
                role: 'error',
                text: ans.text || `Genie One status: ${ans.status}`,
              })
            } else {
              next.push({ role: 'bot', text: ans.text || '', sql: sqls })
            }
            return next
          })
        } else if (ev.type === 'error') {
          setHistory((prev) => {
            const next = prev.slice(0, -1)
            next.push({ role: 'error', text: ev.message })
            return next
          })
        }
      }
    } catch (err) {
      setHistory((prev) => {
        const next = prev.slice(0, -1)
        next.push({ role: 'error', text: (err as Error).message })
        return next
      })
    } finally {
      setBusy(false)
    }
  }

  function renderProgress(progress: ProgressStep[]) {
    if (progress.length === 0) {
      return <div className="dc-chat-thinking">Thinking…</div>
    }
    return (
      <ol className="dc-chat-progress">
        {progress.map((p, i) => (
          <li key={i} className={`dc-chat-progress-item kind-${p.kind}`}>
            <span className="dc-chat-progress-kind">{KIND_LABEL[p.kind]}</span>
            {p.kind === 'sql' ? (
              <pre className="dc-chat-progress-sql">{p.sql}</pre>
            ) : (
              <span className="dc-chat-progress-text">{p.text}</span>
            )}
          </li>
        ))}
        <li className="dc-chat-progress-item kind-waiting">
          <span className="dc-chat-progress-kind">
            <span className="dc-chat-dot" />
            <span className="dc-chat-dot" />
            <span className="dc-chat-dot" />
          </span>
          <span className="dc-chat-progress-text">Composing answer…</span>
        </li>
      </ol>
    )
  }

  return (
    <>
      <button
        className="dc-chat-fab"
        onClick={() => setOpen((v) => !v)}
        title={open ? 'Close chat' : 'Chat with Genie One'}
        aria-label="Chat with Genie One"
      >
        {open ? '×' : '💬'}
      </button>

      <aside className={open ? 'dc-chat-drawer open' : 'dc-chat-drawer'}>
        <header className="dc-chat-header">
          <div>
            <h3>Ask Genie</h3>
            <div className="sub">Workspace-wide data chat (Genie One)</div>
          </div>
          <button
            className="dc-chat-close"
            onClick={() => setOpen(false)}
            aria-label="Close chat"
          >
            ×
          </button>
        </header>

        <div className="dc-chat-body" ref={bodyRef}>
          {history.length === 0 ? (
            <div className="dc-chat-hint">
              Ask about anything in the workspace. Follow-ups stay in the same
              conversation. Try:
              <ul>
                <li>"What tables are in survey_demo?"</li>
                <li>"Which ship class has the highest average NPS?"</li>
                <li>"Show me the worst-performing sailings in 2026"</li>
              </ul>
            </div>
          ) : (
            history.map((m, i) => {
              if (m.role === 'thinking') {
                return (
                  <div key={i} className="dc-chat-msg bot thinking-card">
                    {renderProgress(m.progress ?? [])}
                  </div>
                )
              }
              if (m.role === 'error') {
                return (
                  <div key={i} className="dc-chat-msg bot err">
                    ⚠ {m.text}
                  </div>
                )
              }
              if (m.role === 'user') {
                return (
                  <div key={i} className="dc-chat-msg user">
                    {m.text}
                  </div>
                )
              }
              return (
                <div key={i} className="dc-chat-msg bot">
                  {m.text && (
                    <div className="dc-chat-md">
                      <ReactMarkdown
                        remarkPlugins={[remarkGfm]}
                        components={{
                          // Wrap tables so wide content scrolls inside the
                          // drawer instead of pushing the bubble off-pane.
                          table: ({ node: _n, ...props }) => (
                            <div className="dc-md-table-wrap">
                              <table {...props} />
                            </div>
                          ),
                          // Open any links the markdown contains in a new tab
                          // (Genie One often includes workspace deep-links).
                          a: ({ node: _n, ...props }) => (
                            <a target="_blank" rel="noopener noreferrer" {...props} />
                          ),
                        }}
                      >
                        {m.text}
                      </ReactMarkdown>
                    </div>
                  )}
                  {m.sql && m.sql.length > 0 && (
                    <details className="dc-chat-sql-details">
                      <summary>
                        {m.sql.length === 1 ? 'SQL' : `${m.sql.length} queries`}
                      </summary>
                      {m.sql.map((s, j) => (
                        <pre key={j} className="dc-chat-sql">
                          {s}
                        </pre>
                      ))}
                    </details>
                  )}
                </div>
              )
            })
          )}
        </div>

        <form className="dc-chat-input-row" onSubmit={send}>
          <input
            type="text"
            placeholder="Ask about anything in your data…"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            disabled={busy}
          />
          <button type="submit" disabled={busy || !input.trim()}>
            {busy ? '…' : 'Send'}
          </button>
        </form>
      </aside>
    </>
  )
}
