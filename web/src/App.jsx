import { useState, useEffect } from 'react'
import ServerGrid from './components/ServerGrid'
import PlayerList from './components/PlayerList'
import './App.css'

export default function App() {
  const [servers, setServers] = useState([])
  const [selectedId, setSelectedId] = useState(null)

  useEffect(() => {
    const es = new EventSource('/api/events')
    es.onmessage = (e) => {
      try {
        const { servers } = JSON.parse(e.data)
        setServers(servers ?? [])
      } catch {}
    }
    return () => es.close()
  }, [])

  const totalPlayers = servers.reduce((n, s) => n + s.playerCount, 0)
  const selected = servers.find(s => s.id === selectedId) ?? null

  return (
    <div className="app">
      <header className="header">
        <h1 className="logo">YGGDRASIL</h1>
        <div className="stats">
          <div className="stat-pill">
            <span className="stat-label">SERVERS</span>
            <span className="stat-value">{servers.length}</span>
          </div>
          <div className="stat-pill">
            <span className="stat-label">PLAYERS</span>
            <span className="stat-value">{totalPlayers}</span>
          </div>
        </div>
      </header>
      <main className="main">
        <ServerGrid
          servers={servers}
          selectedId={selectedId}
          onSelect={(id) => setSelectedId(id === selectedId ? null : id)}
        />
      </main>
      {selected && (
        <PlayerList server={selected} onClose={() => setSelectedId(null)} />
      )}
    </div>
  )
}
