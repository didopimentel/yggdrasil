import { useState, useEffect } from 'react'

export default function PlayerList({ server, onClose }) {
  const [players, setPlayers] = useState([])

  useEffect(() => {
    let cancelled = false
    const poll = async () => {
      try {
        const res = await fetch(`/api/servers/${server.id}/players`)
        if (!res.ok) return
        const { players } = await res.json()
        if (!cancelled) setPlayers(players ?? [])
      } catch {}
    }
    poll()
    const id = setInterval(poll, 1000)
    return () => { cancelled = true; clearInterval(id) }
  }, [server.id])

  return (
    <div className="panel-overlay" onClick={onClose}>
      <div className="panel" onClick={e => e.stopPropagation()}>
        <div className="panel-header">
          <span className="panel-title">{server.id}</span>
          <button className="close-btn" onClick={onClose}>✕</button>
        </div>
        <div className="panel-meta">
          {players.length} PLAYER{players.length !== 1 ? 'S' : ''}
        </div>
        <div className="panel-content">
          {players.length === 0 ? (
            <div className="no-players">NO PLAYERS ON THIS SERVER</div>
          ) : (
            players.map(p => (
              <div key={p.id} className="player-row">
                <span className="player-id">{p.id}</span>
                <span className="player-pos">x={p.x.toFixed(1)}</span>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  )
}
