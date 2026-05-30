export default function ServerCard({ server, selected, onClick }) {
  return (
    <div className={`server-card${selected ? ' selected' : ''}`} onClick={onClick}>
      <div className="card-header">
        <span className="server-id">{server.id}</span>
        <span className="status-dot" />
      </div>
      <div className="card-addr">{server.address}:{server.port}</div>
      <div className="card-divider" />
      <div className="card-stat">
        <span className="card-stat-label">Players</span>
        <span className="card-stat-value">{server.playerCount}</span>
      </div>
    </div>
  )
}
