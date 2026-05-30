import ServerCard from './ServerCard'

export default function ServerGrid({ servers, selectedId, onSelect }) {
  return (
    <div className="server-grid">
      {servers.length === 0 && (
        <div className="empty-state">WAITING FOR SERVERS…</div>
      )}
      {servers.map(server => (
        <ServerCard
          key={server.id}
          server={server}
          selected={server.id === selectedId}
          onClick={() => onSelect(server.id)}
        />
      ))}
    </div>
  )
}
