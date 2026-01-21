import './SearchResult.css';

function SearchResult({ result }) {
  const doc = result.doc || result;

  const formatSize = (bytes) => {
    if (!bytes) return '';
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return '';
    try {
      const date = new Date(dateStr);
      return date.toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric'
      });
    } catch {
      return '';
    }
  };

  const getIcon = (mime, nodeType) => {
    if (nodeType === 'dir') {
      return (
        <svg viewBox="0 0 24 24" width="20" height="20" fill="#5f6368">
          <path d="M10 4H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2h-8l-2-2z"/>
        </svg>
      );
    }
    if (mime?.includes('pdf')) {
      return (
        <svg viewBox="0 0 24 24" width="20" height="20" fill="#d93025">
          <path d="M20 2H8c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm-8.5 7.5c0 .83-.67 1.5-1.5 1.5H9v2H7.5V7H10c.83 0 1.5.67 1.5 1.5v1zm5 2c0 .83-.67 1.5-1.5 1.5h-2.5V7H15c.83 0 1.5.67 1.5 1.5v3zm4-3H19v1h1.5V11H19v2h-1.5V7h3v1.5zM9 9.5h1v-1H9v1zM4 6H2v14c0 1.1.9 2 2 2h14v-2H4V6zm10 5.5h1v-3h-1v3z"/>
        </svg>
      );
    }
    if (mime?.includes('image')) {
      return (
        <svg viewBox="0 0 24 24" width="20" height="20" fill="#34a853">
          <path d="M21 19V5c0-1.1-.9-2-2-2H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2zM8.5 13.5l2.5 3.01L14.5 12l4.5 6H5l3.5-4.5z"/>
        </svg>
      );
    }
    if (mime?.includes('video')) {
      return (
        <svg viewBox="0 0 24 24" width="20" height="20" fill="#ea4335">
          <path d="M18 4l2 4h-3l-2-4h-2l2 4h-3l-2-4H8l2 4H7L5 4H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V4h-4z"/>
        </svg>
      );
    }
    if (mime?.includes('audio')) {
      return (
        <svg viewBox="0 0 24 24" width="20" height="20" fill="#fbbc05">
          <path d="M12 3v10.55c-.59-.34-1.27-.55-2-.55-2.21 0-4 1.79-4 4s1.79 4 4 4 4-1.79 4-4V7h4V3h-6z"/>
        </svg>
      );
    }
    return (
      <svg viewBox="0 0 24 24" width="20" height="20" fill="#5f6368">
        <path d="M14 2H6c-1.1 0-1.99.9-1.99 2L4 20c0 1.1.89 2 1.99 2H18c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z"/>
      </svg>
    );
  };

  return (
    <div className="search-result">
      <div className="result-header">
        <span className="result-icon">{getIcon(doc.mime, doc.node_type)}</span>
        <div className="result-meta">
          <span className="result-filename">{doc.filename || 'Unnamed'}</span>
          <span className="result-path">{doc.path}</span>
        </div>
      </div>

      <a href={`/doc/${doc.doc_id || result.id}`} className="result-title">
        {doc.filename_text || doc.filename || 'Untitled'}
      </a>

      {doc.text && (
        <div className="result-snippet">
          {doc.text.substring(0, 200)}
          {doc.text.length > 200 && '...'}
        </div>
      )}

      <div className="result-details">
        <span className="detail-item">
          <strong>Size:</strong> {formatSize(doc.size_bytes)}
        </span>
        <span className="detail-item">
          <strong>Type:</strong> {doc.ext || 'unknown'}
        </span>
        <span className="detail-item">
          <strong>Date:</strong> {formatDate(doc.processed_at)}
        </span>
      </div>

      <div className="result-cid">
        <strong>CID:</strong> {doc.root_cid}
      </div>
    </div>
  );
}

export default SearchResult;
