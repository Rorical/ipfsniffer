import { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import { getDocument } from '../services/api';
import GatewaySelector from '../components/GatewaySelector';
import './Document.css';

function Document() {
  const { id } = useParams();
  const [doc, setDoc] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchDoc = async () => {
      try {
        const data = await getDocument(id);
        setDoc(data);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };
    fetchDoc();
  }, [id]);

  if (loading) return <div className="document-loading">Loading document...</div>;
  if (error) return <div className="document-error">Error: {error}</div>;
  if (!doc) return <div className="document-not-found">Document not found</div>;

  return (
    <div className="document-page">
      <a href="/" className="document-back">
        <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor">
          <path d="M20 11H7.83l5.59-5.59L12 4l-8 8 8 8 1.41-1.41L7.83 13H20v-2z"/>
        </svg>
        Back to search
      </a>

      <div className="document-header">
        <h1>{doc.filename || 'Untitled'}</h1>
        <GatewaySelector cid={doc.root_cid} path={doc.path} />
        <div className="document-meta">
          <span className="meta-item">
            <strong>Path:</strong> {doc.path}
          </span>
          <span className="meta-item">
            <strong>Size:</strong> {formatSize(doc.size_bytes)}
          </span>
          <span className="meta-item">
            <strong>Type:</strong> {doc.mime || 'unknown'}
          </span>
          <span className="meta-item">
            <strong>CID:</strong> <code>{doc.root_cid}</code>
          </span>
        </div>
      </div>

      {doc.text && (
        <div className="document-content">
          <h2>Content</h2>
          <pre>{doc.text}</pre>
        </div>
      )}

      {doc.names_text && (
        <div className="document-path-tree">
          <h2>Path</h2>
          <p>{doc.names_text}</p>
        </div>
      )}
    </div>
  );
}

function formatSize(bytes) {
  if (!bytes) return 'Unknown';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export default Document;
