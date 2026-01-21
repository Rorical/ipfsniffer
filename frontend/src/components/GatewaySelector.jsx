import { useState, useEffect } from 'react';
import { gateways, getPreferredGateway, setPreferredGateway, buildGatewayUrl } from '../config/gateways';
import './GatewaySelector.css';

function GatewaySelector({ cid, path = '', currentGateway, onGatewayChange }) {
  const [selectedGateway, setSelectedGateway] = useState(getPreferredGateway());
  const [showDropdown, setShowDropdown] = useState(false);
  const [previewUrl, setPreviewUrl] = useState('');

  useEffect(() => {
    const gateway = getPreferredGateway();
    setSelectedGateway(gateway);
    onGatewayChange?.(gateway);
  }, [cid]);

  useEffect(() => {
    setPreviewUrl(buildGatewayUrl(selectedGateway, cid, path));
  }, [selectedGateway, cid, path]);

  const handleGatewaySelect = (gateway) => {
    setSelectedGateway(gateway);
    setPreferredGateway(gateway.name);
    onGatewayChange?.(gateway);
    setShowDropdown(false);
  };

  const handleOpenGateway = () => {
    window.open(previewUrl, '_blank', 'noopener,noreferrer');
  };

  const groupedGateways = {
    official: gateways.filter(g => g.priority === 'official'),
    high: gateways.filter(g => g.priority === 'high'),
    medium: gateways.filter(g => g.priority === 'medium'),
    low: gateways.filter(g => g.priority === 'low')
  };

  const PriorityIcon = ({ priority }) => {
    switch (priority) {
      case 'official':
        return (
          <svg viewBox="0 0 24 24" width="14" height="14" fill="#fbbc04">
            <path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/>
          </svg>
        );
      case 'high':
        return (
          <svg viewBox="0 0 24 24" width="14" height="14" fill="#34a853">
            <path d="M12 2.5c-4.97 0-9 4.03-9 9s4.03 9 9 9 9-4.03 9-9-4.03-9-9zm0 16c-3.86 0-7-3.14-7-7s3.14-7 7-7 7 3.14 7 7-3.14 7-7 7zm1-11h-2v5l4.25 2.52.77-1.28-3.52-2.09z"/>
          </svg>
        );
      case 'medium':
        return (
          <svg viewBox="0 0 24 24" width="14" height="14" fill="#5f6368">
            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.95-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z"/>
          </svg>
        );
      case 'low':
        return (
          <svg viewBox="0 0 24 24" width="14" height="14" fill="#9aa0a6">
            <path d="M16 18l-4-4-4 4 4 4 4-4zM12 14V6l6 6h-6z"/>
          </svg>
        );
      default:
        return null;
    }
  };

  return (
    <div className="gateway-selector">
      <label className="gateway-label">View on gateway:</label>
      <div className="gateway-control">
        <button
          className="gateway-dropdown-toggle"
          onClick={() => setShowDropdown(!showDropdown)}
        >
          <span className="gateway-name">{selectedGateway.name}</span>
          <svg viewBox="0 0 24 24" width="16" height="16" className={`dropdown-arrow ${showDropdown ? 'open' : ''}`}>
            <path fill="currentColor" d="M7 10l5 5 5-5z"/>
          </svg>
        </button>

        {showDropdown && (
          <div className="gateway-dropdown">
            {Object.entries(groupedGateways).map(([priority, items]) => (
              items.length > 0 && (
                <div key={priority} className="gateway-group">
                  <div className="gateway-group-title">
                    <PriorityIcon priority={priority} />
                    <span>
                      {priority === 'official' && 'Official'}
                      {priority === 'high' && 'High Priority'}
                      {priority === 'medium' && 'Public'}
                      {priority === 'low' && 'Other'}
                    </span>
                  </div>
                  {items.map(gateway => (
                    <button
                      key={gateway.name}
                      className={`gateway-option ${gateway.name === selectedGateway.name ? 'selected' : ''}`}
                      onClick={() => handleGatewaySelect(gateway)}
                    >
                      {gateway.name}
                    </button>
                  ))}
                </div>
              )
            ))}
          </div>
        )}
      </div>

      <button className="gateway-open-btn" onClick={handleOpenGateway}>
        <svg viewBox="0 0 24 24" width="16" height="16">
          <path fill="currentColor" d="M19 19H5V5h7V3H5c-1.11 0-2 .9-2 2v14c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2v-7h-2v7zM14 3v2h3.59l-9.83 9.83 1.41 1.41L19 6.41V10h2V3h-7z"/>
        </svg>
        Open
      </button>
    </div>
  );
}

export default GatewaySelector;
