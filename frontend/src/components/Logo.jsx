import './Logo.css';

function Logo({ size = 'normal' }) {
  const className = `logo logo-${size}`;

  return (
    <svg className={className} viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <linearGradient id="logoGradient" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor="#3B82F6" />
          <stop offset="50%" stopColor="#8B5CF6" />
          <stop offset="100%" stopColor="#EC4899" />
        </linearGradient>
      </defs>

      <circle cx="32" cy="32" r="30" fill="url(#logoGradient)" opacity="0.1" />

      <g className="logo-magnifier">
        <circle cx="26" cy="26" r="14" stroke="url(#logoGradient)" strokeWidth="4" fill="none" />
        <line x1="35" y1="35" x2="48" y2="48" stroke="url(#logoGradient)" strokeWidth="6" strokeLinecap="round" />
      </g>

      <g className="logo-nodes">
        <circle cx="14" cy="14" r="4" fill="#3B82F6" />
        <circle cx="48" cy="10" r="4" fill="#8B5CF6" />
        <circle cx="52" cy="42" r="4" fill="#EC4899" />
        <circle cx="10" cy="48" r="4" fill="#3B82F6" />

        <line x1="14" y1="14" x2="26" y2="20" stroke="#64748B" strokeWidth="1.5" opacity="0.6" />
        <line x1="48" y1="10" x2="34" y2="18" stroke="#64748B" strokeWidth="1.5" opacity="0.6" />
        <line x1="52" y1="42" x2="36" y2="30" stroke="#64748B" strokeWidth="1.5" opacity="0.6" />
        <line x1="10" y1="48" x2="22" y2="34" stroke="#64748B" strokeWidth="1.5" opacity="0.6" />

        <line x1="14" y1="14" x2="10" y2="48" stroke="#64748B" strokeWidth="1.5" opacity="0.4" />
        <line x1="48" y1="10" x2="52" y2="42" stroke="#64748B" strokeWidth="1.5" opacity="0.4" />
      </g>

      <circle cx="26" cy="26" r="8" fill="#fff" opacity="0.15" />
    </svg>
  );
}

export default Logo;
