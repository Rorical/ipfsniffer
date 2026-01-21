export const gateways = [
  { name: 'IPFS.io', url: 'https://ipfs.io/ipfs/', priority: 'official' },
  { name: 'dweb.link', url: 'https://dweb.link/ipfs/', priority: 'official' },
  { name: 'trustless-gateway.link', url: 'https://trustless-gateway.link/ipfs/', priority: 'official' },
  { name: 'Filebase', url: 'https://ipfs.filebase.io/ipfs/', priority: 'high' },
  { name: '4everland', url: 'https://4everland.io/ipfs/', priority: 'high' },
  { name: 'W3S Link', url: 'https://w3s.link/ipfs/', priority: 'high' },
  { name: 'Pinata', url: 'https://gateway.pinata.cloud/ipfs/', priority: 'high' },
  { name: 'Orbitor', url: 'https://ipfs.orbitor.dev/ipfs/', priority: 'medium' },
  { name: 'Latam Orbitor', url: 'https://latam.orbitor.dev/ipfs/', priority: 'medium' },
  { name: 'APAC Orbitor', url: 'https://apac.orbitor.dev/ipfs/', priority: 'medium' },
  { name: 'EU Orbitor', url: 'https://eu.orbitor.dev/ipfs/', priority: 'medium' },
  { name: 'DGET', url: 'https://dget.top/ipfs/', priority: 'medium' },
  { name: 'FLK-IPFS', url: 'https://flk-ipfs.xyz/ipfs/', priority: 'medium' },
  { name: 'IPFS.cyou', url: 'https://ipfs.cyou/ipfs/', priority: 'medium' },
  { name: 'DLunar', url: 'https://dlunar.net/ipfs/', priority: 'medium' },
  { name: 'Storry TV', url: 'https://storry.tv/ipfs/', priority: 'medium' },
  { name: 'Hardbin', url: 'https://hardbin.com/ipfs/', priority: 'medium' },
  { name: 'RunFission', url: 'https://ipfs.runfission.com/ipfs/', priority: 'medium' },
  { name: 'Eth Aragon', url: 'https://ipfs.eth.aragon.network/ipfs/', priority: 'medium' },
  { name: 'Ecolatam', url: 'https://ipfs.ecolatam.com/ipfs/', priority: 'low' }
];

export function getGatewayUrl(cid, path = '', gatewayIndex = 0) {
  const gateway = gateways[gatewayIndex] || gateways[0];
  return buildGatewayUrl(gateway, cid, path);
}

export function buildGatewayUrl(gateway, cid, path = '') {
  let fullPath = path;
  if (fullPath) {
    const ipfsPrefix = `/ipfs/${cid}`;
    if (fullPath.startsWith(ipfsPrefix)) {
      fullPath = fullPath.slice(ipfsPrefix.length);
    }
    fullPath = `/${fullPath.replace(/^\/+/, '')}`;
  }
  return `${gateway.url}${cid}${fullPath}`;
}

export function getGatewayByName(name) {
  return gateways.find(g => g.name === name) || gateways[0];
}

export function getPreferredGateway() {
  const stored = localStorage.getItem('preferredGateway');
  if (stored) {
    const gateway = gateways.find(g => g.name === stored);
    if (gateway) return gateway;
  }
  return gateways[0];
}

export function setPreferredGateway(name) {
  localStorage.setItem('preferredGateway', name);
}
