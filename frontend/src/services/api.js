import { config } from '../config';

export async function search(query, options = {}) {
  const { page = 1, limit = 10, sortBy, sortOrder } = options;

  const params = new URLSearchParams({
    q: query,
    page: page.toString(),
    limit: limit.toString()
  });

  if (sortBy) params.append('sort', `${sortBy}:${sortOrder || 'desc'}`);

  const response = await fetch(`${config.searchEndpoint}?${params}`);

  if (!response.ok) {
    throw new Error(`Search failed: ${response.statusText}`);
  }

  return response.json();
}

export async function getDocument(id) {
  const response = await fetch(config.docEndpoint(id));

  if (!response.ok) {
    throw new Error(`Failed to get document: ${response.statusText}`);
  }

  const data = await response.json();
  return data.doc;
}

export async function checkHealth() {
  const response = await fetch(config.healthEndpoint);

  if (!response.ok) {
    throw new Error('Service unhealthy');
  }

  return response.json();
}
