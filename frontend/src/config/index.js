const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api';

export const config = {
  apiBaseUrl: API_BASE_URL,
  searchEndpoint: `${API_BASE_URL}/search`,
  docEndpoint: (id) => `${API_BASE_URL}/doc/${id}`,
  healthEndpoint: `${API_BASE_URL}/healthz`
};

export default config;
