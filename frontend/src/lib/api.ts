import axios from 'axios';

const api = axios.create({
  baseURL: 'http://127.0.0.1:8000/api',
});

// Interceptor for errors
api.interceptors.response.use(
  (response) => response,
  (error) => {
    console.error('API Error:', error.response?.data || error.message);
    return Promise.reject(error);
  }
);

export interface ExchangeKey {
  id: number;
  exchange_name: string;
  api_key: string;
  created_at: string;
  is_syncing?: number;
}

export const fetchKeys = async (): Promise<ExchangeKey[]> => {
  const response = await api.get('/keys');
  return response.data;
};

export const createKey = async (exchange_name: string, api_key?: string, api_secret?: string): Promise<ExchangeKey> => {
  const response = await api.post('/keys', { exchange_name, api_key, api_secret });
  return response.data;
};

export const deleteKey = async (id: number): Promise<void> => {
  await api.delete(`/keys/${id}`);
};

export interface DataSource {
  exchange: string;
  api_count: number;
  csv_count: number;
  has_key: boolean;
  key_id?: number;
}

export const fetchDataSources = async (): Promise<DataSource[]> => {
  const response = await api.get('/data-sources');
  return response.data;
};

export const deleteDataSource = async (exchangeName: string): Promise<void> => {
  await api.delete(`/data-sources/${exchangeName}`);
};

export const syncKey = async (id: number): Promise<void> => {
  await api.post(`/sync/${id}`);
};

export default api;
