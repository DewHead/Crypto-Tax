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

export interface AppSetting {
  key: string;
  value: string | null;
}

export const fetchSetting = async (key: string): Promise<AppSetting> => {
  const response = await api.get(`/settings/${key}`);
  return response.data;
};

export const updateSetting = async (key: string, value: string | null): Promise<AppSetting> => {
  const response = await api.post(`/settings/${key}`, { value });
  return response.data;
};

export const deleteDataSource = async (exchangeName: string, wipeCsv: boolean = false): Promise<void> => {
  await api.delete(`/data-sources/${exchangeName}`, { params: { wipe_csv: wipeCsv } });
};

export const syncKey = async (id: number): Promise<void> => {
  await api.post(`/sync/${id}`);
};

export const syncAllData = async (): Promise<void> => {
  await api.post('/sync');
};

export const recalculateTaxes = async (): Promise<void> => {
  await api.post('/recalculate');
};

export const sendTestEmail = async (): Promise<void> => {
  await api.post('/test-email');
};

export const updateManualCostBasis = async (txId: number, manualCostBasisIls: number, manualPurchaseDate?: string): Promise<void> => {
  await api.post(`/transactions/${txId}/manual-cost-basis`, {
    manual_cost_basis_ils: manualCostBasisIls,
    manual_purchase_date: manualPurchaseDate
  });
};

export default api;
