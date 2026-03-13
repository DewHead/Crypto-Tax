'use client';

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import api, { fetchKeys, deleteKey, syncKey, ExchangeKey, fetchDataSources, deleteDataSource, DataSource } from '@/lib/api';
import { RefreshCw, Database, Trash2, AlertTriangle, Key, Plus, FileUp } from 'lucide-react';
import { toast } from 'sonner';
import AddWalletWizard, { WizardStep } from './AddWalletWizard';
import { Button } from '@/components/ui/button';

export default function ApiKeysManager() {
  const queryClient = useQueryClient();
  const [showWizard, setShowWizard] = useState(false);
  const [wizardStep, setWizardStep] = useState<WizardStep>('choose-method');
  const [wizardExchange, setWizardExchange] = useState('binance');

  const { data: keys = [] } = useQuery({
    queryKey: ['keys'],
    queryFn: fetchKeys,
    refetchInterval: (query) => {
      const isAnySyncing = query.state.data?.some((key: ExchangeKey) => key.is_syncing === 1);
      return isAnySyncing ? 3000 : 30000;
    },
  });

  const { data: dataSources = [] } = useQuery({
    queryKey: ['data-sources'],
    queryFn: fetchDataSources,
  });

  const displayExchangeName = (name: string) => {
    if (name === 'krakenfutures') return 'Kraken (Futures)';
    if (name === 'kraken') return 'Kraken (Spot)';
    if (name === 'binance') return 'Binance';
    return name.charAt(0).toUpperCase() + name.slice(1);
  };

  const handleDelete = async (id: number, name: string) => {
    if (!window.confirm(`Are you sure you want to delete ${displayExchangeName(name)}? This will PERMANENTLY delete ALL transaction data (API and CSV) for this wallet and recalculate your taxes.`)) {
      return;
    }
    
    try {
      await deleteKey(id);
      queryClient.invalidateQueries({ queryKey: ['keys'] });
      queryClient.invalidateQueries({ queryKey: ['data-sources'] });
      queryClient.invalidateQueries({ queryKey: ['ledger'] });
      queryClient.invalidateQueries({ queryKey: ['kpi'] });
      toast.success(`${displayExchangeName(name)} and all associated data deleted`);
    } catch (e) {
      console.error("Failed to delete key", e);
      toast.error('Failed to delete API key');
    }
  };

  const handleDeleteDataSource = async (name: string) => {
    if (!window.confirm(`Are you sure you want to delete ALL data for ${displayExchangeName(name)}? This includes both API and CSV transactions and will recalculate your taxes.`)) {
      return;
    }
    
    try {
      await deleteDataSource(name);
      queryClient.invalidateQueries({ queryKey: ['keys'] });
      queryClient.invalidateQueries({ queryKey: ['data-sources'] });
      queryClient.invalidateQueries({ queryKey: ['ledger'] });
      queryClient.invalidateQueries({ queryKey: ['kpi'] });
      toast.success(`All data for ${displayExchangeName(name)} has been deleted`);
    } catch (e) {
      console.error("Failed to delete data source", e);
      toast.error('Failed to delete data source');
    }
  };

  const handleSync = async (id: number) => {
    const toastId = toast.loading('Starting sync...');
    try {
      await syncKey(id);
      queryClient.invalidateQueries({ queryKey: ['keys'] });
      toast.success('Sync started in background', { id: toastId });
    } catch (e) {
      console.error("Failed to sync key", e);
      toast.error('Failed to sync key', { id: toastId });
    }
  };

  const openWizard = (step: WizardStep = 'choose-method', exchange: string = 'binance') => {
    setWizardStep(step);
    setWizardExchange(exchange);
    setShowWizard(true);
  };

  return (
    <div className="space-y-10">
      {showWizard ? (
        <div className="space-y-4">
          <div className="flex justify-between items-center">
            <h2 className="text-xl font-semibold">Adding New Wallet</h2>
            <Button variant="ghost" onClick={() => setShowWizard(false)}>Cancel</Button>
          </div>
          <AddWalletWizard 
            onComplete={() => setShowWizard(false)} 
            initialStep={wizardStep}
            initialExchange={wizardExchange}
          />
        </div>
      ) : (
        <Button 
          onClick={() => openWizard()} 
          className="w-full h-20 text-lg font-bold border-2 border-dashed border-primary/20 bg-primary/5 hover:bg-primary/10 text-primary"
          variant="outline"
        >
          <Plus className="w-6 h-6 mr-2" />
          Add New Wallet (API or CSV)
        </Button>
      )}

      <div className="bg-card text-card-foreground border rounded-lg p-6 shadow-sm">
        <h2 className="text-xl font-semibold mb-4 flex items-center gap-2">
          <Database className="w-5 h-5 text-primary" />
          Wallets & Connections
        </h2>
        {keys.length === 0 ? (
          <p className="text-base text-muted-foreground py-8 text-center border-2 border-dashed rounded-lg">
            No wallets added yet. Click &quot;Add New Wallet&quot; above to get started.
          </p>
        ) : (
          <div className="w-full relative overflow-auto">
            <table className="w-full caption-bottom text-base">
              <thead className="[&_tr]:border-b">
                <tr className="border-b transition-colors hover:bg-muted/50">
                  <th className="h-14 px-6 text-left align-middle font-medium text-muted-foreground">Exchange</th>
                  <th className="h-14 px-6 text-left align-middle font-medium text-muted-foreground">Status</th>
                  <th className="h-14 px-6 text-left align-middle font-medium text-muted-foreground">API Connection</th>
                  <th className="h-14 px-6 text-left align-middle font-medium text-muted-foreground">Actions</th>
                </tr>
              </thead>
              <tbody className="[&_tr:last-child]:border-0">
                {keys.map(key => (
                  <tr key={key.id} className="border-b transition-colors hover:bg-muted/50">
                    <td className="p-6 align-middle font-medium">{displayExchangeName(key.exchange_name)}</td>
                    <td className="p-6 align-middle">
                      {key.api_key ? (
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                          Connected
                        </span>
                      ) : (
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-amber-100 text-amber-800">
                          Setup Pending
                        </span>
                      )}
                    </td>
                    <td className="p-6 align-middle font-mono">
                      {key.api_key ? (
                        <span className="flex items-center gap-2">
                          <Key className="w-3 h-3 text-muted-foreground" />
                          {key.api_key.substring(0, 4)}...{key.api_key.substring(key.api_key.length - 4)}
                        </span>
                      ) : (
                        <span className="text-muted-foreground italic text-sm">No API Key</span>
                      )}
                    </td>
                    <td className="p-6 align-middle space-x-4">
                      {key.api_key ? (
                        <button 
                          onClick={() => handleSync(key.id)}
                          disabled={key.is_syncing === 1}
                          className={`inline-flex items-center text-primary hover:text-primary/80 text-base font-medium disabled:opacity-50`}
                        >
                          <RefreshCw className={`w-4 h-4 mr-1 ${key.is_syncing === 1 ? 'animate-spin' : ''}`} />
                          {key.is_syncing === 1 ? 'Syncing...' : 'Sync Now'}
                        </button>
                      ) : (
                        <button 
                          onClick={() => openWizard('api-setup', key.exchange_name)}
                          className="text-primary hover:text-primary/80 text-base font-medium inline-flex items-center"
                        >
                          <Plus className="w-4 h-4 mr-1" />
                          Connect API
                        </button>
                      )}
                      <button 
                        onClick={() => handleDelete(key.id, key.exchange_name)}
                        className="text-red-500 hover:text-red-700 text-base font-medium inline-flex items-center"
                      >
                        <Trash2 className="w-4 h-4 mr-1" />
                        Delete
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="bg-card text-card-foreground border rounded-lg p-6 shadow-sm">
        <h2 className="text-xl font-semibold mb-4 flex items-center gap-2">
          <Database className="w-5 h-5 text-primary" />
          Data Source Integrity
        </h2>
        <p className="text-muted-foreground mb-6">
          Overview of transaction records imported from various sources.
        </p>
        
        {dataSources.length === 0 ? (
          <p className="text-base text-muted-foreground">No data found in the system.</p>
        ) : (
          <div className="w-full relative overflow-auto">
            <table className="w-full caption-bottom text-base">
              <thead className="[&_tr]:border-b">
                <tr className="border-b transition-colors hover:bg-muted/50">
                  <th className="h-14 px-6 text-left align-middle font-medium text-muted-foreground">Exchange Source</th>
                  <th className="h-14 px-6 text-left align-middle font-medium text-muted-foreground text-center">API Records</th>
                  <th className="h-14 px-6 text-left align-middle font-medium text-muted-foreground text-center">CSV Records</th>
                  <th className="h-14 px-6 text-left align-middle font-medium text-muted-foreground">Actions</th>
                </tr>
              </thead>
              <tbody className="[&_tr:last-child]:border-0">
                {dataSources.map(ds => (
                  <tr key={ds.exchange} className="border-b transition-colors hover:bg-muted/50">
                    <td className="p-6 align-middle font-medium">
                      {displayExchangeName(ds.exchange)}
                      {!ds.has_key && ds.api_count > 0 && (
                        <span className="ml-2 text-[10px] bg-amber-100 text-amber-700 px-1.5 py-0.5 rounded-full font-bold">API KEY MISSING</span>
                      )}
                    </td>
                    <td className="p-6 align-middle text-center font-mono">{ds.api_count.toLocaleString()}</td>
                    <td className="p-6 align-middle text-center font-mono">{ds.csv_count.toLocaleString()}</td>
                    <td className="p-6 align-middle space-x-4">
                      <button 
                        onClick={() => openWizard('csv-setup', ds.exchange)}
                        className="text-blue-500 hover:text-blue-700 text-base font-medium inline-flex items-center"
                      >
                        <FileUp className="w-4 h-4 mr-1" />
                        Add CSV
                      </button>
                      <button 
                        onClick={() => handleDeleteDataSource(ds.exchange)}
                        className="text-red-500 hover:text-red-700 text-base font-medium inline-flex items-center"
                      >
                        <AlertTriangle className="w-4 h-4 mr-1" />
                        Wipe Data
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
