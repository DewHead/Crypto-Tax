'use client';

import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { createKey } from '@/lib/api';
import { Database, Upload, Clock, Loader2, ChevronRight, CheckCircle2, RefreshCw } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';

export type WizardStep = 'choose-method' | 'api-setup' | 'csv-setup' | 'setup-later' | 'success';

interface AddWalletWizardProps {
  onComplete?: () => void;
  initialStep?: WizardStep;
  initialExchange?: string;
}

export default function AddWalletWizard({ onComplete, initialStep = 'choose-method', initialExchange = 'binance' }: AddWalletWizardProps) {
  const queryClient = useQueryClient();
  const [step, setStep] = useState<WizardStep>(initialStep);
  const [exchangeName, setExchangeName] = useState(initialExchange);
  const [apiKey, setApiKey] = useState('');
  const [apiSecret, setApiSecret] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [file, setFile] = useState<File | null>(null);

  const handleApiSetup = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!apiKey || !apiSecret) {
      toast.error('API Key and Secret are required');
      return;
    }
    setIsProcessing(true);
    try {
      await createKey(exchangeName, apiKey, apiSecret);
      setStep('success');
      queryClient.invalidateQueries({ queryKey: ['keys'] });
      queryClient.invalidateQueries({ queryKey: ['data-sources'] });
    } catch (e) {
      console.error("Failed to add key", e);
      toast.error('Failed to add API key');
    } finally {
      setIsProcessing(false);
    }
  };

  const handleSetupLater = async () => {
    setIsProcessing(true);
    try {
      await createKey(exchangeName);
      setStep('success');
      queryClient.invalidateQueries({ queryKey: ['keys'] });
      queryClient.invalidateQueries({ queryKey: ['data-sources'] });
    } catch (e) {
      console.error("Failed to add placeholder", e);
      toast.error('Failed to add wallet placeholder');
    } finally {
      setIsProcessing(false);
    }
  };

  const handleCsvUpload = async () => {
    if (!file) return;

    setIsProcessing(true);
    const formData = new FormData();
    formData.append('file', file);

    try {
      // First create the wallet placeholder if it doesn't exist
      await createKey(exchangeName);
      
      const response = await fetch('http://127.0.0.1:8000/api/upload', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        throw new Error('Failed to upload historical data.');
      }

      setStep('success');
      queryClient.invalidateQueries({ queryKey: ['ledger'] });
      queryClient.invalidateQueries({ queryKey: ['kpi'] });
      queryClient.invalidateQueries({ queryKey: ['data-sources'] });
      queryClient.invalidateQueries({ queryKey: ['keys'] });
    } catch (error) {
      console.error('Upload error:', error);
      toast.error('An error occurred while uploading the file.');
    } finally {
      setIsProcessing(false);
    }
  };

  const reset = () => {
    setStep('choose-method');
    setApiKey('');
    setApiSecret('');
    setFile(null);
    if (onComplete) onComplete();
  };

  if (step === 'choose-method') {
    return (
      <Card className="border-2 border-primary/10 shadow-lg">
        <CardHeader>
          <CardTitle className="text-2xl font-bold flex items-center gap-3">
            <Database className="w-6 h-6 text-primary" />
            Add New Wallet
          </CardTitle>
          <CardDescription className="text-base">
            Choose how you want to import your transaction history.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4">
          <button 
            onClick={() => setStep('api-setup')}
            className="flex items-center gap-4 p-4 rounded-xl border-2 border-muted hover:border-primary hover:bg-primary/5 transition-all text-start group"
          >
            <div className="w-12 h-12 rounded-full bg-primary/10 flex items-center justify-center text-primary group-hover:scale-110 transition-transform">
              <RefreshCw className="w-6 h-6" />
            </div>
            <div className="flex-1">
              <h3 className="font-semibold text-lg">Connect via API</h3>
              <p className="text-sm text-muted-foreground">Automated sync for trades, deposits, and withdrawals.</p>
            </div>
            <ChevronRight className="w-5 h-5 text-muted-foreground" />
          </button>

          <button 
            onClick={() => setStep('csv-setup')}
            className="flex items-center gap-4 p-4 rounded-xl border-2 border-muted hover:border-primary hover:bg-primary/5 transition-all text-start group"
          >
            <div className="w-12 h-12 rounded-full bg-blue-500/10 flex items-center justify-center text-blue-500 group-hover:scale-110 transition-transform">
              <Upload className="w-6 h-6" />
            </div>
            <div className="flex-1">
              <h3 className="font-semibold text-lg">Import CSV / ZIP</h3>
              <p className="text-sm text-muted-foreground">Upload historical data statements manually.</p>
            </div>
            <ChevronRight className="w-5 h-5 text-muted-foreground" />
          </button>

          <button 
            onClick={() => setStep('setup-later')}
            className="flex items-center gap-4 p-4 rounded-xl border-2 border-muted hover:border-primary hover:bg-primary/5 transition-all text-start group"
          >
            <div className="w-12 h-12 rounded-full bg-amber-500/10 flex items-center justify-center text-amber-500 group-hover:scale-110 transition-transform">
              <Clock className="w-6 h-6" />
            </div>
            <div className="flex-1">
              <h3 className="font-semibold text-lg">Setup Later</h3>
              <p className="text-sm text-muted-foreground">Add the wallet now and provide data at a later time.</p>
            </div>
            <ChevronRight className="w-5 h-5 text-muted-foreground" />
          </button>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="border-2 border-primary/10 shadow-lg">
      <CardHeader>
        <button 
          onClick={() => setStep('choose-method')}
          className="text-sm text-muted-foreground hover:text-primary mb-2 flex items-center gap-1"
        >
          ← Back to options
        </button>
        <CardTitle className="text-2xl font-bold flex items-center gap-3">
          {step === 'api-setup' && <><RefreshCw className="w-6 h-6 text-primary" /> API Connection</>}
          {step === 'csv-setup' && <><Upload className="w-6 h-6 text-blue-500" /> CSV Import</>}
          {step === 'setup-later' && <><Clock className="w-6 h-6 text-amber-500" /> Placeholder Wallet</>}
          {step === 'success' && <><CheckCircle2 className="w-6 h-6 text-green-500" /> Success!</>}
        </CardTitle>
      </CardHeader>
      <CardContent>
        {step === 'api-setup' && (
          <form onSubmit={handleApiSetup} className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Exchange</label>
              <select 
                value={exchangeName} 
                onChange={e => setExchangeName(e.target.value)}
                className="w-full h-10 rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              >
                <option value="binance">Binance</option>
                <option value="kraken">Kraken (Spot)</option>
                <option value="krakenfutures">Kraken (Futures)</option>
              </select>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">API Key</label>
              <input 
                type="text" 
                value={apiKey} 
                onChange={e => setApiKey(e.target.value)} 
                className="w-full h-10 rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                placeholder="Key" 
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">API Secret</label>
              <input 
                type="password" 
                value={apiSecret} 
                onChange={e => setApiSecret(e.target.value)} 
                className="w-full h-10 rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                placeholder="Secret" 
              />
            </div>
            <Button type="submit" disabled={isProcessing} className="w-full text-base py-6">
              {isProcessing ? <Loader2 className="w-5 h-5 animate-spin me-2" /> : 'Connect Wallet'}
            </Button>
          </form>
        )}

        {step === 'csv-setup' && (
          <div className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Exchange</label>
              <select 
                value={exchangeName} 
                onChange={e => setExchangeName(e.target.value)}
                className="w-full h-10 rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              >
                <option value="binance">Binance</option>
                <option value="kraken" disabled>Kraken (Coming Soon)</option>
              </select>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Statement File (ZIP)</label>
              <input 
                type="file" 
                accept=".zip"
                onChange={e => setFile(e.target.files?.[0] || null)}
                className="w-full h-12 rounded-md border border-input bg-background px-3 py-2 text-sm file:me-4 file:py-1 file:px-4 file:rounded-full file:border-0 file:text-sm file:font-semibold file:bg-primary/10 file:text-primary hover:file:bg-primary/20"
              />
              <p className="text-xs text-muted-foreground">Currently supports Binance &quot;All Statements&quot; or &quot;Trades&quot; ZIP files.</p>
            </div>
            <Button onClick={handleCsvUpload} disabled={isProcessing || !file} className="w-full text-base py-6">
              {isProcessing ? <Loader2 className="w-5 h-5 animate-spin me-2" /> : 'Upload & Import'}
            </Button>
          </div>
        )}

        {step === 'setup-later' && (
          <div className="space-y-6">
            <div className="space-y-2">
              <label className="text-sm font-medium">Select Exchange</label>
              <select 
                value={exchangeName} 
                onChange={e => setExchangeName(e.target.value)}
                className="w-full h-10 rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
              >
                <option value="binance">Binance</option>
                <option value="kraken">Kraken (Spot)</option>
                <option value="krakenfutures">Kraken (Futures)</option>
              </select>
            </div>
            <p className="text-sm text-muted-foreground">
              This will create a wallet entry in your settings. You can add API keys or upload CSV files later to populate it with data.
            </p>
            <Button onClick={handleSetupLater} disabled={isProcessing} className="w-full text-base py-6">
              {isProcessing ? <Loader2 className="w-5 h-5 animate-spin me-2" /> : 'Create Placeholder'}
            </Button>
          </div>
        )}

        {step === 'success' && (
          <div className="py-6 text-center space-y-4">
            <div className="w-16 h-16 bg-green-100 text-green-600 rounded-full flex items-center justify-center mx-auto">
              <CheckCircle2 className="w-10 h-10" />
            </div>
            <div>
              <h3 className="text-xl font-bold">Wallet Added!</h3>
              <p className="text-muted-foreground">Your wallet has been successfully added to the system.</p>
            </div>
            <Button onClick={reset} className="w-full mt-4">
              Finish
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
