'use client';

import { useSettings } from '@/providers/SettingsProvider';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Filter } from 'lucide-react';

export default function DisplaySettings() {
  const { showOnlyBinanceCSV, setShowOnlyBinanceCSV } = useSettings();

  return (
    <Card className="bg-card/50 backdrop-blur-xl border-muted/50 shadow-xl overflow-hidden">
      <CardHeader>
        <CardTitle className="text-xl font-semibold">Display Preferences</CardTitle>
        <CardDescription>
          Customize how your transaction data is displayed in the ledger.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="flex items-center justify-between p-4 rounded-xl bg-muted/20 border border-muted/50">
          <div className="space-y-1">
            <h4 className="font-medium text-base">Binance Data Filtering</h4>
            <p className="text-sm text-muted-foreground max-w-md">
              When enabled, the transaction ledger will only display Binance data imported from CSV files, excluding API-synced records.
            </p>
          </div>
          <Button
            onClick={() => setShowOnlyBinanceCSV(!showOnlyBinanceCSV)}
            variant={showOnlyBinanceCSV ? "secondary" : "outline"}
            size="lg"
            className="rounded-full px-6 text-base font-medium min-w-[200px]"
          >
            <Filter className={`w-5 h-5 mr-2 ${showOnlyBinanceCSV ? 'fill-current' : ''}`} />
            {showOnlyBinanceCSV ? 'Showing Only CSV' : 'Show All Data'}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
