'use client';

import { useQuery } from '@tanstack/react-query';
import api from '@/lib/api';
import { AlertCircle, RefreshCw } from 'lucide-react';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';

export default function HeaderAlert() {
  const { data: kpi } = useQuery({
    queryKey: ['kpi'],
    queryFn: async () => {
      const { data } = await api.get('/kpi');
      return data;
    },
  });

  const { data: keys } = useQuery({
    queryKey: ['keys'],
    queryFn: async () => {
      const { data } = await api.get('/keys');
      return data;
    },
    refetchInterval: (query) => {
      const isAnySyncing = query.state.data?.some((key: any) => key.is_syncing === 1);
      return isAnySyncing ? 3000 : 10000; // Poll faster if syncing
    },
  });

  const isSyncing = keys?.some((key: any) => key.is_syncing === 1);

  return (
    <div className="flex items-center gap-3">
      {isSyncing && (
        <Tooltip>
          <TooltipTrigger className="flex items-center gap-2 px-3 py-1 rounded-full bg-primary/10 text-primary border border-primary/20 hover:bg-primary/20 transition-colors animate-in fade-in slide-in-from-end-4 duration-500">
            <RefreshCw className="w-4 h-4 animate-spin" />
            <span className="text-xs font-bold uppercase tracking-wider hidden sm:inline">Syncing</span>
          </TooltipTrigger>
          <TooltipContent side="bottom" align="end" className="max-w-[200px] bg-background border-primary/30 shadow-xl">
            <p className="text-xs text-muted-foreground">
              Fetching latest transaction data from connected exchanges...
            </p>
          </TooltipContent>
        </Tooltip>
      )}

      {kpi?.is_business_threshold_crossed && (
        <Tooltip>
          <TooltipTrigger className="flex items-center gap-2 px-3 py-1 rounded-full bg-destructive/10 text-destructive border border-destructive/20 hover:bg-destructive/20 transition-colors animate-in fade-in slide-in-from-end-4 duration-500">
            <AlertCircle className="w-4 h-4 animate-pulse" />
            <span className="text-xs font-bold uppercase tracking-wider hidden sm:inline">ITA Alert</span>
          </TooltipTrigger>
          <TooltipContent side="bottom" align="end" className="max-w-[280px] bg-background border-destructive/30 shadow-xl">
            <div className="flex flex-col gap-1.5 p-1">
              <p className="text-sm font-bold text-destructive flex items-center gap-1.5">
                <AlertCircle className="w-4 h-4" /> High Volume Trader Alert
              </p>
              <p className="text-xs text-muted-foreground leading-relaxed">
                You have exceeded 100 trades this year. The Israeli Tax Authority may classify you as a business, potentially leading to higher tax rates.
              </p>
            </div>
          </TooltipContent>
        </Tooltip>
      )}
    </div>
  );
}
