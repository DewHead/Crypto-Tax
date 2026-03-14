'use client';

import { useQuery } from '@tanstack/react-query';
import api from '@/lib/api';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { AlertCircle, TrendingUp, Hash, ReceiptText, Info, Library } from 'lucide-react';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import { motion, AnimatePresence } from 'framer-motion';

interface TaxSummaryProps {
  selectedYear: number | null;
  taxBracket: number;
}

export default function TaxSummary({ selectedYear, taxBracket }: TaxSummaryProps) {
  const { data: kpi, isLoading } = useQuery({
    queryKey: ['kpi', selectedYear, taxBracket],
    queryFn: async () => {
      const { data } = await api.get('/kpi', {
        params: { 
          year: selectedYear,
          tax_bracket: taxBracket
        }
      });
      return data;
    },
  });

  if (isLoading) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6 mb-8">
        {[1, 2, 3, 4, 5, 6, 7].map((i) => (
          <Card key={i} className="bg-card/50 backdrop-blur-sm border-muted/50">
            <CardHeader className="flex flex-row items-center justify-between pb-2 space-y-0">
              <Skeleton className="h-4 w-24" />
              <Skeleton className="h-4 w-4 rounded-full" />
            </CardHeader>
            <CardContent>
              <Skeleton className="h-10 w-32 mb-2" />
              <Skeleton className="h-3 w-48" />
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  const cards = [
    {
      title: 'Estimated Liability',
      value: `₪${kpi?.estimated_tax_ils?.toLocaleString() || '0'}`,
      subtitle: `Applied ${Math.round(taxBracket * 100)}% Tax Rate`,
      icon: ReceiptText,
      tooltip: 'Calculated by applying the selected tax bracket to your net gains and ordinary income.',
      color: 'text-destructive',
      glow: 'shadow-[0_0_20px_-5px_oklch(0.577_0.245_27.325_/_0.2)]',
      id: 'estimated-liability'
    },
    {
      title: 'Net Capital Gain',
      value: `₪${kpi?.net_capital_gain_ils?.toLocaleString() || '0'}`,
      subtitle: kpi?.carried_forward_loss_ils > 0 
        ? `₪${kpi?.carried_forward_loss_ils?.toLocaleString()} Loss Applied`
        : 'Field 91: Net Real Gain',
      icon: TrendingUp,
      color: kpi?.net_capital_gain_ils >= 0 ? 'text-green-500' : 'text-destructive',
      glow: kpi?.net_capital_gain_ils >= 0 
        ? 'shadow-[0_0_20px_-5px_oklch(0.7_0.2_140_/_0.2)]'
        : 'shadow-[0_0_20px_-5px_oklch(0.577_0.245_27.325_/_0.2)]',
      id: 'total-gain'
    },
    {
      title: 'Inflationary Gain',
      value: `₪${kpi?.inflationary_gain_ils?.toLocaleString() || '0'}`,
      subtitle: 'Field 256: Madad Adjustment',
      icon: TrendingUp,
      tooltip: 'Non-taxable inflationary component of your gains based on CPI (Madad).',
      color: 'text-orange-400',
      glow: 'shadow-[0_0_20px_-5px_oklch(0.7_0.15_60_/_0.1)]',
      id: 'inflationary-gain'
    },
    {
      title: 'Gross Capital Losses',
      value: `₪${kpi?.capital_losses_ils?.toLocaleString() || '0'}`,
      subtitle: 'Field 166: Total Losses',
      icon: TrendingUp,
      tooltip: 'Sum of all realized capital losses before offsetting gains.',
      color: 'text-destructive',
      glow: 'shadow-[0_0_20px_-5px_oklch(0.577_0.245_27.325_/_0.1)]',
      id: 'capital-losses'
    },
    {
      title: 'Ordinary Income',
      value: `₪${kpi?.ordinary_income_ils?.toLocaleString() || '0'}`,
      subtitle: 'Field 258/204: Earned',
      icon: TrendingUp,

      tooltip: 'Income recognized immediately upon receipt at fair market value.',
      color: 'text-blue-500',
      glow: 'shadow-[0_0_20px_-5px_oklch(0.6_0.15_240_/_0.2)]',
      id: 'ordinary-income'
    },
    {
      title: 'Trade Frequency',
      value: kpi?.trade_count,
      subtitle: 'Taxable events identified',
      icon: Hash,
      tooltip: 'Total number of taxable events in the current period.',
      color: 'text-foreground',
      glow: 'shadow-[0_0_20px_-5px_oklch(0.205_0_0_/_0.1)]',
      id: 'trade-count'
    },
    {
      title: 'Total Ledger',
      value: kpi?.total_transactions,
      subtitle: 'All indexed records',
      icon: Library,
      tooltip: 'Complete number of transactions imported from all sources.',
      color: 'text-muted-foreground',
      glow: 'shadow-[0_0_20px_-5px_oklch(0.205_0_0_/_0.05)]',
      id: 'total-transactions'
    }
  ];

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6 mb-8">
      <AnimatePresence mode="popLayout">
        {cards.map((card, idx) => (
          <motion.div
            key={card.title}
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: idx * 0.1, type: 'spring', stiffness: 260, damping: 20 }}
          >
            <Card className={`group relative overflow-hidden transition-all duration-300 hover:scale-[1.02] bg-card/50 backdrop-blur-xl border-muted/50 ${card.glow}`}>
              <CardHeader className="flex flex-row items-center justify-between pb-2 space-y-0">
                <div className="flex items-center gap-2">
                  <CardTitle className="text-xl font-medium text-muted-foreground">{card.title}</CardTitle>
                  {card.tooltip && (
                    <Tooltip>
                      <TooltipTrigger>
                        <Info className="w-4 h-4 text-muted-foreground/50 hover:text-muted-foreground cursor-help transition-colors" />
                      </TooltipTrigger>
                      <TooltipContent>
                        <p className="text-sm">{card.tooltip}</p>
                      </TooltipContent>
                    </Tooltip>
                  )}
                  {card.id === 'trade-count' && kpi?.high_frequency_warning && (
                    <Tooltip>
                      <TooltipTrigger>
                        <AlertCircle className="w-5 h-5 text-destructive animate-pulse cursor-help" />
                      </TooltipTrigger>
                      <TooltipContent side="top" className="max-w-[250px] bg-background border-destructive/30">
                        <div className="flex flex-col gap-1">
                          <p className="text-sm font-bold text-destructive flex items-center gap-1">
                            <AlertCircle className="w-4 h-4" /> ITA Alert
                          </p>
                          <p className="text-xs text-muted-foreground leading-relaxed">
                            High Frequency Trading Detected: Potential Business Classification (Mivchaney Esek). Crossing threshold of &gt;100 trades/yr.
                          </p>
                        </div>
                      </TooltipContent>
                    </Tooltip>
                  )}
                </div>
                <card.icon className="w-5 h-5 text-muted-foreground/70 group-hover:text-foreground transition-colors" />
              </CardHeader>
              <CardContent>
                <div data-testid={card.id} className={`text-4xl font-bold font-mono tracking-tight ${card.color}`}>
                  {card.value}
                </div>
                {card.subtitle && (
                  <p className="text-sm text-muted-foreground/80 mt-2 font-medium">{card.subtitle}</p>
                )}
              </CardContent>
            </Card>
          </motion.div>
        ))}
      </AnimatePresence>
    </div>
  );
}
