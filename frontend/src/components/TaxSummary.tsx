"use client";

import { useQuery } from '@tanstack/react-query';
import api from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { motion, AnimatePresence } from 'framer-motion';
import { 
  TrendingUp, 
  Wallet, 
  Receipt, 
  ShieldCheck, 
  AlertCircle,
  Clock,
  ArrowUpRight
} from 'lucide-react';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface TaxSummaryProps {
  selectedYear: number | null;
  taxBracket: number;
}

export default function TaxSummary({ selectedYear, taxBracket }: TaxSummaryProps) {
  const { data: kpi, isLoading } = useQuery({
    queryKey: ['kpi', selectedYear, taxBracket],
    queryFn: async () => {
      const { data } = await api.get('/kpi', {
        params: { year: selectedYear, tax_bracket: taxBracket }
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
              <Skeleton className="h-8 w-32 mb-2" />
              <Skeleton className="h-3 w-40" />
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  const cards = [
    {
      title: 'Net Realized Gain',
      value: `₪${kpi?.net_capital_gain_ils?.toLocaleString() || '0'}`,
      subtitle: kpi?.carried_forward_loss_ils > 0 
        ? `₪${kpi?.carried_forward_loss_ils?.toLocaleString()} Loss Applied`
        : 'Field 91: Net Real Gain',
      icon: TrendingUp,
      color: kpi?.net_capital_gain_ils >= 0 ? 'text-green-500' : 'text-destructive',
      glow: kpi?.net_capital_gain_ils >= 0 
        ? 'shadow-[0_0_20px_-5px_oklch(0.723_0.219_149.579_/_0.2)]' 
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
      title: 'Est. Tax Liability',
      value: `₪${kpi?.estimated_tax_ils?.toLocaleString() || '0'}`,
      subtitle: 'Before Local Offsets',
      icon: ShieldCheck,
      color: 'text-primary',
      glow: 'shadow-[0_0_20px_-5px_oklch(0.707_0.022_261.625_/_0.2)]',
      id: 'estimated-liability'
    },
    {
      title: 'Trading Activity',
      value: kpi?.trade_count?.toLocaleString() || '0',
      subtitle: 'Taxable Disposals',
      icon: Receipt,
      color: 'text-muted-foreground',
      id: 'trade-count'
    },
    {
      title: 'Audit Health',
      value: kpi?.high_frequency_warning ? 'High Vol' : 'Healthy',
      subtitle: kpi?.high_frequency_warning ? 'Over 100 Trades' : 'Standard Trader',
      icon: kpi?.high_frequency_warning ? AlertCircle : ShieldCheck,
      color: kpi?.high_frequency_warning ? 'text-amber-500' : 'text-green-500',
      id: 'ita-alert'
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
            transition={{ delay: idx * 0.1 }}
            layout
          >
            <Card className={`group bg-card/40 backdrop-blur-xl border-muted/40 hover:border-primary/30 transition-all duration-500 hover:scale-[1.02] active:scale-[0.98] ${card.glow || ''}`}>
              <CardHeader className="flex flex-row items-center justify-between pb-3 space-y-0">
                <CardTitle className="text-xs font-bold uppercase tracking-widest text-muted-foreground/70 group-hover:text-primary/70 transition-colors">
                  {card.title}
                </CardTitle>
                <div className={`p-2 rounded-lg bg-background/50 border border-muted/20 group-hover:border-primary/20 transition-colors`}>
                  <card.icon className={`h-4 w-4 ${card.color}`} />
                </div>
              </CardHeader>
              <CardContent>
                <div className="flex items-baseline gap-2">
                  <div className={`text-3xl font-bold tracking-tight ${card.color}`} data-testid={card.id}>
                    {card.value}
                  </div>
                  {idx < 2 && (
                    <div className="text-[10px] font-bold px-1.5 py-0.5 rounded bg-green-500/10 text-green-500 flex items-center gap-0.5">
                      <ArrowUpRight className="w-2.5 h-2.5" />
                      12%
                    </div>
                  )}
                </div>
                <div className="flex items-center gap-2 mt-2">
                  <span className="text-xs font-medium text-muted-foreground/60">{card.subtitle}</span>
                </div>
              </CardContent>
            </Card>
          </motion.div>
        ))}
      </AnimatePresence>
    </div>
  );
}
