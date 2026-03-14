"use client";

import { useState } from 'react';
import Link from 'next/link';
import { motion } from 'framer-motion';
import TaxSummary from '@/components/TaxSummary';
import TransactionsTable from '@/components/TransactionsTable';
import YearSelector from '@/components/YearSelector';
import { ModeToggle } from '@/components/mode-toggle';
import { Button } from '@/components/ui/button';
import { PlusCircle, Settings } from 'lucide-react';
import AddWalletWizard from '@/components/AddWalletWizard';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { buttonVariants } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { ChevronDown, Percent, AlertTriangle, X } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import api from '@/lib/api';

export default function Home() {
  const [selectedYear, setSelectedYear] = useState<number | null>(new Date().getFullYear());
  const [taxBracket, setTaxBracket] = useState<number>(0.25);
  const [isBannerDismissed, setIsBannerDismissed] = useState(false);

  const { data: kpi } = useQuery({
    queryKey: ['kpi', selectedYear, taxBracket],
    queryFn: async () => {
      const { data } = await api.get('/kpi', {
        params: { year: selectedYear, tax_bracket: taxBracket }
      });
      return data;
    },
  });

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-muted/20">
      <header className="h-20 border-b bg-background/50 backdrop-blur-md sticky top-0 z-50 flex items-center shrink-0">
        <div className="container mx-auto max-w-7xl flex justify-between items-center px-10">
          <div className="flex items-center gap-12">
            <Link href="/" className="flex items-center gap-3 group">
              <div className="bg-primary p-2.5 rounded-xl shadow-lg shadow-primary/20 group-hover:scale-105 transition-transform">
                <div className="w-6 h-6 border-2 border-white rounded-md flex items-center justify-center font-bold text-white text-xs">₪</div>
              </div>
              <h1 className="text-xl font-bold tracking-tight">Crypto Tax <span className="text-primary">Dashboard</span></h1>
            </Link>
            
            <nav className="hidden md:flex items-center gap-1">
              <Link href="/" className={cn(buttonVariants({ variant: "ghost", size: "sm" }), "px-4 font-medium text-primary bg-primary/10 rounded-full")}>Overview</Link>
              <Link href="/settings" className={cn(buttonVariants({ variant: "ghost", size: "sm" }), "px-4 font-medium text-muted-foreground hover:text-foreground rounded-full")}>Data Sources</Link>
            </nav>
          </div>

          <div className="flex items-center gap-6">
            <div className="flex items-center bg-card/50 border rounded-full pl-5 pr-2 py-1.5 shadow-sm">
              <span className="text-xs font-bold uppercase tracking-widest text-muted-foreground mr-4">Tax Year</span>
              <YearSelector selectedYear={selectedYear} onSelect={setSelectedYear} />
              
              <div className="w-px h-6 bg-muted mx-3" />
              
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="ghost" size="sm" className="h-8 rounded-full px-3 hover:bg-muted/50">
                    <Percent className="w-3.5 h-3.5 mr-2 text-primary" />
                    <span className="text-sm font-semibold">{(taxBracket * 100).toFixed(0)}%</span>
                    <ChevronDown className="w-3.5 h-3.5 ml-1.5 opacity-40" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-32 rounded-xl p-1">
                  {[0.25, 0.35, 0.47, 0.50].map((rate) => (
                    <DropdownMenuItem 
                      key={rate} 
                      onClick={() => setTaxBracket(rate)}
                      className={cn("rounded-lg cursor-pointer", taxBracket === rate && "bg-primary/10 text-primary font-bold")}
                    >
                      {(rate * 100).toFixed(0)}% Bracket
                    </DropdownMenuItem>
                  ))}
                </DropdownMenuContent>
              </DropdownMenu>
            </div>

            <div className="flex items-center gap-3">
              <AddWalletWizard />
              <div className="w-px h-6 bg-muted mx-1" />
              <ModeToggle />
            </div>
          </div>
        </div>
      </header>

      <main className="flex-1 flex flex-col min-h-0 space-y-6 p-10 pt-8 container mx-auto max-w-7xl overflow-hidden">
        {kpi?.issue_count > 0 && !isBannerDismissed && (
          <div className="bg-amber-500/10 border border-amber-500/20 rounded-2xl p-6 flex items-start gap-5 animate-in fade-in slide-in-from-top-4 duration-500">
            <div className="bg-amber-500 rounded-full p-2 mt-1">
              <AlertTriangle className="w-6 h-6 text-white" />
            </div>
            <div className="flex-1">
              <h3 className="text-lg font-bold text-amber-700">Warning: Missing Purchase History</h3>
              <p className="text-amber-800/80 mt-1 max-w-2xl leading-relaxed">
                We found {kpi.issue_count} transactions with missing cost basis. For these events, the engine falls back to <b>Zero Cost Basis</b>, potentially inflating your tax bill significantly.
              </p>
              <div className="flex gap-4 mt-4">
                <Link href="/settings" className="text-sm font-bold bg-amber-500 text-white px-4 py-2 rounded-lg hover:bg-amber-600 transition-colors">
                  Upload Missing CSVs
                </Link>
                <button 
                  onClick={() => setIsBannerDismissed(true)}
                  className="text-sm font-bold text-amber-600 hover:text-amber-700"
                >
                  I understand the risk, proceed
                </button>
              </div>
            </div>
            <button 
              onClick={() => setIsBannerDismissed(true)}
              className="text-muted-foreground hover:text-foreground transition-colors"
            >
              <X className="w-5 h-5" />
            </button>
          </div>
        )}
        <TaxSummary selectedYear={selectedYear} taxBracket={taxBracket} />
        <TransactionsTable selectedYear={selectedYear} />
      </main>
    </div>
  );
}
