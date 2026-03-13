'use client';

import { useState } from 'react';
import TaxSummary from '@/components/TaxSummary';
import TransactionsTable from '@/components/TransactionsTable';
import { ModeToggle } from '@/components/mode-toggle';
import HeaderAlert from '@/components/HeaderAlert';
import YearSelector from '@/components/YearSelector';
import Link from 'next/link';
import { 
  DropdownMenu, 
  DropdownMenuContent, 
  DropdownMenuItem, 
  DropdownMenuLabel, 
  DropdownMenuSeparator, 
  DropdownMenuTrigger 
} from '@/components/ui/dropdown-menu';
import { buttonVariants } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { ChevronDown, Percent } from 'lucide-react';

export default function Home() {
  const [selectedYear, setSelectedYear] = useState<number | null>(new Date().getFullYear());
  const [taxBracket, setTaxBracket] = useState<number>(0.25);

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-muted/20">
      <header className="flex-none z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
        <div className="container mx-auto flex h-16 items-center justify-between px-4 max-w-7xl">
          <div className="flex flex-col">
            <h1 className="text-2xl font-bold tracking-tight text-foreground">
              Israeli Crypto Tax Calculator
            </h1>
            <p className="text-sm text-muted-foreground hidden md:block">
              ITA 2026 Compliant • FIFO Matching • Bank of Israel Exchange Rates
            </p>
          </div>
          <div className="flex items-center gap-4">
            <DropdownMenu>
              <DropdownMenuTrigger className={cn(buttonVariants({ variant: "outline" }), "flex items-center gap-2 border-muted/50")}>
                <Percent className="w-4 h-4 text-muted-foreground" />
                <span>{Math.round(taxBracket * 100)}% Bracket</span>
                <ChevronDown className="w-4 h-4 text-muted-foreground" />
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-56">
                <DropdownMenuLabel>Tax Classification</DropdownMenuLabel>
                <DropdownMenuSeparator />
                <DropdownMenuItem onClick={() => setTaxBracket(0.25)}>
                  Capital Gains (25%)
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => setTaxBracket(0.31)}>
                  Business: Bracket 1 (31%)
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => setTaxBracket(0.35)}>
                  Business: Bracket 2 (35%)
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => setTaxBracket(0.47)}>
                  Business: Max (47% + Surtax)
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
            <YearSelector selectedYear={selectedYear} onYearChange={setSelectedYear} />
            <HeaderAlert />
            <Link href="/settings" className="text-base font-medium text-muted-foreground hover:text-foreground transition-colors">
              Settings
            </Link>
            <ModeToggle />
          </div>
        </div>
      </header>

      <main className="flex-1 flex flex-col min-h-0 space-y-6 p-10 pt-8 container mx-auto max-w-7xl overflow-hidden">
        <TaxSummary selectedYear={selectedYear} taxBracket={taxBracket} />
        <TransactionsTable selectedYear={selectedYear} />
      </main>

      <footer className="flex-none py-4 border-t text-center text-muted-foreground text-sm">
        <div className="container mx-auto px-4 max-w-7xl">
          <p>© 2026 Local Crypto Tax Ledger. Generated for Israeli Tax Authority compliance.</p>
        </div>
      </footer>
    </div>
  );
}
