"use client";

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import api from '@/lib/api';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { 
  Download, 
  RefreshCw, 
  ArrowUpDown, 
  ChevronUp, 
  ChevronDown,
  ReceiptText,
  AlertCircle
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { 
  flexRender, 
  getCoreRowModel, 
  useReactTable, 
  getSortedRowModel,
  SortingState,
  ColumnDef
} from '@tanstack/react-table';
import { useState, useMemo, useRef } from 'react';
import { format } from 'date-fns';
import { toast } from 'sonner';
import { motion } from 'framer-motion';
import { cn } from '@/lib/utils';
import { useVirtualizer } from '@tanstack/react-virtual';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface TransactionsTableProps {
  selectedYear: number | null;
}

export default function TransactionsTable({ selectedYear }: TransactionsTableProps) {
  const queryClient = useQueryClient();
  const [sorting, setSorting] = useState<SortingState>([{ id: 'timestamp', desc: true }]);
  const [isSyncing, setIsSyncing] = useState(false);
  
  const { data: transactions = [], isLoading } = useQuery({
    queryKey: ['ledger'],
    queryFn: async () => {
      const { data } = await api.get('/ledger');
      return data;
    },
  });

  const syncMutation = useMutation({
    mutationFn: async () => {
      setIsSyncing(true);
      await api.post('/sync');
    },
    onSuccess: () => {
      toast.success('Synchronization started in background');
      // Periodically refresh while syncing might be active
      const interval = setInterval(() => {
        queryClient.invalidateQueries({ queryKey: ['ledger'] });
        queryClient.invalidateQueries({ queryKey: ['kpi'] });
      }, 3000);
      setTimeout(() => {
        clearInterval(interval);
        setIsSyncing(false);
      }, 30000);
    },
    onError: () => {
      toast.error('Failed to start sync');
      setIsSyncing(false);
    }
  });

  const filteredTransactions = useMemo(() => {
    if (!selectedYear) return transactions;
    return transactions.filter((tx: any) => new Date(tx.timestamp).getFullYear() === selectedYear);
  }, [transactions, selectedYear]);

  const columns = useMemo<ColumnDef<any>[]>(() => [
    {
      accessorKey: 'timestamp',
      header: 'Date',
      cell: ({ row }) => (
        <div className="flex flex-col">
          <span className="font-semibold text-foreground whitespace-nowrap">
            {format(new Date(row.original.timestamp), 'MMM dd, yyyy')}
          </span>
          <span className="text-[10px] text-muted-foreground/60 font-mono">
            {format(new Date(row.original.timestamp), 'HH:mm:ss')}
          </span>
        </div>
      ),
    },
    {
      accessorKey: 'type',
      header: 'Event',
      cell: ({ row }) => {
        const type = row.original.type;
        const colors: any = {
          buy: 'bg-green-500/10 text-green-500 border-green-500/20',
          sell: 'bg-amber-500/10 text-amber-500 border-amber-500/20',
          earn: 'bg-blue-500/10 text-blue-500 border-blue-500/20',
          fee: 'bg-destructive/10 text-destructive border-destructive/20',
        };
        return (
          <Badge variant="outline" className={cn("capitalize font-bold px-2.5 py-0.5 rounded-md", colors[type] || 'bg-muted text-muted-foreground')}>
            {type}
          </Badge>
        );
      },
    },
    {
      id: 'assets',
      header: 'Assets & Amounts',
      cell: ({ row }) => {
        const tx = row.original;
        return (
          <div className="flex flex-col gap-1">
            {tx.amount_from > 0 && (
              <div className="flex items-center gap-2">
                <span className="text-xs font-bold text-destructive/80">-</span>
                <span className="text-sm font-medium">{tx.amount_from.toLocaleString(undefined, { maximumFractionDigits: 8 })}</span>
                <span className="text-[10px] font-black bg-muted px-1.5 py-0.5 rounded text-muted-foreground">{tx.asset_from}</span>
              </div>
            )}
            {tx.amount_to > 0 && (
              <div className="flex items-center gap-2">
                <span className="text-xs font-bold text-green-500/80">+</span>
                <span className="text-sm font-medium">{tx.amount_to.toLocaleString(undefined, { maximumFractionDigits: 8 })}</span>
                <span className="text-[10px] font-black bg-muted px-1.5 py-0.5 rounded text-muted-foreground">{tx.asset_to}</span>
              </div>
            )}
          </div>
        );
      },
    },
    {
      accessorKey: 'cost_basis_ils',
      header: 'Cost Basis',
      cell: ({ row }) => (
        <div className="flex flex-col">
          <span className="text-sm font-bold">₪{row.original.cost_basis_ils?.toLocaleString(undefined, { minimumFractionDigits: 2 })}</span>
          {row.original.purchase_date && (
            <span className="text-[10px] text-muted-foreground/60">Bought {format(new Date(row.original.purchase_date), 'dd/MM/yy')}</span>
          )}
        </div>
      ),
    },
    {
      accessorKey: 'capital_gain_ils',
      header: 'Capital Gain',
      cell: ({ row }) => {
        const gain = row.original.capital_gain_ils;
        const real = row.original.real_gain_ils;
        const infl = row.original.inflationary_gain_ils;
        
        if (row.original.is_taxable_event === 0) return <span className="text-muted-foreground/30">—</span>;
        
        return (
          <div className="flex flex-col">
             <div className={cn("text-sm font-bold flex items-center gap-1.5", gain >= 0 ? "text-green-500" : "text-destructive")}>
              {gain >= 0 ? '+' : ''}₪{gain?.toLocaleString(undefined, { minimumFractionDigits: 2 })}
              {gain < 0 && (
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger>
                      <AlertCircle className="w-3 h-3 opacity-50" />
                    </TooltipTrigger>
                    <TooltipContent className="max-w-xs p-3">
                      <p className="font-bold mb-1">Wash Sale Applied</p>
                      <p className="text-xs">This loss was deferred and added to the cost basis of a replacement purchase within 30 days.</p>
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              )}
            </div>
            {row.original.real_gain_ils !== undefined && (
               <div className="flex gap-2">
                  <span className="text-[9px] font-bold text-muted-foreground/50">REAL: ₪{real?.toFixed(0)}</span>
                  <span className="text-[9px] font-bold text-orange-400/50">INFL: ₪{infl?.toFixed(0)}</span>
               </div>
            )}
          </div>
        );
      },
    },
    {
      accessorKey: 'exchange',
      header: 'Source',
      cell: ({ row }) => (
        <Badge variant="secondary" className="text-[10px] font-black uppercase tracking-tighter opacity-70">
          {row.original.exchange}
        </Badge>
      ),
    },
  ], []);

  const table = useReactTable({
    data: filteredTransactions,
    columns,
    state: {
      sorting,
    },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  const parentRef = useRef<HTMLDivElement>(null);

  const virtualizer = useVirtualizer({
    count: table.getRowModel().rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 75,
    overscan: 10,
  });

  const handleExport = async () => {
    try {
      toast.info('Generating Form 8659 (Tax Report) export...');
      const response = await api.get('/export/8659', { responseType: 'blob' });
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', 'form_8659.csv');
      document.body.appendChild(link);
      link.click();
      toast.success('Export downloaded');
    } catch {
      toast.error('Failed to export Form 8659');
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: 0.5 }}
      className="bg-card/50 backdrop-blur-xl rounded-2xl border border-muted/50 shadow-xl overflow-hidden flex flex-col flex-1 min-h-0"
    >
      <div className="p-8 border-b flex justify-between items-center bg-muted/20">
        <div>
          <h2 className="font-semibold text-2xl tracking-tight">Transaction Ledger</h2>
          <p className="text-base text-muted-foreground mt-1">Detailed history of all identified taxable events.</p>
        </div>
        <div className="flex gap-4 items-center">
          <Button
            onClick={() => syncMutation.mutate()}
            disabled={syncMutation.isPending || isSyncing}
            variant="default"
            size="lg"
            className="rounded-full px-6 shadow-lg shadow-primary/20 text-base"
            data-testid="sync-button"
          >
            <RefreshCw className={`w-5 h-5 mr-2 ${syncMutation.isPending || isSyncing ? 'animate-spin' : ''}`} />
            {syncMutation.isPending || isSyncing ? 'Syncing...' : 'Sync History'}
          </Button>
          <Button
            onClick={handleExport}
            variant="outline"
            size="lg"
            className="rounded-full px-6 bg-background/50 backdrop-blur-sm text-base"
            data-testid="export-button"
          >
            <Download className="w-5 h-5 mr-2" />
            Export 8659 (Tax Report)
          </Button>
        </div>
      </div>      
      <div className="flex-1 overflow-auto scrollbar-thin scrollbar-thumb-muted" ref={parentRef}>
        <Table data-testid="ledger-table" className="relative border-separate border-spacing-0" containerClassName="overflow-visible">
          <TableHeader className="z-20">
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id} className="hover:bg-transparent border-none">
                {headerGroup.headers.map((header) => (
                  <TableHead 
                    key={header.id} 
                    className={cn(
                      "sticky top-0 z-20 bg-background/95 backdrop-blur-md text-xs font-bold uppercase tracking-widest text-muted-foreground py-6 border-b transition-colors",
                      header.column.getCanSort() && "cursor-pointer hover:bg-muted/50 select-none"
                    )}
                    onClick={header.column.getToggleSortingHandler()}
                  >
                    <div className="flex items-center gap-2">
                      {flexRender(header.column.columnDef.header, header.getContext())}
                      {header.column.getCanSort() && (
                        <div className="flex-shrink-0">
                          {{
                            asc: <ChevronUp className="w-4 h-4 text-primary" />,
                            desc: <ChevronDown className="w-4 h-4 text-primary" />,
                          }[header.column.getIsSorted() as string] ?? (
                            <ArrowUpDown className="w-4 h-4 opacity-30" />
                          )}
                        </div>
                      )}
                    </div>
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {isLoading ? (
              <TableRow>
                <TableCell colSpan={columns.length} className="h-64 text-center">
                  <div className="flex flex-col items-center gap-4">
                    <div className="relative">
                      <div className="absolute inset-0 bg-primary/20 blur-xl rounded-full animate-pulse" />
                      <RefreshCw className="h-10 w-10 animate-spin text-primary relative" />
                    </div>
                    <span className="text-base font-medium text-muted-foreground">Retrieving ledger data...</span>
                  </div>
                </TableCell>
              </TableRow>
            ) : filteredTransactions.length === 0 ? (
              <TableRow>
                <TableCell colSpan={columns.length} className="h-64 text-center">
                  <div className="flex flex-col items-center gap-2 text-muted-foreground">
                    <ReceiptText className="h-12 w-12 opacity-20 mb-2" />
                    <p className="text-base font-medium">No transactions found.</p>
                    <p className="text-sm">Check your filter settings or click Sync.</p>
                  </div>
                </TableCell>
              </TableRow>
            ) : (
              <>
                {virtualizer.getVirtualItems().length > 0 && virtualizer.getVirtualItems()[0].start > 0 && (
                  <TableRow style={{ height: `${virtualizer.getVirtualItems()[0].start}px` }} className="border-0">
                    <TableCell colSpan={columns.length} className="p-0"></TableCell>
                  </TableRow>
                )}
                {virtualizer.getVirtualItems().map((virtualRow) => {
                  const row = table.getRowModel().rows[virtualRow.index];
                  return (
                    <TableRow
                      key={row.id}
                      data-index={virtualRow.index}
                      ref={virtualizer.measureElement}
                      className="hover:bg-muted/30 transition-colors border-muted/30"
                    >
                      {row.getVisibleCells().map((cell) => (
                        <TableCell 
                          key={cell.id} 
                          className="py-5 text-base border-b"
                        >
                          {flexRender(cell.column.columnDef.cell, cell.getContext())}
                        </TableCell>
                      ))}
                    </TableRow>
                  );
                })}
                {virtualizer.getVirtualItems().length > 0 && (virtualizer.getTotalSize() - virtualizer.getVirtualItems()[virtualizer.getVirtualItems().length - 1].end) > 0 && (
                  <TableRow style={{ height: `${virtualizer.getTotalSize() - virtualizer.getVirtualItems()[virtualizer.getVirtualItems().length - 1].end}px` }} className="border-0">
                    <TableCell colSpan={columns.length} className="p-0"></TableCell>
                  </TableRow>
                )}
              </>
            )}
          </TableBody>
        </Table>
      </div>
    </motion.div>
  );
}
