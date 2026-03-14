'use client';

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  flexRender,
  createColumnHelper,
  SortingState,
} from '@tanstack/react-table';
import { useVirtualizer } from '@tanstack/react-virtual';
import { useRef, useMemo, useState } from 'react';
import api, { ExchangeKey, updateManualCostBasis } from '@/lib/api';
import { cn, formatDate } from '@/lib/utils';
import { 
  RefreshCw, 
  Download, 
  ReceiptText, 
  AlertTriangle,
  AlertCircle,
  Link as LinkIcon,
  ArrowUpDown,
  ChevronUp,
  ChevronDown,
  X,
  CheckCircle2
} from 'lucide-react';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { toast } from 'sonner';
import { motion } from 'framer-motion';
import { useSettings } from '@/providers/SettingsProvider';

interface Transaction {
  id: number;
  timestamp: string;
  exchange: string;
  type: string;
  asset_from: string;
  amount_from: number;
  asset_to: string;
  amount_to: number;
  cost_basis_ils: number;
  capital_gain_ils: number;
  is_taxable_event: number;
  source: string;
  is_issue: boolean;
  issue_notes?: string;
  category?: string;
  linked_transaction_id?: number;
  manual_cost_basis_ils?: number;
  manual_purchase_date?: string;
}

const columnHelper = createColumnHelper<Transaction>();

const createColumns = (onResolve: (tx: Transaction) => void) => [
  columnHelper.accessor('timestamp', {
    header: 'Date',
    cell: (info) => (
      <div className="flex flex-col">
        <span className="font-mono text-[13px] text-muted-foreground">
          {formatDate(info.getValue())}
        </span>
        {info.row.original.linked_transaction_id && (
          <span className="flex items-center gap-1 text-[10px] text-blue-500 font-bold mt-1">
            <LinkIcon className="w-2.5 h-2.5" />
            LINKED
          </span>
        )}
      </div>
    ),
  }),
  columnHelper.accessor('exchange', {
    header: 'Exchange',
    cell: (info) => (
      <span className="font-medium text-foreground capitalize text-base">{info.getValue()}</span>
    ),
  }),
  columnHelper.accessor('type', {
    header: 'Event',
    cell: (info) => {
      const type = info.getValue();
      const isIssue = info.row.original.is_issue;
      return (
        <div className="flex items-center gap-2">
          <div className="flex flex-col gap-1">
            <Badge 
              variant="outline"
              className={`rounded-full px-3 py-0.5 text-xs font-bold border-none w-fit ${
                type === 'buy' 
                  ? 'bg-blue-500/10 text-blue-500' 
                  : type === 'sell'
                  ? 'bg-orange-500/10 text-orange-600'
                  : 'bg-muted/50 text-muted-foreground'
              }`}
            >
              {type.toUpperCase()}
            </Badge>
            {info.row.original.category && (
              <Badge variant="outline" className="text-[10px] px-1.5 h-4 w-fit opacity-70">
                {info.row.original.category}
              </Badge>
            )}
          </div>
          {isIssue && (
            <div className="flex items-center gap-1.5">
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger>
                    <AlertCircle className="w-4 h-4 text-destructive" />
                  </TooltipTrigger>
                  <TooltipContent className="max-w-xs p-3">
                    <p className="font-bold text-destructive mb-1">Data Issue</p>
                    <p className="text-xs">{info.row.original.issue_notes}</p>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
              <Button 
                variant="ghost" 
                size="sm" 
                className="h-6 px-2 text-[10px] font-black bg-destructive/10 text-destructive hover:bg-destructive hover:text-white rounded-full transition-all"
                onClick={() => onResolve(info.row.original)}
              >
                RESOLVE
              </Button>
            </div>
          )}
        </div>
      );
    },
  }),

  columnHelper.accessor((row) => `${row.amount_to} ${row.asset_to}`, {
    id: 'asset',
    header: 'Asset/Qty',
    cell: (info) => <span className="font-semibold text-base">{info.getValue()}</span>,
  }),
  columnHelper.accessor('cost_basis_ils', {
    header: 'Cost Basis (ILS)',
    cell: (info) => (
      <span className="font-mono tabular-nums text-base">
        {info.getValue() ? `₪${info.getValue().toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '-'}
      </span>
    ),
  }),
  columnHelper.accessor('capital_gain_ils', {
    header: 'Gain/Loss (ILS)',
    cell: (info) => {
      const val = info.getValue();
      if (!val && val !== 0) return '-';
      return (
        <span className={`font-mono tabular-nums font-semibold text-base ${val >= 0 ? 'text-green-500' : 'text-destructive'}`}>
          {val >= 0 ? '+' : ''}₪{val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </span>
      );
    },
  }),
  columnHelper.accessor('is_taxable_event', {
    header: 'Taxable',
    cell: (info) => info.getValue() ? (
      <Badge variant="outline" className="bg-green-500/10 border-green-500/20 text-green-600 font-black text-[11px] px-2 h-5">YES</Badge>
    ) : (
      <Badge variant="outline" className="bg-muted/10 border-muted/20 text-muted-foreground font-black text-[11px] px-2 h-5">NO</Badge>
    ),
  }),
  columnHelper.accessor('source', {
    header: 'Source',
    cell: (info) => (
      <Badge variant="secondary" className="text-[10px] uppercase font-bold px-1.5 h-4 opacity-60">
        {info.getValue()}
      </Badge>
    ),
  }),
];

interface ResolveModalProps {
  tx: Transaction;
  onClose: () => void;
  onSubmit: (costBasis: number, date?: string) => void;
  isSubmitting: boolean;
}

function ResolveModal({ tx, onClose, onSubmit, isSubmitting }: ResolveModalProps) {
  const [costBasis, setCostBasis] = useState<string>('');
  const [date, setDate] = useState<string>('');

  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm animate-in fade-in duration-200">
      <motion.div 
        initial={{ scale: 0.95, opacity: 0 }}
        animate={{ scale: 1, opacity: 1 }}
        className="bg-card border shadow-2xl rounded-2xl w-full max-w-md overflow-hidden"
      >
        <div className="p-6 border-b flex justify-between items-center bg-muted/20">
          <div>
            <h3 className="text-xl font-bold">Resolve Transaction</h3>
            <p className="text-sm text-muted-foreground mt-1">Manual Cost Basis Override</p>
          </div>
          <Button variant="ghost" size="icon" onClick={onClose} className="rounded-full">
            <X className="w-5 h-5" />
          </Button>
        </div>
        
        <div className="p-6 space-y-6">
          <div className="bg-amber-500/5 border border-amber-500/10 rounded-xl p-4 flex gap-3">
            <AlertCircle className="w-5 h-5 text-amber-500 shrink-0 mt-0.5" />
            <div className="text-sm">
              <p className="font-bold text-amber-700">Missing purchase history</p>
              <p className="text-amber-800/70">
                You are selling {tx.amount_from} {tx.asset_from} on {formatDate(tx.timestamp)}. 
                Enter the original cost to avoid zero cost basis.
              </p>
            </div>
          </div>

          <div className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-bold ms-1">Original Cost Basis (Total ILS)</label>
              <div className="relative">
                <div className="absolute start-4 top-1/2 -translate-y-1/2 text-muted-foreground font-bold">₪</div>
                <input 
                  type="number"
                  step="0.01"
                  value={costBasis}
                  onChange={(e) => setCostBasis(e.target.value)}
                  placeholder="0.00"
                  className="w-full bg-muted/30 border-muted/50 rounded-xl py-3 ps-10 pe-4 focus:ring-2 focus:ring-primary/20 focus:border-primary outline-none transition-all font-mono"
                  autoFocus
                />
              </div>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-bold ms-1">Original Purchase Date (Required)</label>
              <input 
                type="date"
                value={date}
                onChange={(e) => setDate(e.target.value)}
                className="w-full bg-muted/30 border-muted/50 rounded-xl py-3 px-4 focus:ring-2 focus:ring-primary/20 focus:border-primary outline-none transition-all"
                required
              />
              <p className="text-[10px] text-muted-foreground ms-1 uppercase font-bold tracking-wider">
                Required for CPI (Madad) inflationary adjustment
              </p>
            </div>
          </div>
        </div>

        <div className="p-6 bg-muted/20 border-t flex gap-3">
          <Button variant="outline" onClick={onClose} className="flex-1 rounded-xl h-12 font-bold">
            Cancel
          </Button>
          <Button 
            onClick={() => onSubmit(parseFloat(costBasis), date)} 
            disabled={!costBasis || !date || isSubmitting}
            className="flex-1 rounded-xl h-12 font-bold shadow-lg shadow-primary/20"
          >
            {isSubmitting ? (
              <RefreshCw className="w-5 h-5 animate-spin" />
            ) : (
              <>
                <CheckCircle2 className="w-5 h-5 me-2" />
                Apply Override
              </>
            )}
          </Button>
        </div>
      </motion.div>
    </div>
  );
}

interface TransactionsTableProps {
  selectedYear: number | null;
}

export default function TransactionsTable({ selectedYear }: TransactionsTableProps) {
  const queryClient = useQueryClient();
  const { showOnlyBinanceCSV } = useSettings();
  
  const [resolvingTx, setResolvingTx] = useState<Transaction | null>(null);

  const { data: transactions = [], isLoading } = useQuery<Transaction[]>({
    queryKey: ['ledger'],
    queryFn: async () => {
      const { data } = await api.get('/ledger');
      return data;
    },
  });

  const resolveMutation = useMutation({
    mutationFn: ({ txId, costBasis, date }: { txId: number, costBasis: number, date?: string }) => 
      updateManualCostBasis(txId, costBasis, date),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['ledger'] });
      queryClient.invalidateQueries({ queryKey: ['kpi'] });
      toast.success('Cost basis updated. Recalculating taxes...');
      setResolvingTx(null);
    },
    onError: (error) => {
      toast.error(`Failed to update: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  });

  const columns = useMemo(() => createColumns(setResolvingTx), []);

  const filteredTransactions = useMemo(() => {
    let filtered = transactions;
    
    // Filter by Binance CSV setting
    if (showOnlyBinanceCSV) {
      filtered = filtered.filter(tx => {
        if (tx.exchange.toLowerCase() === 'binance') {
          return tx.source === 'csv';
        }
        return true;
      });
    }

    // Filter by selected year
    if (selectedYear) {
      filtered = filtered.filter(tx => {
        const txYear = new Date(tx.timestamp).getFullYear();
        return txYear === selectedYear;
      });
    }

    return filtered;
  }, [transactions, showOnlyBinanceCSV, selectedYear]);

  const syncMutation = useMutation({
    mutationFn: () => api.post('/sync'),
    onMutate: () => {
      const toastId = toast.loading('Syncing transaction history...');
      return { toastId };
    },
    onSuccess: async (_, __, context) => {
      await queryClient.invalidateQueries({ queryKey: ['ledger'] });
      await queryClient.invalidateQueries({ queryKey: ['kpi'] });
      toast.success('Sync started in background', { id: context?.toastId });
    },
    onError: (error, __, context) => {
      toast.error(`Sync failed: ${error.message}`, { id: context?.toastId });
    },
  });

  const { data: keys = [] } = useQuery<ExchangeKey[]>({
    queryKey: ['keys'],
    queryFn: async () => {
      const { data } = await api.get('/keys');
      return data;
    },
    refetchInterval: (query) => {
      const isAnySyncing = query.state.data?.some((key) => key.is_syncing === 1);
      return isAnySyncing ? 3000 : 30000;
    },
  });

  const isSyncing = keys.some((key) => key.is_syncing === 1);

  const [sorting, setSorting] = useState<SortingState>([
    { id: 'timestamp', desc: true }
  ]);

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
    estimateSize: () => 53,
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
            <RefreshCw className={`w-5 h-5 me-2 ${syncMutation.isPending || isSyncing ? 'animate-spin' : ''}`} />
            {syncMutation.isPending || isSyncing ? 'Syncing...' : 'Sync History'}
          </Button>
          <Button
            onClick={handleExport}
            variant="outline"
            size="lg"
            className="rounded-full px-6 bg-background/50 backdrop-blur-sm text-base"
            data-testid="export-button"
          >
            <Download className="w-5 h-5 me-2" />
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

      {resolvingTx && (
        <ResolveModal 
          tx={resolvingTx} 
          onClose={() => setResolvingTx(null)}
          isSubmitting={resolveMutation.isPending}
          onSubmit={(costBasis, date) => {
            resolveMutation.mutate({ 
              txId: resolvingTx.id, 
              costBasis, 
              date 
            });
          }}
        />
      )}
    </motion.div>
  );
}
