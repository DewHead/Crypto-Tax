'use client';

import { useQuery } from '@tanstack/react-query';
import api from '@/lib/api';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { cn } from '@/lib/utils';
import { buttonVariants } from '@/components/ui/button';
import { Calendar } from 'lucide-react';

interface YearSelectorProps {
  selectedYear: number | null;
  onYearChange: (year: number | null) => void;
}

export default function YearSelector({ selectedYear, onYearChange }: YearSelectorProps) {
  const { data: years } = useQuery<number[]>({
    queryKey: ['years'],
    queryFn: async () => {
      const { data } = await api.get('/years');
      return data;
    },
  });

  return (
    <div className="flex items-center gap-2">
      <DropdownMenu>
        <DropdownMenuTrigger className={cn(buttonVariants({ variant: 'outline' }), "flex items-center gap-2 min-w-[140px] justify-between")}>
          <div className="flex items-center gap-2">
            <Calendar className="w-4 h-4" />
            <span>{selectedYear || 'All Years'}</span>
          </div>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end">
          <DropdownMenuGroup>
            <DropdownMenuLabel>Tax Year</DropdownMenuLabel>
          </DropdownMenuGroup>
          <DropdownMenuSeparator />
          <DropdownMenuItem onClick={() => onYearChange(null)}>
            All Years
          </DropdownMenuItem>
          {years?.map((year) => (
            <DropdownMenuItem key={year} onClick={() => onYearChange(year)}>
              {year}
            </DropdownMenuItem>
          ))}
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}
