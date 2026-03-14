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
import { Button, buttonVariants } from '@/components/ui/button';
import { Calendar, ChevronDown, ChevronUp } from 'lucide-react';

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

  const handleNextYear = () => {
    if (!years || years.length === 0) return;
    
    if (selectedYear === null) return; // Already at "All Years"

    const currentIndex = years.indexOf(selectedYear);
    if (currentIndex > 0) {
      // Move to a more recent year
      onYearChange(years[currentIndex - 1]);
    } else if (currentIndex === 0) {
      // Move from most recent year to "All Years"
      onYearChange(null);
    }
  };

  const handlePrevYear = () => {
    if (!years || years.length === 0) return;

    if (selectedYear === null) {
      // Move from "All Years" to the most recent year
      onYearChange(years[0]);
      return;
    }

    const currentIndex = years.indexOf(selectedYear);
    if (currentIndex !== -1 && currentIndex < years.length - 1) {
      // Move to an older year
      onYearChange(years[currentIndex + 1]);
    }
  };

  return (
    <div className="flex items-center gap-1">
      <DropdownMenu>
        <DropdownMenuTrigger className={cn(buttonVariants({ variant: 'outline' }), "flex items-center gap-2 min-w-[140px] justify-between h-9")}>
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

      <div className="flex flex-col gap-0">
        <Button
          variant="outline"
          size="icon"
          className="h-[18px] w-8 rounded-b-none border-b-0"
          onClick={handleNextYear}
          disabled={!years || selectedYear === null}
        >
          <ChevronUp className="h-3 w-3" />
        </Button>
        <Button
          variant="outline"
          size="icon"
          className="h-[18px] w-8 rounded-t-none"
          onClick={handlePrevYear}
          disabled={!years || (years.length > 0 && selectedYear === years[years.length - 1])}
        >
          <ChevronDown className="h-3 w-3" />
        </Button>
      </div>
    </div>
  );
}
