'use client';

import React, { useState } from 'react';
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { recalculateTaxes } from '@/lib/api';
import { toast } from 'sonner';
import { Loader2, RefreshCw } from 'lucide-react';

const MaintenanceSettings = () => {
  const [isRecalculating, setIsRecalculating] = useState(false);

  const handleRecalculate = async () => {
    try {
      setIsRecalculating(true);
      await recalculateTaxes();
      toast.success('Tax recalculation started', {
        description: 'The process is running in the background. Your dashboard will update shortly.'
      });
    } catch (error) {
      toast.error('Failed to start recalculation', {
        description: 'Please try again later or check backend logs.'
      });
      console.error('Recalculation error:', error);
    } finally {
      setIsRecalculating(false);
    }
  };

  return (
    <Card className="shadow-md border-border/40 bg-card/50 backdrop-blur-sm">
      <CardHeader>
        <CardTitle className="text-xl font-bold flex items-center gap-2">
          <RefreshCw className="h-5 w-5 text-blue-500" />
          Maintenance Actions
        </CardTitle>
        <CardDescription>
          Perform administrative tasks and manual data processing.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-4">
          <div className="flex items-center justify-between p-4 border rounded-lg bg-background/50">
            <div className="space-y-1">
              <h4 className="text-sm font-medium leading-none">Recalculate Taxes</h4>
              <p className="text-sm text-muted-foreground">
                Force a full recalculation of all tax lots and gains.
              </p>
            </div>
            <Button 
              variant="outline" 
              onClick={handleRecalculate}
              disabled={isRecalculating}
              className="min-w-[140px]"
            >
              {isRecalculating ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Processing...
                </>
              ) : (
                'Recalculate'
              )}
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};

export default MaintenanceSettings;
