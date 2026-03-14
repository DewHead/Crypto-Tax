'use client';

import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Upload, FileUp, Loader2 } from 'lucide-react';
import { toast } from 'sonner';

export default function HistoryUploader() {
  const queryClient = useQueryClient();
  const [isUploading, setIsUploading] = useState(false);
  const [file, setFile] = useState<File | null>(null);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      const selectedFile = e.target.files[0];
      if (selectedFile.name.endsWith('.zip')) {
        setFile(selectedFile);
      } else {
        toast.error('Only ZIP files from Binance are supported.');
        e.target.value = '';
      }
    }
  };

  const handleUpload = async () => {
    if (!file) return;

    setIsUploading(true);
    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await fetch('http://localhost:8000/api/upload', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        throw new Error('Failed to upload historical data.');
      }

      toast.success('History uploaded successfully. Taxes are being recalculated.');
      setFile(null);
      
      // Invalidate queries to refresh data across the app
      queryClient.invalidateQueries({ queryKey: ['ledger'] });
      queryClient.invalidateQueries({ queryKey: ['kpi'] });
      queryClient.invalidateQueries({ queryKey: ['data-sources'] });
    } catch (error) {
      console.error('Upload error:', error);
      toast.error('An error occurred while uploading the file.');
    } finally {
      setIsUploading(false);
    }
  };

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <Upload className="w-5 h-5 text-primary" />
          <CardTitle>Historical Data Import</CardTitle>
        </div>
        <CardDescription>
          Upload Binance "All Statements" or "Trades" ZIP files to fill gaps before Sept 2022.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid w-full items-center gap-1.5">
          <label 
            htmlFor="zip-upload"
            className={`flex flex-col items-center justify-center w-full h-32 border-2 border-dashed rounded-lg cursor-pointer transition-colors ${
              file ? 'border-primary/50 bg-primary/5' : 'border-muted-foreground/25 hover:bg-muted/50'
            }`}
          >
            <div className="flex flex-col items-center justify-center pt-5 pb-6">
              {file ? (
                <>
                  <FileUp className="w-10 h-10 mb-3 text-primary" />
                  <p className="mb-2 text-base font-semibold text-foreground">{file.name}</p>
                  <p className="text-sm text-muted-foreground">Click to change file</p>
                </>
              ) : (
                <>
                  <Upload className="w-10 h-10 mb-3 text-muted-foreground" />
                  <p className="mb-2 text-base text-muted-foreground">
                    <span className="font-semibold text-foreground">Click to upload</span> or drag and drop
                  </p>
                  <p className="text-sm text-muted-foreground">Binance ZIP files only</p>
                </>
              )}
            </div>
            <input 
              id="zip-upload" 
              type="file" 
              className="hidden" 
              accept=".zip"
              onChange={handleFileChange}
              disabled={isUploading}
            />
          </label>
        </div>

        <Button 
          onClick={handleUpload} 
          disabled={!file || isUploading}
          className="w-full text-base"
          size="lg"
        >
          {isUploading ? (
            <>
              <Loader2 className="me-3 h-5 w-5 animate-spin" />
              Processing...
            </>
          ) : (
            <>
              <Upload className="me-3 h-5 w-5" />
              Upload & Ingest History
            </>
          )}
        </Button>
      </CardContent>
    </Card>
  );
}
