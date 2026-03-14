import ApiKeysManager from '@/components/ApiKeysManager';
import HistoryUploader from '@/components/HistoryUploader';
import DisplaySettings from '@/components/DisplaySettings';
import NotificationSettings from '@/components/NotificationSettings';
import MaintenanceSettings from '@/components/MaintenanceSettings';
import Link from 'next/link';

export default function SettingsPage() {
  return (
    <div className="flex flex-col min-h-screen bg-muted/20">
      <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur text-lexend font-lexend">
        <div className="container mx-auto flex h-16 items-center justify-between px-4 max-w-7xl">
          <h1 className="text-2xl font-bold tracking-tight text-foreground">
            Settings & Data Management
          </h1>
          <Link href="/" className="text-base text-blue-500 hover:underline">
            Back to Dashboard
          </Link>
        </div>
      </header>
      <main className="flex-1 space-y-8 p-10 pt-8 container mx-auto max-w-7xl">
        <DisplaySettings />
        <NotificationSettings />
        <MaintenanceSettings />
        <HistoryUploader />
        <ApiKeysManager />
      </main>
    </div>
  );
}
