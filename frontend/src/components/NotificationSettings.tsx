'use client';

import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Mail, Loader2, Save, Send } from 'lucide-react';
import { fetchSetting, updateSetting, sendTestEmail } from '@/lib/api';
import { toast } from 'sonner';

export default function NotificationSettings() {
  const queryClient = useQueryClient();
  const [email, setEmail] = useState('');
  const [isSendingTest, setIsSendingTest] = useState(false);

  const { data: setting, isLoading } = useQuery({
    queryKey: ['settings', 'notification_email'],
    queryFn: () => fetchSetting('notification_email'),
  });

  const mutation = useMutation({
    mutationFn: (newEmail: string) => updateSetting('notification_email', newEmail),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['settings', 'notification_email'] });
      toast.success('Notification settings updated');
    },
    onError: (error) => {
      console.error('Failed to update settings:', error);
      toast.error('Failed to update notification settings');
    },
  });

  useEffect(() => {
    if (setting?.value) {
      setEmail(setting.value);
    }
  }, [setting]);

  const handleSave = () => {
    mutation.mutate(email);
  };

  const handleSendTest = async () => {
    setIsSendingTest(true);
    try {
      await sendTestEmail();
      toast.success('Test email sent successfully');
    } catch (error) {
      console.error('Failed to send test email:', error);
      let message = 'Failed to send test email';
      if (error && typeof error === 'object' && 'response' in error) {
        const axiosError = error as { response?: { data?: { detail?: string } } };
        message = axiosError.response?.data?.detail || message;
      }
      toast.error(message);
    } finally {
      setIsSendingTest(false);
    }
  };

  return (
    <Card className="bg-card/50 backdrop-blur-xl border-muted/50 shadow-xl overflow-hidden">
      <CardHeader>
        <CardTitle className="text-xl font-semibold">Notification Preferences</CardTitle>
        <CardDescription>
          Receive email alerts when background data sync or tax re-calculations are complete.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="flex flex-col md:flex-row items-start md:items-center justify-between p-4 rounded-xl bg-muted/20 border border-muted/50 gap-4">
          <div className="space-y-1">
            <h4 className="font-medium text-base">Email Notifications</h4>
            <p className="text-sm text-muted-foreground max-w-md">
              Enter the email address where you&apos;d like to receive completion notifications.
            </p>
          </div>
          <div className="flex w-full md:w-auto items-center gap-2">
            <div className="relative w-full md:w-[300px]">
              <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input
                type="email"
                placeholder="email@example.com"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="pl-10 rounded-full bg-background/50 border-muted-foreground/20"
                disabled={isLoading || mutation.isPending}
              />
            </div>
            <div className="flex gap-2">
              <Button
                onClick={handleSave}
                disabled={isLoading || mutation.isPending || email === setting?.value}
                className="rounded-full px-6"
              >
                {mutation.isPending ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <>
                    <Save className="w-4 h-4 me-2" />
                    Save
                  </>
                )}
              </Button>
              <Button
                variant="outline"
                onClick={handleSendTest}
                disabled={isLoading || isSendingTest || !setting?.value || email !== setting?.value}
                className="rounded-full px-4"
                title="Send a test email to the saved address"
              >
                {isSendingTest ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <>
                    <Send className="w-4 h-4 me-2" />
                    Test
                  </>
                )}
              </Button>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
