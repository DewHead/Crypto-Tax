import type { Metadata } from "next";
import { Inter, Lexend, JetBrains_Mono } from "next/font/google";
import "./globals.css";
import QueryProvider from "@/providers/QueryProvider";
import { SettingsProvider } from "@/providers/SettingsProvider";
import { ThemeProvider } from "@/components/theme-provider";
import { Toaster } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin"],
});

const lexend = Lexend({
  variable: "--font-lexend",
  subsets: ["latin"],
});

const jetbrainsMono = JetBrains_Mono({
  variable: "--font-jetbrains-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Crypto Tax Calculator (ITA 2026)",
  description: "Calculate crypto taxes according to ITA 2026 rules.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body
        className={`${inter.variable} ${lexend.variable} ${jetbrainsMono.variable} antialiased bg-background min-h-screen text-foreground`}
      >
        <ThemeProvider attribute="class" defaultTheme="system" enableSystem disableTransitionOnChange>
          <QueryProvider>
            <SettingsProvider>
              <TooltipProvider>
                {children}
                <Toaster />
              </TooltipProvider>
            </SettingsProvider>
          </QueryProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
