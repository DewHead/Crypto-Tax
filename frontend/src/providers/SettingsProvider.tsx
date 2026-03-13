'use client';

import React, { createContext, useContext, useEffect, useState } from 'react';

interface SettingsContextType {
  showOnlyBinanceCSV: boolean;
  setShowOnlyBinanceCSV: (value: boolean) => void;
}

const SettingsContext = createContext<SettingsContextType | undefined>(undefined);

export function SettingsProvider({ children }: { children: React.ReactNode }) {
  const [showOnlyBinanceCSV, setShowOnlyBinanceCSV] = useState<boolean>(false);
  const [isLoaded, setIsLoaded] = useState(false);

  useEffect(() => {
    const saved = localStorage.getItem('settings_showOnlyBinanceCSV');
    if (saved !== null) {
      setShowOnlyBinanceCSV(saved === 'true');
    }
    setIsLoaded(true);
  }, []);

  useEffect(() => {
    if (isLoaded) {
      localStorage.setItem('settings_showOnlyBinanceCSV', showOnlyBinanceCSV.toString());
    }
  }, [showOnlyBinanceCSV, isLoaded]);

  return (
    <SettingsContext.Provider value={{ showOnlyBinanceCSV, setShowOnlyBinanceCSV }}>
      {children}
    </SettingsContext.Provider>
  );
}

export function useSettings() {
  const context = useContext(SettingsContext);
  if (context === undefined) {
    throw new Error('useSettings must be used within a SettingsProvider');
  }
  return context;
}
