'use client';

import React, { createContext, useContext, useEffect, useState } from 'react';

interface SettingsContextType {
  showOnlyBinanceCSV: boolean;
  setShowOnlyBinanceCSV: (value: boolean) => void;
  language: 'en' | 'he';
  setLanguage: (lang: 'en' | 'he') => void;
}

const SettingsContext = createContext<SettingsContextType | undefined>(undefined);

export function SettingsProvider({ children }: { children: React.ReactNode }) {
  const [showOnlyBinanceCSV, setShowOnlyBinanceCSV] = useState<boolean>(false);
  const [language, setLanguage] = useState<'en' | 'he'>('he');
  const [isLoaded, setIsLoaded] = useState(false);

  useEffect(() => {
    const savedCsv = localStorage.getItem('settings_showOnlyBinanceCSV');
    if (savedCsv !== null) setShowOnlyBinanceCSV(savedCsv === 'true');
    
    const savedLang = localStorage.getItem('settings_language') as 'en' | 'he';
    if (savedLang === 'en' || savedLang === 'he') setLanguage(savedLang);
    
    setIsLoaded(true);
  }, []);

  useEffect(() => {
    if (isLoaded) {
      localStorage.setItem('settings_showOnlyBinanceCSV', showOnlyBinanceCSV.toString());
      localStorage.setItem('settings_language', language);
      
      // Handle RTL
      document.documentElement.dir = language === 'he' ? 'rtl' : 'ltr';
      document.documentElement.lang = language;
    }
  }, [showOnlyBinanceCSV, language, isLoaded]);

  return (
    <SettingsContext.Provider value={{ showOnlyBinanceCSV, setShowOnlyBinanceCSV, language, setLanguage }}>
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
