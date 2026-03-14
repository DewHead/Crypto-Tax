export type Language = 'en' | 'he';

export const translations = {
  en: {
    title: 'Israeli Crypto Tax Calculator',
    subtitle: 'ITA 2026 Compliant • FIFO Matching • Bank of Israel Exchange Rates',
    real_gain: 'Real Capital Gain',
    nominal_gain: 'Nominal Gain',
    inflationary_gain: 'Inflationary Gain',
    ordinary_income: 'Ordinary Income',
    tax_loss_harvesting: 'Tax-Loss Harvesting',
    form_1391_warning: 'Form 1391 Filing Required',
    form_1391_desc: 'Your foreign assets exceeded 2M ILS this year.',
    resolve: 'Resolve',
    missing_cost_basis: 'Missing Cost Basis',
    manual_override: 'Manual Override',
    ledger: 'Transaction Ledger',
    sync: 'Sync History',
    export: 'Export 8659 (Tax Report)',
  },
  he: {
    title: 'מחשבון מס קריפטו ישראלי',
    subtitle: 'תואם רשות המיסים 2026 • חישוב FIFO • שערי בנק ישראל',
    real_gain: 'רווח הון ריאלי',
    nominal_gain: 'רווח נומינלי',
    inflationary_gain: 'סכום אינפלציוני',
    ordinary_income: 'הכנסה רגילה',
    tax_loss_harvesting: 'תכנון מס (קצירת הפסדים)',
    form_1391_warning: 'חובת דיווח טופס 1391',
    form_1391_desc: 'שווי הנכסים בחו"ל עלה על 2 מיליון ש"ח השנה.',
    resolve: 'פתור',
    missing_cost_basis: 'חסרה עלות רכישה',
    manual_override: 'עדכון ידני',
    ledger: 'ספר תנועות',
    sync: 'סנכרן נתונים',
    export: 'ייצוא דוח 8659',
  }
};

import { useSettings } from '@/providers/SettingsProvider';

export function useTranslation() {
  const { language } = useSettings();
  const t = translations[language];
  return { t, language };
}
