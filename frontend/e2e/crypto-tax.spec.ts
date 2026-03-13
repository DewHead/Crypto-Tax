import { test, expect } from '@playwright/test';

test.describe('Crypto Tax Dashboard E2E', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the dashboard
    await page.goto('/');
  });

  test('Full Data Ingestion and UI Reactivity', async ({ page }) => {
    // Assert initial state: table might be empty
    const initialRows = await page.getByTestId('ledger-table').locator('tbody tr').count();
    console.log(`Initial rows: ${initialRows}`);

    // Click sync button
    const syncBtn = page.getByTestId('sync-button');
    await expect(syncBtn).toBeVisible();
    await syncBtn.click();

    // Check if it shows "Syncing..."
    await expect(syncBtn).toHaveText(/Syncing.../);

    // Wait for the button to change back (max timeout 30s as sync can be slow)
    await expect(syncBtn).toHaveText('Sync History', { timeout: 30000 });

    // Assert that TanStack Query cache invalidation triggers UI update
    // The table should now have rows
    const updatedRows = await page.getByTestId('ledger-table').locator('tbody tr').count();
    console.log(`Updated rows: ${updatedRows}`);
    expect(updatedRows).toBeGreaterThan(0);
  });

  test('Tax Calculations Display Verification', async ({ page }) => {
    // Wait for data to be loaded (assuming sync already happened or we rely on the previous test state if reuse)
    // For E2E tests, it's better if they are independent. We might need to trigger sync if empty.
    const syncBtn = page.getByTestId('sync-button');
    if (await page.getByTestId('ledger-table').locator('tbody tr').count() === 0) {
      await syncBtn.click();
      await expect(syncBtn).toHaveText('Sync History', { timeout: 30000 });
    }

    const estimatedLiability = page.getByTestId('estimated-liability');
    const totalGain = page.getByTestId('total-gain');

    await expect(estimatedLiability).not.toBeEmpty();
    await expect(totalGain).not.toBeEmpty();

    // Assert currency symbol
    await expect(estimatedLiability).toContainText('₪');
    await expect(totalGain).toContainText('₪');
  });

  test('High-Volume Trader Alert (ITA Rule)', async ({ page }) => {
    // This test might depend on the synced data having > 100 transactions.
    // If the mock/live data doesn't have it, we might not see the alert.
    // For now, we'll just check if it's visible IF the condition is met.
    const tradeCountText = await page.getByTestId('trade-count').innerText();
    const tradeCount = parseInt(tradeCountText.replace(/,/g, ''));

    if (tradeCount > 100) {
      await expect(page.getByTestId('ita-alert')).toBeVisible();
    } else {
      await expect(page.getByTestId('ita-alert')).not.toBeVisible();
    }
  });

  test('CSV Export Verification', async ({ page }) => {
    const exportBtn = page.getByTestId('export-button');
    await expect(exportBtn).toBeVisible();

    // Use Playwright's waitForEvent('download') to capture the download.
    const downloadPromise = page.waitForEvent('download');
    await exportBtn.click();
    const download = await downloadPromise;

    // Assert filename
    expect(download.suggestedFilename()).toBe('form_1399.csv');
  });

  test('Theme Toggle Verification', async ({ page }) => {
    const modeToggle = page.getByTestId('mode-toggle');
    await expect(modeToggle).toBeVisible();

    // Check current theme (default usually light or system, but we'll just check if it toggles)
    const html = page.locator('html');
    const initialTheme = await html.getAttribute('class');
    console.log(`Initial theme classes: ${initialTheme}`);

    // Click mode toggle to open menu
    await modeToggle.click();

    // Select 'Dark' theme
    const darkItem = page.getByRole('menuitem', { name: 'Dark' });
    await expect(darkItem).toBeVisible();
    await darkItem.click();

    // Check if 'dark' class is added to html
    await expect(html).toHaveClass(/dark/);
    console.log('Successfully switched to dark theme');

    // Toggle back to 'Light'
    await modeToggle.click();
    const lightItem = page.getByRole('menuitem', { name: 'Light' });
    await expect(lightItem).toBeVisible();
    await lightItem.click();

    // Check if 'dark' class is removed
    await expect(html).not.toHaveClass(/dark/);
    console.log('Successfully switched back to light theme');
  });
});
