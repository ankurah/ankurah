import { Page } from '@playwright/test';

/**
 * Initialize Ankurah WASM in the browser page
 */
export async function initializeAnkurah(page: Page): Promise<void> {
    // Listen to console messages for debugging
    page.on('console', msg => console.log('PAGE LOG:', msg.text()));
    page.on('pageerror', error => console.log('PAGE ERROR:', error.message));

    // Navigate to our test page
    await page.goto('/');

    // Wait a bit for the page to load
    await page.waitForLoadState('networkidle');

    // Check what's happening in the browser
    const pageContent = await page.content();
    console.log('Page loaded, checking for WASM initialization...');

    try {
        // Wait for Ankurah to be initialized with better error handling
        await page.waitForFunction(() => {
            console.log('Checking ankurahReady:', (window as any).ankurahReady, 'ankurahError:', (window as any).ankurahError);
            return (window as any).ankurahReady === true || (window as any).ankurahError;
        }, { timeout: 10000 });

        // Check if there was an initialization error
        const error = await page.evaluate(() => (window as any).ankurahError);
        if (error) {
            console.log('Ankurah initialization error:', error);
            throw new Error(`Ankurah initialization failed: ${JSON.stringify(error)}`);
        }

        console.log('Ankurah initialized successfully!');
    } catch (timeoutError) {
        // Get more debugging info on timeout
        const debugInfo = await page.evaluate(() => ({
            ankurahReady: (window as any).ankurahReady,
            ankurahError: (window as any).ankurahError,
            location: window.location.href,
            hasWasm: typeof WebAssembly !== 'undefined'
        }));
        console.log('Timeout waiting for Ankurah initialization. Debug info:', debugInfo);
        throw new Error(`Ankurah initialization timed out. Debug info: ${JSON.stringify(debugInfo)}`);
    }
}

/**
 * Clear the database for a clean test run
 */
export async function clearDatabase(page: Page): Promise<void> {
    await page.evaluate(async () => {
        const { clear_database } = await import('../../pkg/ankurah_wasm_integration_tests.js');
        await clear_database();
    });
}

/**
 * Get the Ankurah context
 */
export async function getContext(page: Page) {
    return await page.evaluate(async () => {
        const { ctx } = await import('../../pkg/ankurah_wasm_integration_tests.js');
        return ctx();
    });
}
