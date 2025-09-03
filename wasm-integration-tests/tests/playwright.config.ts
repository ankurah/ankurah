import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
    testDir: './specs',
    fullyParallel: true,
    forbidOnly: !!process.env.CI,
    retries: process.env.CI ? 2 : 0,
    workers: process.env.CI ? 1 : undefined,
    reporter: 'html',
    use: {
        baseURL: 'http://localhost:3001/tests',
        trace: 'on-first-retry',
    },

    projects: [
        {
            name: 'chrome',
            use: {
                ...devices['Desktop Chrome'],
                channel: 'chrome', // Use system Chrome instead of bundled Chromium
            },
        },
    ],

    webServer: {
        command: 'cd .. && python3 -m http.server 3001',
        port: 3001,
        reuseExistingServer: !process.env.CI,
    },
});
