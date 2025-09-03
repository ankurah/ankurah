import { test, expect } from '@playwright/test';
import { TestWatcher } from '../helpers/test-watcher';
import { initializeAnkurah, clearDatabase, getContext } from '../helpers/setup';

test.describe('Basic Ankurah Integration Tests', () => {
    test.beforeEach(async ({ page }) => {
        await initializeAnkurah(page);
        await clearDatabase(page);
    });

    test('test_indexeddb - port of test_sled from basic.rs', async ({ page }) => {
        // Get the context
        const context = await getContext(page);

        // Create an album and get its ID
        const albumId = await page.evaluate(async (ctx) => {
            const { Album } = await import('../../pkg/ankurah_wasm_integration_tests.js');

            const trx = ctx.begin();
            const album = await Album.create(trx, {
                name: "The rest of the bowl",
                year: "2024"
            });
            const id = album.id();
            await trx.commit();
            return id;
        }, context);

        // Get a new copy of the album for clarity (like in the Rust test)
        const album = await page.evaluate(async (args) => {
            const { Album } = await import('../../pkg/ankurah_wasm_integration_tests.js');
            return await Album.get(args.ctx, args.albumId);
        }, { ctx: context, albumId });

        // Set up watchers for testing subscriptions
        const viewWatcher = TestWatcher.transform((albumView: any) => [
            albumView,
            albumView.name(),
            albumView.year()
        ]);

        const renderWatcher = TestWatcher.new<string>();

        // Subscribe to the album view changes
        await page.evaluate(async (args) => {
            const album = args.album;

            // Store watchers on window for access from subscriptions
            (window as any).viewWatcher = args.viewWatcher;
            (window as any).renderWatcher = args.renderWatcher;

            // Subscribe to album changes (equivalent to album.subscribe(&view_watcher))
            const subscription1 = album.subscribe((albumView: any) => {
                (window as any).viewWatcher.notify([albumView, albumView.name(), albumView.year()]);
            });

            // Create observer equivalent (like CallbackObserver in Rust)
            const observerCallback = () => {
                const name = album.name();
                const year = album.year();
                (window as any).renderWatcher.notify(`name: ${name}, year: ${year}`);
            };

            // Trigger initial observation
            observerCallback();

            // Store subscription handles to keep them alive
            (window as any).subscriptions = [subscription1];
            (window as any).observerCallback = observerCallback;
        }, { album, viewWatcher: viewWatcher.createCallback(), renderWatcher: renderWatcher.createCallback() });

        // Verify initial render
        const initialRender = await page.evaluate(() => {
            return (window as any).renderWatcher.takeOne();
        });
        expect(initialRender).toBe("name: The rest of the bowl, year: 2024");

        // Edit the album name (remove the "typo" b from bowl)
        await page.evaluate(async (args) => {
            const { ctx, album } = args;
            const trx2 = ctx.begin();
            const albumMut2 = album.edit(trx2);

            // Delete character at position 16 (the 'b' in 'bowl')
            albumMut2.name().delete(16, 1);

            // Don't commit yet - verify no changes are observed
            (window as any).preCommitViewCount = (window as any).viewWatcher.count();
            (window as any).preCommitRenderCount = (window as any).renderWatcher.count();

            // Now commit the transaction
            await trx2.commit();

            // Trigger observer callback after commit
            (window as any).observerCallback();
        }, { ctx: context, album });

        // Verify no changes were observed before commit
        const preCommitViewCount = await page.evaluate(() => (window as any).preCommitViewCount);
        const preCommitRenderCount = await page.evaluate(() => (window as any).preCommitRenderCount);
        expect(preCommitViewCount).toBe(0);
        expect(preCommitRenderCount).toBe(1); // Only the initial render

        // Verify changes are observed after commit
        const viewChange = await page.evaluate(async () => {
            return await (window as any).viewWatcher.takeOne();
        });
        const renderChange = await page.evaluate(async () => {
            return await (window as any).renderWatcher.takeOne();
        });

        expect(viewChange[1]).toBe("The rest of the owl"); // name changed
        expect(viewChange[2]).toBe("2024"); // year unchanged
        expect(renderChange).toBe("name: The rest of the owl, year: 2024");

        // Edit the year
        await page.evaluate(async (args) => {
            const { ctx, album } = args;
            const trx3 = ctx.begin();
            const albumMut3 = album.edit(trx3);
            albumMut3.year().replace("2025");
            await trx3.commit();

            // Trigger observer callback after commit
            (window as any).observerCallback();
        }, { ctx: context, album });

        // Verify year change is observed
        const viewChange2 = await page.evaluate(async () => {
            return await (window as any).viewWatcher.takeOne();
        });
        const renderChange2 = await page.evaluate(async () => {
            return await (window as any).renderWatcher.takeOne();
        });

        expect(viewChange2[1]).toBe("The rest of the owl"); // name unchanged
        expect(viewChange2[2]).toBe("2025"); // year changed
        expect(renderChange2).toBe("name: The rest of the owl, year: 2025");
    });

    test('basic album creation and retrieval', async ({ page }) => {
        const context = await getContext(page);

        // Create an album
        const album = await page.evaluate(async (ctx) => {
            const { Album } = await import('../../pkg/ankurah_wasm_integration_tests.js');

            return await Album.create_one(ctx, {
                name: "Test Album",
                year: "2024"
            });
        }, context);

        // Verify album properties
        const albumData = await page.evaluate((album) => ({
            name: album.name(),
            year: album.year(),
            id: album.id().as_string()
        }), album);

        expect(albumData.name).toBe("Test Album");
        expect(albumData.year).toBe("2024");
        expect(albumData.id).toBeTruthy();
    });

    test('album query functionality', async ({ page }) => {
        const context = await getContext(page);

        // Create multiple albums
        await page.evaluate(async (ctx) => {
            const { Album } = await import('../../pkg/ankurah_wasm_integration_tests.js');

            await Album.create_one(ctx, { name: "Album 2024", year: "2024" });
            await Album.create_one(ctx, { name: "Album 2023", year: "2023" });
            await Album.create_one(ctx, { name: "Another 2024", year: "2024" });
        }, context);

        // Query for albums from 2024
        const albums2024 = await page.evaluate(async (ctx) => {
            const { Album } = await import('../../pkg/ankurah_wasm_integration_tests.js');

            return await Album.fetch(ctx, "year = '2024'");
        }, context);

        const albumNames = await page.evaluate((albums) =>
            albums.map((album: any) => album.name()).sort()
            , albums2024);

        expect(albumNames).toEqual(["Album 2024", "Another 2024"]);
    });
});
