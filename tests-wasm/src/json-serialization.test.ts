/**
 * Tests for Json property serialization to JavaScript.
 *
 * Verifies that Json fields are serialized as plain objects (POJOs),
 * not Map instances.
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import init, {
    create_test_context,
    cleanup_test_db,
    TestConfig,
    Context,
} from "../bindings/pkg/ankurah_tests_wasm_bindings.js";

describe("Json property serialization", () => {
    let ctx: Context;
    const dbName = `test_json_serialization_${Date.now()}`;

    beforeAll(async () => {
        await init();
        ctx = await create_test_context(dbName);
    });

    afterAll(async () => {
        await cleanup_test_db(dbName);
    });

    it("serializes Json fields as plain objects, not Maps", async () => {
        // Create entity with nested Json data
        const config = await TestConfig.create_one(ctx, {
            name: "test-config",
            settings: {
                theme: "dark",
                notifications: {
                    email: true,
                    push: false,
                },
                tags: ["important", "work"],
            },
        });

        // Fetch it back
        const fetched = await TestConfig.get(ctx, config.id);

        // The settings should be a plain object, not a Map
        expect(fetched.settings instanceof Map).toBe(false);
        expect(typeof fetched.settings).toBe("object");

        // Should be able to access properties directly (not via .get())
        expect(fetched.settings.theme).toBe("dark");
        expect(fetched.settings.notifications.email).toBe(true);
        expect(fetched.settings.notifications.push).toBe(false);
        expect(fetched.settings.tags).toEqual(["important", "work"]);
    });

    it("handles nested objects correctly", async () => {
        const config = await TestConfig.create_one(ctx, {
            name: "nested-test",
            settings: {
                level1: {
                    level2: {
                        level3: {
                            value: "deep",
                        },
                    },
                },
            },
        });

        const fetched = await TestConfig.get(ctx, config.id);

        // All nested levels should be plain objects
        expect(fetched.settings.level1 instanceof Map).toBe(false);
        expect(fetched.settings.level1.level2 instanceof Map).toBe(false);
        expect(fetched.settings.level1.level2.level3 instanceof Map).toBe(false);
        expect(fetched.settings.level1.level2.level3.value).toBe("deep");
    });
});

