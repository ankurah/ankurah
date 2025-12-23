/**
 * Tests for Ref<T> preprocessing in FromWasmAbi.
 *
 * These tests verify that View/Ref objects can be passed directly to create()
 * for Ref<T> fields, with the preprocessing extracting .id.to_base64().
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import init, {
    create_test_context,
    cleanup_test_db,
    TestUser,
    TestRoom,
    TestMessage,
    TestUserRef,
    TestRoomRef,
    Context,
} from "../bindings/pkg/ankurah_tests_wasm_bindings.js";

describe("Ref<T> preprocessing in create()", () => {
    let ctx: Context;
    const dbName = `test_ref_preprocessing_${Date.now()}`;

    beforeAll(async () => {
        await init();
        ctx = await create_test_context(dbName);
    });

    afterAll(async () => {
        await cleanup_test_db(dbName);
    });

    it("accepts View objects for Ref<T> fields", async () => {
        // Create user and room
        const user = await TestUser.create_one(ctx, { name: "Alice" });
        const room = await TestRoom.create_one(ctx, { name: "General" });

        // Pass View objects directly - this exercises js_preprocess_ref_field
        const trx = ctx.begin();
        const msg = await TestMessage.create(trx, {
            user: user, // TestUserView
            room: room, // TestRoomView
            text: "Hello from View!",
        });
        await trx.commit();

        // Verify the refs were correctly extracted
        expect(msg.user.id.equals(user.id)).toBe(true);
        expect(msg.room.id.equals(room.id)).toBe(true);
        expect(msg.text).toBe("Hello from View!");
    });

    it("accepts Ref wrapper objects for Ref<T> fields", async () => {
        const user = await TestUser.create_one(ctx, { name: "Bob" });
        const room = await TestRoom.create_one(ctx, { name: "Random" });

        // Create Ref wrappers from Views
        const userRef = TestUserRef.from(user);
        const roomRef = TestRoomRef.from(room);

        // Pass Ref wrappers - also exercises js_preprocess_ref_field
        const trx = ctx.begin();
        const msg = await TestMessage.create(trx, {
            user: userRef, // TestUserRef
            room: roomRef, // TestRoomRef
            text: "Hello from Ref!",
        });
        await trx.commit();

        expect(msg.user.id.equals(user.id)).toBe(true);
        expect(msg.room.id.equals(room.id)).toBe(true);
    });

    it("accepts base64 strings for Ref<T> fields", async () => {
        const user = await TestUser.create_one(ctx, { name: "Charlie" });
        const room = await TestRoom.create_one(ctx, { name: "Dev" });

        // Pass base64 strings - no preprocessing needed, direct serde
        const trx = ctx.begin();
        const msg = await TestMessage.create(trx, {
            user: user.id.to_base64(), // string
            room: room.id.to_base64(), // string
            text: "Hello from string!",
        });
        await trx.commit();

        expect(msg.user.id.equals(user.id)).toBe(true);
        expect(msg.room.id.equals(room.id)).toBe(true);
    });

    it("can fetch referenced entities via Ref.get()", async () => {
        const user = await TestUser.create_one(ctx, { name: "Diana" });
        const room = await TestRoom.create_one(ctx, { name: "Lounge" });

        const trx = ctx.begin();
        const msg = await TestMessage.create(trx, {
            user: user,
            room: room,
            text: "Test traversal",
        });
        await trx.commit();

        // Fetch the message and traverse the refs
        const fetchedMsg = await TestMessage.get(ctx, msg.id);
        const fetchedUser = await fetchedMsg.user.get(ctx);
        const fetchedRoom = await fetchedMsg.room.get(ctx);

        expect(fetchedUser.name).toBe("Diana");
        expect(fetchedRoom.name).toBe("Lounge");
    });
});

