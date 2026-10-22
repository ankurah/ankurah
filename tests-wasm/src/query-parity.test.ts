import { expect, it } from "vitest";
import init, { query_parity_browser } from "../bindings/pkg/ankurah_tests_wasm_bindings.js";

// Vite passes these settings into the browser. Defaults are functional smoke
// coverage; use isolated alternating baseline/candidate invocations for timings.
const setting = (name: string, fallback: string): string => import.meta.env[`VITE_QUERY_PARITY_${name}`] ?? fallback;
const numbers = (name: string, fallback: string): number[] => setting(name, fallback).split(",").map(Number);
const label = setting("LABEL", "candidate");
// An omitted value is unlimited; zero requests an empty ordered result.
const limitSetting = setting("LIMIT", "");
const limit = limitSetting === "" ? undefined : Number(limitSetting);
if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 0)) throw new Error("LIMIT must be a nonnegative integer");
const fixtures: { n: number; rtt: number; bps: number; mode: string; cache: string; repeat: number }[] = [];
for (const n of numbers("SIZES", "8")) {
    for (const rtt of numbers("RTTS", "0")) {
        for (const bps of numbers("BPS", "0")) {
            for (const mode of setting("MODES", "cold,warm_refresh,warm_equal,warm_edit,warm_membership").split(",")) {
                for (const cache of setting("CACHE", "empty,half,all").split(",")) {
                    if ((n === 0 && cache !== "empty") || (mode !== "cold" && n === 0) || (mode !== "cold" && mode !== "warm_refresh" && cache !== "all")) continue;
                    for (let repeat = 0; repeat < Number(setting("REPEATS", "1")); repeat++) {
                        fixtures.push({ n, rtt, bps, mode, cache, repeat });
                    }
                }
            }
        }
    }
}

it.each(fixtures)("public-API query parity $mode/$n/$cache/$rtt ms/$bps Bps/$repeat", async ({ n, rtt, bps, mode, cache, repeat }) => {
    await init();
    const prefix = `query_parity_${label}_${Date.now()}_${crypto.randomUUID()}`;
    const record = JSON.parse(await query_parity_browser(prefix, n, cache, mode, rtt, bps, label, limit));
    expect(record.verified).toBe(true);
    expect(record.entities).toBe(n);
    expect(record.result_limit).toBe(limit ?? null);
    expect(record.expected_results).toBe(Math.min(n, limit ?? n));
    expect(record.mode).toBe(mode);
    expect(record.cache).toBe(cache);
    console.log(`BROWSER_STARTUP ${JSON.stringify({ ...record, repeat })}`);
}, 180_000);
