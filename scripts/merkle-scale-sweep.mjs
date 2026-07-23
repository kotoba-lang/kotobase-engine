import { spawnSync } from "node:child_process";
import { performance } from "node:perf_hooks";

const sizes = (process.env.KOTOBASE_SCALE_SIZES ?? "100000")
  .split(",").map((value) => Number.parseInt(value.trim(), 10));
if (sizes.length === 0
    || sizes.some((size) => !Number.isSafeInteger(size) || size < 1)) {
  throw new Error("KOTOBASE_SCALE_SIZES must contain positive integers");
}
const maxHeap = process.env.KOTOBASE_SCALE_MAX_HEAP ?? "2g";
const writers = process.env.KOTOBASE_SCALE_WRITERS ?? "1";
const enforce = process.env.KOTOBASE_SCALE_ENFORCE ?? "1";
const results = [];

for (const size of sizes) {
  const started = performance.now();
  const child = spawnSync("clojure", ["-M:merkle-bench", String(size)], {
    encoding: "utf8",
    env: {
      ...process.env,
      JAVA_TOOL_OPTIONS: `-Xmx${maxHeap}`,
      MERKLE_BENCH_WRITERS: writers,
      MERKLE_BENCH_ENFORCE_GATE: enforce,
    },
  });
  results.push({
    datoms: size,
    durationMs: Math.round(performance.now() - started),
    exitCode: child.status ?? 1,
    receiptEdn: child.stdout.trim(),
    stderr: child.stderr.trim(),
  });
  if (child.status !== 0) break;
}

const receipt = {
  schema: 1,
  runner: "kotobase-peer-merkle-scale",
  isolatedJvmPerSize: true,
  maxHeap,
  writers: Number(writers),
  enforced: enforce === "1",
  results,
  outcome: results.length === sizes.length
    && results.every((result) => result.exitCode === 0) ? "succeeded" : "failed",
};
console.log(JSON.stringify(receipt, null, 2));
if (receipt.outcome !== "succeeded") process.exit(1);
