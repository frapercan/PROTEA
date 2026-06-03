// FARM-UI.5 unit tests for the cost-rollup pivots. The /cost endpoint
// returns one bucket per (date, agent, model); the helpers pivot the
// same rows into three views without re-fetching, so a single request
// drives the entire dashboard.

import { describe, expect, it } from "vitest";
import {
  dateRange,
  formatUsd,
  modelColor,
  totalsByAgent,
  totalsByDay,
  totalsByModelDay,
} from "@/lib/farmCost";
import type { FarmAgentBudget, FarmCostBucket } from "@/lib/farmApi";

function bucket(
  date: string,
  agent: string,
  model: string,
  usd: number,
  tasks = 1,
): FarmCostBucket {
  return {
    date,
    agent,
    model,
    tasks,
    input_tokens: tasks * 1000,
    output_tokens: tasks * 500,
    cache_tokens: 0,
    usd,
  };
}

function budget(name: string, max: number | null): FarmAgentBudget {
  return {
    name,
    kind: "subagent",
    model: null,
    expected_tokens: null,
    max_usd_per_day: max,
  };
}

describe("totalsByAgent", () => {
  it("sums usd and tasks per agent", () => {
    const rows = [
      bucket("2026-05-17", "executor", "opus-4.7", 3.0, 2),
      bucket("2026-05-18", "executor", "opus-4.7", 2.5, 1),
      bucket("2026-05-18", "janitor", "haiku-4.5", 0.4, 1),
    ];
    const totals = totalsByAgent(rows, [], 7);
    expect(totals).toHaveLength(2);
    const executor = totals.find((t) => t.agent === "executor")!;
    const janitor = totals.find((t) => t.agent === "janitor")!;
    expect(executor.usd).toBeCloseTo(5.5);
    expect(executor.tasks).toBe(3);
    expect(janitor.usd).toBeCloseTo(0.4);
  });

  it("flags overBudget when daily average exceeds cap", () => {
    // 14 usd over 7 days = 2/day average; cap 1.0/day → over.
    const rows = [bucket("2026-05-18", "executor", "opus-4.7", 14, 1)];
    const totals = totalsByAgent(rows, [budget("executor", 1.0)], 7);
    expect(totals[0].overBudget).toBe(true);
  });

  it("does not flag overBudget when within cap", () => {
    // 3.5 over 7 days = 0.5/day average; cap 1.0 → ok.
    const rows = [bucket("2026-05-18", "executor", "opus-4.7", 3.5, 1)];
    const totals = totalsByAgent(rows, [budget("executor", 1.0)], 7);
    expect(totals[0].overBudget).toBe(false);
    expect(totals[0].budget).toBe(1.0);
  });

  it("includes zero-spend agents that have a budget", () => {
    const totals = totalsByAgent([], [budget("shepherd", 1.5)], 7);
    expect(totals).toHaveLength(1);
    expect(totals[0].agent).toBe("shepherd");
    expect(totals[0].usd).toBe(0);
    expect(totals[0].overBudget).toBe(false);
  });

  it("sorts descending by usd then ascending by name", () => {
    const rows = [
      bucket("2026-05-18", "alpha", "m", 2.0),
      bucket("2026-05-18", "beta", "m", 5.0),
      bucket("2026-05-18", "gamma", "m", 5.0),
    ];
    const totals = totalsByAgent(rows, [], 7);
    expect(totals.map((t) => t.agent)).toEqual(["beta", "gamma", "alpha"]);
  });
});

describe("totalsByDay", () => {
  it("fills empty days with zero usd", () => {
    const today = new Date("2026-05-18T12:00:00Z");
    const rows = [bucket("2026-05-17", "exec", "m", 1.5)];
    const series = totalsByDay(rows, 3, today);
    expect(series.map((s) => s.date)).toEqual([
      "2026-05-16",
      "2026-05-17",
      "2026-05-18",
    ]);
    expect(series.map((s) => s.usd)).toEqual([0, 1.5, 0]);
  });

  it("sums multiple buckets on the same day", () => {
    const today = new Date("2026-05-18T12:00:00Z");
    const rows = [
      bucket("2026-05-18", "a", "m1", 1.0),
      bucket("2026-05-18", "b", "m2", 2.0),
    ];
    const series = totalsByDay(rows, 1, today);
    expect(series[0].usd).toBeCloseTo(3.0);
    expect(series[0].tasks).toBe(2);
  });
});

describe("totalsByModelDay", () => {
  it("ranks models by descending total and pivots per-day", () => {
    const today = new Date("2026-05-18T12:00:00Z");
    const rows = [
      bucket("2026-05-17", "x", "opus", 5.0),
      bucket("2026-05-18", "x", "opus", 1.0),
      bucket("2026-05-18", "y", "haiku", 0.5),
    ];
    const { axis, models } = totalsByModelDay(rows, 2, today);
    expect(models).toEqual(["opus", "haiku"]);
    expect(axis).toHaveLength(2);
    const day17 = axis.find((p) => p.date === "2026-05-17")!;
    expect(day17.perModel.opus).toBeCloseTo(5);
    expect(day17.perModel.haiku).toBe(0);
    expect(day17.total).toBeCloseTo(5);
  });

  it("ignores rows outside the day window", () => {
    const today = new Date("2026-05-18T12:00:00Z");
    const rows = [
      bucket("2026-05-01", "x", "opus", 99),
      bucket("2026-05-18", "y", "haiku", 1),
    ];
    const { axis, models } = totalsByModelDay(rows, 2, today);
    expect(models).toEqual(["haiku"]);
    expect(axis.reduce((s, p) => s + p.total, 0)).toBeCloseTo(1);
  });
});

describe("dateRange", () => {
  it("yields inclusive ISO date strings", () => {
    const a = new Date("2026-05-16T00:00:00Z");
    const b = new Date("2026-05-18T00:00:00Z");
    expect(dateRange(a, b)).toEqual([
      "2026-05-16",
      "2026-05-17",
      "2026-05-18",
    ]);
  });
});

describe("formatUsd", () => {
  it("formats zero, sub-dollar, normal and kilo amounts", () => {
    expect(formatUsd(0)).toBe("$0");
    expect(formatUsd(0.005)).toBe("$0.0050");
    expect(formatUsd(2.3)).toBe("$2.30");
    expect(formatUsd(12345.6)).toBe("$12.3k");
  });
});

describe("modelColor", () => {
  it("returns a stable color for the same index", () => {
    expect(modelColor(0)).toBe(modelColor(0));
    expect(modelColor(-1)).toMatch(/^#[0-9a-f]{6}$/i);
  });
});
