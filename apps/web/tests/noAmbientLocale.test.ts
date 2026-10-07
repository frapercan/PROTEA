// No source file may format with the ambient locale.
//
// `x.toLocaleString()` and `new Date(x).toLocaleString([])` read the
// ENVIRONMENT's locale and time zone. Node on the deployment host
// resolves to es-ES / Europe/Madrid and a visitor's browser resolves to
// theirs, so the server renders "754.862" where the browser renders
// "754,862". React refuses to reconcile that: it is a hydration failure
// (React #418, hit on /instrument/datasets on 2026-10-07) and it is why
// the thousands separator disagreed from page to page.
//
// 83 call sites were swept into lib/format.ts on 2026-10-07. This test
// exists so the next one is caught at the keyboard rather than by an
// external audit. Use formatCount / formatDecimal / formatDateTime /
// formatDate, or next-intl's `{n, number}` for prose, which is
// locale-aware and agrees across hydration because both sides know the
// route's locale.

import { readFileSync, readdirSync, statSync } from "node:fs";
import { extname, join } from "node:path";
import { describe, expect, it } from "vitest";

const ROOTS = ["app", "components", "lib", "tests"];
const SKIP = new Set(["node_modules", ".next", "e2e"]);

/** Formatting without an explicit locale, in any of its spellings. */
const AMBIENT = /\.toLocale(?:String|DateString|TimeString)\(\s*(?:\[\s*\]|undefined)?\s*(?:,|\))/;

function sources(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir)) {
    if (SKIP.has(entry)) continue;
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) {
      out.push(...sources(full));
    } else if ([".ts", ".tsx"].includes(extname(entry))) {
      // Tests are included on purpose. lib/cellPopulation.test.ts built
      // its expected string by calling `(6234).toLocaleString()`, the
      // same thing the code called, so it asserted that the function
      // agrees with itself in whatever locale the runner used. It passed
      // for as long as the separator was missing.
      out.push(full);
    }
  }
  return out;
}

describe("formatting is deterministic across hydration", () => {
  const files = ROOTS.flatMap((r) => sources(r));

  it("has files to check, so a passing run means something", () => {
    expect(files.length).toBeGreaterThan(100);
  });

  it("no source formats with the ambient locale", () => {
    const offenders: string[] = [];
    for (const file of files) {
      // lib/format.ts is the one place allowed to name the problem, and
      // it names it in prose rather than calling it.
      if (file.endsWith(join("lib", "format.ts"))) continue;
      readFileSync(file, "utf8")
        .split("\n")
        .forEach((line, i) => {
          const code = line.replace(/\/\/.*$/, "").replace(/^\s*\*.*$/, "");
          if (AMBIENT.test(code)) offenders.push(`${file}:${i + 1}`);
        });
    }
    expect(offenders).toEqual([]);
  });
});
