// Every key the CODE asks for exists in the catalogue.
//
// messagesParity.test.ts checks locale against locale, and that is not the
// same property. A key used in a component and absent from ALL FIVE
// catalogues passes parity and still renders its own path: next-intl prints
// "evaluation.detailTitle" to the reader.
//
// That is not hypothetical. On 2026-10-27 the evaluation-set detail page
// showed "evaluation.detailTitle" on the deployed site, and seven more keys
// on the same page were in the same state. All eight were written as
//
//     t("detailTitle", { default: "Evaluation set detail" })
//
// which looks like a fallback and is not one: next-intl has no `default`
// option, so the second argument is read as interpolation values and the
// English string in it never reaches the page. Eight call sites believed
// they were safe.
//
// So this test scans for that exact pattern and asserts the key resolves.
// Scoped to the pattern rather than to every t() call on purpose: a dynamic
// key cannot be checked statically, and a test that guessed at them would
// fail on correct code and get deleted.

import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import en from "@/messages/en.json";

type Catalogue = Record<string, unknown>;

const ROOTS = ["app", "components"];
const WITH_DEFAULT = /\bt\(\s*"([A-Za-z][\w.]*)"\s*,\s*\{\s*default\s*:/g;
const NAMESPACE = /useTranslations\(\s*"([\w.]+)"\s*\)/;

function tsxFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return tsxFiles(path);
    return path.endsWith(".tsx") ? [path] : [];
  });
}

function resolve(path: string): boolean {
  let node: unknown = en as Catalogue;
  for (const part of path.split(".")) {
    if (node === null || typeof node !== "object") return false;
    node = (node as Catalogue)[part];
    if (node === undefined) return false;
  }
  return typeof node === "string";
}

describe("every key asked for with a default exists anyway", () => {
  const offenders: string[] = [];
  for (const root of ROOTS) {
    for (const file of tsxFiles(join(__dirname, "..", root))) {
      const source = readFileSync(file, "utf8");
      const ns = source.match(NAMESPACE)?.[1];
      if (!ns) continue;
      for (const [, key] of source.matchAll(WITH_DEFAULT)) {
        if (!resolve(`${ns}.${key}`)) offenders.push(`${ns}.${key}  (${file.split("/web/")[1]})`);
      }
    }
  }

  it("resolves in en.json, because the default option does not exist", () => {
    expect(offenders).toEqual([]);
  });
});
