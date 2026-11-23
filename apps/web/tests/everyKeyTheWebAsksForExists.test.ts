// Every translation key the web asks for exists in the English catalogue.
//
// The parity test checks that the five catalogues hold the same keys. It cannot
// see a key that no catalogue holds, and next-intl renders such a key as its own
// path: on 2026-11-21 the window page's results heading read
// "evaluation.resultsTitle", and the API key dialog labelled the new key with
// "apiKeys.dialog.keyLabel". Passing a `default` to the call does not help, since
// next-intl does not read it.
//
// The scan resolves each call's namespace in its own scope: a binding
// `const t = useTranslations("ns")` holds inside the function that declares it,
// so two components of one file that bind `t` to different namespaces are told
// apart. Calls with a key that is not a string literal are not checked.

import fs from "node:fs";
import path from "node:path";
import ts from "typescript";
import { describe, expect, it } from "vitest";
import en from "@/messages/en.json";

const ROOT = path.resolve(__dirname, "..");

function holds(ns: string, key: string): boolean {
  let node: unknown = en;
  for (const part of [...ns.split("."), ...key.split(".")]) {
    if (node === null || typeof node !== "object" || !(part in node)) return false;
    node = (node as Record<string, unknown>)[part];
  }
  return true;
}

function sources(dir: string): string[] {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) return entry.name === "node_modules" ? [] : sources(full);
    return /\.tsx?$/.test(entry.name) && !/\.test\./.test(entry.name) ? [full] : [];
  });
}

function missingKeys(file: string): string[] {
  const src = ts.createSourceFile(
    file,
    fs.readFileSync(file, "utf8"),
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TSX,
  );
  const missing: string[] = [];
  const visit = (node: ts.Node, scope: Map<string, string>): void => {
    const inner = ts.isFunctionLike(node) ? new Map(scope) : scope;
    if (
      ts.isVariableDeclaration(node) &&
      ts.isIdentifier(node.name) &&
      node.initializer &&
      ts.isCallExpression(node.initializer) &&
      ["useTranslations", "getTranslations"].includes(node.initializer.expression.getText(src))
    ) {
      const [arg] = node.initializer.arguments;
      if (arg && ts.isStringLiteral(arg)) scope.set(node.name.text, arg.text);
    }
    if (ts.isCallExpression(node) && ts.isIdentifier(node.expression)) {
      const ns = scope.get(node.expression.text);
      const [arg] = node.arguments;
      if (ns !== undefined && arg && ts.isStringLiteral(arg) && !holds(ns, arg.text)) {
        missing.push(`${path.relative(ROOT, file)}: ${ns}.${arg.text}`);
      }
    }
    ts.forEachChild(node, (child) => visit(child, inner));
  };
  visit(src, new Map());
  return missing;
}

describe("translation keys", () => {
  it("every key a page or component asks for is in the English catalogue", () => {
    const files = ["app", "components", "lib"].flatMap((dir) => sources(path.join(ROOT, dir)));
    expect(files.length).toBeGreaterThan(50);
    expect(files.flatMap(missingKeys)).toEqual([]);
  });
});
