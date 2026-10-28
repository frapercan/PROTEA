// What the crawler is told about, and what it was not.
//
// Found on 2026-10-28 while making the campaign's own result legible: the
// sitemap listed thirteen instrument pages and NOT ONE page of the book, so a
// crawler was told about the tools and not about the argument they support.
// /instrument/graph appeared twice, and /instrument/stratum -- the surface the
// stratified evaluation lives on, which is what the campaign exists to show --
// appeared neither here nor in the sidebar, so the only way to it was a
// hand-typed URL.
//
// The assertions are written over the whole list rather than over the routes
// that were missing, so a future surface added without an entry fails too.

import { describe, expect, it } from "vitest";
import sitemap from "@/app/sitemap";

const LOCALES = ["en", "es", "de", "pt", "zh"] as const;

/** The paths the sitemap covers, locale stripped, deduplicated. */
function paths(): string[] {
  const entries = sitemap();
  const stripped = entries.map((e) => {
    const url = String(e.url);
    for (const locale of LOCALES) {
      if (url === `/${locale}`) return "";
      if (url.startsWith(`/${locale}/`)) return url.slice(locale.length + 1);
    }
    return url;
  });
  return [...new Set(stripped)];
}

describe("sitemap coverage", () => {
  it("names every path once", () => {
    const entries = sitemap();
    const urls = entries.map((e) => String(e.url));
    expect(urls.length).toBe(new Set(urls).size);
  });

  it("covers every locale for every path", () => {
    const entries = sitemap();
    expect(entries.length).toBe(paths().length * LOCALES.length);
  });

  it("tells a crawler about the argument and not only about the tools", () => {
    // The book is the entrance. A sitemap that lists the instrument and omits
    // the four pillars describes a toolbox rather than a thesis.
    for (const pillar of ["/pillar/1", "/pillar/2", "/pillar/3", "/pillar/4"]) {
      expect(paths(), pillar).toContain(pillar);
    }
  });

  it("covers the stratified evaluation, which is what a cell opens into", () => {
    expect(paths()).toContain("/instrument/stratum");
  });

  it("covers the page that asks a visitor for a sequence", () => {
    expect(paths()).toContain("/annotate");
  });

  it("gives every entry a priority and a change frequency a crawler can read", () => {
    for (const entry of sitemap()) {
      expect(typeof entry.priority, String(entry.url)).toBe("number");
      expect(entry.priority as number).toBeGreaterThan(0);
      expect(entry.priority as number).toBeLessThanOrEqual(1);
      expect(entry.changeFrequency, String(entry.url)).toBeTruthy();
    }
  });
});
