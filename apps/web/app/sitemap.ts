import type { MetadataRoute } from "next";

// Public routes that should appear in search engine indexes. Operator,
// admin, auth and profile routes are deliberately excluded; they are
// either gated by middleware or carry user-specific state and have no
// indexable content.
//
// One row per (locale, path) pair so search engines see hreflang
// alternates for the same canonical resource. `alternates.languages`
// covers the rendered <head>; this sitemap mirrors it for crawlers.

const LOCALES = ["en", "es", "de", "pt", "zh"] as const;

const PUBLIC_PATHS: ReadonlyArray<{
  path: string;
  changeFrequency: MetadataRoute.Sitemap[number]["changeFrequency"];
  priority: number;
}> = [
  { path: "", changeFrequency: "daily", priority: 1.0 },
  { path: "/instrument", changeFrequency: "daily", priority: 0.9 },
  { path: "/instrument/graph", changeFrequency: "daily", priority: 0.9 },
  { path: "/instrument/benchmark", changeFrequency: "daily", priority: 0.9 },
  { path: "/instrument/graph", changeFrequency: "daily", priority: 0.9 },
  { path: "/instrument/proteins", changeFrequency: "daily", priority: 0.8 },
  { path: "/instrument/annotations", changeFrequency: "daily", priority: 0.7 },
  { path: "/instrument/embeddings", changeFrequency: "weekly", priority: 0.7 },
  { path: "/instrument/datasets", changeFrequency: "weekly", priority: 0.7 },
  { path: "/instrument/query-sets", changeFrequency: "weekly", priority: 0.6 },
  { path: "/instrument/functional-annotation", changeFrequency: "weekly", priority: 0.7 },
  { path: "/instrument/scoring", changeFrequency: "weekly", priority: 0.6 },
  { path: "/instrument/evaluation", changeFrequency: "weekly", priority: 0.7 },
  { path: "/instrument/reranker", changeFrequency: "weekly", priority: 0.7 },
  { path: "/instrument/jobs", changeFrequency: "hourly", priority: 0.5 },
  { path: "/instrument/stack", changeFrequency: "hourly", priority: 0.4 },
  { path: "/support", changeFrequency: "monthly", priority: 0.3 },
];

export default function sitemap(): MetadataRoute.Sitemap {
  const now = new Date();
  const out: MetadataRoute.Sitemap = [];
  for (const locale of LOCALES) {
    for (const entry of PUBLIC_PATHS) {
      const url = `/${locale}${entry.path}`;
      out.push({
        url,
        lastModified: now,
        changeFrequency: entry.changeFrequency,
        priority: entry.priority,
        alternates: {
          languages: Object.fromEntries(
            LOCALES.map((alt) => [alt, `/${alt}${entry.path}`]),
          ),
        },
      });
    }
  }
  return out;
}
