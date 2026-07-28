import type { NextConfig } from "next";
import createNextIntlPlugin from "next-intl/plugin";

const withNextIntl = createNextIntlPlugin("./i18n/request.ts");

const apiUrl = process.env.PROTEA_API_URL ?? "http://localhost:8000";
const farmApiUrl = process.env.FARM_API_URL ?? "http://localhost:8801";

// "Interface as a book" redesign: the operational dashboard moved under the
// /instrument/* prefix so the top level stays the argument (home + pillars).
// These routes carry every operational / data / experiment surface. The
// book routes (/, /pillar/*, /score/*, /feature-registry, /annotate) and the
// account / auth / ops-chrome routes (/login, /logout, /signup, /auth,
// /profile, /maintenance, /support, /admin/*) intentionally stay top-level.
const MOVED_ROUTES = [
  "benchmark",
  "datasets",
  "embeddings",
  "evaluation",
  "functional-annotation",
  "jobs",
  "proteins",
  "query-sets",
  "reranker",
  "scoring",
  "annotations",
  "stack",
  "farm",
] as const;

// One permanent (308) redirect family per moved route so no old bookmark or
// missed in-app link can 404. Each route gets four sources: bare + nested,
// and locale-prefixed + nested (locale constrained to the five known ones so
// a stray first segment is never mistaken for a locale).
const LOCALES = "en|es|de|pt|zh";
function movedRouteRedirects() {
  return MOVED_ROUTES.flatMap((route) => [
    { source: `/${route}`, destination: `/instrument/${route}`, permanent: true },
    { source: `/${route}/:path*`, destination: `/instrument/${route}/:path*`, permanent: true },
    {
      source: `/:locale(${LOCALES})/${route}`,
      destination: `/:locale/instrument/${route}`,
      permanent: true,
    },
    {
      source: `/:locale(${LOCALES})/${route}/:path*`,
      destination: `/:locale/instrument/${route}/:path*`,
      permanent: true,
    },
  ]);
}

const nextConfig: NextConfig = {
  output: "standalone",
  trailingSlash: true,
  experimental: {
    proxyClientMaxBodySize: 100 * 1024 * 1024, // 100 MB
  },
  async redirects() {
    return movedRouteRedirects();
  },
  async rewrites() {
    return [
      // Thesis PDF is served by the API from a stable mounted path (serve-mount),
      // so it refreshes on a file overwrite with no frontend rebuild.
      { source: "/thesis.pdf", destination: `${apiUrl}/thesis.pdf` },
      { source: "/sphinx/", destination: `${apiUrl}/sphinx/` },
      { source: "/sphinx/:path*", destination: `${apiUrl}/sphinx/:path*` },
      { source: "/docs/:slug/", destination: `${apiUrl}/docs/:slug/` },
      { source: "/docs/:slug/:path*", destination: `${apiUrl}/docs/:slug/:path*` },
      {
        source: "/api-proxy/:path*",
        destination: `${apiUrl}/:path*`,
      },
      {
        source: "/farm-api/:path*",
        destination: `${farmApiUrl}/:path*`,
      },
      {
        source: "/v1/:path*",
        destination: `${apiUrl}/v1/:path*`,
      },
    ];
  },
};

export default withNextIntl(nextConfig);
