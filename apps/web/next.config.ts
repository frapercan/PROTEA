import type { NextConfig } from "next";
import createNextIntlPlugin from "next-intl/plugin";

const withNextIntl = createNextIntlPlugin("./i18n/request.ts");

const apiUrl = process.env.PROTEA_API_URL ?? "http://localhost:8000";
const farmApiUrl = process.env.FARM_API_URL ?? "http://localhost:8801";

const nextConfig: NextConfig = {
  output: "standalone",
  trailingSlash: true,
  experimental: {
    proxyClientMaxBodySize: 100 * 1024 * 1024, // 100 MB
  },
  async rewrites() {
    return [
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
