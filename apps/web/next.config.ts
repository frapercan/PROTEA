import type { NextConfig } from "next";

const apiUrl = process.env.PROTEA_API_URL ?? "http://localhost:8000";

const nextConfig: NextConfig = {
  output: "standalone",
  trailingSlash: true,
  experimental: {
    middlewareClientMaxBodySize: 100 * 1024 * 1024, // 100 MB
  },
  async rewrites() {
    return [
      { source: "/sphinx/", destination: `${apiUrl}/sphinx/` },
      { source: "/sphinx/:path*", destination: `${apiUrl}/sphinx/:path*` },
      {
        source: "/api-proxy/:path*",
        destination: `${apiUrl}/:path*`,
      },
    ];
  },
};

export default nextConfig;
