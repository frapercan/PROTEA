import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  trailingSlash: true,
  async rewrites() {
    return [
      { source: "/sphinx/", destination: "http://localhost:8000/sphinx/" },
      { source: "/sphinx/:path*", destination: "http://localhost:8000/sphinx/:path*" },
      {
        source: "/api-proxy/:path*",
        destination: "http://localhost:8000/:path*",
      },
    ];
  },
};

export default nextConfig;
