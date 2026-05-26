import type { MetadataRoute } from "next";

// SEO baseline robots policy. Allow public pages, disallow operator
// surfaces, auth flows, profile (user-specific), and API proxy paths.
// The sitemap pointer is relative; Next resolves it against
// metadataBase declared in app/[locale]/layout.tsx.

export default function robots(): MetadataRoute.Robots {
  return {
    rules: [
      {
        userAgent: "*",
        allow: "/",
        disallow: [
          "/admin",
          "/maintenance",
          "/auth",
          "/login",
          "/logout",
          "/signup",
          "/profile",
          "/api-proxy",
          "/farm-api",
          "/v1",
        ],
      },
    ],
    sitemap: "/sitemap.xml",
  };
}
