import type { MetadataRoute } from "next";

// SEO baseline robots policy. Allow public pages, disallow operator
// surfaces, auth flows, profile (user-specific), and API proxy paths.
// The sitemap pointer must be ABSOLUTE. Next does not resolve it against
// metadataBase here, so a relative value is emitted verbatim and
// "Sitemap: /sitemap.xml" is not a valid robots.txt directive.

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
    sitemap: "https://protea.ngrok.app/sitemap.xml",
  };
}
