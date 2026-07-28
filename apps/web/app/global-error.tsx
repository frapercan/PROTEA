"use client";

/**
 * Last-resort boundary. Next renders this only when the root layout
 * itself throws (for example if loading the locale messages fails), which
 * means it replaces the whole document and must ship its own <html> and
 * <body>.
 *
 * Because it renders outside the `NextIntlClientProvider`, there is no
 * message catalogue here, so the copy is hardcoded English. This is the
 * one screen that cannot be localized: reaching it means the localization
 * layer is exactly what failed. Every other boundary (`error.tsx`,
 * `not-found.tsx`) runs inside the provider and is translated.
 */

import { useEffect } from "react";

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("[global-error]", error);
  }, [error]);

  return (
    <html lang="en">
      <body
        style={{
          margin: 0,
          minHeight: "100vh",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          background: "#f8fafc",
          color: "#0f172a",
          fontFamily:
            "ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif",
        }}
      >
        <main
          style={{
            maxWidth: "34rem",
            padding: "2rem",
            display: "flex",
            flexDirection: "column",
            gap: "1rem",
          }}
        >
          <h1 style={{ fontSize: "1.5rem", fontWeight: 600, margin: 0 }}>
            The application failed to start this page
          </h1>
          <p style={{ margin: 0, lineHeight: 1.6, color: "#475569" }}>
            A fault in the application shell stopped the interface from
            rendering. Retry once. If it recurs, the frontend build or its
            connection to the backend needs attention. Quote the reference
            below when you check the server logs.
          </p>
          {error.digest ? (
            <p
              style={{
                margin: 0,
                fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
                fontSize: "0.875rem",
                color: "#334155",
              }}
            >
              Reference: {error.digest}
            </p>
          ) : null}
          <div style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap" }}>
            <button
              type="button"
              onClick={reset}
              style={{
                border: "none",
                borderRadius: "0.75rem",
                background: "#2563eb",
                color: "#fff",
                padding: "0.5rem 1rem",
                fontSize: "0.875rem",
                fontWeight: 600,
                cursor: "pointer",
              }}
            >
              Retry
            </button>
            <a
              href="/en"
              style={{
                borderRadius: "0.75rem",
                border: "1px solid #cbd5e1",
                color: "#334155",
                padding: "0.5rem 1rem",
                fontSize: "0.875rem",
                fontWeight: 600,
                textDecoration: "none",
              }}
            >
              Back to the start page
            </a>
          </div>
        </main>
      </body>
    </html>
  );
}
