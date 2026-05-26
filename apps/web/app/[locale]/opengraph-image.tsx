import { ImageResponse } from "next/og";

// SEO baseline OG image. Rendered at build time per locale (and
// regenerated on demand) by Next's built-in next/og runtime. Kept
// dependency-free so the build never pulls fonts off the network.
// If `next/og` ever fails to bundle, drop a static `public/og.png`
// and remove this file; Next will fall back to that path because
// metadata.openGraph.images is omitted.

export const runtime = "edge";
export const alt = "PROTEA, Protein functional Embedding-based Annotation";
export const size = { width: 1200, height: 630 } as const;
export const contentType = "image/png";

export default function OGImage() {
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          justifyContent: "center",
          alignItems: "flex-start",
          padding: "80px 96px",
          background:
            "linear-gradient(135deg, #0f172a 0%, #1e293b 45%, #312e81 100%)",
          color: "#f8fafc",
          fontFamily: "system-ui, sans-serif",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "16px",
            fontSize: "22px",
            letterSpacing: "0.18em",
            textTransform: "uppercase",
            color: "#94a3b8",
            fontWeight: 600,
          }}
        >
          <span
            style={{
              width: "10px",
              height: "10px",
              borderRadius: "9999px",
              background: "#34d399",
            }}
          />
          Open research platform
        </div>

        <div
          style={{
            marginTop: "32px",
            fontSize: "168px",
            fontWeight: 800,
            lineHeight: 1,
            letterSpacing: "-0.04em",
            background:
              "linear-gradient(120deg, #60a5fa 0%, #a78bfa 50%, #f472b6 100%)",
            backgroundClip: "text",
            color: "transparent",
          }}
        >
          PROTEA
        </div>

        <div
          style={{
            marginTop: "32px",
            fontSize: "40px",
            lineHeight: 1.2,
            fontWeight: 500,
            maxWidth: "960px",
            color: "#e2e8f0",
          }}
        >
          Protein functional Embedding-based Annotation
        </div>

        <div
          style={{
            marginTop: "20px",
            fontSize: "26px",
            lineHeight: 1.4,
            color: "#94a3b8",
            maxWidth: "960px",
          }}
        >
          GO term prediction by PLM embedding similarity, alignment features,
          and learned reranking.
        </div>

        <div
          style={{
            position: "absolute",
            bottom: "64px",
            left: "96px",
            display: "flex",
            alignItems: "center",
            gap: "12px",
            fontSize: "22px",
            color: "#cbd5e1",
            letterSpacing: "0.04em",
          }}
        >
          protea.ngrok.app
        </div>
      </div>
    ),
    { ...size },
  );
}
