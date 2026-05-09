"use client";

export function DocLinks() {
  const base = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";
  return (
    <>
      <a
        href={`${base}/docs`}
        target="_blank"
        rel="noopener noreferrer"
        className="hover:text-slate-900"
      >
        Swagger
      </a>
      <a
        href="/sphinx/"
        target="_blank"
        rel="noopener noreferrer"
        className="hover:text-slate-900"
      >
        Docs
      </a>
      <a
        href="/thesis.pdf"
        target="_blank"
        rel="noopener noreferrer"
        className="hover:text-slate-900"
      >
        Thesis
      </a>
    </>
  );
}
