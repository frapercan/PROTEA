"use client";

export function DocLinks() {
  const base = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";
  return (
    <>
      <a
        href={`${base}/docs`}
        target="_blank"
        rel="noopener noreferrer"
        className="hover:text-gray-900"
      >
        Swagger
      </a>
      <a
        href={`${base}/sphinx`}
        target="_blank"
        rel="noopener noreferrer"
        className="hover:text-gray-900"
      >
        Docs
      </a>
    </>
  );
}
