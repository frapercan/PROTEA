"use client";

import { useState } from "react";

export function ResetDbButton() {
  const [showConfirm, setShowConfirm] = useState(false);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<"ok" | "error" | null>(null);

  async function handleReset() {
    setLoading(true);
    setResult(null);
    try {
      const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/admin/reset-db`, {
        method: "POST",
      });
      const data = await res.json();
      setResult(data.ok ? "ok" : "error");
    } catch {
      setResult("error");
    } finally {
      setLoading(false);
      setShowConfirm(false);
    }
  }

  return (
    <>
      <button
        onClick={() => { setResult(null); setShowConfirm(true); }}
        className="rounded-md border border-red-200 bg-red-50 px-3 py-1.5 text-xs font-medium text-red-600 hover:bg-red-100 transition-colors"
      >
        Reset DB
      </button>

      {result === "ok" && (
        <span className="ml-2 text-xs text-green-600 font-medium">✓ Reset OK</span>
      )}
      {result === "error" && (
        <span className="ml-2 text-xs text-red-600 font-medium">✗ Error</span>
      )}

      {showConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
          <div className="w-full max-w-sm rounded-xl border bg-white shadow-xl p-6">
            <h2 className="text-base font-semibold text-gray-900">Reset database?</h2>
            <p className="mt-2 text-sm text-gray-500">
              Se borrarán <span className="font-medium text-red-600">todos los datos</span>: proteínas, anotaciones, embeddings, predicciones y jobs. Esta acción es irreversible.
            </p>
            <div className="mt-5 flex justify-end gap-2">
              <button
                onClick={() => setShowConfirm(false)}
                disabled={loading}
                className="rounded-md border px-4 py-2 text-sm hover:bg-gray-50 disabled:opacity-50"
              >
                Cancelar
              </button>
              <button
                onClick={handleReset}
                disabled={loading}
                className="rounded-md bg-red-600 px-4 py-2 text-sm text-white hover:bg-red-700 disabled:opacity-50"
              >
                {loading ? "Reseteando…" : "Sí, resetear"}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
