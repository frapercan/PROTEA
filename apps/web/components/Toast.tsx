"use client";

import { createContext, useCallback, useContext, useState } from "react";

type ToastType = "success" | "error" | "info";
type Toast = { id: number; type: ToastType; message: string };

const ToastContext = createContext<(msg: string, type?: ToastType) => void>(() => {});

let _nextId = 0;

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);

  const add = useCallback((message: string, type: ToastType = "info") => {
    const id = ++_nextId;
    setToasts((prev) => [...prev, { id, type, message }]);
    setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== id));
    }, 3500);
  }, []);

  const STYLES: Record<ToastType, string> = {
    success: "border-green-200 bg-green-50 text-green-800",
    error:   "border-red-200 bg-red-50 text-red-800",
    info:    "border-blue-200 bg-blue-50 text-blue-800",
  };

  const ICONS: Record<ToastType, string> = {
    success: "✓",
    error:   "✕",
    info:    "ℹ",
  };

  const politeToasts = toasts.filter((t) => t.type !== "error");
  const assertiveToasts = toasts.filter((t) => t.type === "error");

  return (
    <ToastContext.Provider value={add}>
      {children}
      <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2 pointer-events-none">
        {/* Non-error toasts: announced when the screen reader is idle. */}
        <div role="status" aria-live="polite" aria-atomic="true" className="flex flex-col gap-2">
          {politeToasts.map((t) => (
            <div
              key={t.id}
              className={`pointer-events-auto flex items-center gap-2.5 rounded-lg border px-4 py-3 text-sm shadow-lg ${STYLES[t.type]}`}
            >
              <span className="font-semibold" aria-hidden="true">{ICONS[t.type]}</span>
              {t.message}
            </div>
          ))}
        </div>
        {/* Error toasts: interrupt the screen reader. role=alert implies aria-live=assertive. */}
        <div role="alert" aria-atomic="true" className="flex flex-col gap-2">
          {assertiveToasts.map((t) => (
            <div
              key={t.id}
              className={`pointer-events-auto flex items-center gap-2.5 rounded-lg border px-4 py-3 text-sm shadow-lg ${STYLES[t.type]}`}
            >
              <span className="font-semibold" aria-hidden="true">{ICONS[t.type]}</span>
              {t.message}
            </div>
          ))}
        </div>
      </div>
    </ToastContext.Provider>
  );
}

export function useToast() {
  return useContext(ToastContext);
}
