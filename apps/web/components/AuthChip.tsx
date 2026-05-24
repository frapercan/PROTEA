"use client";

import { useEffect, useState } from "react";
import { useTranslations } from "next-intl";
import { baseUrl } from "@/lib/api";
import {
  SESSION_COOKIE_NAME,
  decodeJwtPayload,
  getSessionToken,
  type Role,
  roleFromToken,
} from "@/lib/auth";

/**
 * Auth-state indicator for the header.
 *
 * FEAT-AUTH (2026-05-23): the source of truth is the ``protea_session``
 * cookie minted by ``POST /v1/auth/login``. The legacy
 * ``localStorage["protea.bearer"]`` / ``localStorage["protea.apiKey"]``
 * fallbacks are still inspected so devs who haven't migrated to the
 * cookie flow keep seeing an accurate chip. The role label is read
 * from the JWT payload (``role`` claim) and rendered next to the
 * identity hint.
 */

type AuthMode = "anonymous" | "apiKey" | "bearer";

function readMode(): { mode: AuthMode; hint: string | null; role: Role } {
  if (typeof window === "undefined") return { mode: "anonymous", hint: null, role: "viewer" };
  // Cookie session wins.
  const cookieToken = getSessionToken();
  if (cookieToken) {
    const claims = decodeJwtPayload(cookieToken);
    const sub = typeof claims?.sub === "string" ? claims.sub : cookieToken;
    return {
      mode: "bearer",
      hint: `${sub.slice(0, 6)}…`,
      role: roleFromToken(cookieToken),
    };
  }
  try {
    const bearer = window.localStorage.getItem("protea.bearer");
    if (bearer && bearer.length > 16) {
      return { mode: "bearer", hint: `${bearer.slice(0, 6)}…`, role: roleFromToken(bearer) };
    }
    const apiKey = window.localStorage.getItem("protea.apiKey");
    if (apiKey && apiKey.length > 8) {
      const prefix = apiKey.slice(0, apiKey.indexOf(".") > 0 ? apiKey.indexOf(".") : 6);
      return { mode: "apiKey", hint: `${prefix}…`, role: "viewer" };
    }
  } catch {
    // ignore (private mode, etc.)
  }
  return { mode: "anonymous", hint: null, role: "viewer" };
}

export function AuthChip() {
  const t = useTranslations("nav");
  const [mode, setMode] = useState<AuthMode>("anonymous");
  const [hint, setHint] = useState<string | null>(null);
  const [role, setRole] = useState<Role>("viewer");
  const [open, setOpen] = useState(false);

  useEffect(() => {
    const r = readMode();
    setMode(r.mode);
    setHint(r.hint);
    setRole(r.role);

    // Cross-tab synchronisation: if another tab signs in / out, reflect it.
    const onStorage = (e: StorageEvent) => {
      if (!e.key || e.key.startsWith("protea.")) {
        const r2 = readMode();
        setMode(r2.mode);
        setHint(r2.hint);
        setRole(r2.role);
      }
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  const styles: Record<AuthMode, { bg: string; ring: string; dot: string; text: string }> = {
    anonymous: { bg: "bg-slate-50/80", ring: "ring-slate-200", dot: "bg-slate-400", text: "text-slate-600" },
    apiKey:    { bg: "bg-violet-50",   ring: "ring-violet-200", dot: "bg-violet-500", text: "text-violet-800" },
    bearer:    { bg: "bg-emerald-50",  ring: "ring-emerald-200", dot: "bg-emerald-500", text: "text-emerald-800" },
  };
  const s = styles[mode];
  const labelMap: Record<AuthMode, string> = {
    anonymous: t("authAnonymous"),
    apiKey: t("authApiKey"),
    bearer: t("authBearer"),
  };

  function signOut() {
    try {
      window.localStorage.removeItem("protea.bearer");
      window.localStorage.removeItem("protea.apiKey");
      // Drop the cookie session too. Path=/ so the gate sees it.
      if (typeof document !== "undefined") {
        document.cookie = `${SESSION_COOKIE_NAME}=; Path=/; Max-Age=0; SameSite=Lax`;
      }
    } catch {
      /* ignore */
    }
    setMode("anonymous");
    setHint(null);
    setRole("viewer");
    setOpen(false);
  }

  return (
    <div className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        aria-haspopup="dialog"
        aria-expanded={open}
        aria-label={labelMap[mode]}
        className={`hidden md:inline-flex items-center gap-1.5 h-9 rounded-full px-2.5 text-[11px] font-semibold ${s.bg} ${s.text} ring-1 ring-inset ${s.ring} transition-colors hover:brightness-95`}
      >
        <svg
          className="w-3.5 h-3.5"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
          viewBox="0 0 16 16"
          aria-hidden="true"
        >
          {mode === "anonymous" ? (
            <>
              <circle cx="8" cy="6" r="3" />
              <path d="M2.5 14a5.5 5.5 0 0111 0" strokeLinecap="round" />
            </>
          ) : (
            <>
              <rect x="3" y="7" width="10" height="6.5" rx="1.5" />
              <path d="M5 7V5a3 3 0 016 0v2" strokeLinecap="round" />
            </>
          )}
        </svg>
        <span className="uppercase tracking-wider">{labelMap[mode]}</span>
        {hint && (
          <span className="hidden xl:inline-flex font-mono text-[10px] opacity-70 normal-case">{hint}</span>
        )}
        {mode !== "anonymous" && (
          <span
            className="hidden xl:inline-flex text-[10px] font-semibold uppercase tracking-wider opacity-80"
            aria-label={`role-${role}`}
          >
            {role}
          </span>
        )}
        <span className={`h-1.5 w-1.5 rounded-full ${s.dot}`} aria-hidden />
      </button>

      {open && (
        <>
          <button
            aria-hidden
            tabIndex={-1}
            onClick={() => setOpen(false)}
            className="fixed inset-0 z-40 cursor-default"
          />
          <div
            role="dialog"
            aria-label={labelMap[mode]}
            className="absolute right-0 mt-2 z-50 w-72 rounded-xl border border-slate-200 bg-white shadow-xl overflow-hidden"
          >
            <div className="px-4 py-3 border-b border-slate-100">
              <div className="flex items-center gap-2">
                <span className={`h-2 w-2 rounded-full ${s.dot}`} aria-hidden />
                <span className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-600">
                  {labelMap[mode]}
                </span>
              </div>
              <p className="mt-1.5 text-[12px] text-slate-600 leading-relaxed">
                {mode === "anonymous" ? t("authAnonymousHint") : hint}
              </p>
            </div>
            <div className="p-2 text-[12px]">
              <a
                href={`${baseUrl()}/docs#/auth`}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between gap-2 rounded-lg px-3 py-2 text-slate-600 hover:bg-slate-50 hover:text-slate-800"
              >
                <span>{t("openSwagger")}</span>
                <span aria-hidden className="text-slate-400">↗</span>
              </a>
              {mode !== "anonymous" && (
                <button
                  onClick={signOut}
                  className="w-full text-left flex items-center justify-between gap-2 rounded-lg px-3 py-2 text-red-600 hover:bg-red-50"
                >
                  <span>Sign out</span>
                  <span aria-hidden>×</span>
                </button>
              )}
            </div>
          </div>
        </>
      )}
    </div>
  );
}
