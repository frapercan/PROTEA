"use client";

// FEAT-AUTH (WAVE-2 2026-05-23). React hook that surfaces the current
// session's role to client components. Reads ``protea_session`` from
// document.cookie, decodes the JWT payload locally (no signature
// check -- backend gates do that on every protected request), and
// subscribes to cross-tab cookie changes via ``storage`` events so a
// sign-in in tab A shows up in tab B's chrome without a reload.

import { useEffect, useState } from "react";
import { type Role, getSessionToken, hasRole, roleFromToken } from "@/lib/auth";

function read(): Role {
  return roleFromToken(getSessionToken());
}

export function useRole(): Role {
  // Server-render anonymous so initial HTML hydrates without leaking
  // role-dependent buttons; the client effect overwrites on mount.
  const [role, setRole] = useState<Role>("viewer");

  useEffect(() => {
    setRole(read());
    const onStorage = (e: StorageEvent) => {
      if (!e.key || e.key.startsWith("protea.")) setRole(read());
    };
    // BroadcastChannel-style refresh: a sibling sign-out flips a sentinel
    // localStorage key; we re-read the cookie on that signal. Real cookie
    // changes also re-fire on focus, which we hook below.
    window.addEventListener("storage", onStorage);
    const onFocus = () => setRole(read());
    window.addEventListener("focus", onFocus);
    return () => {
      window.removeEventListener("storage", onStorage);
      window.removeEventListener("focus", onFocus);
    };
  }, []);

  return role;
}

export function useHasRole(required: Role): boolean {
  const current = useRole();
  return hasRole(current, required);
}
