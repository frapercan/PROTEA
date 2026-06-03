"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useCallback, useMemo } from "react";

/**
 * Two-way binding between a single query-string key and a piece of UI
 * state. Refreshing the page or copying the URL preserves the filter,
 * which is the whole point — without this, every chip / dropdown
 * forgets state on reload.
 *
 * Returns ``[value, setValue]`` where ``setValue(null)`` removes the
 * key entirely (cleanest URL when the filter is at its default).
 *
 * Uses ``router.replace`` + ``scroll: false`` so changing a chip
 * doesn't scroll the page or pollute the back-button stack.
 */
export function useUrlParam(
  key: string,
  defaultValue: string | null = null,
): readonly [string | null, (next: string | null) => void] {
  const router = useRouter();
  const pathname = usePathname();
  const params = useSearchParams();

  const value = useMemo(() => {
    const v = params.get(key);
    return v == null ? defaultValue : v;
  }, [params, key, defaultValue]);

  const setValue = useCallback(
    (next: string | null) => {
      const merged = new URLSearchParams(params.toString());
      if (next == null || next === "" || next === defaultValue) {
        merged.delete(key);
      } else {
        merged.set(key, next);
      }
      const qs = merged.toString();
      router.replace(qs ? `${pathname}?${qs}` : pathname, { scroll: false });
    },
    [router, pathname, params, key, defaultValue],
  );

  return [value, setValue] as const;
}

/** Convenience: number-typed variant. Returns null when missing or invalid. */
export function useUrlNumber(
  key: string,
  defaultValue: number | null = null,
): readonly [number | null, (next: number | null) => void] {
  const [raw, setRaw] = useUrlParam(
    key,
    defaultValue == null ? null : String(defaultValue),
  );
  const value = useMemo(() => {
    if (raw == null) return null;
    const n = Number(raw);
    return Number.isFinite(n) ? n : null;
  }, [raw]);
  const setValue = useCallback(
    (next: number | null) => setRaw(next == null ? null : String(next)),
    [setRaw],
  );
  return [value, setValue] as const;
}
