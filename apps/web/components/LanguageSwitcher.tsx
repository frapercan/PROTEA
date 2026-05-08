"use client";
import { useState, useRef, useEffect } from "react";
import { useLocale } from "next-intl";
import { useRouter, usePathname } from "next/navigation";
import { routing } from "@/i18n/routing";

const LOCALE_LABELS: Record<string, string> = {
  en: "EN",
  es: "ES",
  de: "DE",
  pt: "PT",
  zh: "中文",
};

export function LanguageSwitcher() {
  const locale = useLocale();
  const router = useRouter();
  const pathname = usePathname();
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  function switchLocale(newLocale: string) {
    const segments = pathname.split("/");
    segments[1] = newLocale;
    router.push(segments.join("/"));
    setOpen(false);
  }

  const otherLocales = routing.locales.filter((l) => l !== locale);

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen(!open)}
        className="flex h-9 min-w-[44px] items-center justify-center gap-1 rounded-lg bg-slate-100 px-2.5 text-[12px] font-semibold text-slate-700 hover:bg-slate-200 hover:text-slate-900 transition-colors"
        aria-label="Switch language"
        aria-expanded={open}
      >
        <span className="text-[13px]">🌐</span>
        <span>{LOCALE_LABELS[locale]}</span>
      </button>
      {open && (
        <div className="absolute right-0 mt-2 flex flex-col gap-0.5 bg-white border border-slate-200 rounded-xl shadow-xl p-1.5 z-50 min-w-[80px]">
          {otherLocales.map((l) => (
            <button
              key={l}
              onClick={() => switchLocale(l)}
              className="px-3 py-1.5 text-[12px] font-medium rounded-lg transition-colors text-slate-600 hover:bg-slate-100 hover:text-slate-900 whitespace-nowrap text-left"
            >
              {LOCALE_LABELS[l]}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
