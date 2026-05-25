"use client";
import { useState, useRef, useEffect } from "react";
import { useLocale, useTranslations } from "next-intl";
import { useRouter, usePathname } from "next/navigation";
import { routing } from "@/i18n/routing";

const LOCALE_LABELS: Record<string, string> = {
  en: "EN",
  es: "ES",
  de: "DE",
  pt: "PT",
  zh: "中文",
};

// Self-naming so the accessible label reads the language NAME (matching
// Lighthouse label-content-name-mismatch expectations) rather than the
// short code shown on the visible chip.
const LOCALE_FULL_NAMES: Record<string, string> = {
  en: "English",
  es: "Español",
  de: "Deutsch",
  pt: "Português",
  zh: "中文",
};

export function LanguageSwitcher() {
  const locale = useLocale();
  const t = useTranslations("components.languageSwitcher");
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
        className="flex h-9 min-w-[44px] items-center justify-center gap-1 rounded-lg bg-stone-100 px-2.5 text-xs font-semibold text-stone-700 hover:bg-stone-200 hover:text-stone-950 transition-colors"
        aria-label={`${t("label")}: ${LOCALE_FULL_NAMES[locale] ?? LOCALE_LABELS[locale]}`}
        aria-haspopup="listbox"
        aria-expanded={open}
      >
        <span className="text-sm leading-none">🌐</span>
        <span>{LOCALE_LABELS[locale]}</span>
      </button>
      {open && (
        <div
          role="listbox"
          aria-label={t("label")}
          className="absolute left-0 bottom-full mb-2 flex flex-col gap-0.5 bg-white border border-stone-200 rounded-xl shadow-xl p-1.5 z-50 min-w-[96px]"
        >
          {otherLocales.map((l) => (
            <button
              key={l}
              role="option"
              aria-selected={l === locale}
              aria-label={LOCALE_FULL_NAMES[l] ?? LOCALE_LABELS[l]}
              onClick={() => switchLocale(l)}
              className="px-3 py-1.5 text-xs font-medium rounded-lg transition-colors text-stone-700 hover:bg-stone-100 hover:text-stone-950 whitespace-nowrap text-left"
            >
              {LOCALE_LABELS[l]}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
