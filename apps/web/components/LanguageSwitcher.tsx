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
        className="px-2 py-1.5 text-xs rounded transition-colors min-h-[36px] min-w-[36px] flex items-center justify-center bg-blue-100 text-blue-700 font-semibold"
      >
        {LOCALE_LABELS[locale]}
      </button>
      {open && (
        <div className="absolute right-0 mt-1 flex flex-col gap-0.5 bg-white border border-gray-200 rounded shadow-lg p-1 z-50">
          {otherLocales.map((l) => (
            <button
              key={l}
              onClick={() => switchLocale(l)}
              className="px-3 py-1.5 text-xs rounded transition-colors min-h-[36px] min-w-[36px] flex items-center justify-center text-gray-500 hover:text-gray-700 hover:bg-gray-100 whitespace-nowrap"
            >
              {LOCALE_LABELS[l]}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
