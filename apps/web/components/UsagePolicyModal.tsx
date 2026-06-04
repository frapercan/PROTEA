"use client";

import { useCallback, useEffect, useId, useState } from "react";
import { useSearchParams } from "next/navigation";
import { useTranslations } from "next-intl";
import { useFocusTrap } from "@/lib/useFocusTrap";

const STORAGE_KEY = "protea_policy_accepted_v2";

const SECTION_KEYS = [
  "identification",
  "asIs",
  "dataHandling",
  "privacyLog",
  "license",
  "responsibleUse",
  "bugReport",
] as const;

type SectionKey = (typeof SECTION_KEYS)[number];

const LICENSE_URL =
  "https://github.com/frapercan/PROTEA/blob/develop/LICENSE";
const ISSUES_URL = "https://github.com/frapercan/PROTEA/issues";

export function UsagePolicyModal() {
  const t = useTranslations("components.usagePolicyModal");
  // Start hidden. The gate is opened only after the mount-time effect
  // confirms the policy has NOT been accepted and the demo bypass is not
  // in play. Starting hidden avoids a first-paint flash of the legal wall
  // during a live demo.
  const [visible, setVisible] = useState(false);
  const titleId = useId();
  // `useSearchParams` resolves the query reliably across the next-intl
  // locale-prefix rewrite (reading `window.location.search` inside the
  // mount effect can race the rewrite and miss the flag). `?demo=1`
  // marks a live-presentation session.
  const searchParams = useSearchParams();
  const isDemo = searchParams?.get("demo") === "1";

  useEffect(() => {
    // Demo bypass: `?demo=1` pre-accepts the policy so a live presentation
    // is never walled by the legal gate on a fresh tab or refresh. The
    // acceptance is persisted under the same storage key, so once the
    // presenter opens the app with `?demo=1` every later navigation in the
    // session stays clean. Any non-empty acceptance value (returning
    // visitor) likewise suppresses the modal.
    if (isDemo) {
      localStorage.setItem(STORAGE_KEY, "1");
      setVisible(false);
      return;
    }
    setVisible(!localStorage.getItem(STORAGE_KEY));
  }, [isDemo]);

  const accept = useCallback(() => {
    localStorage.setItem(STORAGE_KEY, "1");
    setVisible(false);
  }, []);

  // No Escape close: this is a forced-acceptance gate. Tab focus is
  // still trapped inside the dialog so screen-reader users do not leak
  // back into the inert page underneath.
  const dialogRef = useFocusTrap<HTMLDivElement>(visible);

  // Keyboard shortcut: Enter or Space accepts the policy. Helpful on
  // mobile where the body scrolls long and on desktop for keyboard
  // users (Escape stays disabled by design, see comment above).
  useEffect(() => {
    if (!visible) return;
    function onKey(event: KeyboardEvent) {
      const target = event.target as HTMLElement | null;
      const tag = target?.tagName;
      if (tag === "A" || tag === "BUTTON") return;
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        accept();
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [visible, accept]);

  if (!visible) return null;

  const renderBody = (key: SectionKey) => {
    if (key === "license") {
      return t.rich(`${key}.body`, {
        license: (chunks) => (
          <a
            href={LICENSE_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="text-blue-600 underline hover:text-blue-800"
          >
            {chunks}
          </a>
        ),
        issues: (chunks) => (
          <a
            href={ISSUES_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="text-blue-600 underline hover:text-blue-800"
          >
            {chunks}
          </a>
        ),
      });
    }
    if (key === "bugReport" || key === "responsibleUse") {
      return t.rich(`${key}.body`, {
        issues: (chunks) => (
          <a
            href={ISSUES_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="text-blue-600 underline hover:text-blue-800"
          >
            {chunks}
          </a>
        ),
      });
    }
    return t(`${key}.body`);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className="relative mx-4 w-full max-w-lg rounded-xl bg-white shadow-2xl max-h-[85dvh] flex flex-col"
      >
        {/* Header */}
        <div className="border-b px-6 py-4 flex-shrink-0">
          <h2 id={titleId} className="text-lg font-semibold text-slate-900">
            {t("title")}
          </h2>
          <p className="mt-0.5 text-sm text-slate-500">{t("subtitle")}</p>
        </div>

        {/* Body */}
        <div className="px-6 py-5 overflow-y-auto space-y-4">
          {SECTION_KEYS.map((key) => (
            <section key={key}>
              <h3 className="text-sm font-semibold text-slate-900">
                {t(`${key}.heading`)}
              </h3>
              <p className="mt-1 text-sm text-slate-700">{renderBody(key)}</p>
            </section>
          ))}
        </div>

        {/* Footer: sticky at the bottom of the flex column, with a
            soft top shadow so it reads as anchored on mobile where the
            body scrolls underneath. Keyboard hint sits above the button
            row so the affordance is visible without scrolling. */}
        <div className="border-t bg-white px-6 pt-3 pb-4 flex-shrink-0 shadow-[0_-6px_12px_-8px_rgba(15,23,42,0.18)]">
          <p className="text-[11px] text-slate-500 text-center mb-2">
            {t("keyboardHint")}
          </p>
          <div className="flex items-center justify-between gap-3">
            <span className="text-xs text-slate-500">{t("lastUpdated")}</span>
            <button
              onClick={accept}
              className="rounded-lg bg-blue-600 px-5 py-2 text-sm font-medium text-white hover:bg-blue-700 transition-colors"
            >
              {t("accept")}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
