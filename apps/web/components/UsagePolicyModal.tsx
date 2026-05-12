"use client";

import { useCallback, useEffect, useId, useState } from "react";
import { useTranslations } from "next-intl";
import { useFocusTrap } from "@/lib/useFocusTrap";

const STORAGE_KEY = "protea_policy_accepted_v1";

export function UsagePolicyModal() {
  const t = useTranslations("components.usagePolicyModal");
  const [visible, setVisible] = useState(false);
  const titleId = useId();

  useEffect(() => {
    if (!localStorage.getItem(STORAGE_KEY)) {
      setVisible(true);
    }
  }, []);

  const accept = useCallback(() => {
    localStorage.setItem(STORAGE_KEY, "1");
    setVisible(false);
  }, []);

  // No Escape close: this is a forced-acceptance gate. Tab focus is
  // still trapped inside the dialog so screen-reader users do not leak
  // back into the inert page underneath.
  const dialogRef = useFocusTrap<HTMLDivElement>(visible);

  if (!visible) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className="relative mx-4 w-full max-w-lg rounded-xl bg-white shadow-2xl"
      >
        {/* Header */}
        <div className="border-b px-6 py-4">
          <h2 id={titleId} className="text-lg font-semibold text-slate-900">{t("title")}</h2>
          <p className="mt-0.5 text-sm text-slate-500">{t("subtitle")}</p>
        </div>

        {/* Body */}
        <div className="px-6 py-5 text-sm text-slate-700 space-y-3">
          <p>{t("intro")}</p>
          <ul className="space-y-2 list-none">
            {(["rule1", "rule2", "rule3", "rule4", "rule5", "rule6"] as const).map((key) => (
              <li key={key} className="flex gap-2">
                <span className="mt-0.5 flex-shrink-0 text-blue-600">•</span>
                <span>{key === "rule4" ? t.rich(key, {
                  a: (chunks) => (
                    <a href="https://github.com/frapercan/PROTEA" target="_blank" rel="noopener noreferrer" className="text-blue-600 underline hover:text-blue-800">
                      {chunks}
                    </a>
                  ),
                }) : t(key)}</span>
              </li>
            ))}
          </ul>
          <p className="text-xs text-slate-600 pt-1">{t("dataPublicNote")}</p>
        </div>

        {/* Footer */}
        <div className="border-t px-6 py-4 flex justify-end">
          <button
            onClick={accept}
            className="rounded-lg bg-blue-600 px-5 py-2 text-sm font-medium text-white hover:bg-blue-700 transition-colors"
          >
            {t("accept")}
          </button>
        </div>
      </div>
    </div>
  );
}
