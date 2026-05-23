"use client";

import { useCallback, useEffect, useId, useState } from "react";
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
        className="relative mx-4 w-full max-w-lg rounded-xl bg-white shadow-2xl max-h-[90vh] flex flex-col"
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

        {/* Footer */}
        <div className="border-t px-6 py-4 flex items-center justify-between gap-3 flex-shrink-0">
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
  );
}
