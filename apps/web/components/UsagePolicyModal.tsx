"use client";

import { useEffect, useState } from "react";
import { useTranslations } from "next-intl";

const STORAGE_KEY = "protea_policy_accepted_v1";

export function UsagePolicyModal() {
  const t = useTranslations("components.usagePolicyModal");
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    if (!localStorage.getItem(STORAGE_KEY)) {
      setVisible(true);
    }
  }, []);

  function accept() {
    localStorage.setItem(STORAGE_KEY, "1");
    setVisible(false);
  }

  if (!visible) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="relative mx-4 w-full max-w-lg rounded-xl bg-white shadow-2xl">
        {/* Header */}
        <div className="border-b px-6 py-4">
          <h2 className="text-lg font-semibold text-gray-900">{t("title")}</h2>
          <p className="mt-0.5 text-sm text-gray-500">{t("subtitle")}</p>
        </div>

        {/* Body */}
        <div className="px-6 py-5 text-sm text-gray-700 space-y-3">
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
          <p className="text-xs text-gray-400 pt-1">{t("dataPublicNote")}</p>
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

