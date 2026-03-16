"use client";

import { useEffect, useState } from "react";
import React from "react";

const STORAGE_KEY = "protea_policy_accepted_v1";

export function UsagePolicyModal() {
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
          <h2 className="text-lg font-semibold text-gray-900">Welcome to PROTEA</h2>
          <p className="mt-0.5 text-sm text-gray-500">
            Personal compute resources, openly shared — please read before continuing
          </p>
        </div>

        {/* Body */}
        <div className="px-6 py-5 text-sm text-gray-700 space-y-3">
          <p>
            This platform runs on personal hardware shared freely for research purposes.
            No registration or authorization is required. Please keep the following in mind:
          </p>
          <ul className="space-y-2 list-none">
            {RULES.map((rule, i) => (
              <li key={i} className="flex gap-2">
                <span className="mt-0.5 flex-shrink-0 text-blue-600">•</span>
                <span>{rule}</span>
              </li>
            ))}
          </ul>
          <p className="text-xs text-gray-400 pt-1">
            All data processed here is public. Thank you for using this responsibly.
          </p>
        </div>

        {/* Footer */}
        <div className="border-t px-6 py-4 flex justify-end">
          <button
            onClick={accept}
            className="rounded-lg bg-blue-600 px-5 py-2 text-sm font-medium text-white hover:bg-blue-700 transition-colors"
          >
            Got it, let&apos;s go
          </button>
        </div>
      </div>
    </div>
  );
}

const RULES: React.ReactNode[] = [
  "These are personal resources shared voluntarily. Please be mindful of the load you generate.",
  "Before launching heavy or long-running jobs, reach out first — a quick message explaining your use case is much appreciated.",
  "All processed data is public and open. Feel free to use and share results.",
  <span>PROTEA is free and open source. Any individual or research institution can deploy their own instance — <a href="https://github.com/frapercan/PROTEA" target="_blank" rel="noopener noreferrer" className="text-blue-600 underline hover:text-blue-800">source code on GitHub</a>.</span>,
  "This service runs on a best-effort basis. The system may be taken down for maintenance or personal use at any time without prior notice.",
  "If something breaks or behaves unexpectedly, please report it. Bug reports and feedback are genuinely appreciated.",
];
