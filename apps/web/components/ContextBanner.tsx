"use client";

import { useState } from "react";
import Link from "next/link";

export type Prerequisite = {
  label: string;
  met: boolean;
  href?: string;
};

type ContextBannerProps = {
  title: string;
  description: string;
  prerequisites?: Prerequisite[];
  nextStep?: { label: string; href: string };
};

export function ContextBanner({ title, description, prerequisites, nextStep }: ContextBannerProps) {
  const [collapsed, setCollapsed] = useState(false);

  return (
    <div className="rounded-lg border border-blue-200 bg-blue-50 mb-6">
      <button
        onClick={() => setCollapsed((v) => !v)}
        className="w-full flex items-center justify-between px-4 py-3 text-left"
      >
        <div className="flex items-center gap-2">
          <span className="text-blue-600 text-sm">&#9432;</span>
          <span className="text-sm font-medium text-blue-900">{title}</span>
        </div>
        <span className="text-xs text-blue-400">{collapsed ? "+" : "-"}</span>
      </button>

      {!collapsed && (
        <div className="px-4 pb-3 space-y-2">
          <p className="text-sm text-gray-600">{description}</p>

          {prerequisites && prerequisites.length > 0 && (
            <div className="flex flex-wrap gap-x-4 gap-y-1">
              {prerequisites.map((p) => (
                <span key={p.label} className="inline-flex items-center gap-1 text-xs">
                  <span className={p.met ? "text-green-600" : "text-amber-500"}>
                    {p.met ? "\u2713" : "\u26A0"}
                  </span>
                  {p.href && !p.met ? (
                    <Link href={p.href} className="text-blue-600 underline hover:text-blue-800">
                      {p.label}
                    </Link>
                  ) : (
                    <span className={p.met ? "text-gray-600" : "text-amber-700"}>{p.label}</span>
                  )}
                </span>
              ))}
            </div>
          )}

          {nextStep && (
            <div className="text-xs text-gray-500">
              Next:{" "}
              <Link href={nextStep.href} className="text-blue-600 underline hover:text-blue-800">
                {nextStep.label} &rarr;
              </Link>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
