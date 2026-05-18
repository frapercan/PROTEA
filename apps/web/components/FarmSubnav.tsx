"use client";

// FARM-UI.9 P1.1 intra-farm tab bar. Lives in the farm-route group layout
// (apps/web/app/[locale]/farm/layout.tsx) so every page under /farm/* gets
// a consistent three-tab strip: Tasks (list root), Plan (slice DAG), Cost
// (rollup). The active tab is detected from the current pathname after
// stripping the locale prefix, and the matching link carries
// aria-current="page" for assistive tech. The component renders nothing
// outside /farm/* but the layout already scopes it, so the guard is a
// belt-and-braces no-op.

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useLocale, useTranslations } from "next-intl";

type TabKey = "tasks" | "plan" | "cost";

// Map tab key to URL segment. The "tasks" tab is the section root, the
// other two are dedicated routes. Matching is anchored on the segment
// immediately after `/farm`, so deep links such as /farm/<task_id> still
// resolve to the tasks tab.
const TABS: { key: TabKey; segment: string | null }[] = [
  { key: "tasks", segment: null },
  { key: "plan", segment: "plan" },
  { key: "cost", segment: "cost" },
];

function deriveActive(pathname: string | null): TabKey {
  if (!pathname) return "tasks";
  // Pathname looks like /<locale>/farm[/<segment>...]. Strip the locale
  // and the literal "farm" segment to find the section subpath.
  const stripped = pathname.replace(/^\/[a-z]{2}(?=\/|$)/, "");
  const parts = stripped.split("/").filter(Boolean);
  if (parts.length < 2 || parts[0] !== "farm") return "tasks";
  if (parts[1] === "plan") return "plan";
  if (parts[1] === "cost") return "cost";
  // Anything else (task id, future sub-routes) belongs to the tasks tab.
  return "tasks";
}

export function FarmSubnav() {
  const pathname = usePathname();
  const locale = useLocale();
  const t = useTranslations("farm.subnav");
  const active = deriveActive(pathname);

  return (
    <nav
      aria-label={t("ariaLabel")}
      data-testid="farm-subnav"
      className="mb-4 -mt-2 flex flex-wrap items-center gap-1 border-b border-slate-200"
    >
      {TABS.map(({ key, segment }) => {
        const href = segment
          ? `/${locale}/farm/${segment}`
          : `/${locale}/farm`;
        const isActive = active === key;
        return (
          <Link
            key={key}
            href={href}
            aria-current={isActive ? "page" : undefined}
            data-testid={`farm-subnav-${key}`}
            className={`relative inline-flex min-h-[44px] items-center px-3 sm:px-4 text-sm font-medium transition-colors ${
              isActive
                ? "text-blue-700"
                : "text-slate-600 hover:text-slate-900"
            }`}
          >
            {t(key)}
            {isActive && (
              <span
                aria-hidden
                className="absolute inset-x-2 -bottom-px h-0.5 rounded-full bg-blue-600"
              />
            )}
          </Link>
        );
      })}
    </nav>
  );
}
