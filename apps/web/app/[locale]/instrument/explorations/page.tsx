"use client";

import { useTranslations } from "next-intl";

import { ExplorationsPanel } from "@/components/ExplorationsPanel";

/** Evaluated explorations (ADR-D55): one hypothesis each, its receipt, report and review. */
export default function ExplorationsPage() {
  const t = useTranslations("explorations");
  return (
    <>
      <h1 className="mb-4 text-xl font-semibold">{t("title")}</h1>
      <ExplorationsPanel />
    </>
  );
}
