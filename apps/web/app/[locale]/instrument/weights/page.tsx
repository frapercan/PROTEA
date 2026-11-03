"use client";

import { useTranslations } from "next-intl";

import { ModelWeightsPanel } from "@/components/ModelWeightsPanel";

/** The registry of model weights (ADR-D54): list, detail, upload, register, launch. */
export default function ModelWeightsPage() {
  const t = useTranslations("weights");
  return (
    <>
      <h1 className="mb-4 text-xl font-semibold">{t("title")}</h1>
      <ModelWeightsPanel />
    </>
  );
}
