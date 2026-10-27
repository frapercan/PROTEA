"use client";

import { useHasRole } from "@/lib/useRole";
import { useTranslations } from "next-intl";

/**
 * Whether the viewer may dispatch jobs.
 *
 * `POST /v1/jobs` requires the operator role, and `PROTEA_AUTHN_REQUIRED`
 * defaults to true, so an anonymous visitor who presses a launch button
 * gets 401. Before this, several instrument pages rendered those forms
 * fully, pre-filled, with a live blue "Launch Job" button: an external
 * audit on 2026-10-07 found a visitor could fire one and be answered with
 * the server's own problem document. The honest surface shows the form,
 * because the form is part of what was built, and says who may run it.
 *
 * SSR-safe: the hook reports the anonymous answer before hydration, so
 * the button never flashes enabled for a visitor who cannot use it.
 */
export function useMayLaunchJobs(): boolean {
  return useHasRole("operator");
}

/**
 * The sentence that replaces a dead button.
 *
 * Rendered next to a disabled control rather than instead of it: hiding
 * the form would hide a working piece of the platform, which is the same
 * mistake as hiding the annotate form.
 */
export function OperatorOnlyNotice() {
  const t = useTranslations("components.operatorOnly");
  return (
    <p className="text-xs leading-relaxed text-[var(--muted)]">{t("needsOperator")}</p>
  );
}
