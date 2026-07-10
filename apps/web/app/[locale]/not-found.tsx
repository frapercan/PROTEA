/**
 * General not-found boundary for the localized app. Next renders it for
 * an address that matches no route, and for any `notFound()` call that has
 * no closer boundary. Server component: it runs inside the locale layout,
 * so chrome and translations are available.
 */

import Link from "next/link";
import { getLocale, getTranslations } from "next-intl/server";
import { Home } from "lucide-react";

export default async function NotFound() {
  const t = await getTranslations("errors");
  const locale = await getLocale();

  return (
    <div className="mx-auto flex max-w-2xl flex-col items-start gap-6 py-10">
      <p className="font-mono text-sm font-semibold text-slate-400">404</p>
      <h1 className="text-2xl font-semibold tracking-tight text-slate-900">
        {t("notFound.title")}
      </h1>
      <p className="text-base leading-relaxed text-slate-600">{t("notFound.body")}</p>
      <Link
        href={`/${locale}`}
        className="inline-flex items-center gap-2 rounded-xl border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700 transition hover:bg-slate-50"
      >
        <Home className="h-4 w-4" aria-hidden />
        {t("notFound.home")}
      </Link>
    </div>
  );
}
