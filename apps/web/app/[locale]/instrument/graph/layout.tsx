import type { Metadata } from "next";
import { getTranslations } from "next-intl/server";

export async function generateMetadata(
  { params }: { params: Promise<{ locale: string }> },
): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: "nav" });
  return { title: t("graph") };
}

export default function GraphLayout({ children }: { children: React.ReactNode }) {
  return children;
}
