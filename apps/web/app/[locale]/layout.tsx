import type { Metadata, Viewport } from "next";
import { Inter, Geist_Mono, Newsreader } from "next/font/google";
import "../globals.css";
import { ResetDbButton } from "@/components/ResetDbButton";
import { Sidebar } from "@/components/Sidebar";
import { SupportButton } from "@/components/SupportButton";
import { ToastProvider } from "@/components/Toast";
import { UsagePolicyModal } from "@/components/UsagePolicyModal";
import { FloatingJobsWidget } from "@/components/FloatingJobsWidget";
import { LazyCommandPalette } from "@/components/LazyCommandPalette";
import { CommandPaletteTrigger } from "@/components/CommandPaletteTrigger";
import { SystemStatusPill } from "@/components/SystemStatusPill";
import { FarmChrome } from "@/components/FarmChrome";
import { AuthChip } from "@/components/AuthChip";
import { NextIntlClientProvider } from "next-intl";
import { getMessages, getTranslations } from "next-intl/server";
import { LanguageSwitcher } from "@/components/LanguageSwitcher";
import { Breadcrumbs } from "@/components/Breadcrumbs";

// Inter Variable — primary UI/body. Excellent legibility at small sizes,
// open-source, supports continuous weights for subtle hover transitions.
const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin", "latin-ext"],
  display: "swap",
});
// Geist Mono — retained for IDs, UUIDs, SHAs, code snippets.
const geistMono = Geist_Mono({ variable: "--font-geist-mono", subsets: ["latin"] });
// Newsreader — opt-in serif for hero H1s and landing headlines.
// Apply via `font-serif` utility, kept off-by-default for chrome text.
const newsreader = Newsreader({
  variable: "--font-newsreader",
  subsets: ["latin", "latin-ext"],
  weight: ["400", "500", "600"],
  display: "swap",
});

// SEO baseline. Root metadata block is the source of truth for the
// page <head> on every locale-prefixed route; per-page layouts may
// override individual fields via their own generateMetadata().
//
// metadataBase pins relative URLs (OG images, canonicals) to the
// public ngrok host. When the production domain locks this should
// flip to that origin (single line change).
//
// description is intentionally a real product sentence rather than a
// keyword stuffing. Twitter handle is left unset until the project
// owns one (Next will fall back to the page title/description, which
// is the right behavior).
const SITE_URL = "https://protea.ngrok.app";
const SITE_TITLE = "PROTEA, Protein functional Embedding-based Annotation";
const SITE_DESCRIPTION =
  "PROTEA predicts Gene Ontology functions for protein sequences using protein language model embeddings, KNN transfer, and a learning to rank reranker. Open research platform.";

// Map next-intl locales to BCP-47 + OpenGraph locale tags.
const OG_LOCALE_MAP: Record<string, string> = {
  en: "en_US",
  es: "es_ES",
  de: "de_DE",
  pt: "pt_PT",
  zh: "zh_CN",
};

// Single metadata factory. Next forbids exporting both a static
// `metadata` and a `generateMetadata` from the same file, so all the
// locale-aware fields (canonical, hreflang, openGraph.locale) and the
// site-wide ones share one function.
export async function generateMetadata(
  { params }: { params: Promise<{ locale: string }> },
): Promise<Metadata> {
  const { locale } = await params;
  const ogLocale = OG_LOCALE_MAP[locale] ?? OG_LOCALE_MAP.en;
  const languages: Record<string, string> = {};
  for (const code of Object.keys(OG_LOCALE_MAP)) {
    languages[code] = `/${code}`;
  }
  return {
    metadataBase: new URL(SITE_URL),
    title: {
      template: "%s | PROTEA",
      default: SITE_TITLE,
    },
    description: SITE_DESCRIPTION,
    applicationName: "PROTEA",
    keywords: [
      "PROTEA",
      "Gene Ontology",
      "GO term prediction",
      "protein function",
      "protein language model",
      "CAFA",
      "bioinformatics",
      "ESM",
      "ProstT5",
      "ESMC",
      "LightGBM reranker",
    ],
    authors: [{ name: "Francisco Miguel Perez Canales" }],
    creator: "Francisco Miguel Perez Canales",
    robots: {
      index: true,
      follow: true,
      googleBot: {
        index: true,
        follow: true,
        "max-image-preview": "large",
        "max-snippet": -1,
      },
    },
    alternates: {
      canonical: `/${locale}`,
      languages: {
        ...languages,
        "x-default": "/en",
      },
    },
    openGraph: {
      type: "website",
      url: `/${locale}`,
      siteName: "PROTEA",
      title: SITE_TITLE,
      description: SITE_DESCRIPTION,
      locale: ogLocale,
      alternateLocale: Object.values(OG_LOCALE_MAP).filter((l) => l !== ogLocale),
    },
    twitter: {
      card: "summary_large_image",
      title: SITE_TITLE,
      description: SITE_DESCRIPTION,
    },
    icons: {
      icon: "/favicon.ico",
      apple: "/apple-icon.png",
    },
  };
}

// Ensure mobile browsers render at natural scale (no zoom-out from any
// stray horizontal overflow). width=device-width, initialScale=1.
export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
};

export default async function LocaleLayout({
  children,
  params,
}: {
  children: React.ReactNode;
  params: Promise<{ locale: string }>;
}) {
  const { locale } = await params;
  const messages = await getMessages();
  const t = await getTranslations({ locale, namespace: "nav" });
  // The reset-db backend route is gated by Bearer auth (returns 401 to the
  // public). Until the frontend ships a sign-in flow that can mint that
  // token, the destructive-looking button just produces a red error toast
  // for every public visitor. Hide the button behind an opt-in env var.
  // Read at SSR-time so the bundle does not leak a hidden admin affordance
  // to anonymous clients; flip to "1" (or "true") locally to surface it.
  const resetDbEnabled =
    process.env.NEXT_PUBLIC_ENABLE_DB_RESET === "1" ||
    process.env.NEXT_PUBLIC_ENABLE_DB_RESET === "true";
  return (
    <html lang={locale} suppressHydrationWarning>
      <body
        className={`${inter.variable} ${geistMono.variable} ${newsreader.variable} antialiased min-h-screen bg-[var(--background)] text-[var(--foreground)]`}
        suppressHydrationWarning
      >
        <NextIntlClientProvider messages={messages}>
          <UsagePolicyModal />
          <ToastProvider>
            {/* Skip-to-content for screen-reader / keyboard users.
                Hidden until focused; lands focus on <main>. */}
            <a
              href="#main"
              className="sr-only focus:not-sr-only focus:fixed focus:top-2 focus:left-2 focus:z-[110] focus:rounded-lg focus:bg-blue-600 focus:px-3 focus:py-2 focus:text-sm focus:font-semibold focus:text-white focus:shadow-lg"
            >
              {t("skipToContent")}
            </a>
            <div className="flex min-h-screen">
              <Sidebar
                extras={
                  <>
                    <LanguageSwitcher />
                    {resetDbEnabled && <ResetDbButton />}
                  </>
                }
                mobileTopRight={
                  <>
                    <LanguageSwitcher />
                    <CommandPaletteTrigger />
                    <AuthChip />
                  </>
                }
              />

              {/* Content column: full-width, left-aligned. The pt-14 on < lg
                  reserves room for the fixed mobile top bar. */}
              <div className="flex min-w-0 flex-1 flex-col pt-14 lg:pt-0">
                {/* Desktop utility bar: status + identity + actions, right-aligned. */}
                <div className="hidden lg:flex sticky top-0 z-30 h-16 items-center gap-3 border-b border-stone-200/80 bg-white/85 px-6 backdrop-blur-md supports-[backdrop-filter]:bg-white/70 lg:px-8">
                  <div className="ml-auto flex items-center gap-3">
                    <SystemStatusPill />
                    <FarmChrome slot="pill" />
                    <AuthChip />
                    <CommandPaletteTrigger />
                    <SupportButton />
                  </div>
                </div>

                <Breadcrumbs />

                {/* w-full lets the column claim the row width; mx-auto then
                    centers the max-w-screen-2xl content cap inside the
                    remaining (sidebar-aware) area. Without mx-auto the
                    capped main glues to the left edge of the flex column,
                    which on >=1280 viewports left-shifts everything and
                    becomes especially obvious when the sidebar is
                    collapsed (extra space appears on the right only). */}
                <main
                  id="main"
                  tabIndex={-1}
                  className="mx-auto w-full max-w-screen-2xl px-4 sm:px-6 lg:px-8 py-6 sm:py-8 lg:py-10 focus:outline-none"
                >
                  {children}
                </main>
              </div>
            </div>

            <FloatingJobsWidget />
            <FarmChrome slot="widget" />
            <LazyCommandPalette />
          </ToastProvider>
        </NextIntlClientProvider>
      </body>
    </html>
  );
}
