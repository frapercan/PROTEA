import type { Metadata, Viewport } from "next";
import { Inter, Geist_Mono, Newsreader } from "next/font/google";
import "../globals.css";
import { ResetDbButton } from "@/components/ResetDbButton";
import { Sidebar } from "@/components/Sidebar";
import { SupportButton } from "@/components/SupportButton";
import { ToastProvider } from "@/components/Toast";
import { UsagePolicyModal } from "@/components/UsagePolicyModal";
import { FloatingJobsWidget } from "@/components/FloatingJobsWidget";
import { CommandPalette } from "@/components/CommandPalette";
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

export const metadata: Metadata = {
  title: {
    template: "%s | PROTEA",
    default: "PROTEA — Functional Annotation",
  },
  description: "Protein Functional Embedding-based Annotation, job queue and pipeline management",
};

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
                    <ResetDbButton />
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

                <main
                  id="main"
                  tabIndex={-1}
                  className="w-full max-w-screen-2xl px-4 sm:px-6 lg:px-8 py-6 sm:py-8 lg:py-10 focus:outline-none"
                >
                  {children}
                </main>
              </div>
            </div>

            <FloatingJobsWidget />
            <FarmChrome slot="widget" />
            <CommandPalette />
          </ToastProvider>
        </NextIntlClientProvider>
      </body>
    </html>
  );
}
