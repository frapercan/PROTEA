import type { Metadata } from "next";
import Image from "next/image";
import { Geist, Geist_Mono } from "next/font/google";
import "../globals.css";
import { ResetDbButton } from "@/components/ResetDbButton";
import { NavLinks } from "@/components/NavLinks";
import { SupportButton } from "@/components/SupportButton";
import { ToastProvider } from "@/components/Toast";
import { UsagePolicyModal } from "@/components/UsagePolicyModal";
import { FloatingJobsWidget } from "@/components/FloatingJobsWidget";
import { CommandPalette } from "@/components/CommandPalette";
import { CommandPaletteTrigger } from "@/components/CommandPaletteTrigger";
import { SystemStatusPill } from "@/components/SystemStatusPill";
import { AuthChip } from "@/components/AuthChip";
import { NextIntlClientProvider } from "next-intl";
import { getMessages, getTranslations } from "next-intl/server";
import { LanguageSwitcher } from "@/components/LanguageSwitcher";
import { Breadcrumbs } from "@/components/Breadcrumbs";

const geistSans = Geist({ variable: "--font-geist-sans", subsets: ["latin"] });
const geistMono = Geist_Mono({ variable: "--font-geist-mono", subsets: ["latin"] });

export const metadata: Metadata = {
  title: {
    template: "%s | PROTEA",
    default: "PROTEA — Functional Annotation",
  },
  description: "Protein Functional Embedding-based Annotation — job queue and pipeline management",
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
        className={`${geistSans.variable} ${geistMono.variable} antialiased min-h-screen bg-[var(--background)] text-[var(--foreground)]`}
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
            <header className="sticky top-0 z-40 border-b border-slate-200/70 bg-white/85 backdrop-blur-md supports-[backdrop-filter]:bg-white/70">
              <div className="mx-auto flex h-16 max-w-7xl items-center gap-3 px-4 sm:px-6 lg:px-8">
                <a
                  href={`/${locale}`}
                  className="group flex items-center gap-2.5 shrink-0"
                  aria-label="PROTEA home"
                >
                  <span
                    aria-hidden
                    className="relative flex h-10 w-10 items-center justify-center transition-transform group-hover:scale-105"
                  >
                    <Image
                      src="/protea-mark.png"
                      alt=""
                      width={40}
                      height={40}
                      priority
                      className="h-10 w-10"
                    />
                    <span className="absolute -right-0.5 -bottom-0.5 h-2 w-2 rounded-full bg-emerald-400 ring-2 ring-white" />
                  </span>
                  <span className="flex flex-col leading-tight">
                    <span className="text-[17px] font-bold tracking-tight text-slate-900 group-hover:text-blue-700 transition-colors">
                      PROTEA
                    </span>
                    <span className="hidden sm:block text-[10px] uppercase tracking-[0.14em] text-slate-600 font-medium">
                      {t("subtitleShort")}
                    </span>
                  </span>
                </a>

                <div className="hidden xl:block h-7 w-px bg-slate-200 mx-1" />

                <NavLinks
                  mobileExtras={
                    <>
                      <LanguageSwitcher />
                      <ResetDbButton />
                    </>
                  }
                />

                <div className="ml-auto flex items-center gap-2 sm:gap-3">
                  <SystemStatusPill />
                  <AuthChip />
                  <CommandPaletteTrigger />
                  <div className="hidden xl:flex items-center gap-2">
                    <LanguageSwitcher />
                  </div>
                  <SupportButton />
                  <div className="hidden xl:block">
                    <ResetDbButton />
                  </div>
                </div>
              </div>
            </header>

            <Breadcrumbs />

            <main
              id="main"
              tabIndex={-1}
              className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8 py-6 sm:py-8 lg:py-10 focus:outline-none"
            >
              {children}
            </main>

            <FloatingJobsWidget />
            <CommandPalette />
          </ToastProvider>
        </NextIntlClientProvider>
      </body>
    </html>
  );
}
