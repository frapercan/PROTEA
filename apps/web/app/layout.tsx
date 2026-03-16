import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import { ResetDbButton } from "@/components/ResetDbButton";
import { NavLinks } from "@/components/NavLinks";
import { SupportButton } from "@/components/SupportButton";
import { ToastProvider } from "@/components/Toast";
import { UsagePolicyModal } from "@/components/UsagePolicyModal";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "PROTEA",
  description: "Protein data platform — job queue and pipeline management",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`${geistSans.variable} ${geistMono.variable} antialiased min-h-screen bg-gray-50`}>
        <UsagePolicyModal />
        <ToastProvider>
          <header className="relative border-b bg-white px-4 sm:px-6 py-3 flex items-center gap-3">
            <span className="text-lg font-bold tracking-tight text-blue-700">PROTEA</span>
            <span className="hidden lg:inline text-gray-300">|</span>
            <NavLinks />
            <div className="ml-auto flex items-center gap-2 sm:gap-3">
              <SupportButton />
              <ResetDbButton />
            </div>
          </header>
          <main className="mx-auto max-w-5xl px-4 sm:px-6 py-4 sm:py-6">
            {children}
          </main>
        </ToastProvider>
      </body>
    </html>
  );
}
