import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";

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
        <header className="border-b bg-white px-6 py-3 flex items-center gap-3">
          <span className="text-lg font-bold tracking-tight text-blue-700">PROTEA</span>
          <span className="text-gray-300">|</span>
          <nav className="flex gap-4 text-sm text-gray-600">
            <a href="/jobs" className="hover:text-gray-900">Jobs</a>
          </nav>
        </header>
        <main className="mx-auto max-w-5xl px-6 py-6">
          {children}
        </main>
      </body>
    </html>
  );
}
