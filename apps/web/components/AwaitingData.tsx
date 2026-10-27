import Link from "next/link";

/**
 * A surface that is built, works, and has nothing to show yet.
 *
 * The distinction this draws is the whole point. A tool with no data is
 * not the same as a tool that is loading, and it is not the same as a
 * tool that is broken, but all three look identical if the page just
 * keeps showing a skeleton. The benchmark did exactly that: with the
 * corpus empty the matrix comes back `{"rows":[],"stages":[]}`, so the
 * page's stage was never resolved and its loading branch held forever.
 *
 * So: say that it is built, say what it will show, and say what is
 * happening instead. The visitor learns something either way.
 *
 * Palette note, learned the hard way on 2026-10-07: --muted is #57534E,
 * a TEXT colour from the stone family, not a surface. Backgrounds here
 * come from --primary-soft or --surface, and there is no
 * --muted-foreground token.
 */
export function AwaitingData({
  title,
  body,
  detail,
  children,
}: {
  title: string;
  body: string;
  detail?: string;
  children?: React.ReactNode;
}) {
  return (
    <section
      aria-live="polite"
      className="rounded-xl border border-[var(--border)] bg-[var(--surface)] px-5 py-6 sm:px-7 sm:py-8"
    >
      <p className="protea-eyebrow text-[11px] uppercase tracking-wide text-[var(--primary)]">
        {title}
      </p>
      <p className="mt-3 max-w-prose text-[15px] leading-relaxed text-[var(--foreground)]">
        {body}
      </p>
      {detail ? (
        <p className="mt-3 max-w-prose text-[13.5px] leading-relaxed text-[var(--muted)]">
          {detail}
        </p>
      ) : null}
      {children ? <div className="mt-5 flex flex-wrap gap-x-5 gap-y-2">{children}</div> : null}
    </section>
  );
}

/**
 * The understated link this page family uses: no filled buttons, an
 * underline in the border colour that strengthens on hover.
 */
export function QuietLink({
  href,
  external,
  children,
}: {
  href: string;
  external?: boolean;
  children: React.ReactNode;
}) {
  const className =
    "group inline-flex items-baseline gap-1.5 text-[14px] text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]";
  const body = (
    <>
      {children}
      <span aria-hidden className="transition-transform group-hover:translate-x-0.5">
        →
      </span>
    </>
  );
  // A path with a file extension is not a Next route: /thesis.pdf is
  // served by the API. next/link PREFETCHES, so routing the thesis link
  // through it made every visit to the home page download the whole
  // 1,114,070-byte PDF in the background. Caught by an external audit on
  // 2026-10-07. Files get a plain anchor.
  const isFile = /\.[a-z0-9]{2,5}$/i.test(href.split("?")[0]);
  return external || isFile ? (
    <a
      href={href}
      target={external ? "_blank" : undefined}
      rel={external ? "noopener noreferrer" : undefined}
      className={className}
    >
      {body}
    </a>
  ) : (
    <Link href={href} className={className}>
      {body}
    </Link>
  );
}
