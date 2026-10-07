// Sidebar nav test. Two defects it pins down, both found on 2026-10-07
// while making the public site legible for anonymous visitors:
//
//   1. /instrument/graph was listed twice in the "results" group, same
//      label and icon, so the rail rendered two identical-looking rows
//      pointing at one route.
//   2. /maintenance was offered to everyone, and the edge answers it
//      with a plain-text 403 body (middleware.ts PROTECTED). A visitor
//      following it got a bare "Forbidden: this page requires role
//      'operator'" page.
//
// The duplicate assertion is written over the WHOLE rail rather than
// over that one route, so it fails for any future repeat too.

import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { SESSION_COOKIE_NAME } from "@/lib/auth";
import { DATA_SURFACES_HAVE_DATA } from "@/lib/campaign";

vi.mock("next/navigation", () => ({ usePathname: () => "/en" }));

vi.mock("next-intl", () => ({
  useLocale: () => "en",
  // The rail's labels are irrelevant here; identity keeps them stable
  // and readable in a failure message.
  useTranslations: () => (key: string) => key,
}));

vi.mock("next/link", () => ({
  default: ({ href, children, ...rest }: { href: string; children?: React.ReactNode }) => (
    <a href={href} {...rest}>
      {children}
    </a>
  ),
}));

vi.mock("next/image", () => ({
  // eslint-disable-next-line @next/next/no-img-element
  default: ({ src, alt }: { src: string; alt: string }) => <img src={src} alt={alt} />,
}));

import { Sidebar } from "@/components/Sidebar";

function mintFakeJwt(payload: Record<string, unknown>): string {
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const body = Buffer.from(JSON.stringify(payload)).toString("base64url");
  return `${header}.${body}.signature`;
}

function setCookie(value: string | null) {
  if (value === null) {
    document.cookie = `${SESSION_COOKIE_NAME}=; Path=/; Max-Age=0`;
  } else {
    document.cookie = `${SESSION_COOKIE_NAME}=${encodeURIComponent(value)}; Path=/`;
  }
}

/**
 * Hrefs of the rail a viewer can actually reach.
 *
 * The component mounts the rail TWICE on purpose, a desktop layout and
 * a mobile drawer, and the drawer stays in the DOM (hidden by transform)
 * rather than being conditionally rendered. It carries
 * ``aria-hidden={!open}`` though, so with the drawer closed exactly one
 * of the two is in the accessibility tree and a role query returns it
 * alone. That is what makes a duplicate check over the whole rail
 * meaningful: without the aria-hidden every href would appear twice.
 */
function railHrefs(): string[] {
  const rails = screen.getAllByRole("navigation", { name: "ariaPrimary" });
  expect(rails.length).toBe(1); // the closed mobile drawer is aria-hidden
  return Array.from(rails[0].querySelectorAll("a")).map((a) => a.getAttribute("href") ?? "");
}

beforeEach(() => {
  vi.stubEnv("NEXT_PUBLIC_API_URL", "/api-proxy");
  setCookie(null);
  localStorage.clear();
});

describe("Sidebar nav", () => {
  it("lists every destination at most once", () => {
    render(<Sidebar />);
    const hrefs = railHrefs();
    const seen = new Map<string, number>();
    for (const h of hrefs) seen.set(h, (seen.get(h) ?? 0) + 1);
    const repeated = [...seen.entries()].filter(([, n]) => n > 1);
    expect(repeated).toEqual([]);
  });

  it("never offers the same destination twice, graph included", () => {
    render(<Sidebar />);
    // The original defect was two identical rows for /instrument/graph.
    // The route is hidden while the corpus is empty, so assert the thing
    // that must hold in BOTH states rather than a count that moves with
    // a flag: at most one.
    const graph = railHrefs().filter((h) => h.endsWith("/instrument/graph"));
    expect(graph.length).toBeLessThanOrEqual(1);
    if (DATA_SURFACES_HAVE_DATA) expect(graph).toEqual(["/en/instrument/graph"]);
  });

  it("hides the operator-gated maintenance page from an anonymous visitor", () => {
    render(<Sidebar />);
    expect(railHrefs().some((h) => h.endsWith("/maintenance"))).toBe(false);
  });

  it("offers maintenance to an operator", () => {
    setCookie(mintFakeJwt({ sub: "x", role: "operator", exp: 9_999_999_999 }));
    render(<Sidebar />);
    // Proves the row is role-gated rather than simply deleted.
    expect(railHrefs().some((h) => h.endsWith("/maintenance"))).toBe(true);
    setCookie(null);
  });

  it("points the annotate call to action at a destination that exists", () => {
    render(<Sidebar />);
    // It used to be `/en#annotate-form` while on the home page, and that
    // anchor only exists on the annotate page, so the first-screen CTA
    // scrolled nowhere. usePathname is mocked to "/en" here, which is
    // exactly the case that was broken.
    const cta = screen.getAllByRole("link", { name: /^annotate$/ })[0];
    expect(cta.getAttribute("href")).toBe("/en/annotate");
  });

  it("keeps the surfaces that hold real data visible to an anonymous visitor", () => {
    render(<Sidebar />);
    const hrefs = railHrefs();
    // The middleware is permissive by design, and hiding a surface
    // because it is EMPTY must not spread to the ones that are not.
    // Proteins has 754,862 rows, jobs is the live campaign and stack is
    // the architecture a technical reader comes for; none of those may
    // disappear with the data flag.
    for (const open of ["/instrument/proteins", "/instrument/jobs", "/instrument/stack"]) {
      expect(hrefs.some((h) => h.endsWith(open)), open).toBe(true);
    }
  });

  it("hides the result surfaces only while there are no results", () => {
    render(<Sidebar />);
    const hrefs = railHrefs();
    const awaiting = ["/instrument/benchmark", "/instrument/evaluation", "/instrument/embeddings"];
    for (const route of awaiting) {
      expect(hrefs.some((h) => h.endsWith(route)), route).toBe(DATA_SURFACES_HAVE_DATA);
    }
  });

  it("offers the code, which needs no data at all", () => {
    render(<Sidebar />);
    expect(railHrefs().some((h) => h === "https://github.com/frapercan/PROTEA")).toBe(true);
  });
});
