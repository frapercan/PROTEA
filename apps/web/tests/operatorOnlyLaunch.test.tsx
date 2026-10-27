// An anonymous visitor must not be able to fire a job form.
//
// Found by an external audit on 2026-10-07: /instrument/proteins?tab=insert
// and three sibling tabs rendered a pre-filled operator form with a live
// "Launch Job" button. Pressing it POSTed /v1/jobs, which requires the
// operator role, and the visitor was shown the server's own problem
// document. The form stays on screen, because the operation exists and
// works; what is restricted is the dispatch.

import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { SESSION_COOKIE_NAME } from "@/lib/auth";

vi.mock("next/navigation", () => ({
  useParams: () => ({ locale: "en" }),
  useRouter: () => ({ replace: vi.fn(), push: vi.fn() }),
  usePathname: () => "/en/instrument/annotations",
  useSearchParams: () => new URLSearchParams("tab=snapshots"),
}));

vi.mock("next-intl", () => ({
  useLocale: () => "en",
  useTranslations: () => {
    const t = (key: string) => key;
    (t as unknown as { rich: unknown }).rich = (key: string) => key;
    return t;
  },
}));

vi.mock("next/link", () => ({
  default: ({ href, children }: { href: string; children?: React.ReactNode }) => (
    <a href={href}>{children}</a>
  ),
}));

import { OperatorOnlyNotice, useMayLaunchJobs } from "@/components/OperatorOnly";

function mintFakeJwt(payload: Record<string, unknown>): string {
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const body = Buffer.from(JSON.stringify(payload)).toString("base64url");
  return `${header}.${body}.signature`;
}

function setCookie(value: string | null) {
  document.cookie =
    value === null
      ? `${SESSION_COOKIE_NAME}=; Path=/; Max-Age=0`
      : `${SESSION_COOKIE_NAME}=${encodeURIComponent(value)}; Path=/`;
}

function Probe() {
  return <span data-testid="out">{String(useMayLaunchJobs())}</span>;
}

beforeEach(() => setCookie(null));

describe("the operator gate on job dispatch", () => {
  it("refuses an anonymous visitor", () => {
    render(<Probe />);
    expect(screen.getByTestId("out").textContent).toBe("false");
  });

  it("refuses a viewer, who also cannot POST /v1/jobs", () => {
    setCookie(mintFakeJwt({ sub: "x", role: "viewer", exp: 9_999_999_999 }));
    render(<Probe />);
    expect(screen.getByTestId("out").textContent).toBe("false");
    setCookie(null);
  });

  it("allows an operator", () => {
    setCookie(mintFakeJwt({ sub: "x", role: "operator", exp: 9_999_999_999 }));
    render(<Probe />);
    expect(screen.getByTestId("out").textContent).toBe("true");
    setCookie(null);
  });

  it("says why, rather than leaving a dead control unexplained", () => {
    render(<OperatorOnlyNotice />);
    expect(screen.getByText("needsOperator")).toBeInTheDocument();
  });
});
