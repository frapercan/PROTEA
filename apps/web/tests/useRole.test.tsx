// FEAT-AUTH (WAVE-2 2026-05-23) — useRole hook test. Mounts a tiny
// consumer component in happy-dom so the effect runs and the cookie
// gets read; verifies the hook reflects mid-flight cookie changes
// after a synthetic ``storage`` event.

import { act, render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { useRole, useHasRole } from "@/lib/useRole";
import { SESSION_COOKIE_NAME } from "@/lib/auth";

function mintFakeJwt(payload: Record<string, unknown>): string {
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const body = Buffer.from(JSON.stringify(payload)).toString("base64url");
  return `${header}.${body}.signature`;
}

function setCookie(value: string | null) {
  if (typeof document === "undefined") return;
  if (value === null) {
    document.cookie = `${SESSION_COOKIE_NAME}=; Path=/; Max-Age=0`;
  } else {
    document.cookie = `${SESSION_COOKIE_NAME}=${encodeURIComponent(value)}; Path=/`;
  }
}

function Probe({ pick }: { pick: "role" | "isOperator" | "isAdmin" }) {
  const role = useRole();
  const isOp = useHasRole("operator");
  const isAdmin = useHasRole("admin");
  return (
    <span data-testid="out">
      {pick === "role" ? role : pick === "isOperator" ? String(isOp) : String(isAdmin)}
    </span>
  );
}

describe("useRole", () => {
  it("returns viewer when no cookie is present", () => {
    setCookie(null);
    const { getByTestId } = render(<Probe pick="role" />);
    // First paint is "viewer" (SSR-safe default) and the effect re-reads
    // a still-empty cookie, so the value stays viewer.
    expect(getByTestId("out").textContent).toBe("viewer");
  });

  it("returns the JWT role claim when cookie is set", () => {
    setCookie(mintFakeJwt({ sub: "x", role: "operator", exp: 9_999_999_999 }));
    const { getByTestId } = render(<Probe pick="role" />);
    expect(getByTestId("out").textContent).toBe("operator");
    setCookie(null);
  });

  it("useHasRole returns true for operator JWT at operator floor", () => {
    setCookie(mintFakeJwt({ sub: "x", role: "operator", exp: 9_999_999_999 }));
    const { getByTestId } = render(<Probe pick="isOperator" />);
    expect(getByTestId("out").textContent).toBe("true");
    setCookie(null);
  });

  it("useHasRole(admin) is false for an operator JWT", () => {
    setCookie(mintFakeJwt({ sub: "x", role: "operator", exp: 9_999_999_999 }));
    const { getByTestId } = render(<Probe pick="isAdmin" />);
    expect(getByTestId("out").textContent).toBe("false");
    setCookie(null);
  });

  it("re-reads on cross-tab storage event", () => {
    setCookie(null);
    const { getByTestId } = render(<Probe pick="role" />);
    expect(getByTestId("out").textContent).toBe("viewer");

    setCookie(mintFakeJwt({ sub: "x", role: "admin", exp: 9_999_999_999 }));
    act(() => {
      window.dispatchEvent(new StorageEvent("storage", { key: "protea.signin" }));
    });
    expect(getByTestId("out").textContent).toBe("admin");
    setCookie(null);
  });
});
