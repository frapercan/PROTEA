// Thin helper for landing-flow tests to drive the showcase HTTP mock
// (see ../../mock-server/showcase-server.mjs). The mock is launched by
// Playwright's `webServer` config, so it is already running by the
// time any test executes. Tests POST to /__mock__/showcase to swap
// the response the next /showcase/ fetch will see, or call reset()
// to restore defaults between tests.

const MOCK_BASE =
  process.env.MOCK_API_BASE ?? `http://127.0.0.1:${process.env.MOCK_API_PORT ?? "8000"}`;

export async function setShowcaseResponse(body: unknown, status = 200): Promise<void> {
  const res = await fetch(`${MOCK_BASE}/__mock__/showcase`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ status, body }),
  });
  if (!res.ok) {
    throw new Error(`mock control failed: ${res.status} ${await res.text()}`);
  }
}

export async function resetShowcaseMock(): Promise<void> {
  const res = await fetch(`${MOCK_BASE}/__mock__/reset`, { method: "POST" });
  if (!res.ok) {
    throw new Error(`mock reset failed: ${res.status} ${await res.text()}`);
  }
}
