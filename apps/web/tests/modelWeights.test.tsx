// The model weights page (ADR-D54): what it must show and what it must send.
//
// The author, 2026-11-03: "los pesos almacenados y listado que se puedan subir...
// parecido a las representaciones aprendidas o mejor". Three things here would fail
// quietly if they broke: an upload must send every part, in order, each the slice
// the server expects, and abandon the upload on the server when a part fails; a
// weight's detail must say what uses it; and "Encode a corpus" must queue the
// operation with this vocabulary's key, not one typed by hand.

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  listModelWeights: vi.fn(),
  getModelWeights: vi.fn(),
  listEmbeddingConfigs: vi.fn(),
  createJob: vi.fn(),
  listJobs: vi.fn(),
  listResidueCodes: vi.fn(),
}));

vi.mock("next-intl", () => ({
  useLocale: () => "en",
  useTranslations: () => (key: string, args?: Record<string, unknown>) =>
    args ? `${key}:${JSON.stringify(args)}` : key,
}));

vi.mock("next/link", () => ({
  default: ({ href, children }: { href: string; children?: React.ReactNode }) => <a href={href}>{children}</a>,
}));

vi.mock("@/lib/useRole", () => ({ useHasRole: () => true, useRole: () => "operator" }));

vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return { ...actual, ...mocks };
});

import { ModelWeightsPanel } from "@/components/ModelWeightsPanel";
import { EncodeCorpusDialog } from "@/components/WeightsDialogs";
import type { ModelWeights } from "@/lib/api";

const VOCABULARY: ModelWeights = {
  id: "0d3f6c1e-2b7a-4f0e-9a51-3c2d1e0f9b8a",
  kind: "residue_vocabulary",
  name: "vocabulary-v1",
  description: "nested flat 8,192 over five Ankh layers, read at 128 actives",
  format: "npz",
  uri: "residue-vocabulary/vocabulary-v1-06fed75ca76d17be.npz",
  sha256: "06fed75ca76d17be".padEnd(64, "0"),
  size_bytes: 260233730,
  meta: { dict_dim: 8192, actives: 128 },
  trained_on_annotation_set_id: null,
  created_at: "2026-11-03T10:00:00+00:00",
  created_by: "operator",
  configurations: 1,
};

const READER: ModelWeights = { ...VOCABULARY, id: "r1", kind: "reader", name: "reader-211", configurations: 0 };

describe("ModelWeightsPanel", () => {
  beforeEach(() => {
    Object.values(mocks).forEach((m) => m.mockReset());
  });

  it("lists the weights with their size and counts them by kind", async () => {
    mocks.listModelWeights.mockResolvedValue([VOCABULARY, READER]);
    render(<ModelWeightsPanel />);
    await waitFor(() => expect(screen.getByText("vocabulary-v1")).toBeTruthy());
    expect(screen.getByText("reader-211")).toBeTruthy();
    expect(screen.getAllByText("248.2 MB").length).toBe(2);
    fireEvent.click(screen.getByRole("tab", { name: /kinds\.reader/ }));
    expect(screen.queryByText("vocabulary-v1")).toBeNull();
    expect(screen.getByText("reader-211")).toBeTruthy();
  });

  it("opens a weight into what uses it, and offers to encode with a vocabulary", async () => {
    mocks.listModelWeights.mockResolvedValue([VOCABULARY]);
    mocks.getModelWeights.mockResolvedValue({
      ...VOCABULARY,
      used_by: {
        configurations: [{ id: "c1", name: "residue-vocabulary:06fed75ca76d17be:f3a7fe16", kind: "learned" }],
        jobs: [
          {
            id: "15624da2-3ea7-4d4e-ae05-4200dd101772",
            operation: "encode_residue_vocabulary",
            status: "RUNNING",
            created_at: null,
          },
        ],
      },
    });
    render(<ModelWeightsPanel />);
    fireEvent.click(await screen.findByText("vocabulary-v1"));
    await waitFor(() => expect(screen.getByText("residue-vocabulary:06fed75ca76d17be:f3a7fe16")).toBeTruthy());
    expect(screen.getByText("15624da2").closest("a")?.getAttribute("href")).toBe(
      "/en/instrument/jobs/15624da2-3ea7-4d4e-ae05-4200dd101772",
    );
    expect(screen.getByText(VOCABULARY.uri)).toBeTruthy();
    expect(screen.getByRole("button", { name: "actions.encode" })).toBeTruthy();
  });

  it("says the registry is empty instead of spinning", async () => {
    mocks.listModelWeights.mockResolvedValue([]);
    render(<ModelWeightsPanel />);
    await waitFor(() => expect(screen.getByText("emptyTitle")).toBeTruthy());
  });
});

describe("EncodeCorpusDialog", () => {
  beforeEach(() => {
    Object.values(mocks).forEach((m) => m.mockReset());
  });

  function freeCard() {
    mocks.listJobs.mockResolvedValue([]);
    mocks.listResidueCodes.mockResolvedValue([]);
  }

  it("queues encode_residue_vocabulary with this vocabulary's key over an Ankh source", async () => {
    freeCard();
    mocks.listEmbeddingConfigs.mockResolvedValue([
      { id: "e5e5e5e5-0000-0000-0000-000000000000", model_name: "facebook/esm2", model_backend: "esm" },
      { id: "f3a7fe16-eaf5-5083-b459-7c5fa7a9a9e0", model_name: "ElnaggarLab/ankh-base", model_backend: "ankh" },
      { id: "22166300-a178-599e-835c-afe1c007feb5", model_name: "residue-vocabulary:x", model_backend: "residue-vocabulary" },
    ]);
    mocks.createJob.mockResolvedValue({ id: "9a9a9a9a-0000-0000-0000-000000000000", status: "queued" });
    render(<EncodeCorpusDialog weights={VOCABULARY} onClose={() => undefined} />);
    await waitFor(() =>
      expect((screen.getByLabelText("encode.source") as HTMLSelectElement).value).toBe(
        "f3a7fe16-eaf5-5083-b459-7c5fa7a9a9e0",
      ),
    );
    fireEvent.click(screen.getByRole("button", { name: "encode.submit" }));
    await waitFor(() => expect(mocks.createJob).toHaveBeenCalled());
    expect(mocks.createJob.mock.calls[0][0]).toEqual({
      operation: "encode_residue_vocabulary",
      queue_name: "protea.embeddings",
      payload: {
        source_embedding_config_id: "f3a7fe16-eaf5-5083-b459-7c5fa7a9a9e0",
        vocabulary_artifact_uri: VOCABULARY.uri,
        device: "cuda",
        sequences_per_job: 128,
        residues_per_call: 32768,
      },
    });
    expect(await screen.findByText("9a9a9a9a")).toBeTruthy();
  });
});

describe("EncodeCorpusDialog on a busy card", () => {
  beforeEach(() => {
    Object.values(mocks).forEach((m) => m.mockReset());
  });

  it("says what holds the card and queues only once the operator confirms", async () => {
    mocks.listEmbeddingConfigs.mockResolvedValue([
      { id: "f3a7fe16-eaf5-5083-b459-7c5fa7a9a9e0", model_name: "ElnaggarLab/ankh-base", model_backend: "ankh" },
    ]);
    mocks.listJobs.mockImplementation(async ({ operation }: { operation: string }) =>
      operation === "encode_residue_vocabulary"
        ? [{ id: "83039cad-0000-0000-0000-000000000000", operation, status: "RUNNING", queue_name: "protea.embeddings" }]
        : [],
    );
    mocks.listResidueCodes.mockResolvedValue([
      { name: "residue-vocabulary:06fed75ca76d17be:f3a7fe16", index: { sequences_last_hour: 36000 } },
    ]);
    mocks.createJob.mockResolvedValue({ id: "9a9a9a9a-0000-0000-0000-000000000000", status: "queued" });
    render(<EncodeCorpusDialog weights={VOCABULARY} onClose={() => undefined} />);
    expect(await screen.findByText("encode.busyTitle")).toBeTruthy();
    expect(screen.getByText("83039cad").closest("a")?.getAttribute("href")).toBe(
      "/en/instrument/jobs/83039cad-0000-0000-0000-000000000000",
    );
    expect(
      screen.getByText('encode.busyWriting:{"name":"residue-vocabulary:06fed75ca76d17be:f3a7fe16"}'),
    ).toBeTruthy();
    const submit = screen.getByRole("button", { name: "encode.submit" }) as HTMLButtonElement;
    await waitFor(() =>
      expect((screen.getByLabelText("encode.source") as HTMLSelectElement).value).toBe(
        "f3a7fe16-eaf5-5083-b459-7c5fa7a9a9e0",
      ),
    );
    expect(submit.disabled).toBe(true);
    fireEvent.click(screen.getByRole("checkbox"));
    expect(submit.disabled).toBe(false);
    fireEvent.click(submit);
    await waitFor(() => expect(mocks.createJob).toHaveBeenCalled());
  });
});

describe("uploadModelWeights", () => {
  const realFetch = globalThis.fetch;
  beforeEach(() => {
    vi.stubEnv("NEXT_PUBLIC_API_URL", "/api-proxy/v1");
  });
  afterEach(() => {
    globalThis.fetch = realFetch;
    vi.unstubAllEnvs();
  });

  function json(body: unknown, status = 200) {
    return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
  }

  it("sends every part in order, each the slice the server expects, then completes", async () => {
    const { uploadModelWeights } = await vi.importActual<typeof import("@/lib/api")>("@/lib/api");
    const calls: { method: string; url: string; size?: number }[] = [];
    globalThis.fetch = vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
      const method = (init?.method ?? "GET").toUpperCase();
      const body = init?.body;
      calls.push({ method, url: String(url), size: body instanceof Blob ? body.size : undefined });
      if (String(url).endsWith("/model-weights/uploads")) return json({ upload_id: "u1", part_bytes: 4, parts: 3 }, 201);
      if (String(url).includes("/parts/")) return json({ received: 1, parts: 3 });
      if (String(url).endsWith("/complete")) return json({ ...VOCABULARY }, 201);
      return json({});
    }) as typeof fetch;
    const file = new File([new Uint8Array(10)], "vocabulary-v1.npz");
    const seen: number[] = [];
    const w = await uploadModelWeights(file, { kind: "residue_vocabulary", name: "vocabulary-v1" }, (sent) =>
      seen.push(sent),
    );
    expect(w.uri).toBe(VOCABULARY.uri);
    const parts = calls.filter((c) => c.method === "PUT");
    expect(parts.map((c) => c.url.split("/parts/")[1])).toEqual(["0", "1", "2"]);
    expect(parts.map((c) => c.size)).toEqual([4, 4, 2]);
    expect(seen).toEqual([1, 2, 3]);
    expect(calls.at(-1)?.url.endsWith("/model-weights/uploads/u1/complete")).toBe(true);
  });

  it("abandons the upload on the server when a part fails", async () => {
    const { uploadModelWeights } = await vi.importActual<typeof import("@/lib/api")>("@/lib/api");
    const methods: string[] = [];
    globalThis.fetch = vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
      const method = (init?.method ?? "GET").toUpperCase();
      methods.push(`${method} ${String(url).split("/model-weights")[1]}`);
      if (String(url).endsWith("/model-weights/uploads")) return json({ upload_id: "u2", part_bytes: 4, parts: 2 }, 201);
      if (String(url).includes("/parts/1")) return json({ detail: "part 1 has 9 bytes" }, 422);
      return json({ received: 1, parts: 2 });
    }) as typeof fetch;
    const file = new File([new Uint8Array(6)], "r.pt");
    await expect(uploadModelWeights(file, { kind: "reader", name: "r" })).rejects.toBeTruthy();
    expect(methods.at(-1)).toBe("DELETE /uploads/u2");
  });
});
