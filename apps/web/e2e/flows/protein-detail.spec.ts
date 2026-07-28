// Critical user flow: the protein detail page (/proteins/[accession])
// renders identity + coverage fields from /proteins/{accession}, lazy-
// loads /proteins/{accession}/annotations when the Annotations tab is
// activated, and surfaces a not-found message when the API 404s. Driven
// against mocked endpoints via the mock-api fixture.

import { test, expect } from "./fixtures/mock-api";

const PROTEIN_DETAIL = {
  accession: "P12345",
  entry_name: "TEST_HUMAN",
  gene_name: "TST1",
  organism: "Homo sapiens",
  taxonomy_id: "9606",
  length: 320,
  reviewed: true,
  is_canonical: true,
  canonical_accession: "P12345",
  isoforms: [],
  sequence_id: 42,
  embedding_count: 1,
  go_annotation_count: 3,
  metadata: {
    function_cc: "Catalyses the hydrolysis of a model substrate.",
    ec_number: "3.1.1.1",
    catalytic_activity: null,
    pathway: null,
    keywords: "Hydrolase;Cytoplasm",
    cofactor: null,
    activity_regulation: null,
  },
};

const PROTEIN_ANNOTATIONS = [
  {
    go_id: "GO:0003674",
    name: "molecular_function",
    aspect: "F",
    qualifier: "enables",
    evidence_code: "IDA",
    assigned_by: "UniProtKB",
    db_reference: "PMID:1",
    annotation_set_id: "as-001",
    annotation_set_source: "quickgo",
    annotation_set_version: "2026-05-01",
  },
  {
    go_id: "GO:0008150",
    name: "biological_process",
    aspect: "P",
    qualifier: "involved_in",
    evidence_code: "IEA",
    assigned_by: "InterPro",
    db_reference: null,
    annotation_set_id: "as-001",
    annotation_set_source: "quickgo",
    annotation_set_version: "2026-05-01",
  },
];

test.describe("protein detail flow", () => {
  test("renders identity, coverage, and Swiss-Prot pill from /proteins/{accession}", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/proteins/P12345", PROTEIN_DETAIL);

    await page.goto("/en/instrument/proteins/P12345");

    await expect(
      page.getByRole("heading", { name: "P12345", level: 1 }),
    ).toBeVisible();
    await expect(page.getByText("TEST_HUMAN")).toBeVisible();
    await expect(page.getByText("Swiss-Prot")).toBeVisible();
    await expect(page.getByText("Homo sapiens")).toBeVisible();
    // Length renders with toLocaleString plus the aa suffix.
    await expect(page.getByText(/320 aa/)).toBeVisible();
    // Functional metadata block surfaces the function_cc text.
    await expect(
      page.getByText("Catalyses the hydrolysis of a model substrate."),
    ).toBeVisible();
  });

  test("annotations tab lazy-loads /proteins/{accession}/annotations and groups by aspect", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/proteins/P12345", PROTEIN_DETAIL);
    mockApi.override("/proteins/P12345/annotations", PROTEIN_ANNOTATIONS);

    await page.goto("/en/instrument/proteins/P12345");

    // Tab label embeds the annotation count from the detail payload.
    const annotationsTab = page.getByRole("button", {
      name: /GO Annotations \(3\)/,
    });
    await annotationsTab.click();

    // After load the per-aspect summary cards render labels for the
    // three GO namespaces. We assert by visible text rather than ARIA
    // role because the page renders these as <p> labels.
    await expect(page.getByText("Molecular Function").first()).toBeVisible();
    await expect(page.getByText("Biological Process").first()).toBeVisible();
    await expect(page.getByText("GO:0003674")).toBeVisible();
    await expect(page.getByText("GO:0008150")).toBeVisible();
    // Evidence codes appear in the per-aspect table rows.
    await expect(page.getByText("IDA", { exact: true })).toBeVisible();
    await expect(page.getByText("IEA", { exact: true })).toBeVisible();
  });

  test("404 from /proteins/{accession} surfaces the not-found message", async ({
    page,
    mockApi,
  }) => {
    mockApi.override(
      "/proteins/GHOST",
      { detail: "protein not found" },
      404,
    );

    await page.goto("/en/instrument/proteins/GHOST");

    await expect(page.getByRole("heading", { name: /Protein not found/i })).toBeVisible();
  });
});
