// A protein's current UniProt entry says which release it is, and that it is today's.
//
// The metadata on a protein page is knowledge from after every cut the platform
// evaluates on. The card names the release that served it and says it is shown,
// never used on an earlier cut; an entry fetched before releases were recorded
// says so instead of passing for current.

import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

vi.mock("next-intl", () => ({
  useTranslations: () => (key: string, values?: Record<string, string>) =>
    values ? `${key} ${Object.values(values).join(" ")}` : key,
}));

import { CurrentEntryCard } from "@/components/CurrentEntryCard";

describe("the current entry card", () => {
  it("names the release that served the entry and shows its fields", () => {
    render(
      <CurrentEntryCard
        entry={{
          uniprot_release: "2026_04",
          uniprot_release_date: "04-November-2026",
          entry_name: "HBA_HUMAN",
          reviewed: true,
          organism_name: "Homo sapiens (Human)",
          organism_id: 9606,
          length: 142,
        }}
      />,
    );
    expect(screen.getByRole("note").textContent).toBe("currentEntryNote 2026_04 04-November-2026");
    expect(screen.getByText("HBA_HUMAN")).toBeTruthy();
    expect(screen.getByText("9606")).toBeTruthy();
    expect(screen.getByText("yes")).toBeTruthy();
  });

  it("says when the entry does not record its release", () => {
    render(<CurrentEntryCard entry={{ entry_name: "HBA_HUMAN" }} />);
    expect(screen.getByRole("note").textContent).toBe("unknownRelease");
  });
});
