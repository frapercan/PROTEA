// Each figure names the file it was read from and the digest its result recorded.

import { describe, expect, it } from "vitest";

import {
  GRID_FILE,
  HEADLINE_FILE,
  describeSource,
  fileSource,
} from "@/lib/cellSource";

const RESULT = "5cb2d2b7-d7d6-4ac3-8625-3a5208b80ac6";
const WINDOW = "ebf6f6ab-baec-437a-b1b9-310e1ebc1d43";
const HEADLINE_KEY = `eval_artifacts/${RESULT}/PK/${HEADLINE_FILE}`;
const DIGEST = "ab".repeat(32);

describe("fileSource", () => {
  it("names the file, the digest recorded for it and where it is served", () => {
    const record = { keys: [HEADLINE_KEY], sha256: { [HEADLINE_KEY]: DIGEST } };
    expect(fileSource("/api-proxy", WINDOW, RESULT, record, `PK/${HEADLINE_FILE}`)).toEqual({
      relpath: `PK/${HEADLINE_FILE}`,
      sha256: DIGEST,
      href: `/api-proxy/annotations/evaluation-sets/${WINDOW}/results/${RESULT}/artifacts/PK/${HEADLINE_FILE}`,
    });
  });

  it("keeps the file of a result made before the digests, without one", () => {
    const source = fileSource("/api-proxy", WINDOW, RESULT, { keys: [HEADLINE_KEY] }, `PK/${HEADLINE_FILE}`);
    expect(source?.sha256).toBeNull();
    expect(describeSource(source!)).toMatch(/no digest recorded/);
  });

  it("is null for a file the result did not write", () => {
    const record = { keys: [HEADLINE_KEY], sha256: { [HEADLINE_KEY]: DIGEST } };
    expect(fileSource("/api-proxy", WINDOW, RESULT, record, `PK/${GRID_FILE}`)).toBeNull();
    expect(fileSource("/api-proxy", WINDOW, RESULT, undefined, `PK/${HEADLINE_FILE}`)).toBeNull();
  });

  it("names the whole digest in the tooltip", () => {
    const record = { keys: [HEADLINE_KEY], sha256: { [HEADLINE_KEY]: DIGEST } };
    const source = fileSource("/api-proxy", WINDOW, RESULT, record, `PK/${HEADLINE_FILE}`)!;
    expect(describeSource(source)).toBe(`PK/${HEADLINE_FILE}, sha256 ${DIGEST}`);
  });
});
