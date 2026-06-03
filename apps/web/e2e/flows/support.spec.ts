// Critical user flow: support page renders the hero counter from the
// /support endpoint, breaks the total into "with comments" vs
// "anonymous", and lists user-submitted comments when present.
// Hermetic via the mock-api fixture.

import { test, expect } from "./fixtures/mock-api";

const SUPPORT_WITH_COMMENTS = {
  count: 137,
  comments: [
    {
      id: "c1",
      comment: "Great work on PROTEA, looking forward to CAFA 7.",
      created_at: "2026-05-15T12:00:00Z",
    },
    {
      id: "c2",
      comment: "The maintenance vacuum saved us a week of cleanup.",
      created_at: "2026-05-14T08:00:00Z",
    },
  ],
};

const SUPPORT_ANONYMOUS_ONLY = {
  count: 5,
  comments: [],
};

test.describe("support flow", () => {
  test("hero shows the supporter count from /support", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/support", SUPPORT_WITH_COMMENTS);

    await page.goto("/en/support/");

    await expect(
      page.getByRole("heading", { name: "Back the project", level: 1 }),
    ).toBeVisible();
    // toLocaleString on 137 renders as "137" (no thousands sep needed).
    await expect(page.getByText("137").first()).toBeVisible();
  });

  test("breakdown splits the total into 'with comments' and 'anonymous'", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/support", SUPPORT_WITH_COMMENTS);

    await page.goto("/en/support/");

    // 2 of 137 left a comment, so 135 are anonymous.
    await expect(page.getByText(/2 with comments/)).toBeVisible();
    await expect(page.getByText(/135 anonymous/)).toBeVisible();
  });

  test("comments section lists each comment body", async ({ page, mockApi }) => {
    mockApi.override("/support", SUPPORT_WITH_COMMENTS);

    await page.goto("/en/support/");

    await expect(
      page.getByRole("heading", { name: "What people are saying" }),
    ).toBeVisible();
    await expect(
      page.getByText("Great work on PROTEA, looking forward to CAFA 7."),
    ).toBeVisible();
    await expect(
      page.getByText("The maintenance vacuum saved us a week of cleanup."),
    ).toBeVisible();
  });

  test("empty comments collapse to the no-comments-yet message", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/support", SUPPORT_ANONYMOUS_ONLY);

    await page.goto("/en/support/");

    await expect(page.getByText("No comments yet. Be the first!")).toBeVisible();
    // Heading for the comments section should not be present.
    await expect(
      page.getByRole("heading", { name: "What people are saying" }),
    ).toHaveCount(0);
  });
});
