"use client";

import dynamic from "next/dynamic";

/**
 * Client wrapper that lazy-loads the CommandPalette body.
 *
 * The palette is mounted once in the locale layout but the modal is
 * only ever opened by an explicit cmd+k / ctrl+k chord or by a click
 * on the header trigger. There is no reason to spend the SSR pass and
 * the React hydration cycle wiring up the dialog body, fuzzy search,
 * and the agent-farm action hooks on first paint. `dynamic({ ssr:
 * false })` skips the SSR work and lets Next ship the palette chunk
 * as an async script tag (does not block LCP / TBT critical path).
 *
 * `next/dynamic` with `ssr: false` is only valid inside a client
 * component, which is why this wrapper exists between the server-
 * rendered layout and the (already `"use client"`) CommandPalette
 * module.
 */
const CommandPaletteImpl = dynamic(
  () =>
    import("@/components/CommandPalette").then((m) => ({
      default: m.CommandPalette,
    })),
  { ssr: false, loading: () => null },
);

export function LazyCommandPalette() {
  return <CommandPaletteImpl />;
}
