import { Skeleton } from "@/components/Skeleton";

/**
 * Route-level loading UI for every page under `[locale]/*`.
 *
 * Next.js App Router renders this scaffold inside an automatic Suspense
 * boundary while the next route chunk is fetched, parsed and hydrated.
 * Because every page under this segment is currently a `"use client"`
 * component that fetches data inside `useEffect`, the in-page Skeleton
 * only appears after hydration. This file closes the perceived gap by
 * showing a generic skeleton instantly on navigation.
 *
 * Keep this file dependency free (server component, no client hooks) so
 * it streams from the server with zero JS cost.
 */
export default function Loading() {
  return (
    <div className="space-y-8" aria-busy="true" aria-live="polite">
      <Skeleton className="h-8 w-64" />
      <Skeleton className="h-96 rounded-lg" />
    </div>
  );
}
