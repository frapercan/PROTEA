// FARM-UI.9 P1.1 nested layout for the farm section. Mounts the
// FarmSubnav tab bar above every page under /[locale]/farm so operators
// can reach Tasks / Plan / Cost in one click regardless of which view
// they landed on.
//
// UX-ADMIN-AUDIT P0-FRM-1 (2026-05-26): the farm surfaces expose task
// catalogs, plan DAGs, cost charts, heartbeats and spawn_args that may
// contain prompts or task parameters. They must not be reachable by
// anonymous visitors. We delegate the gate to the FarmAuthGate client
// island below so the SSR shell stays minimal and the redirect uses
// the existing client-side session cookie ("protea_session").

import { FarmSubnav } from "@/components/FarmSubnav";
import { FarmAuthGate } from "@/components/FarmAuthGate";

export default function FarmLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <FarmAuthGate>
      <FarmSubnav />
      {children}
    </FarmAuthGate>
  );
}
