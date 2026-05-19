// FARM-UI.9 P1.1 nested layout for the farm section. Mounts the
// FarmSubnav tab bar above every page under /[locale]/farm so operators
// can reach Tasks / Plan / Cost in one click regardless of which view
// they landed on. The component is a server component (no hooks needed)
// because the subnav itself is the client island; this layout is just a
// structural wrapper.

import { FarmSubnav } from "@/components/FarmSubnav";

export default function FarmLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <>
      <FarmSubnav />
      {children}
    </>
  );
}
