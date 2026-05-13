import { renderHook, act } from "@testing-library/react";
import { describe, it, expect, vi } from "vitest";
import { useFocusTrap } from "@/lib/useFocusTrap";

describe("useFocusTrap", () => {
  it("returns a ref", () => {
    const { result } = renderHook(() => useFocusTrap(false));
    expect(result.current).toHaveProperty("current", null);
  });

  it("restores focus on unmount when active", async () => {
    const outside = document.createElement("button");
    outside.textContent = "outside";
    document.body.appendChild(outside);
    outside.focus();

    const div = document.createElement("div");
    const btn = document.createElement("button");
    btn.textContent = "inside";
    div.appendChild(btn);
    document.body.appendChild(div);

    const { result, unmount } = renderHook(() => useFocusTrap(true));
    (result.current as React.MutableRefObject<HTMLDivElement>).current = div;

    unmount();
    expect(document.activeElement).toBe(outside);
    document.body.removeChild(div);
    document.body.removeChild(outside);
  });

  it("does nothing when not active", () => {
    const onClose = vi.fn();
    const { unmount } = renderHook(() => useFocusTrap(false, onClose));
    unmount();
    expect(onClose).not.toHaveBeenCalled();
  });
});
