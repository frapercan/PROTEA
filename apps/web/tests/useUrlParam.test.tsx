import { renderHook, act } from "@testing-library/react";
import { describe, it, expect, vi } from "vitest";
import { useUrlParam, useUrlNumber } from "@/lib/useUrlParam";

const mockReplace = vi.fn();
const mockPathname = "/test";
let mockParams = new URLSearchParams("");

vi.mock("next/navigation", () => ({
  useRouter: () => ({ replace: mockReplace }),
  usePathname: () => mockPathname,
  useSearchParams: () => mockParams,
}));

beforeEach(() => {
  mockReplace.mockClear();
  mockParams = new URLSearchParams("");
});

describe("useUrlParam", () => {
  it("returns defaultValue when param is absent", () => {
    const { result } = renderHook(() => useUrlParam("key", "default"));
    expect(result.current[0]).toBe("default");
  });

  it("returns param value when present", () => {
    mockParams = new URLSearchParams("key=value");
    const { result } = renderHook(() => useUrlParam("key"));
    expect(result.current[0]).toBe("value");
  });

  it("calls router.replace on setValue", () => {
    const { result } = renderHook(() => useUrlParam("key"));
    act(() => result.current[1]("new-value"));
    expect(mockReplace).toHaveBeenCalledWith("/test?key=new-value", {
      scroll: false,
    });
  });

  it("removes key when setValue(null)", () => {
    const { result } = renderHook(() => useUrlParam("key", "default"));
    act(() => result.current[1](null));
    expect(mockReplace).toHaveBeenCalledWith("/test", { scroll: false });
  });

  it("removes key when setValue matches defaultValue", () => {
    const { result } = renderHook(() => useUrlParam("key", "default"));
    act(() => result.current[1]("default"));
    expect(mockReplace).toHaveBeenCalledWith("/test", { scroll: false });
  });
});

describe("useUrlNumber", () => {
  it("returns null for missing param", () => {
    const { result } = renderHook(() => useUrlNumber("k"));
    expect(result.current[0]).toBeNull();
  });

  it("returns parsed number", () => {
    mockParams = new URLSearchParams("k=42");
    const { result } = renderHook(() => useUrlNumber("k"));
    expect(result.current[0]).toBe(42);
  });

  it("returns null on NaN", () => {
    mockParams = new URLSearchParams("k=not-a-number");
    const { result } = renderHook(() => useUrlNumber("k"));
    expect(result.current[0]).toBeNull();
  });

  it("calls setValue with stringified number", () => {
    const { result } = renderHook(() => useUrlNumber("k"));
    act(() => result.current[1](99));
    expect(mockReplace).toHaveBeenCalledWith("/test?k=99", { scroll: false });
  });
});
