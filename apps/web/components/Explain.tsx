"use client";

import { useId, useRef, useState } from "react";
import { HelpCircle } from "lucide-react";

/**
 * A definition a reader can reach, instead of one the browser hides.
 *
 * WHY THIS EXISTS. The instrument surfaces carried five hundred `title`
 * attributes. The native tooltip is technically accessible, so an automated
 * audit reports nothing, and in practice it is unreadable: it is truncated by
 * the browser at a width nobody controls, it cannot wrap, it takes a second to
 * appear, and on a touch screen it does not appear at all. A reviewer reading
 * these pages said exactly that, of the one place it mattered most: he could
 * not read what came up over the knowledge categories.
 *
 * So the explanation becomes a control. It opens on click and on Enter or
 * Space, it closes on Escape, it is reachable by keyboard, it exists on touch,
 * and it wraps to as many lines as the sentence needs.
 *
 * It also answers the other half of that review, which asked for the long
 * prose under a heading to fold away behind a question mark. Same mechanism:
 * the text is not removed, it is put one deliberate action away. Nothing is
 * hidden from a reader who looks, which is the property these pages are for.
 */

type ExplainProps = {
  /** The definition. Prose, as long as it needs to be. */
  children: React.ReactNode;
  /** What is being explained, for the button's accessible name. */
  label: string;
  /** Where the panel opens. Defaults to below and left-aligned. */
  align?: "left" | "right";
};

export function Explain({ children, label, align = "left" }: ExplainProps) {
  const [open, setOpen] = useState(false);
  const id = useId();
  const button = useRef<HTMLButtonElement>(null);

  return (
    <span
      className="relative inline-flex"
      onKeyDown={(e) => {
        // On the wrapper, not on the panel. Opening leaves focus on the button,
        // so a handler attached to the panel never sees the key: the reader
        // presses Escape, nothing happens, and the only way out is the mouse.
        if (e.key === "Escape" && open) {
          e.stopPropagation();
          setOpen(false);
          button.current?.focus();
        }
      }}
    >
      <button
        ref={button}
        type="button"
        aria-expanded={open}
        aria-controls={open ? id : undefined}
        // The name says what it explains. "More information" tells a reader
        // cycling through controls nothing about which one to open.
        aria-label={label}
        onClick={() => setOpen((v) => !v)}
        className="inline-flex h-4 w-4 items-center justify-center rounded-full text-slate-400 transition hover:text-slate-700 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-600"
      >
        <HelpCircle className="h-3.5 w-3.5" aria-hidden />
      </button>
      {open && (
        <span
          id={id}
          role="note"
          className={`absolute top-6 z-30 w-72 rounded-lg border border-slate-200 bg-white p-3 text-xs font-normal leading-relaxed text-slate-700 shadow-lg ${
            align === "right" ? "right-0" : "left-0"
          }`}
        >
          {children}
        </span>
      )}
    </span>
  );
}
