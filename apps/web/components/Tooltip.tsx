"use client";

export function Tooltip({ text, children }: { text: string; children: React.ReactNode }) {
  return (
    <span className="relative inline-block group">
      {children}
      <span className="pointer-events-none absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 z-20 hidden group-hover:block w-56 rounded-md border border-gray-200 bg-white px-3 py-2 text-xs text-gray-600 shadow-lg leading-relaxed">
        {text}
      </span>
    </span>
  );
}

export function HelpDot({ text }: { text: string }) {
  return (
    <Tooltip text={text}>
      <span className="inline-flex items-center justify-center w-4 h-4 rounded-full bg-gray-200 text-gray-500 text-[10px] font-bold cursor-help select-none ml-1 align-middle">
        ?
      </span>
    </Tooltip>
  );
}
