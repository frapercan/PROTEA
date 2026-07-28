import Link from "next/link";
import { getLocale, getTranslations } from "next-intl/server";
import {
  Atom,
  Tags,
  Sliders,
  ArrowUpDown,
  Archive,
  Dna,
  Tag,
  FolderOpen,
  BarChart3,
  Gauge,
  Inbox,
  Boxes,
  Bot,
  type LucideIcon,
} from "lucide-react";

/**
 * The instrument hub: a tab, not the entrance.
 *
 * The front door `/` is the argument (the book); the operational dashboard
 * lives one level in, under `/instrument/*`. This page is the doorway to that
 * dashboard: it lists every operational / data / experiment tool, grouped the
 * same way the sidebar groups them, so an operator (or a stranger who cloned
 * the platform) can find the machinery without hunting. It fabricates no data,
 * it only links to the tools that already exist.
 */

type Tool = {
  href: string;
  labelKey: string;
  hint: string;
  icon: LucideIcon;
};

type ToolGroup = {
  titleKey: string;
  hintKey: string;
  tools: Tool[];
};

const GROUPS: ToolGroup[] = [
  {
    titleKey: "pipelineGroup",
    hintKey: "pipelineHint",
    tools: [
      { href: "/instrument/embeddings", labelKey: "embeddings", hint: "PLM embedding configs across the eight backbones", icon: Atom },
      { href: "/instrument/functional-annotation", labelKey: "functionalAnnotation", hint: "Embedding-similarity GO annotation, BPO / MFO / CCO", icon: Tags },
      { href: "/instrument/scoring", labelKey: "scoring", hint: "Combine distance, alignment, taxonomy, evidence", icon: Sliders },
      { href: "/instrument/reranker", labelKey: "reranker", hint: "LightGBM reranker over scored predictions", icon: ArrowUpDown },
      { href: "/instrument/datasets", labelKey: "datasets", hint: "Frozen reranker dumps and export dispatcher", icon: Archive },
    ],
  },
  {
    titleKey: "data",
    hintKey: "dataHint",
    tools: [
      { href: "/instrument/proteins", labelKey: "proteins", hint: "UniProt entries, Swiss-Prot + TrEMBL, isoforms", icon: Dna },
      { href: "/instrument/annotations", labelKey: "annotations", hint: "GO ontology snapshots and ground-truth sets", icon: Tag },
      { href: "/instrument/query-sets", labelKey: "querySets", hint: "FASTA uploads grouped for batch runs", icon: FolderOpen },
    ],
  },
  {
    titleKey: "results",
    hintKey: "resultsHint",
    tools: [
      { href: "/instrument/benchmark", labelKey: "benchmark", hint: "The nine-cell benchmark matrix", icon: BarChart3 },
      { href: "/instrument/evaluation", labelKey: "evaluation", hint: "CAFA-style delta evaluation", icon: Gauge },
    ],
  },
  {
    titleKey: "system",
    hintKey: "operationsHint",
    tools: [
      { href: "/instrument/jobs", labelKey: "jobs", hint: "Live job queue and event audit trail", icon: Inbox },
      { href: "/instrument/stack", labelKey: "stack", hint: "Eight repositories, open PRs, deploy targets", icon: Boxes },
      { href: "/instrument/farm", labelKey: "farmSection", hint: "Agent-farm tasks, plan DAG and cost", icon: Bot },
    ],
  },
];

export default async function InstrumentHubPage() {
  const tNav = await getTranslations("nav");
  const tHub = await getTranslations("instrument");
  const locale = await getLocale();

  return (
    <div className="mx-auto w-full max-w-screen-xl px-4 sm:px-6 lg:px-8 py-8 lg:py-10 space-y-10">
      <header className="max-w-3xl space-y-3">
        <p className="protea-eyebrow text-xs uppercase tracking-wider text-blue-700">
          {tHub("eyebrow")}
        </p>
        <h1 className="text-3xl sm:text-4xl font-bold tracking-tight text-slate-900">
          {tHub("title")}
        </h1>
        <p className="text-base sm:text-lg leading-relaxed text-slate-600">
          {tHub("subtitle")}
        </p>
      </header>

      <div className="space-y-10">
        {GROUPS.map((group) => (
          <section key={group.titleKey} className="space-y-4">
            <div className="flex items-baseline gap-3">
              <h2 className="text-lg font-semibold tracking-tight text-slate-900">
                {tNav(group.titleKey)}
              </h2>
              <span className="text-sm text-slate-500">{tNav(group.hintKey)}</span>
            </div>
            <ul className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
              {group.tools.map((tool) => {
                const Icon = tool.icon;
                return (
                  <li key={tool.href}>
                    <Link
                      href={`/${locale}${tool.href}`}
                      className="group flex h-full items-start gap-3 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm transition-colors hover:border-blue-300 hover:bg-blue-50/40"
                    >
                      <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-slate-100 text-slate-600 transition-colors group-hover:bg-blue-100 group-hover:text-blue-700">
                        <Icon className="h-5 w-5" aria-hidden strokeWidth={1.9} />
                      </span>
                      <span className="min-w-0 flex-1">
                        <span className="block text-sm font-semibold text-slate-900 group-hover:text-blue-800">
                          {tNav(tool.labelKey)}
                        </span>
                        <span className="mt-0.5 block text-xs leading-relaxed text-slate-500">
                          {tool.hint}
                        </span>
                      </span>
                    </Link>
                  </li>
                );
              })}
            </ul>
          </section>
        ))}
      </div>
    </div>
  );
}
