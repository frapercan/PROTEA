"use client";

import { useState } from "react";
import { createJob } from "@/lib/api";

type OperationDef = {
  label: string;
  queue: string;
  payloadTemplate: Record<string, any>;
};

const OPERATIONS: Record<string, OperationDef> = {
  ping: {
    label: "Ping (smoke test)",
    queue: "protea.ping",
    payloadTemplate: {},
  },
  insert_proteins: {
    label: "Insert Proteins (UniProt FASTA)",
    queue: "protea.jobs",
    payloadTemplate: {
      search_criteria: "organism_id:9606",
      page_size: 500,
      total_limit: null,
      include_isoforms: true,
    },
  },
  fetch_uniprot_metadata: {
    label: "Fetch UniProt Metadata (TSV)",
    queue: "protea.jobs",
    payloadTemplate: {
      search_criteria: "organism_id:9606",
      page_size: 500,
      total_limit: null,
    },
  },
};

type Props = {
  onCreated: (id: string) => void;
  onClose: () => void;
};

export function JobForm({ onCreated, onClose }: Props) {
  const [operation, setOperation] = useState("ping");
  const [payloadText, setPayloadText] = useState(
    JSON.stringify(OPERATIONS.ping.payloadTemplate, null, 2)
  );
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);

  function onOperationChange(op: string) {
    setOperation(op);
    setPayloadText(JSON.stringify(OPERATIONS[op].payloadTemplate, null, 2));
  }

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError("");

    let payload: Record<string, any>;
    try {
      payload = JSON.parse(payloadText || "{}");
    } catch {
      setError("Payload is not valid JSON.");
      return;
    }

    setSubmitting(true);
    try {
      const def = OPERATIONS[operation];
      const res = await createJob({ operation, queue_name: def.queue, payload });
      onCreated(res.id);
    } catch (err: any) {
      setError(String(err));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
      <div className="w-full max-w-lg rounded-xl border bg-white shadow-xl">
        <div className="flex items-center justify-between border-b px-5 py-4">
          <h2 className="text-base font-semibold">New Job</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 text-lg leading-none">✕</button>
        </div>

        <form onSubmit={onSubmit} className="space-y-4 px-5 py-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Operation</label>
            <select
              value={operation}
              onChange={(e) => onOperationChange(e.target.value)}
              className="w-full rounded-md border px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {Object.entries(OPERATIONS).map(([key, def]) => (
                <option key={key} value={key}>{def.label}</option>
              ))}
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Queue: <span className="font-mono text-xs text-gray-500">{OPERATIONS[operation].queue}</span>
            </label>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Payload (JSON)</label>
            <textarea
              value={payloadText}
              onChange={(e) => setPayloadText(e.target.value)}
              rows={8}
              spellCheck={false}
              className="w-full rounded-md border px-3 py-2 font-mono text-xs focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>

          {error && (
            <p className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{error}</p>
          )}

          <div className="flex justify-end gap-2 pt-1">
            <button
              type="button"
              onClick={onClose}
              className="rounded-md border px-4 py-2 text-sm hover:bg-gray-50"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={submitting}
              className="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
            >
              {submitting ? "Submitting…" : "Submit"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
