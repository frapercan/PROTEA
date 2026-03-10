"use client";

import { useEffect, useRef, useState } from "react";
import { createQuerySet, deleteQuerySet, listQuerySets, QuerySet } from "@/lib/api";
import { SkeletonTableRow } from "@/components/Skeleton";
import { useToast } from "@/components/Toast";

function formatDate(iso?: string | null) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "medium" });
}

export default function QuerySetsPage() {
  const toast = useToast();
  const [sets, setSets] = useState<QuerySet[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  // Upload modal state
  const [showModal, setShowModal] = useState(false);
  const [uploadName, setUploadName] = useState("");
  const [uploadDescription, setUploadDescription] = useState("");
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState("");
  const [dragOver, setDragOver] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);

  // Expanded row
  const [expandedId, setExpandedId] = useState<string | null>(null);

  async function load() {
    setLoading(true);
    setError("");
    try {
      setSets(await listQuerySets());
    } catch (e: any) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { load(); }, []);

  function openModal() {
    setUploadName("");
    setUploadDescription("");
    setUploadFile(null);
    setUploadError("");
    if (fileRef.current) fileRef.current.value = "";
    setShowModal(true);
  }

  async function handleUpload(e: React.FormEvent) {
    e.preventDefault();
    if (!uploadFile) { setUploadError("Select a FASTA file."); return; }
    if (!uploadName.trim()) { setUploadError("Name is required."); return; }
    setUploadError("");
    setUploading(true);
    try {
      const created = await createQuerySet(uploadFile, uploadName.trim(), uploadDescription.trim() || undefined);
      setSets((prev) => [created, ...prev]);
      setShowModal(false);
      toast(`Query set "${created.name}" uploaded — ${created.entry_count} sequences`, "success");
    } catch (err: any) {
      setUploadError(String(err));
    } finally {
      setUploading(false);
    }
  }

  async function handleDelete(id: string, name: string) {
    if (!confirm(`Delete query set "${name}"? This cannot be undone.`)) return;
    try {
      await deleteQuerySet(id);
      setSets((prev) => prev.filter((s) => s.id !== id));
      toast(`Deleted "${name}"`, "info");
    } catch (err: any) {
      setError(String(err));
      toast(String(err), "error");
    }
  }

  function handleDrop(e: React.DragEvent) {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer.files[0];
    if (file) {
      setUploadFile(file);
      if (!uploadName) setUploadName(file.name.replace(/\.(fasta|fa|faa|txt)$/i, ""));
    }
  }

  const inputClass = "w-full rounded-md border px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500";
  const labelClass = "block text-sm font-medium text-gray-700 mb-1";

  return (
    <>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-xl font-semibold">Query Sets</h1>
        <button
          onClick={openModal}
          className="rounded-md bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700"
        >
          + Upload FASTA
        </button>
      </div>

      {error && (
        <pre className="mb-4 whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {error}
        </pre>
      )}

      {/* List */}
      <div className="overflow-hidden rounded-lg border bg-white shadow-sm">
        <div className="grid grid-cols-[1fr_100px_160px_80px] gap-2 border-b bg-gray-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-gray-500">
          <div>Name</div>
          <div>Sequences</div>
          <div>Created</div>
          <div></div>
        </div>

        {loading && Array.from({ length: 3 }).map((_, i) => (
          <SkeletonTableRow key={i} cols={4} />
        ))}

        {!loading && sets.length === 0 && (
          <div className="px-4 py-10 text-center">
            <p className="text-sm text-gray-400 mb-2">No query sets yet.</p>
            <button
              onClick={openModal}
              className="text-sm text-blue-600 underline"
            >
              Upload a FASTA file to get started
            </button>
          </div>
        )}

        {sets.map((qs) => (
          <div key={qs.id} className="border-b last:border-0">
            <div
              className="grid grid-cols-[1fr_100px_160px_80px] gap-2 px-4 py-3 text-sm items-center hover:bg-blue-50 cursor-pointer transition-colors"
              onClick={() => setExpandedId(expandedId === qs.id ? null : qs.id)}
            >
              <div>
                <span className="font-medium text-gray-900">{qs.name}</span>
                {qs.description && (
                  <span className="ml-2 text-xs text-gray-400">{qs.description}</span>
                )}
                <div className="font-mono text-xs text-gray-300 mt-0.5">{qs.id}</div>
              </div>
              <div className="text-gray-700">{qs.entry_count}</div>
              <div className="text-xs text-gray-400">{formatDate(qs.created_at)}</div>
              <div className="flex justify-end">
                <button
                  onClick={(e) => { e.stopPropagation(); handleDelete(qs.id, qs.name); }}
                  className="rounded border border-red-200 px-2 py-1 text-xs text-red-600 hover:bg-red-50 transition-colors"
                >
                  Delete
                </button>
              </div>
            </div>

            {expandedId === qs.id && qs.entries && (
              <div className="border-t bg-gray-50 px-6 py-3">
                <p className="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">
                  Entries ({qs.entries.length})
                </p>
                <div className="max-h-48 overflow-y-auto space-y-1">
                  {qs.entries.map((entry) => (
                    <div key={entry.accession} className="flex gap-4 text-xs text-gray-600 font-mono">
                      <span className="text-gray-900">{entry.accession}</span>
                      <span className="text-gray-400">seq_id={entry.sequence_id}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Upload Modal */}
      {showModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
          <div className="w-full max-w-md rounded-xl border bg-white shadow-xl flex flex-col max-h-[90vh]">
            <div className="flex items-center justify-between border-b px-5 py-4">
              <h2 className="text-base font-semibold">Upload FASTA</h2>
              <button
                onClick={() => setShowModal(false)}
                className="text-gray-400 hover:text-gray-600 text-xl leading-none"
              >
                ×
              </button>
            </div>

            <form onSubmit={handleUpload} className="flex-1 overflow-y-auto p-5 space-y-4">
              <div>
                <label className={labelClass}>
                  Name <span className="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  value={uploadName}
                  onChange={(e) => setUploadName(e.target.value)}
                  placeholder="e.g. human_novel_proteins"
                  required
                  className={inputClass}
                />
              </div>

              <div>
                <label className={labelClass}>Description (optional)</label>
                <input
                  type="text"
                  value={uploadDescription}
                  onChange={(e) => setUploadDescription(e.target.value)}
                  placeholder="Short description"
                  className={inputClass}
                />
              </div>

              <div>
                <label className={labelClass}>
                  FASTA file <span className="text-red-500">*</span>
                </label>

                {/* Drag & drop zone */}
                <div
                  onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
                  onDragLeave={() => setDragOver(false)}
                  onDrop={handleDrop}
                  onClick={() => fileRef.current?.click()}
                  className={`flex flex-col items-center justify-center gap-2 rounded-lg border-2 border-dashed px-4 py-6 cursor-pointer transition-colors text-sm ${
                    dragOver
                      ? "border-blue-400 bg-blue-50 text-blue-700"
                      : uploadFile
                      ? "border-green-300 bg-green-50 text-green-700"
                      : "border-gray-200 bg-gray-50 text-gray-500 hover:border-gray-300 hover:bg-gray-100"
                  }`}
                >
                  {uploadFile ? (
                    <>
                      <span className="text-lg">✓</span>
                      <span className="font-medium">{uploadFile.name}</span>
                      <span className="text-xs opacity-70">
                        {(uploadFile.size / 1024).toFixed(1)} KB — click to change
                      </span>
                    </>
                  ) : (
                    <>
                      <span className="text-2xl">↑</span>
                      <span>Drop FASTA here or <span className="underline">browse</span></span>
                      <span className="text-xs opacity-60">.fasta · .fa · .faa · .txt</span>
                    </>
                  )}
                </div>
                <input
                  ref={fileRef}
                  type="file"
                  accept=".fasta,.fa,.faa,.txt"
                  onChange={(e) => {
                    const f = e.target.files?.[0] ?? null;
                    setUploadFile(f);
                    if (f && !uploadName) setUploadName(f.name.replace(/\.(fasta|fa|faa|txt)$/i, ""));
                  }}
                  className="hidden"
                />
              </div>

              {uploadError && (
                <pre className="whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
                  {uploadError}
                </pre>
              )}

              <div className="flex justify-end gap-2 pt-1">
                <button
                  type="button"
                  onClick={() => setShowModal(false)}
                  className="rounded-md border px-4 py-2 text-sm hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={uploading}
                  className="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
                >
                  {uploading ? "Uploading…" : "Upload"}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </>
  );
}
