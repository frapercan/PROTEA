"use client";

import { useState, useRef, useCallback, useEffect } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { useLocale, useTranslations } from "next-intl";
import {
  annotateProteins,
  getGpuAvailability,
  getJob,
  launchPredictGoTerms,
  resolvePredictionSet,
  type AnnotateResult,
  type GpuAvailability,
} from "@/lib/api";

type Stage = "idle" | "uploading" | "embedding" | "predicting" | "done" | "error";

const POLL_MS = 3_000;
const QUEUE_POLL_MS = 30_000;

const EXAMPLE_FASTA = `>sp|P01116|RASK_HUMAN GTPase KRas OS=Homo sapiens OX=9606 GN=KRAS PE=1 SV=1
MTEYKLVVVGAGGVGKSALTIQLIQNHFVDEYDPTIEDSYRKQVVIDGETCLLDILDTAG
QEEYSAMRDQYMRTGEGFLCVFAINNTKSFEDIHHYREQIKRVKDSEDVPMVLVGNKCDL
PSRTVDTKQAQDLARSYGIPFIETSAKTRQRVEDAFYTLVREIRQYRLKKISKEEKTPGC
VKIKKCIIM
>sp|P04637|P53_HUMAN Cellular tumor antigen p53 OS=Homo sapiens OX=9606 GN=TP53 PE=1 SV=4
MEEPQSDPSVEPPLSQETFSDLWKLLPENNVLSPLPSQAMDDLMLSPDDIEQWFTEDPGP
DEAPRMPEAAPPVAPAPAAPTPAAPAPAPSWPLSSSVPSQKTYQGSYGFRLGFLHSGTAK
SVTCTYSPALNKMFCQLAKTCPVQLWVDSTPPPGTRVRAMAIYKQSQHMTEVVRRCPHHE
RCSDSDGLAPPQHLIRVEGNLRVEYLDDRNTFRHSVVVPYEPPEVGSDCTTIHYNYMCNS
SCMGGMNRRPILTIITLEDSSGNLLGRNSFEVRVCACPGRDRRTEEENLRKKGEPHHELP
PGSTKRALPNNTSSSPQPKKKPLDGEYFTLQIRGRERFEMFRELNEALELKDAQAGKEPG
GSRAHSSHLKSKKGQSTSRHKKLMFKTEGPDSD`;

export function AnnotateForm() {
  const t = useTranslations("home");
  const locale = useLocale();
  const router = useRouter();

  const [fasta, setFasta] = useState("");
  const [stage, setStage] = useState<Stage>("idle");
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState<string>("");
  const [predictionSetId, setPredictionSetId] = useState<string | null>(null);
  const [rerankerId, setRerankerId] = useState<string | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);
  const abortRef = useRef(false);

  // Drag-and-drop state
  const [dragOver, setDragOver] = useState(false);

  // Queue-awareness: poll the backend GPU-availability signal and block
  // submission only while real GPU work is in flight. The backend gates
  // `busy` on freshly-leased running jobs + genuinely queued work, so
  // stale/zombie rows (dead worker, no RMQ message) never falsely block
  // the form (FIX-ANNOTATE-BANNER-ACCURACY).
  const [gpu, setGpu] = useState<GpuAvailability | null>(null);
  // Whether the user has opened the technical-details disclosure of the
  // queue-blocked banner. Default closed so first-time visitors do not
  // see opaque operation names.
  const [showQueueDetails, setShowQueueDetails] = useState(false);

  const handleFile = (file: File) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      const text = e.target?.result;
      if (typeof text === "string") setFasta(text);
    };
    reader.readAsText(file);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer.files?.[0];
    if (file) handleFile(file);
  };

  const pollJob = useCallback(
    async (jobId: string): Promise<"succeeded" | "failed"> => {
      while (!abortRef.current) {
        try {
          const job = await getJob(jobId);
          // Only surface a percentage once there is genuine forward motion.
          // Coordinator jobs that finalize in deferred child batches sit at
          // a stale 0/1, so a naive "0%" would freeze on screen and read as
          // stuck. When there's no real percent yet, leave `progress` empty
          // and let the animated stage bar carry the sense of liveness.
          if (job.progress_total && job.progress_current) {
            const pct = Math.round((job.progress_current / job.progress_total) * 100);
            if (pct > 0) setProgress(`${pct}%`);
          }
          const st = String(job.status ?? "").toLowerCase();
          if (st === "succeeded") return "succeeded";
          if (st === "failed" || st === "cancelled") return "failed";
        } catch {
          // transient error, keep polling
        }
        await new Promise((r) => setTimeout(r, POLL_MS));
      }
      return "failed";
    },
    [],
  );

  const handleSubmit = async () => {
    if (!fasta.trim()) return;
    abortRef.current = false;
    setError(null);
    setStage("uploading");
    setProgress("");

    try {
      // Step 1: Upload FASTA + create embedding job
      setProgress(t("annotateUploading" as any));
      const result: AnnotateResult = await annotateProteins({
        fastaText: fasta,
        name: `Annotation ${new Date().toISOString().slice(0, 16)}`,
      });

      // Step 2: Poll embedding job
      setStage("embedding");
      setProgress("");
      const embedResult = await pollJob(result.embedding_job_id);
      if (embedResult === "failed") {
        throw new Error("Embedding computation failed");
      }

      // Step 3: Launch prediction
      setStage("predicting");
      setProgress("");
      const predictJob = await launchPredictGoTerms(result.predict_payload as Parameters<typeof launchPredictGoTerms>[0]);

      // Step 4: Poll prediction job
      const predictResult = await pollJob(predictJob.id);
      if (predictResult === "failed") {
        throw new Error("Prediction failed");
      }

      // Step 5: Resolve the prediction set created for this query_set via a
      // cheap, uncached lookup. The predict job can report SUCCEEDED a moment
      // before the row is queryable, so retry briefly. (The old approach
      // scanned the 5-min-cached listing, which would not yet contain the new
      // set and left the flow stuck on "redirecting" forever.)
      let resolvedId: string | null = null;
      for (let attempt = 0; attempt < 8 && !abortRef.current; attempt++) {
        try {
          const hit = await resolvePredictionSet(
            result.query_set_id,
            result.embedding_config_id,
          );
          if (hit?.id) {
            resolvedId = hit.id;
            break;
          }
        } catch {
          // 404 until the row lands; keep retrying
        }
        await new Promise((r) => setTimeout(r, 1500));
      }
      if (resolvedId) {
        setPredictionSetId(resolvedId);
      }
      if (result.reranker_id) {
        setRerankerId(result.reranker_id);
      }

      setStage("done");
      setProgress("");
    } catch (err: any) {
      setStage("error");
      setError(err?.message ?? "Unknown error");
    }
  };

  // Auto-redirect when done. If the prediction set resolved, go straight to
  // its results; otherwise fall back to the functional-annotation listing so
  // the flow never dead-ends on "redirecting" with nowhere to go.
  useEffect(() => {
    if (stage !== "done") return;
    const timer = setTimeout(() => {
      if (predictionSetId) {
        const qs = rerankerId ? `?reranker_id=${rerankerId}` : "";
        router.push(`/${locale}/functional-annotation/${predictionSetId}${qs}`);
      } else {
        router.push(`/${locale}/functional-annotation`);
      }
    }, 1500);
    return () => clearTimeout(timer);
  }, [stage, predictionSetId, rerankerId, router, locale]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      abortRef.current = true;
    };
  }, []);

  // Poll the truthful GPU-availability signal to know whether the GPU
  // pipeline is genuinely busy (vs. a stale row left behind by a dead
  // worker).
  useEffect(() => {
    let cancelled = false;
    const fetchAvailability = async () => {
      if (typeof document !== "undefined" && document.visibilityState === "hidden") return;
      try {
        const next = await getGpuAvailability();
        if (cancelled) return;
        setGpu(next);
      } catch {
        // ignore transient errors; keep prior state
      }
    };
    fetchAvailability();
    const id = setInterval(fetchAvailability, QUEUE_POLL_MS);
    const onVisibility = () => {
      if (document.visibilityState === "visible") fetchAvailability();
    };
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      cancelled = true;
      clearInterval(id);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, []);

  // Surface CPU-only mode so users understand why a novel sequence is slow.
  // `gpu_present` is undefined on older backends; only warn when explicitly false.
  const cpuOnly = gpu?.gpu_present === false;
  const isRunning = stage === "uploading" || stage === "embedding" || stage === "predicting";
  // A running local annotation flow already owns the UI; don't double-block.
  // Only block on genuinely-active GPU work (backend `busy`), never on
  // stale/zombie rows.
  const isQueueBlocked = !isRunning && (gpu?.busy ?? false);
  const runningOperation = (gpu?.running_fresh ?? 0) > 0 ? gpu?.active_operation ?? null : null;
  const runningPct =
    gpu && gpu.progress_total && gpu.progress_current
      ? Math.round((gpu.progress_current / gpu.progress_total) * 100)
      : null;
  const queuedCount = gpu?.queued ?? 0;

  return (
    <section className="rounded-2xl border-2 border-blue-100 bg-gradient-to-b from-blue-50/60 to-white p-6 sm:p-8">
      <h2 className="text-xl sm:text-2xl font-bold text-slate-900 mb-1">
        {t("annotateTitle" as any)}
      </h2>
      <p className="text-sm text-slate-500 mb-5">
        {t("annotateDescription" as any)}
      </p>

      {/* Queue-busy banner — blocks submission while the GPU pipeline is
          saturated. Designed to leave first-time visitors with somewhere
          to go: friendly explanation, link to the benchmark (existing
          public results), and a collapsed disclosure for the raw queue
          state. */}
      {isQueueBlocked && (
        <div
          role="status"
          className="mb-5 rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-900"
        >
          <div className="flex items-start gap-2">
            <span aria-hidden className="text-base leading-none">⏳</span>
            <div className="flex-1 min-w-0">
              <p className="font-semibold">
                {t("annotateQueueBlockedTitle" as any)}
              </p>
              <p className="mt-1 text-amber-800">
                {t("annotateQueueBlockedFriendly" as any)}
              </p>
              <div className="mt-3 flex flex-wrap items-center gap-3">
                <Link
                  href={`/${locale}/benchmark`}
                  className="inline-flex min-h-[40px] items-center gap-1 rounded-md bg-white px-3 py-2 text-xs font-semibold text-amber-900 ring-1 ring-inset ring-amber-200 hover:bg-amber-100 transition-colors"
                >
                  {t("annotateQueueViewResults" as any)}
                  <span aria-hidden>→</span>
                </Link>
                <button
                  type="button"
                  onClick={() => setShowQueueDetails((v) => !v)}
                  aria-expanded={showQueueDetails}
                  className="inline-flex min-h-[40px] items-center gap-1 rounded-md px-2 py-2 text-xs font-medium text-amber-800 underline hover:text-amber-900 transition-colors"
                >
                  {showQueueDetails
                    ? t("annotateQueueHideDetails" as any)
                    : t("annotateQueueShowDetails" as any)}
                </button>
              </div>
              {showQueueDetails && (
                <ul className="mt-3 space-y-0.5 text-xs text-amber-800 border-t border-amber-200 pt-2">
                  {runningOperation && (
                    <li>
                      <span className="font-mono break-all">{runningOperation}</span>
                      {", "}
                      {t("annotateQueueRunningLabel" as any)}
                      {runningPct != null ? ` (${runningPct}%)` : ""}
                    </li>
                  )}
                  {queuedCount > 0 && (
                    <li>
                      {t("annotateQueueWaitingLabel" as any)}: {queuedCount}
                    </li>
                  )}
                </ul>
              )}
            </div>
          </div>
        </div>
      )}

      {/* CPU-only notice: no CUDA GPU is visible to the workers, so embedding
          a sequence not already in the database runs on CPU and is slower.
          Purely informational, never blocks submission. */}
      {cpuOnly && !isQueueBlocked && (
        <div
          role="status"
          className="mb-5 rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700"
        >
          <div className="flex items-start gap-2">
            <span aria-hidden className="text-base leading-none">🖥️</span>
            <p className="flex-1 min-w-0">{t("annotateCpuModeNotice" as any)}</p>
          </div>
        </div>
      )}

      {/* FASTA input */}
      <div
        className={`relative rounded-lg border-2 transition-colors ${
          dragOver
            ? "border-blue-400 bg-blue-50"
            : "border-slate-200 bg-white"
        }`}
        onDragOver={(e) => {
          e.preventDefault();
          setDragOver(true);
        }}
        onDragLeave={() => setDragOver(false)}
        onDrop={handleDrop}
      >
        <label htmlFor="annotate-fasta-input" className="sr-only">
          {t("annotateInputAriaLabel" as any)}
        </label>
        <textarea
          id="annotate-fasta-input"
          value={fasta}
          onChange={(e) => setFasta(e.target.value)}
          placeholder={t("annotatePlaceholder" as any)}
          aria-label={t("annotateInputAriaLabel" as any)}
          rows={6}
          disabled={isRunning || isQueueBlocked}
          className="w-full rounded-lg p-4 text-xs font-mono text-slate-700 placeholder:text-slate-600 focus:outline-none focus:ring-2 focus:ring-blue-300 resize-y disabled:opacity-50 disabled:cursor-not-allowed bg-transparent"
        />
        {!fasta && !isRunning && !isQueueBlocked && (
          <div className="absolute bottom-2 right-2 flex gap-1">
            <button
              type="button"
              onClick={() => setFasta(EXAMPLE_FASTA)}
              className="inline-flex min-h-[44px] items-center justify-center rounded-md px-3 py-2 text-xs font-medium text-blue-700 hover:text-blue-800 hover:bg-blue-50 underline transition-colors"
            >
              {t("annotateTryExample" as any)}
            </button>
            <button
              type="button"
              onClick={() => fileRef.current?.click()}
              className="inline-flex min-h-[44px] items-center justify-center rounded-md px-3 py-2 text-xs font-medium text-slate-700 hover:text-slate-900 hover:bg-slate-50 underline transition-colors"
            >
              {t("annotateUploadFile" as any)}
            </button>
          </div>
        )}
      </div>
      <input
        ref={fileRef}
        type="file"
        accept=".fasta,.fa,.faa,.txt"
        className="hidden"
        onChange={(e) => {
          const file = e.target.files?.[0];
          if (file) handleFile(file);
        }}
      />

      {/* Action row */}
      <div className="mt-4 flex flex-wrap items-center gap-3 sm:gap-4">
        <button
          onClick={handleSubmit}
          disabled={!fasta.trim() || isRunning || isQueueBlocked}
          title={isQueueBlocked ? t("annotateQueueBlockedTitle" as any) : undefined}
          className="rounded-lg bg-blue-600 px-6 py-2.5 text-sm font-semibold text-white hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {isRunning ? (
            <span className="flex items-center gap-2">
              <svg
                className="animate-spin h-4 w-4"
                viewBox="0 0 24 24"
                fill="none"
              >
                <circle
                  className="opacity-25"
                  cx="12"
                  cy="12"
                  r="10"
                  stroke="currentColor"
                  strokeWidth="4"
                />
                <path
                  className="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
                />
              </svg>
              {stage === "uploading" && t("annotateUploading" as any)}
              {stage === "embedding" && t("annotateEmbedding" as any)}
              {stage === "predicting" && t("annotatePredicting" as any)}
            </span>
          ) : (
            t("annotateButton" as any)
          )}
        </button>

        {/* Inline "Use our example" promo: only when no sequence is loaded
            yet, the local job isn't running, and the queue isn't blocked.
            Gives first-time visitors a one-click path into the demo
            without having to type or upload anything. */}
        {!fasta && !isRunning && !isQueueBlocked && (
          <button
            type="button"
            onClick={() => setFasta(EXAMPLE_FASTA)}
            className="inline-flex min-h-[44px] items-center gap-1.5 rounded-lg px-3 py-2 text-sm font-medium text-blue-700 hover:text-blue-800 hover:bg-blue-50 underline transition-colors"
          >
            {t("annotateUseExample" as any)}
            <span aria-hidden>→</span>
          </button>
        )}

        {isRunning && progress && (
          <span className="text-sm text-slate-500 tabular-nums">{progress}</span>
        )}

        {stage === "done" && (
          <span className="text-sm text-green-600 font-medium">
            {t("annotateDone" as any)}
          </span>
        )}

        {stage === "error" && (
          <span className="text-sm text-red-600">{error}</span>
        )}
      </div>

      {/* Progress bar */}
      {isRunning && (
        <div className="mt-3">
          <div className="flex gap-1">
            {(["uploading", "embedding", "predicting"] as const).map((s) => {
              const active = stage === s;
              const done =
                (s === "uploading" && (stage === "embedding" || stage === "predicting")) ||
                (s === "embedding" && stage === "predicting");
              return (
                <div
                  key={s}
                  className={`h-1.5 flex-1 rounded-full transition-colors ${
                    done
                      ? "bg-blue-500"
                      : active
                        ? "bg-blue-300 animate-pulse"
                        : "bg-slate-200"
                  }`}
                />
              );
            })}
          </div>
          <div className="flex justify-between mt-1 text-[10px] text-slate-600">
            <span>{t("annotateStepUpload" as any)}</span>
            <span>{t("annotateStepEmbed" as any)}</span>
            <span>{t("annotateStepPredict" as any)}</span>
          </div>
        </div>
      )}
    </section>
  );
}
