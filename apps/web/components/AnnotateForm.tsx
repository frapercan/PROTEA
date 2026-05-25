"use client";

import { useState, useRef, useCallback, useEffect } from "react";
import { useRouter } from "next/navigation";
import { useTranslations } from "next-intl";
import {
  annotateProteins,
  getJob,
  launchPredictGoTerms,
  listJobs,
  listPredictionSets,
  type AnnotateResult,
  type Job,
} from "@/lib/api";

type Stage = "idle" | "uploading" | "embedding" | "predicting" | "done" | "error";

const POLL_MS = 3_000;
const QUEUE_POLL_MS = 30_000;

// Operations that occupy the shared GPU pipeline. While any of these is
// queued or running we block new user annotation requests, since they won't
// actually enter the queue in a reasonable time frame.
const BLOCKING_OPERATIONS = new Set([
  "compute_embeddings",
  "compute_embeddings_batch",
  "predict_go_terms",
  "predict_go_terms_batch",
]);

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

  // Queue-awareness: poll active jobs and block submission while any
  // embedding/prediction operation is queued or running, because our
  // single-GPU setup can't absorb another request in reasonable time.
  const [blockingJobs, setBlockingJobs] = useState<Job[] | null>(null);

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
          if (job.progress_total && job.progress_current) {
            const pct = Math.round((job.progress_current / job.progress_total) * 100);
            setProgress(`${pct}%`);
          }
          if (job.status === "succeeded") return "succeeded";
          if (job.status === "failed" || job.status === "cancelled") return "failed";
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
      setProgress("0%");
      const embedResult = await pollJob(result.embedding_job_id);
      if (embedResult === "failed") {
        throw new Error("Embedding computation failed");
      }

      // Step 3: Launch prediction
      setStage("predicting");
      setProgress("0%");
      const predictJob = await launchPredictGoTerms(result.predict_payload as Parameters<typeof launchPredictGoTerms>[0]);

      // Step 4: Poll prediction job
      const predictResult = await pollJob(predictJob.id);
      if (predictResult === "failed") {
        throw new Error("Prediction failed");
      }

      // Step 5: Find the prediction set created for this query_set
      const sets = await listPredictionSets();
      const match = sets.find(
        (s) =>
          (s as any).query_set_id === result.query_set_id &&
          s.embedding_config_id === result.embedding_config_id,
      );
      if (match) {
        setPredictionSetId(match.id);
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

  // Auto-redirect when done
  useEffect(() => {
    if (stage === "done" && predictionSetId) {
      const timer = setTimeout(() => {
        const qs = rerankerId ? `?reranker_id=${rerankerId}` : "";
        router.push(`/functional-annotation/${predictionSetId}${qs}`);
      }, 1500);
      return () => clearTimeout(timer);
    }
  }, [stage, predictionSetId, rerankerId, router]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      abortRef.current = true;
    };
  }, []);

  // Poll for active embedding/prediction jobs to know whether the GPU
  // pipeline is currently saturated.
  useEffect(() => {
    let cancelled = false;
    const fetchBlocking = async () => {
      if (typeof document !== "undefined" && document.visibilityState === "hidden") return;
      try {
        const [queued, running] = await Promise.all([
          listJobs({ limit: 100, status: "queued" }),
          listJobs({ limit: 100, status: "running" }),
        ]);
        if (cancelled) return;
        const merged = [...running, ...queued].filter((j) =>
          BLOCKING_OPERATIONS.has(j.operation),
        );
        setBlockingJobs(merged);
      } catch {
        // ignore transient errors; keep prior state
      }
    };
    fetchBlocking();
    const id = setInterval(fetchBlocking, QUEUE_POLL_MS);
    const onVisibility = () => {
      if (document.visibilityState === "visible") fetchBlocking();
    };
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      cancelled = true;
      clearInterval(id);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, []);

  const isRunning = stage === "uploading" || stage === "embedding" || stage === "predicting";
  // A running local annotation flow already owns the UI; don't double-block.
  const isQueueBlocked = !isRunning && (blockingJobs?.length ?? 0) > 0;
  const runningJob = blockingJobs?.find((j) => j.status === "running") ?? null;
  const runningPct =
    runningJob && runningJob.progress_total && runningJob.progress_current
      ? Math.round((runningJob.progress_current / runningJob.progress_total) * 100)
      : null;
  const queuedCount =
    blockingJobs?.filter((j) => j.status === "queued").length ?? 0;

  return (
    <section className="rounded-2xl border-2 border-blue-100 bg-gradient-to-b from-blue-50/60 to-white p-6 sm:p-8">
      <h2 className="text-xl sm:text-2xl font-bold text-slate-900 mb-1">
        {t("annotateTitle" as any)}
      </h2>
      <p className="text-sm text-slate-500 mb-5">
        {t("annotateDescription" as any)}
      </p>

      {/* Queue-busy banner ─ blocks submission while the GPU pipeline is saturated */}
      {isQueueBlocked && (
        <div
          role="status"
          className="mb-5 rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-900"
        >
          <div className="flex items-start gap-2">
            <span aria-hidden className="text-base leading-none">⏳</span>
            <div className="flex-1">
              <p className="font-semibold">
                {t("annotateQueueBlockedTitle" as any)}
              </p>
              <p className="mt-1 text-amber-800">
                {t("annotateQueueBlockedBody" as any)}
              </p>
              <ul className="mt-2 space-y-0.5 text-xs text-amber-800">
                {runningJob && (
                  <li>
                    <span className="font-mono">{runningJob.operation}</span>
                    {" — "}
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
            </div>
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
      <div className="mt-4 flex items-center gap-4">
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
