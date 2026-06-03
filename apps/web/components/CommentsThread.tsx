"use client";

import { useCallback, useEffect, useState } from "react";
import { useTranslations } from "next-intl";
import { createJobComment, listJobComments, type JobComment } from "@/lib/api";
import { Skeleton } from "@/components/Skeleton";
import { useToast } from "@/components/Toast";

/**
 * Curator / operator comment thread attached to a Job (D11 narrative
 * surface). Distinct from the JobEvent timeline: events are
 * machine-emitted, comments are human notes (post-mortems, follow-ups,
 * "re-running with k=10 because k=5 hit variance ceiling on PK").
 *
 * Backend: ``GET /jobs/{id}/comments`` lists oldest-first;
 * ``POST /jobs/{id}/comments`` accepts ``{body, author?}`` and the
 * server stamps ``created_at``. Both endpoints already gate writes on
 * the configured auth mode; we surface a friendly error if the API
 * refuses (e.g. anonymous on an auth-required deployment).
 *
 * Author defaults to ``localStorage["protea.author"]`` when present so
 * a single operator's notes stay attributable across sessions without
 * a full login flow. The field is intentionally free-form to mirror
 * the backend contract (bot ids, cron names, GitHub handles).
 */

type Props = {
  jobId: string;
};

const AUTHOR_LOCAL_KEY = "protea.author";

function readDefaultAuthor(): string {
  if (typeof window === "undefined") return "";
  try {
    return window.localStorage.getItem(AUTHOR_LOCAL_KEY) ?? "";
  } catch {
    return "";
  }
}

function persistAuthor(author: string) {
  if (typeof window === "undefined") return;
  try {
    if (author) window.localStorage.setItem(AUTHOR_LOCAL_KEY, author);
    else window.localStorage.removeItem(AUTHOR_LOCAL_KEY);
  } catch {
    /* ignore (private mode etc.) */
  }
}

function formatStamp(iso: string): string {
  try {
    return new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "short" });
  } catch {
    return iso;
  }
}

export function CommentsThread({ jobId }: Props) {
  const t = useTranslations("jobs.commentsThread");
  const toast = useToast();
  const [comments, setComments] = useState<JobComment[] | null>(null);
  const [body, setBody] = useState("");
  const [author, setAuthor] = useState("");
  const [posting, setPosting] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [postError, setPostError] = useState<string | null>(null);

  // Seed author from localStorage on mount so a single operator's
  // notes stay attributable across sessions.
  useEffect(() => {
    setAuthor(readDefaultAuthor());
  }, []);

  const refresh = useCallback(async () => {
    setLoadError(null);
    try {
      const rows = await listJobComments(jobId, { limit: 500 });
      setComments(rows);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setLoadError(msg);
      setComments([]);
    }
  }, [jobId]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  async function handlePost(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = body.trim();
    if (!trimmed) return;
    setPosting(true);
    setPostError(null);
    try {
      const created = await createJobComment(jobId, {
        body: trimmed,
        author: author.trim() || null,
      });
      setComments((prev) => (prev ? [...prev, created] : [created]));
      setBody("");
      persistAuthor(author.trim());
      toast(t("posted"), "success");
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setPostError(msg);
      toast(msg, "error");
    } finally {
      setPosting(false);
    }
  }

  return (
    <section className="mt-6" aria-labelledby="job-comments-heading">
      <h2 id="job-comments-heading" className="mb-3 text-base font-semibold">
        {t("title")}{" "}
        {comments && (
          <span className="text-xs font-normal text-slate-600">
            ({comments.length})
          </span>
        )}
      </h2>

      <div className="rounded-lg border bg-white shadow-sm">
        {/* Comment list */}
        <div className="divide-y divide-slate-100">
          {comments === null && (
            <div className="p-4 space-y-2">
              <Skeleton className="h-3 w-1/3" />
              <Skeleton className="h-3 w-3/4" />
            </div>
          )}
          {loadError && (
            <pre className="m-3 whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-xs text-red-700">
              {loadError}
            </pre>
          )}
          {comments && comments.length === 0 && !loadError && (
            <p className="px-4 py-6 text-center text-sm text-slate-500">
              {t("empty")}
            </p>
          )}
          {comments &&
            comments.map((c) => (
              <article key={c.id} className="px-4 py-3 text-sm">
                <header className="mb-1 flex items-center gap-2 text-xs text-slate-500">
                  <span className="font-semibold text-slate-700">
                    {c.author?.trim() || t("anonymousAuthor")}
                  </span>
                  <span aria-hidden="true">·</span>
                  <time dateTime={c.created_at}>{formatStamp(c.created_at)}</time>
                </header>
                <p className="whitespace-pre-wrap leading-snug text-slate-800">
                  {c.body}
                </p>
              </article>
            ))}
        </div>

        {/* Compose form */}
        <form
          onSubmit={handlePost}
          className="border-t border-slate-100 bg-slate-50/70 p-4 space-y-2"
          aria-label={t("formLabel")}
        >
          <label className="block">
            <span className="block text-xs font-medium text-slate-600 mb-1">
              {t("bodyLabel")}
            </span>
            <textarea
              value={body}
              onChange={(e) => setBody(e.target.value)}
              rows={3}
              required
              placeholder={t("bodyPlaceholder")}
              className="w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </label>
          <div className="flex flex-wrap items-end gap-2">
            <label className="flex-1 min-w-[160px]">
              <span className="block text-xs font-medium text-slate-600 mb-1">
                {t("authorLabel")}
              </span>
              <input
                type="text"
                value={author}
                onChange={(e) => setAuthor(e.target.value)}
                placeholder={t("authorPlaceholder")}
                className="w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </label>
            <button
              type="submit"
              disabled={posting || !body.trim()}
              className="rounded-md bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
            >
              {posting ? t("posting") : t("postButton")}
            </button>
          </div>
          {postError && (
            <pre className="whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-xs text-red-700">
              {postError}
            </pre>
          )}
          <p className="text-[11px] text-slate-500 leading-snug">
            {t("hint")}
          </p>
        </form>
      </div>
    </section>
  );
}
