"use client";

import { useEffect, useState } from "react";
import { useTranslations } from "next-intl";
import { createApiKey, type ApiKeyRole, type CreateApiKeyResponse } from "@/lib/api";
import { HelpDot } from "@/components/Tooltip";
import { useFocusTrap } from "@/lib/useFocusTrap";

/**
 * Modal that mints a new API key via ``POST /v1/auth/api-keys``.
 *
 * Two-phase flow:
 *
 * 1. Form phase: caller picks a label, a role (viewer/operator/admin)
 *    and an optional expiry timestamp. Submit dispatches the request.
 * 2. Reveal phase: the dialog swaps to a one-shot view of the raw key
 *    in a copyable monospace block with a red warning that the value
 *    cannot be retrieved again. Closing the dialog here is final;
 *    the form cannot be re-entered without minting another key.
 *
 * The two-phase split mirrors how secrets dashboards on common SaaS
 * platforms behave (GitHub PAT, Linear, etc.): hard separation makes
 * it impossible to dismiss the reveal screen by mistake while the
 * key is still scrolled off the page.
 *
 * REUSE: visual shell (overlay, backdrop close, header + footer,
 * field grid, Toggle-ish controls) is copied from ``NewDatasetDialog``
 * verbatim where applicable; deviations are explicitly the reveal
 * phase and the role chip group.
 */

type Props = {
  open: boolean;
  onClose: () => void;
  onMinted?: (summary: { id: string; name: string; role: ApiKeyRole | null }) => void;
};

const ROLE_OPTIONS: { value: ApiKeyRole; labelKey: string; helpKey: string }[] = [
  { value: "viewer", labelKey: "roleViewer", helpKey: "roleViewerHelp" },
  { value: "operator", labelKey: "roleOperator", helpKey: "roleOperatorHelp" },
  { value: "admin", labelKey: "roleAdmin", helpKey: "roleAdminHelp" },
];

export function NewApiKeyDialog({ open, onClose, onMinted }: Props) {
  const t = useTranslations("apiKeys.dialog");
  const [name, setName] = useState("");
  const [role, setRole] = useState<ApiKeyRole>("viewer");
  const [expiresAt, setExpiresAt] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [minted, setMinted] = useState<CreateApiKeyResponse | null>(null);
  const [copied, setCopied] = useState(false);

  // Reset the dialog every time it reopens so a stale failure or a
  // previously revealed key doesn't greet the next caller.
  useEffect(() => {
    if (!open) return;
    setError(null);
    setMinted(null);
    setCopied(false);
    setName("");
    setRole("viewer");
    setExpiresAt("");
  }, [open]);

  const safeClose = () => {
    if (loading) return;
    onClose();
  };

  const dialogRef = useFocusTrap<HTMLDivElement>(open, safeClose);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    if (!name.trim()) {
      setError(t("errors.nameRequired"));
      return;
    }
    setLoading(true);
    try {
      const res = await createApiKey({
        name: name.trim(),
        role,
        expires_at: expiresAt ? new Date(expiresAt).toISOString() : null,
      });
      setMinted(res);
      onMinted?.({ id: res.id, name: res.name, role: (res.role ?? role) as ApiKeyRole });
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    } finally {
      setLoading(false);
    }
  }

  function copyKey() {
    if (!minted?.key) return;
    void navigator.clipboard
      .writeText(minted.key)
      .then(() => {
        setCopied(true);
        setTimeout(() => setCopied(false), 2500);
      })
      .catch(() => setCopied(false));
  }

  if (!open) return null;

  const inputClass =
    "w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500";
  const labelClass = "block text-xs font-medium uppercase tracking-wider text-slate-500 mb-1";

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onClick={safeClose}
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby="new-apikey-title"
        className="w-full max-w-lg rounded-xl border bg-white shadow-xl flex flex-col max-h-[92vh]"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b px-5 py-4">
          <div>
            <h2 id="new-apikey-title" className="text-base font-semibold text-slate-900">
              {minted ? t("revealTitle") : t("title")}
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              {minted ? t("revealSubtitle") : t("subtitle")}
            </p>
          </div>
          <button
            onClick={safeClose}
            aria-label={t("close")}
            disabled={loading}
            className="text-slate-500 hover:text-slate-900 text-xl leading-none disabled:opacity-40"
          >
            ×
          </button>
        </div>

        {!minted ? (
          <form onSubmit={handleSubmit} className="flex-1 overflow-y-auto p-5 space-y-5">
            {/* Name */}
            <div>
              <label className={labelClass} htmlFor="ak-name">
                {t("nameLabel")}<span className="text-red-500 ml-0.5">*</span>
              </label>
              <input
                id="ak-name"
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder={t("namePlaceholder")}
                required
                autoFocus
                className={`${inputClass} font-mono text-[13px]`}
              />
              <p className="mt-1 text-[11px] text-slate-500">{t("nameHelp")}</p>
            </div>

            {/* Role */}
            <div>
              <label className={labelClass}>
                {t("roleLabel")}
                <HelpDot text={t("roleHelp")} />
              </label>
              <div role="radiogroup" aria-label={t("roleLabel")} className="grid grid-cols-1 gap-1.5">
                {ROLE_OPTIONS.map((opt) => {
                  const active = role === opt.value;
                  // Role-dot palette mirrors the role-chip palette on the
                  // /admin/api-keys page (amber=admin, blue=operator,
                  // stone=viewer) so a viewer can correlate the picker with
                  // the badge they will see in the list afterwards.
                  const dotClass =
                    opt.value === "admin"
                      ? "bg-amber-500"
                      : opt.value === "operator"
                        ? "bg-blue-500"
                        : "bg-stone-400";
                  return (
                    <button
                      key={opt.value}
                      type="button"
                      role="radio"
                      aria-checked={active}
                      onClick={() => setRole(opt.value)}
                      className={`text-left rounded-lg border px-3 py-2 transition-colors ${
                        active
                          ? "border-blue-500 bg-blue-50/70 ring-1 ring-blue-200"
                          : "border-stone-200 bg-white hover:border-stone-300 hover:bg-stone-50"
                      }`}
                    >
                      <div className="flex items-center gap-2">
                        <span aria-hidden className={`h-2 w-2 rounded-full ${dotClass}`} />
                        <span className="text-sm font-semibold text-stone-900">{t(opt.labelKey)}</span>
                      </div>
                      <p className="mt-0.5 text-[11px] leading-snug text-stone-500">{t(opt.helpKey)}</p>
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Expiry */}
            <div>
              <label className={labelClass} htmlFor="ak-expiry">
                {t("expiryLabel")}
                <HelpDot text={t("expiryHelp")} />
              </label>
              <input
                id="ak-expiry"
                type="datetime-local"
                value={expiresAt}
                onChange={(e) => setExpiresAt(e.target.value)}
                className={inputClass}
              />
              <p className="mt-1 text-[11px] text-slate-500">{t("expiryFootnote")}</p>
            </div>

            {error && (
              <pre className="whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
                {error}
              </pre>
            )}

            <div className="flex justify-end gap-2 border-t border-slate-100 pt-4">
              <button
                type="button"
                onClick={safeClose}
                disabled={loading}
                className="rounded-md border border-slate-300 px-4 py-2 text-sm hover:bg-slate-50 disabled:opacity-50"
              >
                {t("cancel")}
              </button>
              <button
                type="submit"
                disabled={loading}
                className="rounded-md bg-blue-700 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-800 disabled:opacity-50"
              >
                {loading ? t("minting") : t("mint")}
              </button>
            </div>
          </form>
        ) : (
          <div className="flex-1 overflow-y-auto p-5 space-y-4">
            {/* Reveal phase. The warning is intentionally first + red so
                the user notices it before they reach for the close x. */}
            <div
              role="alert"
              className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800"
            >
              <p className="font-semibold">{t("warningTitle")}</p>
              <p className="mt-1 text-[13px] leading-snug">{t("warningBody")}</p>
            </div>

            <div>
              <label className={labelClass}>{t("keyLabel")}</label>
              <div className="flex items-stretch gap-2">
                <code className="flex-1 break-all rounded-md border border-slate-300 bg-slate-50 px-3 py-2 font-mono text-[12px] text-slate-900 select-all">
                  {minted.key}
                </code>
                <button
                  type="button"
                  onClick={copyKey}
                  aria-label={t("copy")}
                  className="shrink-0 rounded-md border border-slate-300 bg-white px-3 py-2 text-xs font-semibold text-slate-700 hover:bg-slate-50"
                >
                  {copied ? t("copied") : t("copy")}
                </button>
              </div>
            </div>

            <dl className="grid grid-cols-2 gap-x-4 gap-y-1.5 text-xs">
              <dt className="text-slate-500">{t("nameLabel")}</dt>
              <dd className="font-mono text-slate-800 break-all">{minted.name}</dd>
              <dt className="text-slate-500">{t("prefixLabel")}</dt>
              <dd className="font-mono text-slate-800">{minted.prefix}</dd>
              <dt className="text-slate-500">{t("roleLabel")}</dt>
              <dd className="text-slate-800">{minted.role ?? role}</dd>
            </dl>

            <div className="flex justify-end gap-2 border-t border-slate-100 pt-4">
              <button
                type="button"
                onClick={onClose}
                className="rounded-md bg-blue-700 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-800"
              >
                {t("done")}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
