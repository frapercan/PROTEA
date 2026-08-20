// What is running, on a page otherwise about what was finished.
//
// The front door argues from a sealed board whose figures are withdrawn
// and whose window closed in March. A reader reaching the end of it has
// no way to tell whether this is a finished thesis or a live instrument,
// and the honest answer is the second: 432 arms were measured overnight
// and a rung is open now.
//
// It sits after the board deliberately. The board is what was settled;
// this is what is not. Putting it first would lead with an unfinished
// thing, and putting it in the footer would bury the only part of the
// page that changes.
//
// The logic is the instrument's own: `currentRung`, `rungProgress`,
// `progressLabel` and `frameLabel` are the same functions the benchmark
// spine uses, so the two surfaces cannot disagree about where a rung
// stands. Only the presentation is new, because the spine speaks in the
// instrument's chrome and this page speaks in prose.

import { currentRung, frameLabel, progressLabel, rungProgress, type Rung } from "@/lib/rungs";

/**
 * One sentence naming the question, and one naming its progress.
 *
 * Renders nothing when there are no rungs, which is what a failed fetch
 * looks like from here. A front door that cannot read the campaign should
 * say nothing about it rather than show a shape with no content: the
 * absence is honest and a skeleton is not.
 */
export function CampaignNow({ rungs }: { rungs: Rung[] }) {
  const rung = currentRung(rungs);
  if (!rung) return null;

  const progress = rungProgress(rung);
  // Finished, not merely quiet. Both conditions matter: a rung with every
  // arm scored and one still running is not settled, and a rung with
  // nothing running and arms unscored is stalled rather than done.
  const settled = !progress.live && progress.scored === progress.total;
  const frame = frameLabel(rung);
  // The rung before this one, if it finished, is what the open question
  // rests on. Naming it stops the current rung reading as the whole work.
  const previous = rungs
    .filter((r) => Number(r.rung) < Number(rung.rung))
    .sort((a, b) => Number(b.rung) - Number(a.rung))[0];

  return (
    <section
      aria-labelledby="campaign-now-heading"
      className="mt-14 border-t border-[var(--border)] pt-10"
    >
      <h2
        id="campaign-now-heading"
        className="protea-eyebrow text-[12px] uppercase tracking-wide text-[var(--primary)]"
      >
        {settled ? "Last question answered" : "Open question"}
      </h2>

      <p className="mt-5 font-serif text-[17px] leading-relaxed text-[var(--foreground)]">
        {/*
          A rung whose arms are all scored with none running is finished,
          and calling it open would be the same class of claim this page
          spent a day removing. The heading and the verb both follow the
          numbers rather than assuming there is always something running.
        */}
        The board above is sealed. The work is not: rung {rung.rung}{" "}
        {settled ? "asked" : "is open, and asks"} {rung.question}.
      </p>

      <p className="mt-3 text-[14px] leading-relaxed text-[var(--muted)]">
        {progressLabel(progress)}
        {progress.live ? ", and some are still computing" : ""}.
        {frame ? ` Scored over ${frame}.` : ""}
        {rung.failed > 0
          ? ` ${rung.failed} failed, which is shown rather than dropped.`
          : ""}
      </p>

      {previous ? (
        <p className="mt-3 text-[14px] leading-relaxed text-[var(--muted)]">
          {/*
            Two steps that lag independently, so a rung whose arms are all
            computed can still have nothing to say. Reporting the scored
            count rather than the computed one keeps that distinction on
            the page.
          */}
          It rests on rung {previous.rung}, which{" "}
          {rungProgress(previous).scored === rungProgress(previous).total
            ? `settled its ${previous.arms} arms`
            : `reached ${progressLabel(rungProgress(previous))}`}
          .
        </p>
      ) : null}
    </section>
  );
}
