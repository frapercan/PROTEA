/**
 * Put the static assets where the standalone server resolves them.
 *
 * `next build` with `output: "standalone"` emits a server at
 * .next/standalone/server.js and does NOT copy .next/static or public/ beside
 * it. A tree without them serves HTML whose stylesheet and chunk URLs all 404,
 * so the whole application renders as unstyled markup while every process is up
 * and every health check passes.
 *
 * This has happened twice. On 2026-08-20 a rebuild replaced the standalone
 * directory under a running server and one mismatched chunk stopped hydration,
 * which is why scripts/deploy-check.sh exists. It happened again the moment
 * someone ran `npm run build` directly instead of going through manage.sh,
 * which is the only place the copy lived.
 *
 * So the copy runs from `postbuild` instead: npm invokes it after every build,
 * and there is no longer a way to produce a standalone tree without it.
 */
import { cp, mkdir, access } from "node:fs/promises";
import { constants } from "node:fs";

const STANDALONE = ".next/standalone";

async function exists(path) {
  try {
    await access(path, constants.F_OK);
    return true;
  } catch {
    return false;
  }
}

if (!(await exists(STANDALONE))) {
  process.exit(0);
}

await mkdir(`${STANDALONE}/.next`, { recursive: true });
await cp(".next/static", `${STANDALONE}/.next/static`, { recursive: true });

if (await exists("public")) {
  await mkdir(`${STANDALONE}/public`, { recursive: true });
  await cp("public", `${STANDALONE}/public`, { recursive: true });
}

// Assert rather than announce: a copy that silently placed nothing looks
// exactly like one that worked, and that is the failure this file prevents.
if (!(await exists(`${STANDALONE}/.next/static`))) {
  console.error("standalone tree has no .next/static after the copy");
  process.exit(1);
}
console.log("standalone assets in place");
