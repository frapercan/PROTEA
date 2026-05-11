Ngrok Deploy Recovery
=====================

The public PROTEA endpoint is served via a static ngrok domain
(``https://protea.ngrok.app``) that tunnels to the Next.js frontend on
port 3000. API calls from the browser go through the Next.js reverse
proxy (``/api-proxy/*`` rewrites to ``http://localhost:8000`` defined in
``apps/web/next.config.ts``), so only one tunnel is needed.

The tunnel is started by ``bash scripts/expose.sh`` and runs in the
foreground. If the process dies, the public URL becomes unreachable even
though the local stack is healthy.

A separate concern is the deploy slot: ``scripts/deploy.sh`` manages a
git worktree at ``~/Thesis2/worktrees/protea-deploy`` that hosts the
production-mode stack (frontend pre-built, workers running). If that
worktree does not exist, ``deploy.sh`` exits with an error on every
invocation.

Symptoms
--------

- ``https://protea.ngrok.app`` returns a 502 Bad Gateway or "Tunnel not
  found" page from ngrok.
- A supervisor or external reviewer cannot reach the demo endpoint.
- ``bash scripts/expose.sh`` exits immediately without opening a tunnel
  (local stack not running, or ngrok not authenticated).
- ``bash scripts/deploy.sh --status`` prints
  ``deploy slot ~/Thesis2/worktrees/protea-deploy does not exist`` and exits
  with code 1.

Diagnosis
---------

1. **Confirm the local stack is running:**

   .. code-block:: bash

      curl -sf http://localhost:8000/jobs > /dev/null && echo "API OK"
      curl -sf http://localhost:3000 -o /dev/null && echo "Frontend OK"
      bash scripts/manage.sh status

2. **Check whether the ngrok tunnel process is alive:**

   .. code-block:: bash

      pgrep -fa ngrok

   No output means the tunnel is down.

3. **Test ngrok authentication:**

   .. code-block:: bash

      ngrok config check

   If the config file is missing or the authtoken is absent, authentication
   will fail silently when the tunnel is started.

4. **Check the deploy slot exists** (relevant if using ``deploy.sh``):

   .. code-block:: bash

      ls ~/Thesis2/worktrees/protea-deploy

   If the directory is absent, the slot must be re-created (see Fix step 4).

5. **Inspect ngrok logs** (stdout of ``expose.sh`` or the ngrok agent log):

   .. code-block:: bash

      # If expose.sh output was redirected:
      tail -50 logs/expose.log
      # Or check the ngrok web interface:
      # http://localhost:4040  (ngrok local dashboard)

Fix
---

1. **Restart the tunnel** (most common fix when the process died):

   .. code-block:: bash

      cd ~/Thesis2/worktrees/protea-deploy   # or repositories/PROTEA for dev
      bash scripts/expose.sh

   The script validates the local stack before opening the tunnel. If it
   fails, resolve the stack issue first (step 2).

2. **Restart the local stack** if the API or frontend is down:

   .. code-block:: bash

      # From the deploy slot
      cd ~/Thesis2/worktrees/protea-deploy
      bash scripts/manage.sh stop
      bash scripts/manage.sh start
      # Then re-run expose.sh

3. **Re-authenticate ngrok** if the authtoken is missing or expired:

   .. code-block:: bash

      ngrok config add-authtoken <TOKEN>
      # TOKEN is available at https://dashboard.ngrok.com/get-started/your-authtoken

4. **Re-create the deploy slot** if the worktree does not exist:

   .. code-block:: bash

      git -C ~/Thesis2/repositories/PROTEA fetch --quiet origin
      git -C ~/Thesis2/repositories/PROTEA worktree add \
          ~/Thesis2/worktrees/protea-deploy \
          -b feat/deploy-tooling \
          origin/develop

   Then run a full deploy:

   .. code-block:: bash

      cd ~/Thesis2/repositories/PROTEA
      bash scripts/deploy.sh

   This will install dependencies, build the frontend, start the stack, and
   run the health check. After it completes, open the tunnel:

   .. code-block:: bash

      cd ~/Thesis2/worktrees/protea-deploy
      bash scripts/expose.sh

5. **Run the expose script in the background** (optional, for unattended
   operation):

   .. code-block:: bash

      nohup bash scripts/expose.sh >> logs/expose.log 2>&1 &
      echo $! > logs/pids/expose.pid

6. **Verify the tunnel is live:**

   .. code-block:: bash

      curl -sf https://protea.ngrok.app -o /dev/null && echo "Tunnel OK"

Prevention
----------

- Run ``expose.sh`` inside a process supervisor (e.g. a ``manage.sh``-tracked
  background process or a systemd unit) so it restarts automatically on crash.
- After any ``manage.sh stop / start`` cycle, check the tunnel process is still
  alive: ``pgrep -fa ngrok``.
- The deploy slot worktree (``~/Thesis2/worktrees/protea-deploy``) must be
  created manually once before ``deploy.sh`` can be used. Keep a note that
  it will not be re-created automatically if the directory is deleted.
- Related sources: ``scripts/expose.sh``, ``scripts/deploy.sh``,
  ``apps/web/next.config.ts`` (API proxy rewrite rules).
