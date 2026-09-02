# Production notes

What actually breaks automation, and what to do about it. Everything here is
implemented in this repository; the module names point at the code.

---

## 1. At-least-once, at-most-once, and the "exactly-once" that does not exist

Every step in a workflow ends in one of three states from the caller's point of
view: it definitely happened, it definitely did not, or **we do not know**.
That third state is not a gap in the implementation. It is what happens when a
process is killed between "send the request" and "record that we sent it", and
no amount of engineering removes it -- you can only decide which side of it to
land on.

- **At-least-once**: on doubt, do it again. You may duplicate.
- **At-most-once**: on doubt, do not. You may lose.

"Exactly-once delivery" is marketing. What is real is **exactly-once effect**:
at-least-once delivery plus a receiver that recognises a repeat and ignores it.
That is why `flowforge/idempotency.py` is content-addressed. The key is derived
from what the step is about to do, so the same work produces the same key, and
the second attempt is recognisable as a repeat.

The store keeps three states, and the distinction between the last two is the
whole point:

| state | meaning | what a re-run should do |
|---|---|---|
| `completed` | the effect happened, and we recorded the result | reuse the result, do not run |
| `failed` | the step raised **and our process survived to write that down** | run again; the effect almost certainly did not happen |
| `started` | we wrote "about to do it" and never wrote anything else | genuinely unknown |

`started` is the honest one. The process was killed mid-flight. So the policy
is yours to set, per workload, on `IdempotencyGuard(on_ambiguous=...)`:

- `rerun` (default) -- at-least-once. Correct for anything naturally repeatable:
  an upsert, a `PUT`, writing a file, a webhook the receiver de-duplicates.
- `skip` -- at-most-once. Correct when a duplicate is worse than a gap and
  somebody reconciles afterwards.
- `error` -- refuse to guess, stop, page a human. Correct for payments.

Whichever you choose, choose it deliberately. The common failure is not
picking the wrong one; it is not knowing the choice exists.

---

## 2. Why a naive retry duplicates side effects

```python
for attempt in range(3):
    try:
        requests.post(url, json=payload)     # the naive retry
        break
    except Exception:
        time.sleep(2)
```

This is wrong in four separate ways.

**A timeout is not a failure.** The most common reason that `post` raises is a
read timeout, and a read timeout tells you nothing about whether the server
processed the request. It very often did, and the response was lost on the way
back. The retry sends it again. Now there are two orders.

**It retries things that cannot succeed.** A `400` because a field is missing
will be a `400` on all three attempts. The cost is not just wasted time: the
run now fails six seconds later with a generic exception instead of
immediately with the real message, and the alert says "timeout" instead of
"missing field `customer_id`". `flowforge/retry.py` retries an explicit
allowlist, and `PermanentError` is never retried, even if a caller adds it to
the allowlist by mistake.

**Fixed sleeps synchronise a stampede.** If a service wobbles and every client
retries after exactly two seconds, the retries arrive together and knock it
over properly. Exponential backoff spreads them; jitter breaks the remaining
correlation. Ours takes an injected `random.Random`, so the sequence is
reproducible in a test rather than "roughly a second-ish".

**Retrying a service that is down makes it worse.** Once a dependency is
clearly unhealthy, more attempts are load, not resilience. `CircuitBreaker`
opens after N consecutive failures and fails fast until a probe succeeds. The
run fails in milliseconds with `CircuitOpen`, which is both cheaper and far
clearer in a log than thirty seconds of timeouts.

The rule we apply: **retry only a step that is idempotent, or that carries an
idempotency key.** `Workflow.lint()` enforces it as a warning, because the
combination "retries, side-effecting, no key" is exactly the setup that mails
your customers twice:

```
lint: task 'push_metrics' retries up to 2 times but is not declared idempotent
      and has no idempotency key; a retry after a partial success will repeat
      its side effect
```

---

## 3. Why "just use cron" stops scaling

Cron is excellent at one job: start this command at this time. Everything a
workflow needs beyond that, it does not do.

- **No dependencies.** Two jobs that must run in order become two crontab lines
  five minutes apart, and five minutes is a guess. When the first one takes six,
  the second reads yesterday's file and nobody finds out.
- **No partial-failure semantics.** A shell script's exit code is one bit for
  the whole pipeline. It cannot say "the extract worked, the notification did
  not". So either the run is red and you re-run everything, or somebody adds
  `|| true` and it is green forever.
- **No idempotency.** Re-running is the only remedy cron offers, and it is
  exactly the operation that duplicates side effects.
- **No state.** "Did last night's report go out?" is answered by grepping
  `/var/log/syslog` and hoping the script printed something useful.
- **Overlap.** A job that usually takes four minutes and is scheduled every
  five will, one day, take eleven. Now two copies are writing the same file.
- **Silence.** `MAILTO` goes to an inbox nobody reads. A job that stops being
  scheduled at all -- the host was rebuilt, the crontab was not restored --
  produces no signal whatsoever, because nothing has failed.
- **DST.** In a zone that observes it, a `30 2 * * *` job does not exist on one
  night a year and happens twice on another. Most implementations differ from
  each other here, and the behaviour is rarely what the author assumed.

What we do keep from cron is the **trigger**. A long-lived scheduler daemon is
another process to keep alive, monitor and restart. So `flowforge/schedule.py`
computes fire times, timezone-aware, with DST rules that are written down and
tested -- and something small and boring (cron, a systemd timer, a CI cron) is
still what invokes `flowforge run`. Dependencies, retries, idempotency, state
and reporting move into the engine, where the exit code can distinguish
success (0), failure (1) and **degraded** (2).

---

## 4. How to make a workflow safely re-runnable

The property to aim for: **running it twice leaves the world in the same state
as running it once.** Concretely, in the order we apply them:

1. **Put the side effects in as few tasks as possible.** A task that reads,
   transforms and sends is impossible to make safely re-runnable. Split it; the
   read and the transform are then free to repeat.
2. **Make the effect naturally idempotent where you can.** `PUT /orders/1234`
   over `POST /orders`. `INSERT ... ON CONFLICT DO UPDATE` over `INSERT`. Write
   to a deterministic filename rather than one with a timestamp in it.
3. **Give the rest a content-addressed key.** `content_key("email_report",
   date=..., rows=...)`. Derive it from the content, never from
   `datetime.now()` or `uuid4()` -- a key that changes per attempt protects
   nothing.
4. **Write files atomically.** Temp file in the same directory, then
   `os.replace`. A workflow killed mid-write should leave the previous output
   intact, not a truncated file the next step happily reads. Both the state
   store and the filesystem/CSV connectors do this.
5. **Do not trust a file that is still arriving.** The filesystem connector
   only reports files whose mtime has been stable for a window. Better still,
   have the producer write to a temp name and rename.
6. **Make the run resumable, then resume it instead of re-running it.**
   `flowforge resume` executes only the tasks that did not succeed and restores
   upstream outputs from the result archive.
7. **Keep task outputs JSON-serialisable.** That is what makes resume able to
   hand a value to a downstream task. When a value cannot be persisted, we
   record that and the downstream task fails with the reason rather than
   silently receiving `None`.
8. **Never let a partial state be indistinguishable from a complete one.**
   Write a marker as the last task, or make the final step atomic.

Resume and idempotency are not redundant. Resume protects against re-running
the **workflow**; the key protects against re-running the **task** -- by a
retry, a second operator, or a colleague running the script by hand.

---

## 5. Alerting on silent partial failure

The dangerous run is not the one that fails. It is the one that half-worked and
reported success.

**Statuses that mean different things must be different statuses.** In our run
state: `failed` (this task raised), `upstream_failed` (never got the chance),
`skipped` (a policy dropped it), `timed_out`, `cancelled`. Collapsing those
into one bucket is how a broken branch survives for a month.

**Have a status for "worked, but not entirely".** A run where a non-fatal task
failed finishes `degraded`, and the CLI exits **2**. Alert on it. It is the
state that rots quietly, because at a glance everything looks fine.

**Alert on absence, not only on failure.** A run that never started emits no
error. Assert the *expected* instead: nothing succeeded in the last 26 hours;
the row count was zero; the last successful run id has not changed. The state
store is a directory of JSON files precisely so that check is a small script,
not a query language.

**Make the alert carry the root cause.** Eleven red tasks are one incident and
ten consequences. `explain_failure()` separates them:

```
root cause (1 task(s) raised):
  push_metrics: TransientError: metrics gateway refused the connection
    attempts=2 duration=0.011s status=failed
    blocked 1 downstream task(s):
      update_dashboard (skipped)

completed anyway (5): email_report, fetch_customers, fetch_orders, join, write_csv
```

Send that, not "workflow failed".

**Track the numbers that predict the next outage.** `metrics()` gives success
rate, p50/p95 task duration, and retry counts per run. Retries climbing while
the success rate holds is a dependency degrading in advance of failing --
usually days of warning. p95 approaching a task's timeout means the timeout is
about to start firing. Both are visible before anything breaks.

**Log so the run is reconstructible.** One JSON object per line, run id and
task id on every line, including the retry decision (`retry:attempt=2`,
`attempts-exhausted:3`, `deadline-would-be-exceeded:...`). "Why did it stop
retrying?" should be answerable from the log, not inferred from timings.

---

## 6. Things we would do differently at larger scale

Honest boundaries of this design.

- **One host.** The JSON stores are correct for a single process. For several
  workers, put a database behind the `IdempotencyStore` protocol (four methods)
  and the state store, and take a lock per `(workflow, run key)`.
- **Threads, not processes.** Good for IO-bound automation, wrong for CPU-bound
  work, and unable to hard-kill a hung task. Steps that must be killable belong
  in a subprocess -- which is why the `shell` connector's timeout is real.
- **Backfills need a first-class concept.** Re-running a range of dates is a
  different operation from resuming a failed run, and today it is a loop over
  parameters. If backfills are routine, model the date as a partition key and
  make the idempotency key include it (as the DSL's
  `"notify:{param:run_date}"` template does).
- **Secrets.** Nothing here manages them. Pass them through the environment and
  keep them out of task arguments -- run state records input *digests*, not
  values, but a credential in a task id or an idempotency key would be written
  to disk in clear.
