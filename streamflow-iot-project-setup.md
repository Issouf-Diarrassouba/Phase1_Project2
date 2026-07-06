# StreamFlow IoT — Project Setup Guide

This guide helps your team of 4 set up the project the right way. It covers four things:

1. Folder structure (using an IoT device telemetry domain)
2. A Kanban board for agile tracking
3. Story points using the Fibonacci scale
4. A Git workflow built for a first-time GitHub team

---

## 1. Folder Structure (IoT Domain)

Your project asks teams to pick an event domain. For IoT, the best fit is **device telemetry** — things like temperature, battery level, and signal strength sent by sensors.

Here is the folder layout. Create these folders and empty files before writing any code. This gives everyone a shared map of where things go.

```text
streamflow-iot/
  airflow/
    dags/
      streamflow_daily_summary.py
  docker/
    airflow.Dockerfile
    compose.yml
    producer.Dockerfile
  kafka/
  spark/
    jobs/
      streaming_ingest.py
      daily_summary.py
  src/
    streamflow/
      __init__.py
      producer.py
      schemas.py
      quality.py
  data/
    raw/
    curated/
    rejects/
    checkpoints/
  tests/
  README.md
```

### What each folder is for

| Folder | What lives here |
| --- | --- |
| `airflow/dags/` | The Airflow DAG that runs your summary job |
| `docker/` | All Docker setup files, including the file that starts every service |
| `kafka/` | Any Kafka or Redpanda config files |
| `spark/jobs/` | The two required Spark jobs: streaming ingest and daily summary |
| `src/streamflow/` | Your Python code: the event producer, schema definitions, and validation rules |
| `data/raw/` | Events straight from the stream, no changes made |
| `data/curated/` | Cleaned, valid event data |
| `data/rejects/` | Events that failed validation, with a reason attached |
| `data/checkpoints/` | Spark's progress tracker, so streaming can resume safely |
| `tests/` | Unit and integration tests |

### Suggested IoT event types

Use these as your `event_type` values, since your device telemetry domain needs a consistent set:

- `temperature_reading`
- `battery_status`
- `connectivity_status`
- `motion_detected`
- `device_error`

Each event's `payload` field will look different depending on the type. For example, a `temperature_reading` payload might hold a value and a unit, while a `battery_status` payload might hold a percentage.

---

## 2. Kanban Board Setup

Use **GitHub Projects** (built into GitHub, free, and works well for a first-time team). Set up a board with these columns:

| Column | Meaning |
| --- | --- |
| **Backlog** | All planned work, not yet ready to start |
| **Ready** | Work that is fully understood and can be picked up next |
| **In Progress** | Someone is actively working on it |
| **In Review** | A pull request is open and waiting for a teammate to check it |
| **Done** | Merged into `main` and confirmed working |

### Rules that keep the board honest

- **Limit "In Progress" to 2 items per person.** This stops people from starting five things and finishing none.
- **A card only moves to "In Review" once a pull request is open.** Not before.
- **A card only moves to "Done" after the pull request is merged.** Not when the code is "basically working" on someone's laptop.
- **Every card should be small enough to finish in 1–3 days.** If it feels bigger, split it into two cards.

### Definition of Ready (before a card enters "Ready")

- The task has a clear, one-sentence goal.
- It's clear which file or component it touches.
- It has a story point estimate (see below).

### Definition of Done (before a card enters "Done")

- Code is merged into `main`.
- Tests pass, if the task included tests.
- Logs or output prove the feature works, matching the "Logging and Observability" section of your project doc.
- No secrets or passwords were committed.

---

## 3. Story Points (Fibonacci Scale)

Story points measure **effort and uncertainty**, not hours. Use this scale: **1, 2, 3, 5, 8, 13**.

| Points | Meaning | Example |
| --- | --- | --- |
| **1** | Trivial, no real unknowns | Add a config value to `pipeline.yml` |
| **2** | Small, well understood | Write a unit test for timestamp parsing |
| **3** | Normal-sized task | Build the IoT event schema in `schemas.py` |
| **5** | Some real complexity or a few moving parts | Write the Spark Structured Streaming ingest job |
| **8** | Complex, touches multiple components | Set up Docker Compose with Redpanda, Spark, and Airflow together |
| **13** | Large and risky, should probably be split | "Build the whole pipeline end to end" — split this into smaller cards |

**Rule of thumb:** if a task feels like a 13, break it into two or three smaller cards before it enters the backlog. Big vague cards are where teams lose track of progress.

### How to estimate as a team

1. Read the card out loud.
2. Each person picks a number in their head, then reveals it at the same time (this is called Planning Poker, and you can do it with fingers on a video call).
3. If everyone agrees, write it down.
4. If numbers are far apart, talk for two minutes about why, then re-vote once.

---

## 4. Initial Backlog with Story Points

Here is a starter backlog, grouped by the five steps in your Phase 1 prototype. Copy these into your board as cards.

### Epic A: Event Generation

| Card | Points |
| --- | --- |
| Define the IoT event JSON schema (`schemas.py`) | 3 |
| Build the synthetic IoT event generator (`producer.py`) | 5 |
| Add config for topic name and broker address | 2 |
| Write unit tests for event generation | 2 |

### Epic B: Kafka / Redpanda Setup

| Card | Points |
| --- | --- |
| Add Redpanda service to `compose.yml` | 3 |
| Create the `streamflow.events` topic | 1 |
| Confirm producer can publish messages (manual check) | 2 |

### Epic C: Spark Streaming Ingest

| Card | Points |
| --- | --- |
| Write the Spark Structured Streaming ingest job | 8 |
| Add checkpointing to the ingest job | 3 |
| Write raw output to Parquet | 3 |
| Add JSON parsing and schema validation | 5 |
| Split valid vs. rejected records with reasons | 5 |
| Write a streaming smoke test | 3 |

### Epic D: Spark Batch Summary

| Card | Points |
| --- | --- |
| Write the daily summary Spark job | 5 |
| Add event counts by type, source, and time window | 3 |
| Write summary output to the curated folder | 2 |

### Epic E: Airflow Orchestration

| Card | Points |
| --- | --- |
| Set up the Airflow service in Docker Compose | 5 |
| Write the DAG that triggers the summary job | 5 |
| Add a task that validates output files exist | 3 |
| Confirm the DAG imports and runs (smoke test) | 2 |

### Epic F: Docker & Setup

| Card | Points |
| --- | --- |
| Build the full `compose.yml` linking all services | 8 |
| Write the producer Dockerfile | 3 |
| Write the Airflow Dockerfile | 3 |
| Confirm `docker compose up` starts everything cleanly | 3 |

### Epic G: Documentation & Wrap-up

| Card | Points |
| --- | --- |
| Write the README with setup and run steps | 3 |
| Document troubleshooting tips | 2 |
| Record a short end-to-end demo run | 2 |

**Total estimated points:** roughly 90, across weeks 6–8. A team of 4 should plan for around 30 points per week if everyone contributes evenly — adjust after your first week once you see your actual pace.

---

## 5. Git Workflow (First-Time GitHub Team)

Use **GitHub Flow**. It's simpler than other Git workflows and works well for small teams and short projects.

### The core rule

**Nobody pushes code directly to `main`.** All work happens on a branch, then goes through a pull request.

### Step-by-step workflow

1. **Pull the latest `main` before starting anything.**
   ```
   git checkout main
   git pull
   ```

2. **Create a new branch for your card.** Name it clearly, using this pattern:
   ```
   <type>/<short-description>
   ```
   Examples:
   - `feat/iot-event-schema`
   - `feat/spark-streaming-ingest`
   - `fix/checkpoint-path-bug`
   - `docs/readme-setup-steps`
   - `test/producer-unit-tests`

   ```
   git checkout -b feat/iot-event-schema
   ```

3. **Commit often, with clear messages.** Use this pattern:
   ```
   <type>: <short summary in plain English>
   ```
   Examples:
   - `feat: add IoT event schema with payload validation`
   - `fix: correct checkpoint path in ingest job`
   - `test: add unit test for timestamp parsing`

   Keep each commit focused on one change. Small commits are easier for teammates to review.

4. **Push your branch and open a pull request (PR).**
   ```
   git push -u origin feat/iot-event-schema
   ```
   On GitHub, click "Compare & pull request." Fill in:
   - What the change does
   - Which card/issue it closes (link it, e.g. `Closes #12`)
   - Any manual testing you did

5. **Get one teammate to review before merging.** This is the single most important habit for a first-time GitHub team. A second pair of eyes catches bugs and spreads knowledge of the codebase across the team.

   Reviewers should check:
   - Does the code do what the PR says it does?
   - Are there secrets, passwords, or API keys anywhere? (There should not be.)
   - Does it follow the folder structure and naming from Section 1?

6. **Merge using "Squash and merge."** This keeps `main`'s history clean — one commit per feature instead of ten messy ones.

7. **Delete the branch after merging.** GitHub will prompt you to do this automatically.

8. **Pull the latest `main` again before starting your next branch.**

### Protecting `main`

In your GitHub repo settings, turn on **branch protection** for `main`:
- Require a pull request before merging.
- Require at least 1 approval.
- Do not allow direct pushes to `main`.

This takes two minutes to set up and prevents accidental broken commits from reaching everyone.

### Handling merge conflicts

If GitHub says your branch has a conflict:
```
git checkout main
git pull
git checkout your-branch-name
git merge main
```
Fix the conflicting lines in your editor (they'll be marked with `<<<<<<<` and `>>>>>>>`), save the file, then:
```
git add .
git commit
git push
```

### A quick reference card for your team

| Action | Command |
| --- | --- |
| Start fresh | `git checkout main && git pull` |
| New branch | `git checkout -b feat/your-task` |
| Save work | `git add . && git commit -m "feat: message"` |
| Share work | `git push -u origin feat/your-task` |
| Update your branch with latest main | `git merge main` |

---

## Suggested Weekly Rhythm (Weeks 6–8)

| Week | Focus | Kanban activity |
| --- | --- | --- |
| Week 6 | Epics A, B, and C (events, broker, streaming ingest) | Monday: pick cards from Backlog into Ready. Friday: move finished cards to Done, review what's left. |
| Week 7 | Epics D and E (batch summary, Airflow) | Same rhythm. Re-estimate any card that turns out bigger than expected. |
| Week 8 | Epics F and G (Docker polish, docs, demo) | Focus on finishing "In Progress" cards before starting new ones. Freeze new feature work by mid-week to leave room for demo prep. |

Hold a 10-minute stand-up 2–3 times a week (even async in a chat channel works): each person shares what they finished, what they're working on, and anything blocking them.

---

**Next step:** create the empty repo on GitHub, add all four teammates as collaborators, turn on branch protection for `main`, then create the folder structure from Section 1 in your first PR.
