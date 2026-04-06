# Review

Human-in-the-loop decision engine.

Owns:

- Review queue
- Review cases (grouped patterns of uncertainty)
- Review decisions and overrides
- Reprocessing triggers

Does NOT own:

- Raw data ingestion
- Mapping definitions
- Final reporting logic

Notes:
Only decision-required cases should enter the queue.
