# Working rules

This repository is developed with Claude Code in review mode: the assistant proposes,
the author decides, checks and commits. These rules apply to every change.

1. **Concept before code.** Explain what the change does and why before writing it.
2. **Proof before commit.** Every change is backed by evidence shown before committing:
   a query and its result, a diff, or an execution log.
3. **One commit per change.** A commit does one thing and its message says what.
4. **No unverified claim in the README.** Every number or statement in the README comes
   from a query, a file or a run that can be reproduced; what is not measured is not written.
