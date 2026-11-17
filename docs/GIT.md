### Git Commit Guidelines

Maintain a clean, professional Git history throughout development.

1. Create commits at meaningful development milestones, not after every minor file modification.
2. Keep commit messages short, natural, and descriptive.
3. Prefer conventional messages such as:
   - `Add document ingestion pipeline`
   - `Implement hybrid retrieval`
   - `Add agent execution guardrails`
   - `Fix retrieval ranking`
   - `Improve error handling`
4. Avoid unnecessarily long commit messages, lengthy descriptions, excessive bullet points, and AI-generated-sounding explanations.
5. Never mention Claude, AI assistance, or automated code generation in commit messages.
6. Avoid meaningless commits such as `updates`, `misc changes`, `final changes`, or `code improvements`.
7. Don't create multiple commits for trivial changes that belong to the same logical task.
8. Keep commits focused so each one represents a meaningful, understandable change.
9. Review the staged diff before committing. Never include secrets, credentials, `.env` files, generated artifacts, or unrelated changes.
10. Maintain the existing local Git identity and configuration.
11. Do not configure external Git remotes, authenticate with external Git services, or push anything. Keep Git operations local.

The goal is a concise, readable Git history that reflects normal, deliberate software engineering practices.


### Git Bundle Management

Maintain a reliable incremental Git bundle export workflow.

**Initial export:**
1. Create `pipeline-initial.bundle` containing the complete local Git history.
2. Verify it using `git bundle verify`.
3. Record the latest exported commit SHA in a local checkpoint file named `.git-bundle-checkpoint`.
4. Do not update the checkpoint until the bundle has been successfully created and verified.

**Subsequent exports:**
1. Read the previous exported commit SHA from `.git-bundle-checkpoint`.
2. Identify all new commits since that checkpoint.
3. Create an incremental bundle containing only those new commits.
4. Use filenames such as `pipeline-update-001.bundle`, `pipeline-update-002.bundle`, etc.
5. Verify every generated bundle.
6. Update the checkpoint only after successful bundle creation and verification.
7. If there are no new commits, report that no bundle is required.
8. Never overwrite previous bundles.
9. Never delete previous bundles or modify existing Git history.

**Important constraints:**
- Keep all operations local.
- Never configure Git remotes or authenticate with external Git services.
- Never execute `git push`.
- Preserve commit authorship and commit history.
- Report the bundle filename, location, commit range, and verification status after each export.