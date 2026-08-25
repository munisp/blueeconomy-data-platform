# Strict Main-Branch Review Policy

The `main` branch must require the `Repository Governance` check, two approving reviews, code-owner review, dismissal of stale approvals, approval after the latest push, resolved conversations, linear history, and enforcement for administrators. Force pushes and branch deletion are prohibited.

Apply the adjacent `branch-protection-main.json` with the repository owner or organization administrator through the GitHub branch-protection API or Settings UI. The repository-level workflow verifies that the policy source and CODEOWNERS file remain present, but only an authorized GitHub administrator can apply or verify the live branch setting.
