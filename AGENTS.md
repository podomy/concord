1. **Never use `git commit --no-verify`** the pre-commit hook must always run.
2. **Never overwrite the user's code.** Make targeted edits that preserve the user's existing variable names, error handling style, and control flow. If you need to change behavior, ask first.
3. **Error handling style** use explicit `err :=` / `err =` assignments followed by `if err != nil { return ... }`. Do not use `if err := foo(); err != nil {` inline style. The user's code always separates the call and the check.
4. **Error wrapping** wrap errors from external packages with `fmt.Errorf("context: %w", err)` for `wrapcheck` compliance.
5. **Comments** add doc comments on all functions (exported and unexported). Keep them concise.
6. **Never use em dashes** in any text or code. Use a plain hyphen or two hyphens instead if needed, don't add unless absolutely necessary.
7. **Verify APIs before writing code** read `go doc` or the library source to confirm types, function signatures, and behavior before writing code that uses an external package. Do not write code based on memory or assumptions.
8. **Minimal changes** only change what is necessary to fix the specific issue or implement the specific request. Do not rename variables, restructure code, or change style unless explicitly asked.
9. **Discuss and collaborate** discuss the approach, show examples, explain tradeoffs, and let me write the code. Only write code directly when it is refactoring, highly repetitive, or a chore I explicitly ask you to handle.
10. **Define invariants first** before implementing any complex feature, we must agree on the invariants that the system must maintain. Document them concisely before writing code.
11. **Insist on understanding** if I seem confused or unsure about a part of the code, do not let go until I understand it completely and you are sure of it. Persist even if I try to move on.
12. **Second pass for correctness and security** after writing any code (by me or the user), make another pass to verify correctness, edge cases, and security before presenting it as done. Before responding to any request, run `git diff` and `git diff --stat` to check what has changed in the working tree. Do not assume the repo is clean.
13. **Never subvert the toolchain** never bypass the pre-commit hook, skip linters, or modify lint config to make things easier or to get a commit through. These checks exist for a reason.
14. **Commit format** follow COMMITS file for commit message format. Keep messages minimal but including a description. Descriptions should be detailed enough to understand the change without reading the diff but still kept concise.
