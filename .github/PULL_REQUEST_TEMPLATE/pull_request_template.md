- [ ] Add [CHANGELOG](https://github.com/timescale/timescaledb/blob/master/CHANGELOG.md) updates
- [ ] Needs [documentation](https://github.com/timescale/docs) updates

Fixes #<issue number>.

---
Cut everything below the above line.

## Guidelines for Pull requests 

Checklist for a pull request:

- Try to follow the [the seven rules of a great Git commit
message](https://chris.beams.io/posts/git-commit/).
- Ideally, the PR should be merged as a single commit.
- Rebase on latest master code.
- Reference the issue(s) resolved with Fixes #<issue number>, or
  Closes #<issue number>.
- Make sure the PR has appropriate CHANGELOG updates, including thanks
  to people that reported issues.
- Include appropriate documentation changes.
- Two approvals are necessary to merge.

### The seven rules of a great Git commit message

1. Separate subject from body with a blank line
2. Limit the subject line to 50 characters
3. Capitalize the subject line
4. Do not end the subject line with a period
5. Use the imperative mood in the subject line
6. Wrap the body at 72 characters
7. Use the body to explain what and why vs. how

### Single commit PR

The simplify reviewing and to protect against accidental merges of
work-in-progress commits, the CI system enforces that a PR is merged
as a single commit. However, sometimes a multi-commit PR is warranted
if each commit is a distinct change that makes sense to submit in the
same PR. The single-commit enforcement can be disabled by adding the
following trailer:

Disable-check: commit-count

Generally, though, multi-commit PRs are discouraged as that either
means the code change is too big, the PR mixes unrelated changes, or
has a lot of work-in-progress commits that are not appropriate for a
well-formatted commit log.


### Documentation

If the code change adds a new feature, limitation, or change in
behavior, the PR might need to include documentation or a separate
follow-up PR to the [documentation repository](https://github.com/timescale/docs).
