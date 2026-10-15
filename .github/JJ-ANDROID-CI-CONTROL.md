# JJ NuvioMobile isolated CI control

This branch is deliberately the default of the personal fork only; it is not the application release branch.
Feature source remains on `feature/android-segmented-download-jj-20261015`, draft PR #1 targeting `cmp-rewrite`.

## Safety
- The 11 inherited workflows were moved from `.github/workflows/` into `.github/inherited-workflows-disabled/` to prevent inherited scheduled maintenance, release and issue-related triggers.
- The only active workflow, `jj-android-verify.yml`, is manual-only and read-only. No schedule, push, release, or pull-request triggers.
- CI compiles a pinned commit and uploads an unverified debug APK; it never installs or publishes to users.
- Direct Git clone avoids the `libass-android` gitlink checkout error from the inherited repository.
- Do not change the fork default branch back to `cmp-rewrite` without inspecting inherited workflows first.

## Evidence
- Original Sept Android CI: https://github.com/jaskaransingh11/NuvioDesktop/actions/runs/36479221962
- Current feature PR: https://github.com/jaskaransingh11/NuvioMobile/pull/1
- October validation: https://github.com/jaskaransingh11/NuvioMobile/actions/runs/38721198446

## Controlled build
Run GitHub Actions > JJ Android Safe Validation manually, on branch `jj/android-ci-control-20261015`. Review exact pinned app commit, CI results and APK signing/package identity before installing. Normal Nuvio Android app data must not be overwritten.
