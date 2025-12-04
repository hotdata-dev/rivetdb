# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A federated query engine built with Rust/DataFusion that provides on-demand caching of remote databases. The engine discovers remote tables, syncs them to local Parquet cache, and executes SQL queries through DataFusion.

## Git Commits

Use conventional format: `<type>(<scope>): <subject>` where type = feat|fix|docs|style|refactor|test|chore|perf. Subject: 50 chars max, imperative mood ("add" not "added"), no period. For small changes: one-line commit only. For complex changes: add body explaining what/why (72-char lines) and reference issues. Keep commits atomic (one logical change) and self-explanatory. Split into multiple commits if addressing different concerns. Don't mention anthropic or claude (no co-author).

## Tools

Use `rg` not grep, `fd` not find, `tree` is installed

## Tests

Implement comprehensive tests. Design tests that are meaningful and accurately reflect real usage.