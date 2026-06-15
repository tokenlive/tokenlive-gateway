# Rename Policy Tables to policy_ Prefix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the six core LLM gateway policy tables in the SQL schema files to start with the `policy_` prefix (e.g. `policy_tagging`, `policy_limit`, etc.).

**Architecture:** We will modify the tables' creation statements in both workspace schema files (`tokenlive-gateway/docs/schema.sql` and `tokenlive-gateway-admin/scripts/init.sql`) using clean Option A names, maintaining comment consistency.

**Tech Stack:** SQL (MySQL/MariaDB dialect).

---

### Task 1: Rename policy tables in `tokenlive-gateway` schema file

**Files:**

- Modify: `docs/schema.sql`

- [ ] **Step 1: Locate and modify table names in docs/schema.sql**

Rename the following tables at their respective lines:

1. Line 141: `CREATE TABLE tagging_policies` -> `CREATE TABLE policy_tagging`
   Also update its end comment `) COMMENT ='染色打标策略表';`
2. Line 160: `CREATE TABLE load_balance_policies` -> `CREATE TABLE policy_load_balance`
   Also update its end comment `) COMMENT ='负载均衡策略表';`
3. Line 176: `CREATE TABLE invoke_policies` -> `CREATE TABLE policy_invoke`
   Also update its end comment `) COMMENT ='调用与重试策略表';`
4. Line 202: `CREATE TABLE limit_policies` -> `CREATE TABLE policy_limit`
   Also update its end comment `) COMMENT ='限流策略表';`
5. Line 224: `CREATE TABLE route_policies` -> `CREATE TABLE policy_route`
   Also update its end comment `) COMMENT ='标签路由策略表';`
6. Line 241: `CREATE TABLE circuit_break_policies` -> `CREATE TABLE policy_circuit_break`
   Also update its end comment `) COMMENT ='熔断隔离策略表';`

- [ ] **Step 2: Commit the changes in tokenlive-gateway repository**

Run:

```bash
git add docs/schema.sql docs/specs/2026-05-30-rename-policy-tables-design.md
git commit -m "db: rename core policy tables to start with policy_"
```

---

### Task 2: Rename policy tables in `tokenlive-gateway-admin` initialization script

**Files:**

- Modify: `/Users/chenzhiguo/Projects/tokenlive-gateway-admin/scripts/init.sql`

- [ ] **Step 1: Locate and modify table names in scripts/init.sql**

Rename the appended core policy tables starting at line 298:

1. Line 298: `CREATE TABLE tagging_policies` -> `CREATE TABLE policy_tagging`
2. Line 317: `CREATE TABLE load_balance_policies` -> `CREATE TABLE policy_load_balance`
3. Line 333: `CREATE TABLE invoke_policies` -> `CREATE TABLE policy_invoke`
4. Line 359: `CREATE TABLE limit_policies` -> `CREATE TABLE policy_limit`
5. Line 381: `CREATE TABLE route_policies` -> `CREATE TABLE policy_route`
6. Line 398: `CREATE TABLE circuit_break_policies` -> `CREATE TABLE policy_circuit_break`

- [ ] **Step 2: Commit the changes in tokenlive-gateway-admin repository**

Run:

```bash
git add scripts/init.sql
git commit -m "db: rename core LLM gateway policy tables to start with policy_"
```
