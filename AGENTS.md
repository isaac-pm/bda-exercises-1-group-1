# AGENTS.md

Operational guide for coding agents in this repository.

## 1) Repository Snapshot
- Primary code is in `problem_1/`.
- Java Hadoop MapReduce sources:
  - `HadoopWordCount.java`
  - `HadoopWordPairs.java`
  - `HadoopWordStripes.java`
- `problem_2/` and `problem_3/` are placeholders.
- No Maven/Gradle wrapper, no Makefile, no CI config detected.
- `.class` and `.jar` artifacts exist in `problem_1/`.

## 2) Cursor/Copilot Rules
Checked:
- `.cursorrules`
- `.cursor/rules/`
- `.github/copilot-instructions.md`
Current result:
- No Cursor rule files found.
- No Copilot instruction file found.
If such files are added, their instructions override this document.

## 3) Environment Assumptions
- `javac` and `jar` are installed.
- `hadoop` and `hdfs` CLIs are available.
- `hadoop classpath` resolves successfully.
- Commands are run from repo root.

## 4) Build Commands
Compile all Java files:
```bash
javac -cp "$(hadoop classpath)" problem_1/*.java
```
Compile one class:
```bash
javac -cp "$(hadoop classpath)" problem_1/HadoopWordCount.java
```
Compile with strict warnings:
```bash
javac -Xlint:all -Werror -cp "$(hadoop classpath)" problem_1/*.java
```
Create/recreate runnable jar:
```bash
jar -cvf problem_1/HadoopWordCount.jar -C problem_1 .
```

## 5) Lint Commands
No Checkstyle/PMD/SpotBugs config exists.
Use compiler linting as baseline:
```bash
javac -Xlint:all -cp "$(hadoop classpath)" problem_1/*.java
```
Use `-Werror` for stricter validation when feasible.

## 6) Test Commands (Smoke)
No JUnit/TestNG suite is present.
Use small deterministic job runs as tests.
HDFS test input setup:
```bash
printf "to be or not to be\n" > /tmp/bda_input.txt
hdfs dfs -rm -r -f /tmp/bda-in /tmp/bda-out
hdfs dfs -mkdir -p /tmp/bda-in
hdfs dfs -put -f /tmp/bda_input.txt /tmp/bda-in/
```
WordCount smoke test:
```bash
hadoop jar problem_1/HadoopWordCount.jar HadoopWordCount /tmp/bda-in /tmp/bda-out
hdfs dfs -cat /tmp/bda-out/part-r-00000
```
WordPairs smoke test:
```bash
hadoop jar problem_1/HadoopWordCount.jar HadoopWordPairs /tmp/bda-in /tmp/bda-out
hdfs dfs -cat /tmp/bda-out/part-r-00000
```
WordStripes smoke test:
```bash
hadoop jar problem_1/HadoopWordCount.jar HadoopWordStripes /tmp/bda-in /tmp/bda-out
hdfs dfs -cat /tmp/bda-out/part-r-00000
```

## 7) Running a Single Test
In this repository, a single test = one job on tiny fixed input.
Example single test for `HadoopWordCount`:
```bash
javac -cp "$(hadoop classpath)" problem_1/HadoopWordCount.java
jar -cvf problem_1/HadoopWordCount.jar -C problem_1 .
printf "a a b\n" > /tmp/bda_input.txt
hdfs dfs -rm -r -f /tmp/bda-in /tmp/bda-out
hdfs dfs -mkdir -p /tmp/bda-in
hdfs dfs -put -f /tmp/bda_input.txt /tmp/bda-in/
hadoop jar problem_1/HadoopWordCount.jar HadoopWordCount /tmp/bda-in /tmp/bda-out
hdfs dfs -cat /tmp/bda-out/part-r-00000
```
Expected output includes counts for `a` and `b`.

## 8) Code Style Guidelines
### Imports
- Use explicit imports; avoid wildcard imports.
- Order imports with Java stdlib first, Hadoop packages second.
- Remove unused imports before finalizing.

### Formatting
- Match existing style in `problem_1/*.java` (tab-indented blocks).
- Keep one statement per line.
- Prefer braces for all control blocks in new code.
- Keep methods focused; extract helpers for repeated logic.

### Types and Generics
- Use concrete Hadoop writable types (`Text`, `IntWritable`, `MapWritable`).
- Keep Mapper/Reducer generic signatures exact.
- Avoid raw types and unchecked casts when possible.

### Naming
- Classes: `PascalCase`.
- Methods/fields/variables: `camelCase`.
- Constants: `UPPER_SNAKE_CASE`.
- Keep nested class names `Map` and `Reduce` for consistency.

### Error Handling
- Validate `args` length in `run(String[] args)` before indexing.
- Return non-zero exit code for invalid usage or failed jobs.
- Surface clear failure messages; do not swallow exceptions.
- Keep thrown exceptions specific where practical.

### Hadoop Job Configuration
- Explicitly set jar class, mapper, reducer, key/value classes, and formats.
- Ensure output path does not already exist before execution.
- Use `job.waitForCompletion(true)` and map result to exit status.

### Data Handling
- Be explicit about tokenization (`split("\\W+")` vs `split(" ")`).
- Guard against empty tokens from split operations.
- Normalize case only when required by assignment rules.

## 9) Agent Change Policy
- Prefer source-only edits; avoid committing regenerated binaries unless requested.
- Do not introduce new frameworks unless task requires them.
- If tooling/tests are added later, update this file with exact commands.

## 10) Pre-PR Checklist
- Compile passes: `javac -cp "$(hadoop classpath)" problem_1/*.java`.
- Lint reviewed: `javac -Xlint:all ...`.
- Relevant smoke test executed on deterministic input.
- Output sanity-checked for expected keys/counts.
- Documentation and command snippets updated if behavior changed.
