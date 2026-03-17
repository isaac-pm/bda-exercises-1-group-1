# AGENTS.md

Operational guide for coding agents in this repository.

## 1) Repository Snapshot
- Primary code: `problem_1/*.java` (Hadoop MapReduce jobs)
- Java sources: `HadoopWordCount.java`, `HadoopWordPairs.java`, `HadoopWordStripes.java`, `HadoopPartB.java`
- `problem_2/` and `problem_3/` are placeholders
- `.class` and `.jar` artifacts in `problem_1/`
- No Maven/Gradle/CI config

## 2) Cursor/Copilot Rules
Checked: `.cursorrules`, `.cursor/rules/`, `.github/copilot-instructions.md`
- No Cursor/Copilot rules found

## 3) Environment
- `javac`, `jar`, `hadoop`, `hdfs` available
- `hadoop classpath` resolves
- Commands run from repo root
- Java: OpenJDK 8+, Hadoop: 3.3+

## 4) Build Commands
```bash
# Compile all Java files
javac -cp "$(hadoop classpath)" problem_1/*.java

# Compile with strict warnings
javac -Xlint:all -Werror -cp "$(hadoop classpath)" problem_1/*.java

# Create runnable jar
jar -cvf problem_1/HadoopWordCount.jar -C problem_1 .
```

## 5) Lint Commands
```bash
javac -Xlint:all -cp "$(hadoop classpath)" problem_1/*.java
```

## 6) Test Commands (Smoke)
HDFS test input setup:
```bash
printf "to be or not to be\n" > /tmp/bda_input.txt
hdfs dfs -rm -r -f /tmp/bda-in /tmp/bda-out
hdfs dfs -mkdir -p /tmp/bda-in
hdfs dfs -put -f /tmp/bda_input.txt /tmp/bda-in/
```

Run smoke tests:
```bash
# WordCount
hadoop jar problem_1/HadoopWordCount.jar HadoopWordCount /tmp/bda-in /tmp/bda-out
hdfs dfs -cat /tmp/bda-out/part-r-00000

# WordPairs
hadoop jar problem_1/HadoopWordCount.jar HadoopWordPairs /tmp/bda-in /tmp/bda-out

# WordStripes
hadoop jar problem_1/HadoopWordCount.jar HadoopWordStripes /tmp/bda-in /tmp/bda-out
```

## 7) Running a Single Test
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

## 8) Code Style Guidelines

### Imports
- Explicit imports only; no wildcards
- Order: Java stdlib first, then Hadoop packages
- Remove unused imports

```java
// Good
import java.util.ArrayList;
import org.apache.hadoop.io.Text;

// Bad
import java.util.*;
import org.apache.hadoop.io.*;
```

### Regex Patterns
Define as `static final Pattern`:
```java
private static final Pattern SPLIT_PATTERN = Pattern.compile("[^a-z0-9.-]+");
private static final Pattern WORD_PATTERN = Pattern.compile("[a-z-]{6,24}");
private static final Pattern NUMBER_PATTERN = Pattern.compile("-?[0-9.]{4,16}");
```

### Formatting
- Tab-indented blocks (match existing style)
- One statement per line
- Braces for all control blocks
- Keep methods focused

### Types and Generics
- Use concrete Hadoop types: `Text`, `IntWritable`, `MapWritable`
- Keep Mapper/Reducer signatures exact
- Avoid raw types

### Naming
- Classes: `PascalCase`
- Methods/fields/variables: `camelCase`
- Constants: `UPPER_SNAKE_CASE`
- Nested classes: `Map` and `Reduce`

### Error Handling
- Validate `args` length before indexing
- Return non-zero exit code for errors
- Clear failure messages

### Hadoop Job Configuration
- Set jar class, mapper, reducer, key/value classes, formats explicitly
- Use `job.waitForCompletion(true)` and map result to exit status

### Data Handling
- Be explicit about tokenization
- Guard against empty tokens
- Use `Locale.ROOT`: `line.toLowerCase(Locale.ROOT)`

### Token Filtering Rules
- **Words**: lowercase a-z + dash `-`, length 6-24
- **Numbers**: digits 0-9 + decimal `.` + optional leading `-`, length 4-16

### Part B Implementation
- Combine tasks into few MapReduce jobs
- Single reducer (`setNumReduceTasks(1)`) for global top-100
- Use PriorityQueue for top-k in reducer
- Emit with tags: TASK1, TASK2, TASK3, TASK4

## 9) Pre-PR Checklist
- [ ] Compile passes
- [ ] Lint reviewed
- [ ] Smoke test executed on deterministic input
- [ ] Output sanity-checked

## 10) Part B Test Commands
```bash
rm -rf problem_1/HadoopPartB_Output_AA
hadoop jar problem_1/HadoopWordCount.jar HadoopPartB "./Wikipedia-En-41784-Articles/AA/*" problem_1/HadoopPartB_Output_AA
hdfs dfs -cat problem_1/HadoopPartB_Output_AA/part-r-00000 | head -20
```

Output format:
- `TASK1\t<word>\t<count>` - words with count exactly 1000
- `TASK2\t<word1:word2>\t<count>` - word pairs with count exactly 1000
- `TASK3\t<word>\t<count>` - top-100 most frequent words
- `TASK4\t<number:word>\t<count>` - top-100 number:word pairs
