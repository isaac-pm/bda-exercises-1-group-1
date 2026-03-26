#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

SUBDIRS=(AA AB AC AD AE AF AG AH AI AJ AK)

mkdir -p benchmark_logs
SUMMARY="benchmark_logs/problem2_scalability.tsv"

printf "step\tlabel\tfiles\treal\tuser\tsys\ttask1\ttask2\ttask3\ttask4\ttotal_lines\n" > "$SUMMARY"

for ((idx=0; idx<${#SUBDIRS[@]}; idx++)); do
  PARTS=("${SUBDIRS[@]:0:$((idx+1))}")

  LABEL=""
  INPUTS=""
  FILES=0

  for part in "${PARTS[@]}"; do
    if [[ -z "$LABEL" ]]; then
      LABEL="$part"
    else
      LABEL="${LABEL}_$part"
    fi

    if [[ -z "$INPUTS" ]]; then
      INPUTS="../Wikipedia-En-41784-Articles/$part"
    else
      INPUTS="${INPUTS},../Wikipedia-En-41784-Articles/$part"
    fi

    COUNT=$(find "../Wikipedia-En-41784-Articles/$part" -maxdepth 1 -type f | wc -l)
    FILES=$((FILES + COUNT))
  done

  OUTDIR="test_output/problem2_${LABEL}"
  TIMELOG="benchmark_logs/${LABEL}.time"

  rm -rf "$OUTDIR"

  /usr/bin/time -p -o "$TIMELOG" \
    spark-submit --master local[*] --class SparkProblem2 SparkProblem2.jar "$INPUTS" "$OUTDIR"

  REAL=$(awk '/^real / {print $2}' "$TIMELOG")
  USER=$(awk '/^user / {print $2}' "$TIMELOG")
  SYS=$(awk '/^sys / {print $2}' "$TIMELOG")

  TASK1=$(grep -c $'^TASK1\t' "$OUTDIR/part-00000" || true)
  TASK2=$(grep -c $'^TASK2\t' "$OUTDIR/part-00000" || true)
  TASK3=$(grep -c $'^TASK3\t' "$OUTDIR/part-00000" || true)
  TASK4=$(grep -c $'^TASK4\t' "$OUTDIR/part-00000" || true)
  TOTAL=$(wc -l < "$OUTDIR/part-00000")

  printf "%d\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$((idx+1))" "$LABEL" "$FILES" "$REAL" "$USER" "$SYS" \
    "$TASK1" "$TASK2" "$TASK3" "$TASK4" "$TOTAL" >> "$SUMMARY"
done

cat "$SUMMARY"
