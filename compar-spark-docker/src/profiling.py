"""
Medição de cada operação do pipeline (baseline e antes/depois de cada otimização).

Como se usa no main.py:
    profiler = Profiler(spark, args.label, "output/metrics", {"years": ..., "fraction": ...})
    profiler.start("M2")
    ...  código da operação  ...
    profiler.stop()
    ...
    profiler.report()

start() mete os jobs seguintes num job group com o nome da operação (é esse o nome que aparece
no Spark UI) e começa a contar o tempo. stop() acaba de contar e lê a REST API do Spark UI para
somar, nos stages dessa operação: tasks, linhas lidas dos ficheiros, shuffle read/write,
spill e skew (task mais lenta ÷ mediana).
Os bytes lidos (input_mb) também ficam no CSV, mas no primeiro teste deram valores impossíveis
(0,1 a 0,5 MB por operação, quando cada uma lê os 5 ficheiros Parquet). Por isso, para saber
quanto se leu, usamos as linhas lidas (input_records).
Cada operação é logo acrescentada a output/metrics/:
  operations.csv  uma linha por operação
  stages.csv      uma linha por stage
Os ficheiros só crescem: cada corrida acrescenta linhas com o seu run_id e a sua label.
"""
import csv
import json
import os
import time
import urllib.request
from datetime import datetime

MB = 1024.0 * 1024.0
# Só se calcula o skew em stages cuja task mais lenta demora pelo menos isto;
# em tasks de milissegundos a razão não quer dizer nada.
MIN_TASK_S_FOR_SKEW = 1.0

OP_FIELDS = ["run_id", "label", "years", "fraction", "executors", "cores",
             "op", "status", "wall_s", "jobs", "stages", "tasks", "input_records", "input_mb",
             "shuffle_read_mb", "shuffle_write_mb", "spill_mb", "max_skew", "slowest_stage_s"]
STAGE_FIELDS = ["run_id", "label", "op", "stage_id", "attempt", "name", "tasks", "duration_s",
                "input_records", "input_mb", "shuffle_read_mb", "shuffle_write_mb", "spill_mb",
                "task_median_s", "task_max_s", "skew"]


def _parse_time(value):
    return datetime.strptime(value.replace("GMT", ""), "%Y-%m-%dT%H:%M:%S.%f")


def _duration_s(stage):
    try:
        return round((_parse_time(stage["completionTime"]) -
                      _parse_time(stage["submissionTime"])).total_seconds(), 2)
    except (KeyError, ValueError, AttributeError):
        return ""


def _append_csv(path, rows, fields):
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    new_file = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new_file:
            writer.writeheader()
        writer.writerows(rows)


class Profiler:
    def __init__(self, spark, label, metrics_dir, meta=None):
        self.sc = spark.sparkContext
        self.label = label
        self.metrics_dir = metrics_dir
        self.meta = meta or {}
        self.run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.op_rows = []
        self._op = None
        self._t0 = None
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        ui = self.sc.uiWebUrl
        self._api = f"{ui}/api/v1/applications/{self.sc.applicationId}" if ui else None

    # ------------------------------------------------------------------ medir uma operação
    def start(self, op):
        self.sc.setJobGroup(op, f"{self.label}: {op}")
        self._op = op
        print(f"\n===== {op} =====", flush=True)
        self._t0 = time.perf_counter()

    def stop(self, status="ok"):
        wall_s = round(time.perf_counter() - self._t0, 2)
        op = self._op
        stage_rows = []
        row = {"op": op, "status": status, "wall_s": wall_s}
        row.update(self._collect(op, stage_rows))
        self._save(row, stage_rows)
        print(f"[{op}] {status} em {wall_s:.1f} s", flush=True)
        self._op = None

    def run(self, op, action):
        """Alternativa a start/stop: corre a função action() e mede-a."""
        self.start(op)
        try:
            action()
        except Exception:
            self.stop("FAILED")
            raise
        self.stop()

    # ------------------------------------------------------------------ REST API do Spark UI
    def _get(self, path):
        with self._opener.open(f"{self._api}/{path}", timeout=30) as response:
            return json.load(response)

    def _jobs_of(self, op, timeout_s=15):
        time.sleep(1.0)  # o Spark UI atualiza-se com um pequeno atraso
        deadline = time.time() + timeout_s
        while True:
            jobs = [j for j in self._get("jobs") if j.get("jobGroup") == op]
            if all(j.get("status") != "RUNNING" for j in jobs) or time.time() > deadline:
                return jobs
            time.sleep(0.5)

    def _task_times(self, stage):
        try:
            q = self._get(f"stages/{stage['stageId']}/{stage['attemptId']}"
                          f"/taskSummary?quantiles=0.5,1.0")
            run_ms = q.get("executorRunTime") or q.get("duration")
            return run_ms[0] / 1000.0, run_ms[1] / 1000.0
        except Exception:
            return None, None

    def _collect(self, op, stage_rows):
        if not self._api:
            return {}
        try:
            jobs = self._jobs_of(op)
            stage_ids = {sid for job in jobs for sid in job.get("stageIds", [])}
            stages = sorted((s for s in self._get("stages")
                             if s.get("stageId") in stage_ids and s.get("status") == "COMPLETE"),
                            key=lambda s: (s["stageId"], s.get("attemptId", 0)))
        except Exception as e:
            print(f"[{op}] aviso: não consegui ler as métricas do Spark UI ({e})")
            return {}

        totals = {"jobs": len(jobs), "stages": len(stages), "tasks": 0, "input_records": 0,
                  "input_mb": 0.0,
                  "shuffle_read_mb": 0.0, "shuffle_write_mb": 0.0, "spill_mb": 0.0,
                  "max_skew": "", "slowest_stage_s": ""}
        for s in stages:
            median_s, max_s = self._task_times(s)
            skew = ""
            if median_s and max_s is not None and max_s >= MIN_TASK_S_FOR_SKEW:
                skew = round(max_s / median_s, 1)
            row = {
                "op": op,
                "stage_id": s["stageId"],
                "attempt": s.get("attemptId", 0),
                "name": s.get("name", ""),
                "tasks": s.get("numTasks", 0),
                "duration_s": _duration_s(s),
                "input_records": s.get("inputRecords", 0),
                "input_mb": round(s.get("inputBytes", 0) / MB, 1),
                "shuffle_read_mb": round(s.get("shuffleReadBytes", 0) / MB, 1),
                "shuffle_write_mb": round(s.get("shuffleWriteBytes", 0) / MB, 1),
                "spill_mb": round(s.get("diskBytesSpilled", 0) / MB, 1),
                "task_median_s": "" if median_s is None else round(median_s, 2),
                "task_max_s": "" if max_s is None else round(max_s, 2),
                "skew": skew,
            }
            stage_rows.append(row)
            totals["tasks"] += row["tasks"]
            totals["input_records"] += row["input_records"]
            for key in ("input_mb", "shuffle_read_mb", "shuffle_write_mb", "spill_mb"):
                totals[key] += row[key]
            if skew != "" and (totals["max_skew"] == "" or skew > totals["max_skew"]):
                totals["max_skew"] = skew
            if row["duration_s"] != "" and (totals["slowest_stage_s"] == "" or
                                            row["duration_s"] > totals["slowest_stage_s"]):
                totals["slowest_stage_s"] = row["duration_s"]
        for key in ("input_mb", "shuffle_read_mb", "shuffle_write_mb", "spill_mb"):
            totals[key] = round(totals[key], 1)
        return totals

    def _executors(self):
        if not self._api:
            return "", ""
        try:
            executors = [e for e in self._get("executors") if e.get("id") != "driver"]
            return len(executors), sum(e.get("totalCores", 0) for e in executors)
        except Exception:
            return "", ""

    # ------------------------------------------------------------------ guardar e resumir
    def _save(self, row, stage_rows):
        executors, cores = self._executors()
        common = dict(self.meta, run_id=self.run_id, label=self.label,
                      executors=executors, cores=cores)
        row = dict(common, **row)
        self.op_rows.append(row)
        _append_csv(os.path.join(self.metrics_dir, "operations.csv"), [row], OP_FIELDS)
        _append_csv(os.path.join(self.metrics_dir, "stages.csv"),
                    [dict(common, **s) for s in stage_rows], STAGE_FIELDS)

    def report(self):
        executors = self.op_rows[-1]["executors"] if self.op_rows else ""
        cores = self.op_rows[-1]["cores"] if self.op_rows else ""
        print(f"\n=== Resumo  label={self.label}  run_id={self.run_id}  "
              f"executors={executors}  cores={cores} ===")
        header = (f"{'op':<4} {'estado':<7} {'tempo_s':>8} {'jobs':>5} {'stages':>6} {'tasks':>6} "
                  f"{'linhas_lidas':>13} {'shuf_read_MB':>12} {'shuf_write_MB':>13} {'spill_MB':>9} {'skew':>5}")
        print(header)
        print("-" * len(header))
        for r in self.op_rows:
            print(f"{r['op']:<4} {r['status']:<7} {r['wall_s']:>8} {r.get('jobs', ''):>5} "
                  f"{r.get('stages', ''):>6} {r.get('tasks', ''):>6} {r.get('input_records', ''):>13} "
                  f"{r.get('shuffle_read_mb', ''):>12} {r.get('shuffle_write_mb', ''):>13} "
                  f"{r.get('spill_mb', ''):>9} {r.get('max_skew', ''):>5}")
        total = round(sum(r["wall_s"] for r in self.op_rows), 1)
        print(f"{'TOTAL':<12} {total:>8}")
        print(f"Métricas guardadas em {self.metrics_dir}/operations.csv e stages.csv")
