from __future__ import annotations
import re
import threading
import queue
import unicodedata
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

# ============================================================
# PROGRESO (GUI)
# ============================================================

class ProgressUI:
    def __init__(self, title="Calidad mes anterior - Progreso", size="860x540"):
        self.root = tk.Tk()
        self.root.title(title)
        self.root.geometry(size)

        self.q: "queue.Queue[dict]" = queue.Queue()

        self.status_var = tk.StringVar(value="Listo para iniciar...")
        self.lbl = ttk.Label(self.root, textvariable=self.status_var, wraplength=820)
        self.lbl.pack(padx=12, pady=(12, 6), anchor="w")

        self.pb = ttk.Progressbar(self.root, mode="determinate", maximum=100)
        self.pb.pack(padx=12, pady=(0, 10), fill="x")

        self.txt = tk.Text(self.root, height=20, wrap="word")
        self.txt.pack(padx=12, pady=(0, 12), fill="both", expand=True)
        self.txt.configure(state="disabled")

        self._indeterminate = False

    def _log_line(self, s: str) -> None:
        self.txt.configure(state="normal")
        self.txt.insert("end", s + "\n")
        self.txt.see("end")
        self.txt.configure(state="disabled")

    def _set_indeterminate(self, on: bool) -> None:
        if on and not self._indeterminate:
            self.pb.config(mode="indeterminate")
            self.pb.start(10)
            self._indeterminate = True
        elif (not on) and self._indeterminate:
            self.pb.stop()
            self.pb.config(mode="determinate")
            self._indeterminate = False

    def progress(self, stage: str, current: int | None, total: int | None, message: str) -> None:
        self.q.put({
            "type": "progress",
            "stage": stage,
            "current": current,
            "total": total,
            "message": message
        })

    def done(self, output_path: str, meta_msg: str) -> None:
        self.q.put({"type": "done", "output": output_path, "meta": meta_msg})

    def error(self, err: str) -> None:
        self.q.put({"type": "error", "error": err})

    def poll(self):
        try:
            while True:
                msg = self.q.get_nowait()

                if msg["type"] == "progress":
                    message = msg.get("message", "")
                    current = msg.get("current", None)
                    total = msg.get("total", None)

                    self.status_var.set(message)
                    self._log_line(message)

                    if total is None or total <= 0 or current is None:
                        self._set_indeterminate(True)
                    else:
                        self._set_indeterminate(False)
                        pct = int(max(0, min(100, (current / total) * 100)))
                        self.pb["value"] = pct

                elif msg["type"] == "done":
                    self._set_indeterminate(False)
                    self.pb["value"] = 100
                    self.status_var.set("✅ Finalizado.")
                    self._log_line(f"✅ Archivo generado: {msg['output']}")
                    if msg.get("meta"):
                        self._log_line(msg["meta"])
                    messagebox.showinfo("Calidad mes anterior", f"Se generó:\n{msg['output']}")

                elif msg["type"] == "error":
                    self._set_indeterminate(False)
                    self.status_var.set("❌ Error.")
                    self._log_line("❌ ERROR: " + msg["error"])
                    messagebox.showerror("Error", msg["error"])

        except queue.Empty:
            pass

        self.root.after(120, self.poll)

    def run(self):
        self.poll()
        self.root.mainloop()


# ============================================================
# MÓDULO 1 — CARGA
# ============================================================

@dataclass
class LoadResult:
    input_path: Path
    participantes: pd.DataFrame
    servicios: pd.DataFrame


def norm_col(c: str) -> str:
    s = str(c)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s


def normalize_blank_cells(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace(r"^\s*$", np.nan, regex=True)


def drop_empty_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")
    df = df.dropna(axis=1, how="all")
    return df


def parse_date_val(x) -> pd.Timestamp:
    if pd.isna(x):
        return pd.NaT

    if isinstance(x, (pd.Timestamp, datetime, date)):
        return pd.to_datetime(x, errors="coerce")

    if isinstance(x, (int, float)) and not isinstance(x, bool):
        xx = float(x)
        if 10000 < xx < 60000:
            return pd.to_datetime(xx, unit="D", origin="1899-12-30", errors="coerce")
        return pd.NaT

    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none", "nat"):
        return pd.NaT

    s = s.replace("00:00:00", "").strip()
    if "/" in s:
        return pd.to_datetime(s, dayfirst=True, errors="coerce")
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return pd.to_datetime(s, errors="coerce")
    return pd.to_datetime(s, errors="coerce")


def read_excel_all_sheets(path: Path) -> Dict[str, pd.DataFrame]:
    suffix = path.suffix.lower()
    if suffix == ".xlsx":
        return pd.read_excel(path, sheet_name=None, engine="openpyxl", dtype=object)
    if suffix == ".xls":
        try:
            return pd.read_excel(path, sheet_name=None, engine="xlrd", dtype=object)
        except Exception as e:
            raise RuntimeError(
                "El archivo es .xls. Para leerlo necesitas instalar xlrd:\n"
                "pip install xlrd\n\n"
                f"Detalle: {e}"
            )
    raise RuntimeError("Formato no soportado. Use .xlsx o .xls.")


def pick_sheet(dfs: Dict[str, pd.DataFrame], names: List[str]) -> Optional[pd.DataFrame]:
    lk = {k.lower(): k for k in dfs.keys()}
    for n in names:
        if n.lower() in lk:
            return dfs[lk[n.lower()]]
    return None


def load_file_and_base_sheets(input_path: str, progress_cb) -> LoadResult:
    path = Path(input_path).expanduser().resolve()

    progress_cb("carga", None, None, "Cargando Excel (read_excel: todas las hojas)...")
    dfs = read_excel_all_sheets(path)
    sheet_names = list(dfs.keys())
    progress_cb("carga", 1, 1, f"Excel cargado. Hojas detectadas: {len(sheet_names)}")

    total = max(1, len(sheet_names))
    progress_cb("prepro", 0, total, "Normalizando hojas (columnas/blancos/rangos)...")
    for i, sh in enumerate(sheet_names, start=1):
        df = dfs[sh]
        df.columns = [norm_col(c) for c in df.columns]
        df = normalize_blank_cells(df)
        df = drop_empty_frame(df)
        dfs[sh] = df
        progress_cb("prepro", i, total, f"Preprocesada {i}/{total}: {sh}")

    progress_cb("base", None, None, "Detectando hojas base: PARTICIPANTES y SERVICIOS...")
    participantes = pick_sheet(dfs, ["participantes", "participants", "participant"])
    servicios = pick_sheet(dfs, ["servicios", "services", "service"])

    if participantes is None or servicios is None:
        raise RuntimeError("No encontré hojas base 'participantes' y 'servicios'. Revisa nombres de hojas.")

    progress_cb("base", 1, 1, "Hojas base encontradas. Continuando a chequeo de calidad...")
    return LoadResult(input_path=path, participantes=participantes, servicios=servicios)


# ============================================================
# MÓDULO 2 — CALIDAD + DOS MÉTRICAS + DEPARTAMENTO
# ============================================================

def month_bounds_previous(ref_date: date) -> Tuple[pd.Timestamp, pd.Timestamp, str]:
    first_this_month = pd.Timestamp(ref_date.replace(day=1))
    last_prev_month = first_this_month - pd.Timedelta(days=1)
    first_prev_month = pd.Timestamp(last_prev_month.date().replace(day=1))
    start = first_prev_month.normalize()
    end = pd.Timestamp(last_prev_month.date()) + pd.Timedelta(hours=23, minutes=59, seconds=59)
    label = first_prev_month.strftime("%Y-%m")
    return start, end, label


def is_filled(series: pd.Series) -> pd.Series:
    filled = ~series.isna()
    as_str = series.astype(str)
    filled &= ~(as_str.str.strip().eq("") | as_str.str.lower().isin(["nan", "none", "nat"]))
    return filled


def find_first_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        cc = norm_col(c)
        if cc in df.columns:
            return cc
    return None


def slug_field(logical: str) -> str:
    s = norm_col(logical)
    s = re.sub(r"[^A-Z0-9]+", "_", s).strip("_")
    return s[:60]


def normalize_cat_series(s: pd.Series, fill: str) -> pd.Series:
    out = s.astype(str).str.strip()
    out = out.replace({"": np.nan, "NAN": np.nan, "NONE": np.nan, "NAT": np.nan})
    out = out.fillna(fill)
    return out


def build_required_maps() -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    # PARTICIPANTES (mínimos) — se agregan DEPARTAMENTO/MUNICIPIO (RESIDENCIA)
    req_part = {
        "TIPO PARTICIPANTE": ["TIPO PARTICIPANTE"],
        "NOMBRES Y APELLIDOS DEL PARTICIPANTE": ["NOMBRES Y APELLIDOS DEL PARTICIPANTE", "NOMBRE COMPLETO", "NOMBRES Y APELLIDOS"],
        "TIPO DE DOCUMENTO": ["TIPO DE DOCUMENTO"],
        "NUMERO DE DOCUMENTO": ["NUMERO DE DOCUMENTO", "NÚMERO DE DOCUMENTO", "DOCUMENTO", "NUM DOCUMENTO"],
        "FECHA DE NACIMIENTO": ["FECHA DE NACIMIENTO"],
        "SEXO DEL PARTICIPANTE": ["SEXO DEL PARTICIPANTE", "SEXO"],
        "GENERO DEL PARTICIPANTE": ["GENERO DEL PARTICIPANTE", "GÉNERO DEL PARTICIPANTE", "GENERO", "GÉNERO"],
        "DEFENSOR/A": ["DEFENSOR/A", "DEFENSOR", "DEFENSORA", "DEFENSOR(A)"],
        "FECHA APERTURA PARD": ["FECHA APERTURA PARD", "FECHA DE APERTURA PARD"],

        # ✅ NUEVOS mínimos obligatorios
        "DEPARTAMENTO (RESIDENCIA)": ["DEPARTAMENTO (RESIDENCIA)", "DEPARTAMENTO RESIDENCIA", "DEPARTAMENTO"],
        "MUNICIPIO (RESIDENCIA)": ["MUNICIPIO (RESIDENCIA)", "MUNICIPIO RESIDENCIA", "MUNICIPIO"],
    }

    # SERVICIOS (mínimos) — NO incluye salida/motivo salida
    req_svc = {
        "PROGRAMA": ["PROGRAMA"],
        "GRUPO DE SERVICIO (FEDERATIVOS)": ["GRUPO DE SERVICIO (FEDERATIVOS)", "GRUPO DE SERVICIO FEDERATIVOS", "GRUPO DE SERVICIO"],
        "SERVICIO (FEDERATIVOS)": ["SERVICIO (FEDERATIVOS)", "SERVICIO FEDERATIVOS", "SERVICIO"],
        "SUB-SERVICIO": ["SUB-SERVICIO", "SUB SERVICIO", "SUBSERVICIO"],
        "FAMILIA DE ORIGEN": ["FAMILIA DE ORIGEN", "ID FAMILIA DE ORIGEN EN DFE", "ID FAMILIA DE ORIGEN"],
        "FAMILIA / CASA DE ACOGIDA": ["FAMILIA / CASA DE ACOGIDA", "ID FAMILIA / CASA DE ACOGIDA", "ID FAMILIA CASA DE ACOGIDA"],
        "FECHA DE INGRESO A ALDEAS": ["FECHA DE INGRESO A ALDEAS"],
        "FECHA DE ENTRADA AL SERVICIO": ["FECHA DE ENTRADA AL SERVICIO"],
        "MOTIVO DE ENTRADA AL SERVICIO": ["MOTIVO DE ENTRADA AL SERVICIO", "MOTIVO DE INGRESO AL SERVICIO"],
    }

    return req_part, req_svc


def compute_presence_masks(
    df: pd.DataFrame,
    required_map: Dict[str, List[str]]
) -> Tuple[Dict[str, Optional[str]], Dict[str, pd.Series], pd.Series]:
    col_map: Dict[str, Optional[str]] = {}
    miss_mask: Dict[str, pd.Series] = {}

    for logical, candidates in required_map.items():
        col = find_first_col(df, candidates)
        col_map[logical] = col
        if col is None:
            miss_mask[logical] = pd.Series([True] * len(df), index=df.index)
        else:
            miss_mask[logical] = ~is_filled(df[col])

    ok = pd.Series([True] * len(df), index=df.index)
    for logical in required_map.keys():
        ok &= ~miss_mask[logical]

    return col_map, miss_mask, ok


def missing_list_per_row(miss_mask: Dict[str, pd.Series], order: List[str]) -> pd.Series:
    idx = next(iter(miss_mask.values())).index if miss_mask else pd.Index([])
    out = []
    for i in range(len(idx)):
        miss = [k for k in order if bool(miss_mask[k].iat[i])]
        out.append("; ".join(miss))
    return pd.Series(out, index=idx)


def pct_campos_ok(missmask: Dict[str, pd.Series], idx: pd.Index, fields: List[str]) -> float:
    total_cells = len(idx) * len(fields)
    if total_cells == 0:
        return float("nan")
    missing_cells = 0
    for f in fields:
        missing_cells += int(missmask[f].loc[idx].sum())
    return round((1 - (missing_cells / total_cells)) * 100, 2)


def n_missing_cells(missmask: Dict[str, pd.Series], idx: pd.Index, fields: List[str]) -> int:
    missing_cells = 0
    for f in fields:
        missing_cells += int(missmask[f].loc[idx].sum())
    return missing_cells


def default_output_path(input_path: Path, month_label: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return input_path.with_name(f"CALIDAD_{month_label}_{input_path.stem}_{ts}.xlsx")


def quality_previous_month_by_program(
    participantes: pd.DataFrame,
    servicios: pd.DataFrame,
    ref_date: date,
    progress_cb
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    req_part, req_svc = build_required_maps()

    progress_cb("setup", None, None, "Identificando columnas clave (IDs, programa, fecha de entrada)...")
    pid_part = find_first_col(participantes, ["ID DEL PARTICIPANTE (PRIMARIA)", "ID DEL PARTICIPANTE", "ID PARTICIPANTE"])
    pid_svc = find_first_col(servicios, ["ID DEL PARTICIPANTE", "ID PARTICIPANTE"])
    sid_svc = find_first_col(servicios, ["ID SERVICIO", "ID DEL SERVICIO", "SERVICIO ID"])
    prog_col = find_first_col(servicios, ["PROGRAMA"])
    entrada_col = find_first_col(servicios, ["FECHA DE ENTRADA AL SERVICIO"])

    if pid_part is None or pid_svc is None:
        raise RuntimeError("No pude identificar columnas de ID participante para unir PARTICIPANTES ↔ SERVICIOS.")
    if sid_svc is None:
        raise RuntimeError("No pude identificar la columna ID SERVICIO en SERVICIOS.")
    if prog_col is None:
        raise RuntimeError("No encontré la columna PROGRAMA en SERVICIOS.")
    if entrada_col is None:
        raise RuntimeError("No encontré FECHA DE ENTRADA AL SERVICIO en SERVICIOS (requerida para cohorte).")

    start_prev, end_prev, month_label = month_bounds_previous(ref_date)
    progress_cb("cohorte", None, None, f"Filtrando cohorte mes anterior ({month_label}): {start_prev.date()} a {end_prev.date()}...")

    svc = servicios.copy()
    svc[entrada_col + "_DT"] = svc[entrada_col].map(parse_date_val)

    svc_cohort = svc.loc[
        svc[entrada_col + "_DT"].notna() &
        (svc[entrada_col + "_DT"] >= start_prev) &
        (svc[entrada_col + "_DT"] <= end_prev)
    ].copy()

    # trazabilidad + fix índice
    svc_cohort["_ROW_ORIG_SERVICIOS"] = svc_cohort.index
    svc_cohort = svc_cohort.reset_index(drop=True)

    progress_cb("cohorte", 1, 1, f"Cohorte filtrada. Filas encontradas: {len(svc_cohort)}")

    if len(svc_cohort) == 0:
        resumen = pd.DataFrame([{
            "PROGRAMA": "(SIN REGISTROS MES ANTERIOR)",
            "DEPARTAMENTO_RESIDENCIA": "N/A",
            "N_FILAS_COHORTE": 0,
            "PCT_FILAS_OK_PARTICIPANTE": np.nan,
            "PCT_FILAS_OK_SERVICIO": np.nan,
            "PCT_FILAS_OK_TOTAL": np.nan,
            "PCT_CAMPOS_OK_PARTICIPANTE": np.nan,
            "PCT_CAMPOS_OK_SERVICIO": np.nan,
            "PCT_CAMPOS_OK_TOTAL": np.nan,
            "MES_EVALUADO": month_label,
            "RANGO_EVALUADO": f"{start_prev.date()} a {end_prev.date()}",
            "FECHA_EJECUCION": str(ref_date),
        }])
        detalle = pd.DataFrame()
        meta = {"month_label": month_label, "range_start": str(start_prev.date()), "range_end": str(end_prev.date()),
                "n_filas_cohorte": 0, "n_filas_incompletas": 0}
        return resumen, detalle, meta

    progress_cb("eval", None, None, "Evaluando completitud mínima en SERVICIOS (por fila)...")
    svc_colmap, svc_missmask, svc_ok = compute_presence_masks(svc_cohort, req_svc)

    progress_cb("eval", None, None, "Evaluando completitud mínima en PARTICIPANTES (por participante)...")
    part_colmap, part_missmask, part_ok = compute_presence_masks(participantes, req_part)

    # Map ID participante -> ok participante
    part_ids = participantes[pid_part].astype(str).str.strip()
    part_ok_map = pd.Series(part_ok.values, index=part_ids).to_dict()

    # Participantes mínimos para merge (para ver campos)
    part_keep_cols = [pid_part] + [c for c in part_colmap.values() if c is not None]
    part_keep_cols = list(dict.fromkeys(part_keep_cols))
    part_min = participantes[part_keep_cols].copy()
    part_min["_PID_KEY"] = participantes[pid_part].astype(str).str.strip()

    svc_cohort["_PID_KEY"] = svc_cohort[pid_svc].astype(str).str.strip()

    progress_cb("merge", None, None, "Uniendo SERVICIOS cohorte con mínimos de PARTICIPANTES (para departamento/municipio y detalle)...")
    merged = svc_cohort.merge(
        part_min.drop(columns=[pid_part], errors="ignore"),
        on="_PID_KEY",
        how="left"
    )

    ok_part_for_svc = merged["_PID_KEY"].map(lambda x: bool(part_ok_map.get(x, False)))

    # Detectar existencia del participante
    any_part_field = None
    for logical, actual in part_colmap.items():
        if actual is not None:
            any_part_field = actual
            break
    if any_part_field is None:
        participant_exists = pd.Series([False] * len(merged), index=merged.index)
    else:
        participant_exists = merged[any_part_field].notna() | merged["_PID_KEY"].isin(set(part_ids.dropna()))

    # Campos mínimos
    part_fields = list(req_part.keys())
    svc_fields = list(req_svc.keys())

    # Missing mask participantes “alineado por fila de servicio”
    part_missmask_on_svc: Dict[str, pd.Series] = {}
    for logical, actual in part_colmap.items():
        if actual is None:
            part_missmask_on_svc[logical] = pd.Series([True] * len(merged), index=merged.index)
        else:
            part_missmask_on_svc[logical] = ~is_filled(merged[actual])

    for logical in part_missmask_on_svc.keys():
        part_missmask_on_svc[logical] = part_missmask_on_svc[logical] | (~participant_exists)

    part_missing_list = missing_list_per_row(part_missmask_on_svc, part_fields)
    part_missing_list = part_missing_list.where(
        participant_exists,
        "PARTICIPANTE_NO_ENCONTRADO; " + part_missing_list
    )
    svc_missing_list = missing_list_per_row(svc_missmask, svc_fields)

    # Métrica estricta por fila
    ok_total = svc_ok & ok_part_for_svc
    n_incompletas = int((~ok_total).sum())

    # --------------------------------------------------------
    # DEPARTAMENTO (para filtrar en ambas pestañas)
    # --------------------------------------------------------
    dep_logical = "DEPARTAMENTO (RESIDENCIA)"
    mun_logical = "MUNICIPIO (RESIDENCIA)"

    dep_actual = part_colmap.get(dep_logical)
    mun_actual = part_colmap.get(mun_logical)

    if dep_actual is not None and dep_actual in merged.columns:
        dep_series = merged[dep_actual]
    else:
        dep_series = pd.Series([np.nan] * len(merged), index=merged.index)

    if mun_actual is not None and mun_actual in merged.columns:
        mun_series = merged[mun_actual]
    else:
        mun_series = pd.Series([np.nan] * len(merged), index=merged.index)

    dep_norm = normalize_cat_series(dep_series, "SIN_DEPARTAMENTO")
    mun_norm = normalize_cat_series(mun_series, "SIN_MUNICIPIO")

    # Si no existe participante, marcamos el depto para gestión
    dep_norm = dep_norm.where(participant_exists, "PARTICIPANTE_NO_ENCONTRADO")
    mun_norm = mun_norm.where(participant_exists, "PARTICIPANTE_NO_ENCONTRADO")

    # Programa normalizado
    prog_norm = normalize_cat_series(merged[prog_col], "SIN_PROGRAMA")

    # ========================================================
    # RESUMEN POR PROGRAMA + DEPARTAMENTO (para filtrar Caribe)
    # ========================================================
    progress_cb("resumen", None, None, "Calculando resumen por PROGRAMA + DEPARTAMENTO (incluye métricas por fila y por campos mínimos)...")

    grouped = pd.DataFrame({
        "PROGRAMA": prog_norm,
        "DEPARTAMENTO_RESIDENCIA": dep_norm
    }).groupby(["PROGRAMA", "DEPARTAMENTO_RESIDENCIA"]).groups

    rows = []
    total_groups = max(1, len(grouped))

    for i, ((prog, dep), idx) in enumerate(grouped.items(), start=1):
        n = len(idx)

        # Por FILA
        pct_filas_part = float(ok_part_for_svc.loc[idx].mean() * 100)
        pct_filas_svc  = float(svc_ok.loc[idx].mean() * 100)
        pct_filas_tot  = float(ok_total.loc[idx].mean() * 100)

        # Por CAMPOS
        pct_campos_part = pct_campos_ok(part_missmask_on_svc, idx, part_fields)
        pct_campos_svc  = pct_campos_ok(svc_missmask, idx, svc_fields)

        miss_part = n_missing_cells(part_missmask_on_svc, idx, part_fields)
        miss_svc  = n_missing_cells(svc_missmask, idx, svc_fields)
        total_cells = n * (len(part_fields) + len(svc_fields))
        pct_campos_tot = round((1 - ((miss_part + miss_svc) / total_cells)) * 100, 2) if total_cells else float("nan")

        rows.append({
            "PROGRAMA": prog,
            "DEPARTAMENTO_RESIDENCIA": dep,
            "N_FILAS_COHORTE": n,

            "PCT_FILAS_OK_PARTICIPANTE": round(pct_filas_part, 2),
            "PCT_FILAS_OK_SERVICIO": round(pct_filas_svc, 2),
            "PCT_FILAS_OK_TOTAL": round(pct_filas_tot, 2),

            "PCT_CAMPOS_OK_PARTICIPANTE": pct_campos_part,
            "PCT_CAMPOS_OK_SERVICIO": pct_campos_svc,
            "PCT_CAMPOS_OK_TOTAL": pct_campos_tot,

            "MES_EVALUADO": month_label,
            "RANGO_EVALUADO": f"{start_prev.date()} a {end_prev.date()}",
            "FECHA_EJECUCION": str(ref_date),
        })

        progress_cb("resumen", i, total_groups, f"Resumen {i}/{total_groups}: {prog} | {dep}")

    resumen = pd.DataFrame(rows).sort_values(
        ["PROGRAMA", "DEPARTAMENTO_RESIDENCIA"],
        ascending=[True, True]
    )

    # TOTAL_GENERAL (en una fila)
    pct_filas_part = round(float(ok_part_for_svc.mean() * 100), 2)
    pct_filas_svc  = round(float(svc_ok.mean() * 100), 2)
    pct_filas_tot  = round(float(ok_total.mean() * 100), 2)

    pct_campos_part = pct_campos_ok(part_missmask_on_svc, merged.index, part_fields)
    pct_campos_svc  = pct_campos_ok(svc_missmask, merged.index, svc_fields)

    miss_part = n_missing_cells(part_missmask_on_svc, merged.index, part_fields)
    miss_svc  = n_missing_cells(svc_missmask, merged.index, svc_fields)
    total_cells = len(merged) * (len(part_fields) + len(svc_fields))
    pct_campos_tot = round((1 - ((miss_part + miss_svc) / total_cells)) * 100, 2) if total_cells else float("nan")

    resumen_total = pd.DataFrame([{
        "PROGRAMA": "TOTAL_GENERAL",
        "DEPARTAMENTO_RESIDENCIA": "TODOS",
        "N_FILAS_COHORTE": int(len(merged)),

        "PCT_FILAS_OK_PARTICIPANTE": pct_filas_part,
        "PCT_FILAS_OK_SERVICIO": pct_filas_svc,
        "PCT_FILAS_OK_TOTAL": pct_filas_tot,

        "PCT_CAMPOS_OK_PARTICIPANTE": pct_campos_part,
        "PCT_CAMPOS_OK_SERVICIO": pct_campos_svc,
        "PCT_CAMPOS_OK_TOTAL": pct_campos_tot,

        "MES_EVALUADO": month_label,
        "RANGO_EVALUADO": f"{start_prev.date()} a {end_prev.date()}",
        "FECHA_EJECUCION": str(ref_date),
    }])

    resumen = pd.concat([resumen, resumen_total], ignore_index=True)

    # ========================================================
    # DETALLE COHORTE (todas las filas) + señalización faltantes
    # ========================================================
    progress_cb("detalle", None, None, "Construyendo detalle_cohorte (todas las filas, con señalización de faltantes y filtro por departamento)...")

    def get_part_val(df: pd.DataFrame, logical: str) -> pd.Series:
        actual = part_colmap.get(logical)
        if actual is None or actual not in df.columns:
            return pd.Series([np.nan] * len(df), index=df.index)
        return df[actual]

    def get_svc_val(df: pd.DataFrame, logical: str) -> pd.Series:
        actual = svc_colmap.get(logical)
        if actual is None or actual not in df.columns:
            return pd.Series([np.nan] * len(df), index=df.index)
        return df[actual]

    # Conteos por fila
    n_falt_part = sum(part_missmask_on_svc[f].astype(int) for f in part_fields)
    n_falt_svc = sum(svc_missmask[f].astype(int) for f in svc_fields)

    detalle = pd.DataFrame({
        "PROGRAMA": prog_norm,
        "DEPARTAMENTO_RESIDENCIA": dep_norm,   # ✅ filtro directo
        "MUNICIPIO_RESIDENCIA": mun_norm,      # útil para gestión

        "ROW_ORIG_SERVICIOS": merged["_ROW_ORIG_SERVICIOS"],
        "ID SERVICIO": merged[sid_svc].astype(str).str.strip(),
        "ID DEL PARTICIPANTE": merged[pid_svc].astype(str).str.strip(),
        "FECHA DE ENTRADA AL SERVICIO": merged[entrada_col],

        "OK_PARTICIPANTE": ok_part_for_svc.values,
        "OK_SERVICIO": svc_ok.values,
        "OK_TOTAL": ok_total.values,
        "FALTANTES_PARTICIPANTE": part_missing_list.values,
        "FALTANTES_SERVICIO": svc_missing_list.values,

        "N_FALTANTES_PARTICIPANTE": n_falt_part.values,
        "N_FALTANTES_SERVICIO": n_falt_svc.values,
    })
    detalle["N_FALTANTES_TOTAL"] = detalle["N_FALTANTES_PARTICIPANTE"] + detalle["N_FALTANTES_SERVICIO"]

    # Valores mínimos (para diligenciar)
    for logical in part_fields:
        detalle[f"P_{logical}"] = get_part_val(merged, logical)
    for logical in svc_fields:
        detalle[f"S_{logical}"] = get_svc_val(merged, logical)

    # Banderas por campo (para filtros rápidos)
    for logical in part_fields:
        detalle[f"MISS_P_{slug_field(logical)}"] = part_missmask_on_svc[logical].values
    for logical in svc_fields:
        detalle[f"MISS_S_{slug_field(logical)}"] = svc_missmask[logical].values

    # Orden: primero lo más crítico (no OK total y más faltantes)
    detalle = detalle.sort_values(["OK_TOTAL", "N_FALTANTES_TOTAL"], ascending=[True, False])

    meta = {
        "month_label": month_label,
        "range_start": str(start_prev.date()),
        "range_end": str(end_prev.date()),
        "entrada_col_usada": entrada_col,
        "n_filas_cohorte": int(len(merged)),
        "n_filas_incompletas": n_incompletas,
    }

    progress_cb("fin_eval", 1, 1, f"Evaluación terminada. Filas NO OK total: {meta['n_filas_incompletas']} / {meta['n_filas_cohorte']}")
    return resumen, detalle, meta


def write_report_excel(output_path: Path, resumen: pd.DataFrame, detalle: pd.DataFrame, progress_cb) -> None:
    progress_cb("write", 0, 2, "Escribiendo Excel: resumen_por_programa...")
    with pd.ExcelWriter(output_path, engine="openpyxl") as w:
        resumen.to_excel(w, index=False, sheet_name="resumen_por_programa")
        progress_cb("write", 1, 2, "Escribiendo Excel: detalle_cohorte...")
        detalle.to_excel(w, index=False, sheet_name="detalle_cohorte")
        progress_cb("write", 2, 2, "Aplicando filtros/formato y guardando...")

        ws_sum = w.sheets["resumen_por_programa"]
        ws_det = w.sheets["detalle_cohorte"]

        ws_sum.freeze_panes = "A2"
        ws_sum.auto_filter.ref = ws_sum.dimensions

        ws_det.freeze_panes = "A2"
        ws_det.auto_filter.ref = ws_det.dimensions

        # Resaltar filas donde OK_TOTAL = FALSE en detalle_cohorte
        try:
            from openpyxl.styles import PatternFill
            from openpyxl.formatting.rule import FormulaRule
            from openpyxl.utils import get_column_letter

            header = [cell.value for cell in ws_det[1]]
            ok_idx = header.index("OK_TOTAL") + 1
            ok_col = get_column_letter(ok_idx)

            last_col = get_column_letter(ws_det.max_column)
            last_row = ws_det.max_row

            fill = PatternFill(start_color="FFFFC7CE", end_color="FFFFC7CE", fill_type="solid")
            rule = FormulaRule(formula=[f"${ok_col}2=FALSE"], fill=fill)

            ws_det.conditional_formatting.add(f"A2:{last_col}{last_row}", rule)
        except Exception:
            pass


# ============================================================
# ORQUESTACIÓN
# ============================================================

def run_pipeline(input_xlsx: str, progress_cb) -> Tuple[Path, str]:
    progress_cb("inicio", None, None, "Iniciando proceso...")

    lr = load_file_and_base_sheets(input_xlsx, progress_cb)

    resumen, detalle, meta = quality_previous_month_by_program(
        participantes=lr.participantes,
        servicios=lr.servicios,
        ref_date=date.today(),
        progress_cb=progress_cb
    )

    out = default_output_path(lr.input_path, meta["month_label"])
    write_report_excel(out, resumen, detalle, progress_cb)

    meta_msg = (
        f"Mes evaluado: {meta['month_label']} ({meta['range_start']} a {meta['range_end']})\n"
        f"Filas cohorte (SERVICIOS): {meta['n_filas_cohorte']}\n"
        f"Filas NO OK total: {meta['n_filas_incompletas']}\n\n"
        f"Filtros clave:\n"
        f"  - resumen_por_programa: filtrar DEPARTAMENTO_RESIDENCIA\n"
        f"  - detalle_cohorte: filtrar DEPARTAMENTO_RESIDENCIA y OK_TOTAL\n"
        f"Nota: DEPARTAMENTO/MUNICIPIO (RESIDENCIA) son mínimos obligatorios en PARTICIPANTES."
    )

    progress_cb("final", 1, 1, "✅ Listo. Archivo generado.")
    return out, meta_msg


# ============================================================
# MAIN — Selección archivo + hilo + UI
# ============================================================

def main():
    ui = ProgressUI()

    ui.root.withdraw()
    input_xlsx = filedialog.askopenfilename(
        title="Seleccione el archivo Excel a evaluar",
        filetypes=[("Excel files", "*.xlsx;*.xls")]
    )
    if not input_xlsx:
        ui.root.destroy()
        return
    ui.root.deiconify()

    ui._log_line(f"Archivo seleccionado: {input_xlsx}")
    ui.progress("inicio", None, None, "Preparando ejecución...")

    def worker():
        try:
            out, meta_msg = run_pipeline(input_xlsx, ui.progress)
            ui.done(str(out), meta_msg)
        except Exception as e:
            ui.error(str(e))

    t = threading.Thread(target=worker, daemon=True)
    t.start()

    ui.run()


if __name__ == "__main__":
    main()
