import os
import sqlite3
from datetime import date, datetime
import pandas as pd
import streamlit as st

# =========================
# Config
# =========================
EXCEL_PATH = "Prueba Tecnica2_LIMPIO.xlsx"
DB_PATH = "capacitaciones.db"

st.set_page_config(page_title="Capacitaciones - RRHH", layout="wide")


# =========================
# Helpers
# =========================
def month_start(d: date) -> str:
    return date(d.year, d.month, 1).isoformat()


def safe_is_p(val) -> bool:
    """Detecta si una celda indica programación/aplica (ej: 'P')."""
    if val is None or (isinstance(val, float) and pd.isna(val)) or pd.isna(val):
        return False
    s = str(val).strip().upper()
    return s == "P" or s.startswith("P")


def conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    with conn() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS personas(
            id_persona TEXT PRIMARY KEY,
            nombre TEXT,
            cargo TEXT,
            proceso TEXT,
            lugar_trabajo TEXT
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS eventos(
            id_evento TEXT PRIMARY KEY,
            tipo_evento TEXT,
            tema_general TEXT,
            nombre_evento TEXT,
            esquema_evento TEXT,
            duracion_horas REAL
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS programacion(
            id_evento TEXT,
            cargo TEXT,
            mes TEXT,
            PRIMARY KEY (id_evento, cargo, mes)
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS registro(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_persona TEXT,
            id_evento TEXT,
            fecha_ejecucion TEXT,
            horas REAL,
            resultado TEXT
        )""")
        c.commit()


def read_df(query: str, params=()):
    with conn() as c:
        return pd.read_sql_query(query, c, params=params)


def exec_sql(query: str, params=()):
    with conn() as c:
        c.execute(query, params)
        c.commit()


def normalize_cols(df: pd.DataFrame):
    df.columns = [str(c).strip() for c in df.columns]
    return df


def apply_colmap_fuzzy(df: pd.DataFrame, colmap: dict, required: list, table_name: str):
    df = normalize_cols(df)

    def key(x):
        return str(x).strip().lower()

    df_cols_map = {key(c): c for c in df.columns}
    cmap_norm = {key(k): v for k, v in colmap.items()}

    rename = {}
    for k_norm, dst in cmap_norm.items():
        if k_norm in df_cols_map:
            rename[df_cols_map[k_norm]] = dst

    df = df.rename(columns=rename)

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"En '{table_name}' faltan columnas requeridas: {missing}. "
            f"Columnas detectadas: {list(df.columns)}"
        )
    return df


def upsert_many_personas(df: pd.DataFrame):
    with conn() as c:
        for _, r in df.iterrows():
            c.execute("""
            INSERT INTO personas(id_persona, nombre, cargo, proceso, lugar_trabajo)
            VALUES(?,?,?,?,?)
            ON CONFLICT(id_persona) DO UPDATE SET
              nombre=excluded.nombre,
              cargo=excluded.cargo,
              proceso=excluded.proceso,
              lugar_trabajo=excluded.lugar_trabajo
            """, (str(r["id_persona"]), r["nombre"], r["cargo"], r["proceso"], r["lugar_trabajo"]))
        c.commit()


def upsert_many_eventos(df: pd.DataFrame):
    with conn() as c:
        for _, r in df.iterrows():
            c.execute("""
            INSERT INTO eventos(id_evento, tipo_evento, tema_general, nombre_evento, esquema_evento, duracion_horas)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(id_evento) DO UPDATE SET
              tipo_evento=excluded.tipo_evento,
              tema_general=excluded.tema_general,
              nombre_evento=excluded.nombre_evento,
              esquema_evento=excluded.esquema_evento,
              duracion_horas=excluded.duracion_horas
            """, (
                str(r["id_evento"]),
                r["tipo_evento"],
                r["tema_general"],
                r["nombre_evento"],
                r["esquema_evento"],
                float(r["duracion_horas"])
            ))
        c.commit()


def upsert_many_programacion(df: pd.DataFrame):
    if df is None or len(df) == 0:
        return
    with conn() as c:
        for _, r in df.iterrows():
            c.execute("""
            INSERT OR IGNORE INTO programacion(id_evento, cargo, mes)
            VALUES(?,?,?)
            """, (str(r["id_evento"]), str(r["cargo"]), str(r["mes"])))
        c.commit()


# =========================
# Import Excel
# =========================
def parse_programacion_from_matriz(xl: pd.ExcelFile) -> pd.DataFrame:
    """
    Matriz Programación:
    - Filas: eventos (Id Evento, Tema General, Evento Formativo, Tipo de Evento, etc.)
    - Columnas: meses (datetime 2022-01-01...) con 'P' indicando mes programado
    - Columnas: cargos (texto) con 'P' indicando que el evento aplica a ese cargo
    Regla: si (cargo=P) y (mes=P) => programacion(id_evento, cargo, mes)
    """
    dfm = pd.read_excel(xl, sheet_name="Matriz Programación")
    dfm = normalize_cols(dfm)

    # Asegurar Id Evento (viene como 'Id Evento')
    if "Id Evento" not in dfm.columns:
        raise ValueError("No encontré 'Id Evento' en Matriz Programación.")

    # Meses = columnas tipo datetime
    month_cols = [c for c in dfm.columns if isinstance(c, (datetime, pd.Timestamp))]
    if len(month_cols) == 0:
        # fallback: intenta parsear columnas que parezcan fechas
        for c in dfm.columns:
            try:
                dt = pd.to_datetime(c, errors="raise")
                month_cols.append(dt.to_pydatetime())
            except Exception:
                pass

    # Cargos = columnas string (no Unnamed, no columnas base)
    base_cols = {"Id Evento", "Tema General", "Evento Formativo", "Tipo de Evento"}
    cargo_cols = [
        c for c in dfm.columns
        if isinstance(c, str)
        and c not in base_cols
        and not c.lower().startswith("unnamed")
        and c.strip() != ""
    ]

    records = []
    for _, row in dfm.iterrows():
        id_evento = row.get("Id Evento", None)
        if pd.isna(id_evento):
            continue
        id_evento = str(id_evento).strip()

        # Cargos aplicables
        cargos_aplican = []
        for cc in cargo_cols:
            if safe_is_p(row.get(cc, None)):
                cargos_aplican.append(str(cc).strip())

        if len(cargos_aplican) == 0:
            continue

        # Meses programados
        meses_programados = []
        for mc in month_cols:
            if safe_is_p(row.get(mc, None)):
                # mc ya es datetime/timestamp
                dt = pd.to_datetime(mc).date()
                meses_programados.append(month_start(dt))

        if len(meses_programados) == 0:
            continue

        for cargo in cargos_aplican:
            for mes in meses_programados:
                records.append({"id_evento": id_evento, "cargo": cargo, "mes": mes})

    if len(records) == 0:
        return pd.DataFrame(columns=["id_evento", "cargo", "mes"])

    dfp = pd.DataFrame(records).drop_duplicates()
    return dfp


def import_from_excel(path: str):
    SHEET_PERSONAS = "Empleados"
    SHEET_EVENTOS = "Eventos formativos"
    SHEET_REGISTRO = "Registro Eventos Formativos"

    COLMAP_PERSONAS = {
        "Id persona": "id_persona",
        "Id Persona": "id_persona",
        "IdPersona": "id_persona",
        "ID Persona": "id_persona",
        "Nombre Completo": "nombre",
        "Nombre": "nombre",
        "Cargo": "cargo",
        "cargo": "cargo",
        "Proceso": "proceso",
        "proceso": "proceso",
        "Lugar de Trabajo": "lugar_trabajo",
        "Lugar de trabajo": "lugar_trabajo",
        "lugar_trabajo": "lugar_trabajo",
        "LugarTrabajo": "lugar_trabajo",
    }
    REQ_PERSONAS = ["id_persona", "nombre", "cargo", "proceso", "lugar_trabajo"]

    COLMAP_EVENTOS = {
        "Id Evento": "id_evento",
        "Id evento": "id_evento",
        "IdEvento": "id_evento",
        "Tema General": "tema_general",
        "Tema general": "tema_general",
        "TemaGeneral": "tema_general",
        "Evento Formativo": "nombre_evento",
        "Nombre del Evento": "nombre_evento",
        "NombreEvento": "nombre_evento",
        "Esquema de Evento": "esquema_evento",
        "Esquema evento": "esquema_evento",
        "EsquemaEvento": "esquema_evento",
    }
    REQ_EVENTOS = ["id_evento", "tema_general", "nombre_evento", "esquema_evento"]

    COLMAP_REG = {
        "Id Persona": "id_persona",
        "Id persona": "id_persona",
        "IdPersona": "id_persona",
        "Id Evento": "id_evento",
        "Id evento": "id_evento",
        "IdEvento": "id_evento",
        "Fecha": "fecha_ejecucion",
        "Fecha Ejecución": "fecha_ejecucion",
        "FechaEjecucion": "fecha_ejecucion",
        "Horas": "horas",
        "Duración": "horas",
        "Resultado": "resultado",
        "Aprobó/Reprobó": "resultado",
    }
    REQ_REG_MIN = ["id_persona", "id_evento"]

    xl = pd.ExcelFile(path)

    # Personas
    dfp = pd.read_excel(xl, sheet_name=SHEET_PERSONAS)
    dfp = apply_colmap_fuzzy(dfp, COLMAP_PERSONAS, REQ_PERSONAS, "Personas")
    dfp = dfp[REQ_PERSONAS].copy()
    dfp["id_persona"] = dfp["id_persona"].astype(str).str.strip()
    dfp = dfp.dropna(subset=["id_persona"])
    dfp = dfp[dfp["id_persona"] != ""]

    # Eventos (la hoja real trae 'Esquema de Evento' como CAPACITACION)
    dfe = pd.read_excel(xl, sheet_name=SHEET_EVENTOS)
    dfe = apply_colmap_fuzzy(dfe, COLMAP_EVENTOS, REQ_EVENTOS, "Eventos")

    # Defaults / limpieza
    dfe["tipo_evento"] = "CAPACITACIÓN"
    if "duracion_horas" not in dfe.columns:
        dfe["duracion_horas"] = 1.0
    dfe["duracion_horas"] = pd.to_numeric(dfe["duracion_horas"], errors="coerce").fillna(1.0)

    dfe = dfe[REQ_EVENTOS + ["tipo_evento", "duracion_horas"]].copy()
    dfe["id_evento"] = dfe["id_evento"].astype(str).str.strip()
    dfe = dfe.dropna(subset=["id_evento"])
    dfe = dfe[dfe["id_evento"] != ""]

    # Programación desde Matriz
    dfprog = parse_programacion_from_matriz(xl)

    # Registro (opcional)
    dfr = pd.read_excel(xl, sheet_name=SHEET_REGISTRO)
    dfr = normalize_cols(dfr)
    rename_reg = {k: v for k, v in COLMAP_REG.items() if k in dfr.columns}
    dfr = dfr.rename(columns=rename_reg)

    if all(c in dfr.columns for c in REQ_REG_MIN):
        if "fecha_ejecucion" not in dfr.columns:
            dfr["fecha_ejecucion"] = pd.NaT
        if "horas" not in dfr.columns:
            dfr["horas"] = None
        if "resultado" not in dfr.columns:
            dfr["resultado"] = None

        dfr = dfr[["id_persona", "id_evento", "fecha_ejecucion", "horas", "resultado"]].copy()
        dfr["id_persona"] = dfr["id_persona"].astype(str).str.strip()
        dfr["id_evento"] = dfr["id_evento"].astype(str).str.strip()
        dfr = dfr.dropna(subset=["id_persona", "id_evento"])
    else:
        dfr = pd.DataFrame(columns=["id_persona", "id_evento", "fecha_ejecucion", "horas", "resultado"])

    return dfp, dfe, dfprog, dfr


def load_demo_data(force: bool = False):
    init_db()

    if not os.path.exists(EXCEL_PATH):
        st.error(f"No encontré {EXCEL_PATH}. Debe estar en la misma carpeta que app.py.")
        return

    if force:
        exec_sql("DELETE FROM registro")
        exec_sql("DELETE FROM programacion")
        exec_sql("DELETE FROM eventos")
        exec_sql("DELETE FROM personas")

    # Importar siempre (si falla, que falle aquí y lo veas)
    dfp, dfe, dfprog, dfr = import_from_excel(EXCEL_PATH)

    upsert_many_personas(dfp)
    upsert_many_eventos(dfe)
    upsert_many_programacion(dfprog)

    # Registro (si viene con algo)
    if len(dfr) > 0:
        for _, r in dfr.iterrows():
            fecha = r.get("fecha_ejecucion", pd.NaT)
            if pd.isna(fecha):
                fecha_str = date.today().isoformat()
            else:
                fecha_str = pd.to_datetime(fecha).date().isoformat()

            horas = r.get("horas", None)
            if horas is None or pd.isna(horas):
                evh = read_df("SELECT duracion_horas FROM eventos WHERE id_evento=?", (r["id_evento"],))
                horas = float(evh["duracion_horas"].iloc[0]) if len(evh) else 1.0
            else:
                horas = float(horas)

            resultado = r.get("resultado", None)
            if resultado is None or pd.isna(resultado) or str(resultado).strip() == "":
                resultado = "Aprobó"

            exec_sql("""
            INSERT INTO registro(id_persona, id_evento, fecha_ejecucion, horas, resultado)
            VALUES(?,?,?,?,?)
            """, (r["id_persona"], r["id_evento"], fecha_str, horas, str(resultado)))

    # Mensaje con conteos para asegurar que NO quede vacío
    n_p = read_df("SELECT COUNT(*) n FROM personas")["n"].iloc[0]
    n_e = read_df("SELECT COUNT(*) n FROM eventos")["n"].iloc[0]
    n_pr = read_df("SELECT COUNT(*) n FROM programacion")["n"].iloc[0]
    n_r = read_df("SELECT COUNT(*) n FROM registro")["n"].iloc[0]
    st.success(f"✅ Cargado desde Excel → Personas: {n_p} | Eventos: {n_e} | Programación: {n_pr} | Registro: {n_r}")


# =========================
# App
# =========================
st.title("📚 Gestión de Eventos Formativos (RRHH)")

with st.sidebar:
    st.header("⚙️ Datos")
    st.caption("Excel + SQLite (persistente)")
    try:
        init_db()
        n_p = read_df("SELECT COUNT(*) n FROM personas")["n"].iloc[0]
        n_e = read_df("SELECT COUNT(*) n FROM eventos")["n"].iloc[0]
        n_pr = read_df("SELECT COUNT(*) n FROM programacion")["n"].iloc[0]
        n_r = read_df("SELECT COUNT(*) n FROM registro")["n"].iloc[0]
        st.write(f"👥 Personas: **{n_p}**")
        st.write(f"🧾 Eventos: **{n_e}**")
        st.write(f"🗓️ Programación: **{n_pr}**")
        st.write(f"✅ Registro: **{n_r}**")
    except Exception as e:
        st.error(f"DB error: {e}")

    st.divider()
    if st.button("🔄 Forzar recarga total (Excel → DB)"):
        load_demo_data(force=True)
        st.rerun()

    if st.button("🧨 Reset DB (borrar archivo)"):
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        st.success("DB eliminada. Vuelve a forzar recarga.")
        st.rerun()


# Auto-carga si está vacío (solo la primera vez)
try:
    if read_df("SELECT COUNT(*) n FROM personas")["n"].iloc[0] == 0:
        load_demo_data(force=False)
except Exception:
    init_db()
    load_demo_data(force=False)

tabs = st.tabs(["1) Personas", "2) Eventos", "3) Programación", "4) Ejecución", "5) Dashboard", "6) Export / Entrega"])

# =========================
# 1) Personas (CRUD)
# =========================
with tabs[0]:
    st.subheader("👥 Personas — CRUD")
    dfp = read_df("SELECT * FROM personas ORDER BY cargo, nombre")

    c1, c2 = st.columns([2, 1])
    with c1:
        st.dataframe(dfp, use_container_width=True, height=420)

    with c2:
        st.markdown("### Crear / Editar")
        idp = st.text_input("Id Persona", key="p_id")
        nombre = st.text_input("Nombre", key="p_nombre")
        cargo = st.text_input("Cargo", key="p_cargo")
        proceso = st.text_input("Proceso", key="p_proceso")
        lugar = st.text_input("Lugar de Trabajo", key="p_lugar")

        colA, colB = st.columns(2)
        with colA:
            if st.button("💾 Guardar persona"):
                if not idp.strip():
                    st.error("Id Persona es obligatorio.")
                else:
                    exec_sql("""
                    INSERT INTO personas(id_persona, nombre, cargo, proceso, lugar_trabajo)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(id_persona) DO UPDATE SET
                      nombre=excluded.nombre, cargo=excluded.cargo, proceso=excluded.proceso, lugar_trabajo=excluded.lugar_trabajo
                    """, (idp.strip(), nombre, cargo, proceso, lugar))
                    st.success("Persona guardada.")
                    st.rerun()
        with colB:
            if st.button("🗑️ Eliminar persona"):
                if not idp.strip():
                    st.error("Indica Id Persona.")
                else:
                    exec_sql("DELETE FROM personas WHERE id_persona=?", (idp.strip(),))
                    st.success("Eliminada.")
                    st.rerun()

    st.info("Tip: Copia un Id Persona de la tabla, pégalo arriba y edítalo.")

# =========================
# 2) Eventos (CRUD)
# =========================
with tabs[1]:
    st.subheader("🧾 Eventos formativos — CRUD")
    dfe = read_df("SELECT * FROM eventos ORDER BY tema_general, nombre_evento")

    c1, c2 = st.columns([2, 1])
    with c1:
        st.dataframe(dfe, use_container_width=True, height=420)

    with c2:
        st.markdown("### Crear / Editar")
        ide = st.text_input("Id Evento", key="e_id")
        tipo = st.text_input("Tipo de Evento", value="CAPACITACIÓN", key="e_tipo")
        tema = st.text_input("Tema General", key="e_tema")
        nombre_ev = st.text_input("Evento Formativo (Nombre)", key="e_nombre")
        esquema = st.text_input("Esquema de Evento", value="CAPACITACION", key="e_esquema")
        dur = st.number_input("Duración (horas)", min_value=0.25, value=1.0, step=0.25, key="e_dur")

        colA, colB = st.columns(2)
        with colA:
            if st.button("💾 Guardar evento"):
                if not ide.strip():
                    st.error("Id Evento es obligatorio.")
                else:
                    exec_sql("""
                    INSERT INTO eventos(id_evento, tipo_evento, tema_general, nombre_evento, esquema_evento, duracion_horas)
                    VALUES(?,?,?,?,?,?)
                    ON CONFLICT(id_evento) DO UPDATE SET
                      tipo_evento=excluded.tipo_evento,
                      tema_general=excluded.tema_general,
                      nombre_evento=excluded.nombre_evento,
                      esquema_evento=excluded.esquema_evento,
                      duracion_horas=excluded.duracion_horas
                    """, (ide.strip(), tipo, tema, nombre_ev, esquema, float(dur)))
                    st.success("Evento guardado.")
                    st.rerun()
        with colB:
            if st.button("🗑️ Eliminar evento"):
                if not ide.strip():
                    st.error("Indica Id Evento.")
                else:
                    exec_sql("DELETE FROM eventos WHERE id_evento=?", (ide.strip(),))
                    st.success("Eliminado.")
                    st.rerun()

# =========================
# 3) Programación (Cargo/Mes + Herencia)
# =========================
with tabs[2]:
    st.subheader("🗓️ Programación (Cargo + Mes + Evento) y herencia a personas")

    cargos = read_df("SELECT DISTINCT cargo FROM personas WHERE cargo IS NOT NULL AND cargo<>'' ORDER BY cargo")["cargo"].tolist()
    eventos = read_df("SELECT id_evento, tema_general, nombre_evento, duracion_horas FROM eventos ORDER BY tema_general, nombre_evento")

    if len(cargos) == 0 or len(eventos) == 0:
        st.warning("Primero carga personas y eventos (usa ‘Forzar recarga’ en la barra lateral).")
    else:
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            cargo_sel = st.selectbox("Cargo", cargos)
        with col2:
            mes_sel = month_start(st.date_input("Mes", value=date.today()))
            st.caption(f"Mes normalizado: {mes_sel}")
        with col3:
            st.markdown("### Agregar eventos a la programación")
            ev_labels = {f"{r['tema_general']} — {r['nombre_evento']} ({r['id_evento']})": r["id_evento"]
                         for _, r in eventos.iterrows()}
            picks = st.multiselect("Eventos", list(ev_labels.keys()))
            if st.button("✅ Guardar programación"):
                for label in picks:
                    exec_sql(
                        "INSERT OR IGNORE INTO programacion(id_evento, cargo, mes) VALUES(?,?,?)",
                        (str(ev_labels[label]), cargo_sel, mes_sel)
                    )
                st.success("Programación guardada.")
                st.rerun()

        st.divider()

        prog = read_df("""
            SELECT p.mes, p.cargo, e.id_evento, e.tema_general, e.nombre_evento, e.duracion_horas
            FROM programacion p
            JOIN eventos e ON e.id_evento = p.id_evento
            WHERE p.cargo=? AND p.mes=?
            ORDER BY e.tema_general, e.nombre_evento
        """, (cargo_sel, mes_sel))

        pers_cargo = read_df("""
            SELECT id_persona, nombre, cargo, proceso, lugar_trabajo
            FROM personas
            WHERE cargo=?
            ORDER BY nombre
        """, (cargo_sel,))

        cA, cB = st.columns(2)
        with cA:
            st.write("**Eventos programados (cargo/mes)**")
            st.dataframe(prog, use_container_width=True, height=320)
            if len(prog) > 0 and st.button("🧹 Eliminar programación cargo/mes (todos)"):
                exec_sql("DELETE FROM programacion WHERE cargo=? AND mes=?", (cargo_sel, mes_sel))
                st.success("Programación eliminada para ese cargo/mes.")
                st.rerun()

        with cB:
            st.write("**Personas que heredan la programación (mismo cargo)**")
            st.dataframe(pers_cargo, use_container_width=True, height=320)

        st.markdown("### Vista heredada: Personas → Eventos programados")
        if len(prog) == 0:
            st.info("No hay eventos programados para ese cargo/mes.")
        else:
            view = pers_cargo.assign(key=1).merge(prog.assign(key=1), on="key").drop(columns=["key"])
            # el merge puede crear cargo_x/cargo_y
            if "cargo" not in view.columns:
                if "cargo_x" in view.columns:
                    view = view.rename(columns={"cargo_x": "cargo"})
                elif "cargo_y" in view.columns:
                    view = view.rename(columns={"cargo_y": "cargo"})

            cols = ["id_persona", "nombre", "cargo", "mes", "id_evento", "tema_general", "nombre_evento", "duracion_horas"]
            cols = [c for c in cols if c in view.columns]  # por seguridad
            view = view[cols]

            st.dataframe(view, use_container_width=True, height=420)

# =========================
# 4) Ejecución (simula Forms) + Cruce con Programación
# =========================
with tabs[3]:
    st.subheader("✅ Registro de ejecución (simula Forms) y validación vs programación")

    personas = read_df("SELECT id_persona, nombre, cargo FROM personas ORDER BY nombre")
    eventos = read_df("SELECT id_evento, nombre_evento, tema_general, duracion_horas FROM eventos ORDER BY tema_general, nombre_evento")

    if len(personas) == 0 or len(eventos) == 0:
        st.warning("Carga personas y eventos.")
    else:
        c1, c2, c3 = st.columns([2, 2, 2])
        with c1:
            p_label = st.selectbox("Persona", [f"{r['nombre']} ({r['id_persona']})" for _, r in personas.iterrows()])
        with c2:
            e_label = st.selectbox("Evento", [f"{r['tema_general']} — {r['nombre_evento']} ({r['id_evento']})" for _, r in eventos.iterrows()])
        with c3:
            fecha = st.date_input("Fecha de ejecución", value=date.today())

        pid = p_label.split("(")[-1].replace(")", "").strip()
        eid = e_label.split("(")[-1].replace(")", "").strip()

        dur = float(eventos[eventos["id_evento"] == eid]["duracion_horas"].iloc[0])
        colA, colB, colC = st.columns([1, 1, 2])
        with colA:
            horas = st.number_input("Horas", min_value=0.25, value=dur, step=0.25)
        with colB:
            resultado = st.selectbox("Resultado", ["Aprobó", "Reprobó"])
        with colC:
            cargo_persona = personas[personas["id_persona"] == pid]["cargo"].iloc[0]
            mes = month_start(fecha)
            is_prog = read_df("""
                SELECT COUNT(*) AS n
                FROM programacion
                WHERE id_evento=? AND cargo=? AND mes=?
            """, (eid, cargo_persona, mes))["n"].iloc[0] > 0

            if is_prog:
                st.success(f"✅ Coincide con programación (Cargo: {cargo_persona} | Mes: {mes})")
            else:
                st.warning(f"⚠️ Fuera de programación (Cargo: {cargo_persona} | Mes: {mes})")

        if st.button("💾 Guardar ejecución"):
            exec_sql("""
                INSERT INTO registro(id_persona, id_evento, fecha_ejecucion, horas, resultado)
                VALUES(?,?,?,?,?)
            """, (pid, eid, fecha.isoformat(), float(horas), resultado))
            st.success("Ejecución registrada.")
            st.rerun()

        st.divider()
        st.markdown("### Últimos registros de ejecución")
        reg = read_df("""
            SELECT r.id, r.fecha_ejecucion, r.id_persona, p.nombre, p.cargo,
                   r.id_evento, e.nombre_evento, r.horas, r.resultado
            FROM registro r
            LEFT JOIN personas p ON p.id_persona = r.id_persona
            LEFT JOIN eventos e ON e.id_evento = r.id_evento
            ORDER BY r.fecha_ejecucion DESC, r.id DESC
            LIMIT 250
        """)
        st.dataframe(reg, use_container_width=True, height=420)

# =========================
# 5) Dashboard (KPIs)
# =========================
with tabs[4]:
    st.subheader("📊 Dashboard — Indicadores solicitados")

    pers = read_df("SELECT * FROM personas")
    ev = read_df("SELECT * FROM eventos")
    prog = read_df("SELECT * FROM programacion")
    reg = read_df("SELECT * FROM registro")

    if len(pers) == 0 or len(ev) == 0:
        st.warning("Carga datos primero.")
    else:
        # joins para dashboard
        if len(reg) > 0:
            reg2 = reg.merge(ev[["id_evento", "tema_general", "nombre_evento"]], on="id_evento", how="left") \
                     .merge(pers[["id_persona", "nombre", "cargo"]], on="id_persona", how="left")
            reg2["fecha_ejecucion"] = pd.to_datetime(reg2["fecha_ejecucion"], errors="coerce")
            reg2["mes"] = reg2["fecha_ejecucion"].dt.to_period("M").dt.to_timestamp().dt.date.astype(str)
        else:
            reg2 = pd.DataFrame()

        # programación heredada a personas
        if len(prog) > 0:
            pers_cargo = pers[["id_persona", "cargo"]].copy()
            prog_persona = prog.merge(pers_cargo, on="cargo", how="left")  # id_evento,cargo,mes,id_persona
        else:
            prog_persona = pd.DataFrame(columns=["id_evento", "cargo", "mes", "id_persona"])

        sub_tabs = st.tabs([
            "Horas por Persona",
            "Resultado por Temas",
            "Cobertura",
            "Eficacia",
            "Cumplimiento Mes a Mes"
        ])

        with sub_tabs[0]:
            st.markdown("## ⏱️ Cantidad de horas de Formación por Persona")
            if len(reg2) == 0:
                st.info("No hay ejecuciones registradas aún.")
            else:
                horas_persona = reg2.groupby(["id_persona", "nombre", "cargo"], as_index=False)["horas"].sum()
                horas_persona = horas_persona.sort_values("horas", ascending=False)
                st.dataframe(horas_persona, use_container_width=True, height=420)

        with sub_tabs[1]:
            st.markdown("## ✅ Resultado (Aprobó / Reprobó) según temas")
            if len(reg2) == 0:
                st.info("No hay ejecuciones registradas aún.")
            else:
                res_tema = reg2.groupby(["tema_general", "resultado"], as_index=False).size().rename(columns={"size": "conteo"})
                st.dataframe(res_tema, use_container_width=True, height=420)

        with sub_tabs[2]:
            st.markdown("## 🎯 Cobertura: Personas Programadas vs Personas que han recibido formación")
            personas_programadas = int(prog_persona["id_persona"].nunique()) if len(prog_persona) else 0
            personas_formadas = int(reg["id_persona"].nunique()) if len(reg) else 0

            cA, cB, cC = st.columns(3)
            cA.metric("Personas programadas", personas_programadas)
            cB.metric("Personas con formación recibida", personas_formadas)
            cC.metric("Cobertura (%)", round(100 * personas_formadas / personas_programadas, 2) if personas_programadas else 0.0)

        with sub_tabs[3]:
            st.markdown("## 🧪 Eficacia: Programados vs Ejecutados")
            eventos_programados = int(len(prog))
            if len(reg2) > 0:
                eventos_ejecutados = int(reg2[["id_evento", "fecha_ejecucion"]].dropna().drop_duplicates().shape[0])
            else:
                eventos_ejecutados = 0

            cA, cB, cC = st.columns(3)
            cA.metric("Eventos programados (cargo/mes/evento)", eventos_programados)
            cB.metric("Eventos ejecutados (evento/fecha)", eventos_ejecutados)
            cC.metric("Eficacia (%)", round(100 * eventos_ejecutados / eventos_programados, 2) if eventos_programados else 0.0)

        with sub_tabs[4]:
            st.markdown("## 📅 Cumplimiento de la programación mes a mes")
            if len(prog) == 0:
                st.info("No hay programación aún.")
            else:
                prog_mes = prog.groupby("mes", as_index=False).size().rename(columns={"size": "programados"})
                if len(reg2) > 0:
                    reg_mes = reg2.groupby("mes", as_index=False).size().rename(columns={"size": "ejecutados"})
                    cum = prog_mes.merge(reg_mes, on="mes", how="left").fillna(0)
                else:
                    cum = prog_mes.assign(ejecutados=0)

                cum["cumplimiento_%"] = cum.apply(
                    lambda r: 0 if r["programados"] == 0 else round(100 * r["ejecutados"] / r["programados"], 2),
                    axis=1
                )
                st.dataframe(cum.sort_values("mes"), use_container_width=True, height=420)

# =========================
# 6) Export / Entrega
# =========================
with tabs[5]:
    st.subheader("📦 Exportables + Nota para entregar")

    pers = read_df("SELECT * FROM personas")
    ev = read_df("SELECT * FROM eventos")
    prog = read_df("SELECT * FROM programacion")
    reg = read_df("SELECT * FROM registro")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### Exportables")
        st.download_button("⬇️ Personas.csv", pers.to_csv(index=False).encode("utf-8"), "Personas.csv", "text/csv")
        st.download_button("⬇️ Eventos.csv", ev.to_csv(index=False).encode("utf-8"), "Eventos.csv", "text/csv")
        st.download_button("⬇️ Programacion.csv", prog.to_csv(index=False).encode("utf-8"), "Programacion.csv", "text/csv")
        st.download_button("⬇️ Registro.csv", reg.to_csv(index=False).encode("utf-8"), "Registro.csv", "text/csv")
        st.caption("Power BI puede conectarse a estos CSV o directo a la base SQLite (capacitaciones.db).")

    with c2:
        st.markdown("### Enfoque Microsoft 365")
        st.write(
            "Para esta prueba armé un prototipo funcional que replica el flujo de Power Platform sin depender de licencias "
            "o restricciones del tenant. La lógica está diseñada para migrarse fácil a Microsoft 365:\n\n"
            "- **SharePoint Lists / Dataverse**: este SQLite representa las listas (Personas, Eventos, Programación y Registro). "
            "La estructura es la misma que usaría en SharePoint: llaves por Id y una tabla puente de programación por cargo y mes.\n"
            "- **Power Apps**: las pestañas de Personas y Eventos cubren el CRUD y validaciones de negocio.\n"
            "- **Programación**: la hoja **Matriz Programación** se transforma automáticamente a registros (Cargo + Mes + Evento), "
            "que es el formato correcto para explotar y auditar la programación.\n"
            "- **Forms**: la pestaña Ejecución simula el formulario de asistencia/resultado; al guardar, se valida contra la programación.\n"
            "- **Power Automate**: el equivalente sería un flujo que al recibir un Forms crea el registro y notifica si fue ‘dentro’ o ‘fuera’ de programación.\n"
            "- **Power BI**: el dashboard ya está calculado dentro de la app y además dejo exportables para conectarlos a un PBIX.\n"
        )