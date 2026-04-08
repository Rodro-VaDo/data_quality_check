# =============================================================================
# Consolidador de Bases de Datos — Aldeas Infantiles SOS Colombia
# =============================================================================

import os
import sys
import datetime
import threading
import subprocess
import platform
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd


# ──────────────────────────────────────────────────────────────────────────────
# SECCIÓN 1 — ESTILOS Y PALETA DE COLORES
# Paleta basada en la identidad visual de Aldeas Infantiles SOS Colombia
# ──────────────────────────────────────────────────────────────────────────────

# ── Colores corporativos ──────────────────────────────────────────────────────
AZUL_PRIMARIO  = "#009FE3"   # Azul principal Aldeas Infantiles SOS
AZUL_OSCURO    = "#007AB8"   # Azul para estados hover y activos
AZUL_CLARO     = "#EBF7FD"   # Fondo suave azul para secciones
BLANCO         = "#FFFFFF"
GRIS_FONDO     = "#F4F6F9"   # Fondo general de pantallas
GRIS_CLARO     = "#E2E8EF"   # Bordes, separadores y fondos inactivos
GRIS_MEDIO     = "#9CA3AF"   # Texto de ayuda y secundario
GRIS_TEXTO     = "#6B7280"   # Texto no prioritario
NEGRO_SUAVE    = "#1F2937"   # Texto principal
VERDE_EXITO    = "#10B981"   # Estado completado exitosamente
VERDE_CLARO    = "#D1FAE5"   # Fondo para mensajes de éxito
ROJO_ERROR     = "#EF4444"   # Errores críticos
ROJO_CLARO     = "#FEE2E2"   # Fondo para mensajes de error
NARANJA_AVISO  = "#F59E0B"   # Advertencias y avisos

# ── Tipografía ────────────────────────────────────────────────────────────────
FUENTE_TITULO       = ("Segoe UI", 15, "bold")
FUENTE_SUBTITULO    = ("Segoe UI", 12, "bold")
FUENTE_CUERPO       = ("Segoe UI", 10)
FUENTE_CUERPO_BOLD  = ("Segoe UI", 10, "bold")
FUENTE_PEQUEÑA      = ("Segoe UI", 9)
FUENTE_PEQUEÑA_BOLD = ("Segoe UI", 9, "bold")
FUENTE_MONO         = ("Consolas", 9)   # Para el log de resultados

# ── Espaciado ─────────────────────────────────────────────────────────────────
PAD_S  = 6
PAD_M  = 12
PAD_L  = 20
PAD_XL = 30


def configurar_estilos(estilo):
    """
    Aplica el tema y estilos personalizados a todos los widgets ttk.
    Debe llamarse una vez al iniciar la aplicación.
    """
    estilo.theme_use('clam')

    # ── Botón primario ────────────────────────────────────────────────────────
    estilo.configure(
        'Primario.TButton',
        background=AZUL_PRIMARIO, foreground=BLANCO,
        font=FUENTE_CUERPO_BOLD, padding=(20, 9),
        borderwidth=0, focusthickness=0, relief='flat',
    )
    estilo.map('Primario.TButton',
        background=[('active', AZUL_OSCURO), ('disabled', GRIS_CLARO)],
        foreground=[('disabled', GRIS_TEXTO)],
    )

    # ── Botón secundario (contorno azul) ──────────────────────────────────────
    estilo.configure(
        'Secundario.TButton',
        background=BLANCO, foreground=AZUL_PRIMARIO,
        font=FUENTE_CUERPO_BOLD, padding=(20, 9),
        borderwidth=1, focusthickness=0, relief='solid',
    )
    estilo.map('Secundario.TButton',
        background=[('active', AZUL_CLARO)],
    )

    # ── Botón de peligro / eliminar ───────────────────────────────────────────
    estilo.configure(
        'Peligro.TButton',
        background=ROJO_CLARO, foreground=ROJO_ERROR,
        font=FUENTE_CUERPO, padding=(12, 7),
        borderwidth=0, focusthickness=0,
    )
    estilo.map('Peligro.TButton',
        background=[('active', '#FECACA')],
    )

    # ── Barra de progreso ─────────────────────────────────────────────────────
    estilo.configure(
        'SOS.Horizontal.TProgressbar',
        background=AZUL_PRIMARIO, troughcolor=GRIS_CLARO,
        thickness=14, borderwidth=0,
    )

    # ── Scrollbar ─────────────────────────────────────────────────────────────
    estilo.configure(
        'TScrollbar',
        background=GRIS_CLARO, troughcolor=GRIS_FONDO,
        borderwidth=0, arrowsize=12,
    )
    estilo.map('TScrollbar', background=[('active', GRIS_MEDIO)])


# ──────────────────────────────────────────────────────────────────────────────
# SECCIÓN 2 — LÓGICA DE PROCESAMIENTO
# Lectura, concatenación, filtrado y exportación de archivos Excel
# ──────────────────────────────────────────────────────────────────────────────

# ── Pestañas mínimas obligatorias ─────────────────────────────────────────────
# Cada pestaña fija tiene asociada una columna ID para filtrar filas vacías.
# Si el valor es None (ej. 'Listas'), se importa sin filtro de ID.
PESTANAS_FIJAS = {
    'participantes':   'ID DEL PARTICIPANTE (PRIMARIA)',
    'servicios':       'ID DEL PARTICIPANTE',
    'familia_acogida': 'ID FAMILIA / CASA DE ACOGIDA',
    'familia_origen':  'ID FAMILIA DE ORIGEN EN DFE',
    'defensor':        'ID DEFENSORÍA',
    'Listas':          None,
}


def obtener_pestanas_disponibles(ruta_archivo):
    """
    Lee los nombres de todas las pestañas del primer archivo seleccionado.
    Retorna la lista completa de nombres de hojas.
    """
    try:
        excel = pd.ExcelFile(ruta_archivo, engine='openpyxl')
        return excel.sheet_names
    except Exception as e:
        raise RuntimeError(f"No se pudo leer el archivo: {e}")


def obtener_pestanas_opcionales(ruta_archivo):
    """
    Retorna las pestañas del archivo que NO son obligatorias.
    Son las candidatas a selección opcional por el usuario.
    """
    todas = obtener_pestanas_disponibles(ruta_archivo)
    nombres_fijos = set(PESTANAS_FIJAS.keys())
    return [h for h in todas if h not in nombres_fijos]


def procesar(archivos, pestanas_extra_sel, ruta_salida,
             callback_log=None, callback_progreso=None):
    """
    Función principal de procesamiento.

    Parámetros
    ----------
    archivos          : list[str]  — Rutas de los archivos .xlsx a concatenar
    pestanas_extra_sel: list[str]  — Nombres de pestañas adicionales seleccionadas
    ruta_salida       : str        — Ruta completa del archivo de salida (.xlsx)
    callback_log      : callable   — Función(mensaje, tipo) para registrar eventos
    callback_progreso : callable   — Función(valor 0-100) para la barra de progreso
    """

    def log(msg, tipo='info'):
        if callback_log:
            callback_log(msg, tipo)

    def progreso(val):
        if callback_progreso:
            callback_progreso(val)

    # ── Todas las pestañas a procesar ─────────────────────────────────────────
    pestanas_a_procesar = list(PESTANAS_FIJAS.keys()) + pestanas_extra_sel
    total_pasos = len(archivos) * len(pestanas_a_procesar) + 1
    paso_actual = 0

    acumulados       = {h: [] for h in pestanas_a_procesar}
    archivos_fallidos  = []
    archivos_exitosos  = 0

    # ── Lectura archivo por archivo ───────────────────────────────────────────
    for archivo in archivos:
        nombre    = os.path.basename(archivo)
        archivo_ok = True
        log(f"\n📂 Procesando: {nombre}")

        for hoja in pestanas_a_procesar:
            try:
                df = pd.read_excel(archivo, sheet_name=hoja, engine='openpyxl')
                df['archivo_origen'] = nombre
                acumulados[hoja].append(df)
            except Exception as e:
                log(f"   ⚠ Pestaña '{hoja}' no encontrada o con error: {e}", 'aviso')
                archivo_ok = False

            paso_actual += 1
            progreso(int(paso_actual / total_pasos * 85))

        if archivo_ok:
            archivos_exitosos += 1
        else:
            archivos_fallidos.append(nombre)

    # ── Concatenación y limpieza por pestaña ──────────────────────────────────
    log("\n🔗 Concatenando y limpiando datos...")
    consolidados = {}

    for hoja, lista_df in acumulados.items():
        if not lista_df:
            log(f"   ⚠ Sin datos para '{hoja}', se omite.", 'aviso')
            continue

        combinado = pd.concat(lista_df, ignore_index=True)

        if hoja in PESTANAS_FIJAS:
            # Pestañas fijas: filtrar por columna ID
            col_id = PESTANAS_FIJAS[hoja]
            if col_id and col_id in combinado.columns:
                antes    = len(combinado)
                combinado = combinado.dropna(subset=[col_id])
                omitidas  = antes - len(combinado)
                if omitidas > 0:
                    log(f"   ↳ '{hoja}': {omitidas} filas sin ID eliminadas.")
        else:
            # Pestañas adicionales: conservar solo filas con ≥ 4 campos diligenciados
            antes    = len(combinado)
            combinado = combinado.dropna(thresh=4)
            omitidas  = antes - len(combinado)
            if omitidas > 0:
                log(f"   ↳ '{hoja}': {omitidas} filas con menos de 4 campos eliminadas.")

        consolidados[hoja] = combinado
        log(f"   ✔ '{hoja}': {len(combinado):,} filas consolidadas.")

    progreso(90)

    # ── Exportación al archivo de salida ──────────────────────────────────────
    log(f"\n💾 Guardando archivo de salida...")
    try:
        os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
        with pd.ExcelWriter(ruta_salida, engine='openpyxl') as writer:
            for hoja, df in consolidados.items():
                df.to_excel(writer, sheet_name=hoja, index=False)

        progreso(100)
        log(f"\n✅ Archivo guardado exitosamente en:", 'exito')
        log(f"   {ruta_salida}", 'exito')

    except Exception as e:
        log(f"\n❌ Error al guardar el archivo: {e}", 'error')
        raise

    # ── Resumen final ─────────────────────────────────────────────────────────
    log(f"\n{'─'*50}")
    log(f"📊 Resumen del proceso:")
    log(f"   • Archivos procesados: {archivos_exitosos} de {len(archivos)}")
    log(f"   • Pestañas exportadas: {len(consolidados)}")
    if archivos_fallidos:
        log(f"   • Archivos con errores parciales:", 'aviso')
        for f in archivos_fallidos:
            log(f"     - {f}", 'aviso')
    log(f"{'─'*50}")

    return {
        'exitosos': archivos_exitosos,
        'fallidos': archivos_fallidos,
        'pestanas': list(consolidados.keys()),
        'ruta':     ruta_salida,
    }


# ──────────────────────────────────────────────────────────────────────────────
# SECCIÓN 3 — PANTALLA 1: SELECCIÓN DE CARPETA FUENTE
# ──────────────────────────────────────────────────────────────────────────────

class PantallaArchivos(tk.Frame):
    """
    Primera pantalla del flujo.
    El usuario selecciona una carpeta y la aplicación detecta automáticamente
    todos los archivos .xlsx que contiene. La lista resultante se muestra para
    revisión, con opción de excluir archivos individualmente antes de continuar.
    """

    def __init__(self, padre, app):
        super().__init__(padre, bg=BLANCO)
        self.app = app
        self._construir_ui()
        self._cargar_estado()

    def _construir_ui(self):
        """Construye todos los elementos visuales de la pantalla."""

        # ── Título y descripción ──────────────────────────────────────────────
        tk.Label(
            self, text="Selección de carpeta fuente",
            font=FUENTE_TITULO, fg=NEGRO_SUAVE, bg=BLANCO
        ).pack(anchor='w', pady=(0, 4))

        tk.Label(
            self,
            text="Selecciona la carpeta que contiene las bases de datos. "
                 "Se detectarán automáticamente todos los archivos .xlsx.",
            font=FUENTE_CUERPO, fg=GRIS_TEXTO, bg=BLANCO,
            wraplength=680, justify='left'
        ).pack(anchor='w', pady=(0, PAD_M))

        # ── Barra de navegación (primero para garantizar espacio) ─────────────
        tk.Frame(self, bg=GRIS_CLARO, height=1).pack(
            side='bottom', fill='x', pady=(PAD_M, 0)
        )
        nav = tk.Frame(self, bg=BLANCO)
        nav.pack(side='bottom', fill='x', pady=(PAD_S, 0))

        self.lbl_estado = tk.Label(
            nav, text="", font=FUENTE_PEQUEÑA, fg=GRIS_TEXTO, bg=BLANCO
        )
        self.lbl_estado.pack(side='right', padx=PAD_M)

        self.btn_siguiente = ttk.Button(
            nav, text="Siguiente  →",
            style='Primario.TButton',
            command=self._ir_siguiente, state='disabled',
        )
        self.btn_siguiente.pack(side='right')

        # ── Selector de carpeta ───────────────────────────────────────────────
        frame_carpeta = tk.Frame(self, bg=BLANCO)
        frame_carpeta.pack(fill='x', pady=(0, PAD_M))

        self.var_carpeta = tk.StringVar()
        self.entry_carpeta = tk.Entry(
            frame_carpeta, textvariable=self.var_carpeta,
            font=FUENTE_CUERPO, bg=GRIS_FONDO, fg=NEGRO_SUAVE,
            relief='flat', bd=0, highlightthickness=1,
            highlightbackground=GRIS_CLARO, highlightcolor=AZUL_PRIMARIO,
            state='readonly', readonlybackground=GRIS_FONDO,
        )
        self.entry_carpeta.pack(
            side='left', fill='x', expand=True, ipady=8, padx=(0, PAD_S)
        )

        ttk.Button(
            frame_carpeta, text="📂  Seleccionar carpeta",
            style='Primario.TButton',
            command=self._seleccionar_carpeta,
        ).pack(side='right')

        # ── Lista de archivos encontrados ─────────────────────────────────────
        self.lbl_contador = tk.Label(
            self, text="Archivos encontrados  (0)",
            font=FUENTE_CUERPO_BOLD, fg=NEGRO_SUAVE, bg=BLANCO
        )
        self.lbl_contador.pack(anchor='w', pady=(0, 6))

        # Marco con borde para la lista
        marco = tk.Frame(self, bg=GRIS_CLARO, padx=1, pady=1)
        marco.pack(fill='both', expand=True)

        frame_int = tk.Frame(marco, bg=BLANCO)
        frame_int.pack(fill='both', expand=True)

        scrollbar = ttk.Scrollbar(frame_int)
        scrollbar.pack(side='right', fill='y')

        self.listbox = tk.Listbox(
            frame_int, yscrollcommand=scrollbar.set,
            font=FUENTE_PEQUEÑA, bg=BLANCO, fg=NEGRO_SUAVE,
            selectbackground=AZUL_CLARO, selectforeground=AZUL_PRIMARIO,
            activestyle='none', borderwidth=0, highlightthickness=0,
            relief='flat', selectmode='extended',
        )
        self.listbox.pack(fill='both', expand=True, padx=8, pady=8)
        scrollbar.configure(command=self.listbox.yview)

        # Mensaje cuando la lista está vacía
        self.lbl_vacio = tk.Label(
            self.listbox,
            text="Selecciona una carpeta para ver los archivos detectados.",
            font=FUENTE_PEQUEÑA, fg=GRIS_MEDIO, bg=BLANCO, justify='center'
        )
        self.lbl_vacio.place(relx=0.5, rely=0.45, anchor='center')

        # Botón para excluir archivos individualmente
        frame_excluir = tk.Frame(self, bg=BLANCO, pady=PAD_S)
        frame_excluir.pack(fill='x')

        tk.Label(
            frame_excluir,
            text="Selecciona un archivo en la lista y pulsa el botón para excluirlo.",
            font=FUENTE_PEQUEÑA, fg=GRIS_TEXTO, bg=BLANCO
        ).pack(side='left')

        ttk.Button(
            frame_excluir, text="✕  Excluir seleccionado",
            style='Peligro.TButton',
            command=self._excluir_seleccionado,
        ).pack(side='right')

    # ── Manejo de carpeta y archivos ──────────────────────────────────────────

    def _seleccionar_carpeta(self):
        """
        Abre el diálogo de selección de carpeta (más liviano que el de archivos)
        y escanea automáticamente todos los .xlsx que contiene.
        """
        carpeta = filedialog.askdirectory(
            title="Seleccionar carpeta con bases de datos",
            initialdir=self.app.estado.get('carpeta_fuente', '') or None,
        )
        if not carpeta:
            return

        # Escanear la carpeta en busca de archivos .xlsx
        archivos = sorted([
            os.path.join(carpeta, f)
            for f in os.listdir(carpeta)
            if f.endswith('.xlsx') and not f.startswith('~$')  # excluir temporales de Excel
        ])

        if not archivos:
            messagebox.showwarning(
                "Sin archivos",
                f"No se encontraron archivos .xlsx en la carpeta seleccionada:\n{carpeta}"
            )
            return

        # Guardar carpeta y lista de archivos en el estado
        self.app.estado['carpeta_fuente'] = carpeta
        self.app.estado['archivos']       = archivos
        self.var_carpeta.set(carpeta)
        self._actualizar_lista()

    def _excluir_seleccionado(self):
        """Excluye el archivo seleccionado en la lista del proceso."""
        seleccion = list(self.listbox.curselection())
        if not seleccion:
            messagebox.showinfo(
                "Sin selección", "Selecciona un archivo en la lista para excluirlo."
            )
            return
        for i in reversed(seleccion):
            del self.app.estado['archivos'][i]
        self._actualizar_lista()

    def _actualizar_lista(self):
        """Sincroniza la listbox con la lista actual de archivos en el estado."""
        self.listbox.delete(0, 'end')
        archivos = self.app.estado['archivos']

        if archivos:
            self.lbl_vacio.place_forget()
            for ruta in archivos:
                self.listbox.insert('end', f"  {os.path.basename(ruta)}")
            self.btn_siguiente.configure(state='normal')
            self.lbl_estado.configure(
                text=f"{len(archivos)} archivo(s) detectado(s)", fg=VERDE_EXITO
            )
        else:
            self.lbl_vacio.place(relx=0.5, rely=0.45, anchor='center')
            self.btn_siguiente.configure(state='disabled')
            self.lbl_estado.configure(text="", fg=GRIS_TEXTO)

        self.lbl_contador.configure(
            text=f"Archivos encontrados  ({len(archivos)})"
        )

    def _cargar_estado(self):
        """Restaura la carpeta y lista si el usuario regresó de una pantalla posterior."""
        if self.app.estado.get('carpeta_fuente'):
            self.var_carpeta.set(self.app.estado['carpeta_fuente'])
        if self.app.estado['archivos']:
            self._actualizar_lista()

    def _ir_siguiente(self):
        """Valida que haya archivos y avanza a la pantalla de pestañas."""
        if not self.app.estado['archivos']:
            messagebox.showwarning(
                "Sin archivos", "No hay archivos para procesar. Selecciona una carpeta."
            )
            return
        self.app.ir_a_pantalla(2)


# ──────────────────────────────────────────────────────────────────────────────
# SECCIÓN 4 — PANTALLA 2: SELECCIÓN DE PESTAÑAS
# ──────────────────────────────────────────────────────────────────────────────

class PantallaPestanas(tk.Frame):
    """
    Segunda pantalla del flujo.
    Muestra las pestañas obligatorias (bloqueadas) y las opcionales
    detectadas en el primer archivo, con checkboxes de selección libre.
    """

    def __init__(self, padre, app):
        super().__init__(padre, bg=BLANCO)
        self.app = app
        self._vars_opcionales = {}   # {nombre_hoja: BooleanVar}
        self._anim_id = None         # ID del after() para animación de carga
        self._construir_ui()
        # Pequeño retardo para que la pantalla se pinte antes de leer el archivo
        self.after(80, self._cargar_pestanas_async)

    def _construir_ui(self):
        """Construye todos los elementos visuales de la pantalla."""

        # ── Título ────────────────────────────────────────────────────────────
        tk.Label(
            self, text="Selección de pestañas",
            font=FUENTE_TITULO, fg=NEGRO_SUAVE, bg=BLANCO
        ).pack(anchor='w', pady=(0, 4))

        tk.Label(
            self,
            text="Las pestañas obligatorias siempre se incluyen. "
                 "Selecciona las pestañas adicionales que deseas consolidar.",
            font=FUENTE_CUERPO, fg=GRIS_TEXTO, bg=BLANCO,
            wraplength=680, justify='left'
        ).pack(anchor='w', pady=(0, PAD_M))

        # ── Navegación (se empaqueta PRIMERO para garantizar espacio) ─────────
        tk.Frame(self, bg=GRIS_CLARO, height=1).pack(
            side='bottom', fill='x', pady=(PAD_M, 0)
        )
        nav = tk.Frame(self, bg=BLANCO)
        nav.pack(side='bottom', fill='x', pady=(PAD_S, 0))

        ttk.Button(
            nav, text="←  Atrás",
            style='Secundario.TButton',
            command=lambda: self.app.ir_a_pantalla(1)
        ).pack(side='left')

        ttk.Button(
            nav, text="Siguiente  →",
            style='Primario.TButton',
            command=self._ir_siguiente,
        ).pack(side='right')

        # ── Dos columnas (se empaquetan DESPUÉS para ocupar el espacio restante)
        columnas = tk.Frame(self, bg=BLANCO)
        columnas.pack(fill='both', expand=True)
        columnas.columnconfigure(0, weight=1)
        columnas.columnconfigure(2, weight=1)

        self._construir_seccion_fijas(columnas)

        tk.Frame(columnas, bg=GRIS_CLARO, width=1).grid(
            row=0, column=1, sticky='ns', padx=PAD_L, pady=4
        )

        self._construir_seccion_opcionales(columnas)

    def _construir_seccion_fijas(self, padre):
        """Construye la sección de pestañas obligatorias (no editables)."""
        col = tk.Frame(padre, bg=BLANCO)
        col.grid(row=0, column=0, sticky='nsew', padx=(0, 0))

        enc = tk.Frame(col, bg=AZUL_CLARO, padx=PAD_M, pady=PAD_S)
        enc.pack(fill='x', pady=(0, PAD_S))

        tk.Label(
            enc, text="🔒  Pestañas obligatorias",
            font=FUENTE_CUERPO_BOLD, fg=AZUL_OSCURO, bg=AZUL_CLARO
        ).pack(side='left')

        tk.Label(
            enc, text=str(len(PESTANAS_FIJAS)),
            font=FUENTE_PEQUEÑA_BOLD, fg=BLANCO, bg=AZUL_PRIMARIO,
            padx=8, pady=2
        ).pack(side='right')

        tk.Label(
            col,
            text="Estas pestañas siempre se incluyen en el consolidado.",
            font=FUENTE_PEQUEÑA, fg=GRIS_TEXTO, bg=BLANCO
        ).pack(anchor='w', pady=(0, PAD_S))

        for nombre, col_id in PESTANAS_FIJAS.items():
            fila = tk.Frame(col, bg=BLANCO, pady=3)
            fila.pack(fill='x')

            tk.Label(
                fila, text="✔", font=FUENTE_CUERPO,
                fg=VERDE_EXITO, bg=BLANCO, width=2
            ).pack(side='left')

            tk.Label(
                fila, text=nombre, font=FUENTE_CUERPO,
                fg=NEGRO_SUAVE, bg=BLANCO
            ).pack(side='left', padx=(4, 0))

            if col_id:
                tk.Label(
                    fila,
                    text=f"ID: {col_id[:35]}{'…' if len(col_id) > 35 else ''}",
                    font=FUENTE_PEQUEÑA, fg=GRIS_TEXTO, bg=BLANCO
                ).pack(side='right')

            tk.Frame(col, bg=GRIS_CLARO, height=1).pack(fill='x', pady=(2, 0))

    def _construir_seccion_opcionales(self, padre):
        """Construye la sección de pestañas adicionales con checkboxes."""
        col = tk.Frame(padre, bg=BLANCO)
        col.grid(row=0, column=2, sticky='nsew')

        self.enc_opc = tk.Frame(col, bg=GRIS_FONDO, padx=PAD_M, pady=PAD_S)
        self.enc_opc.pack(fill='x', pady=(0, PAD_S))

        tk.Label(
            self.enc_opc, text="☑  Pestañas adicionales",
            font=FUENTE_CUERPO_BOLD, fg=NEGRO_SUAVE, bg=GRIS_FONDO
        ).pack(side='left')

        self.lbl_conteo_opc = tk.Label(
            self.enc_opc, text="0",
            font=FUENTE_PEQUEÑA_BOLD, fg=BLANCO, bg=GRIS_MEDIO,
            padx=8, pady=2
        )
        self.lbl_conteo_opc.pack(side='right')

        tk.Label(
            col,
            text="Selecciona las pestañas extra que quieres consolidar.\n"
                 "Se incluirán filas con al menos 4 campos diligenciados.",
            font=FUENTE_PEQUEÑA, fg=GRIS_TEXTO, bg=BLANCO, justify='left'
        ).pack(anchor='w', pady=(0, PAD_S))

        # Marco scrollable
        marco = tk.Frame(col, bg=GRIS_CLARO, padx=1, pady=1)
        marco.pack(fill='both', expand=True)

        interior = tk.Frame(marco, bg=BLANCO)
        interior.pack(fill='both', expand=True)

        scrollbar = ttk.Scrollbar(interior)
        scrollbar.pack(side='right', fill='y')

        self.canvas_opc = tk.Canvas(
            interior, bg=BLANCO,
            yscrollcommand=scrollbar.set,
            highlightthickness=0,
        )
        self.canvas_opc.pack(side='left', fill='both', expand=True)
        scrollbar.configure(command=self.canvas_opc.yview)

        self.frame_checks = tk.Frame(self.canvas_opc, bg=BLANCO)
        self.canvas_window = self.canvas_opc.create_window(
            (0, 0), window=self.frame_checks, anchor='nw'
        )

        self.frame_checks.bind('<Configure>', self._actualizar_scroll)
        self.canvas_opc.bind('<Configure>', self._ajustar_ancho)

        tk.Label(
            self.frame_checks,
            text="Leyendo pestañas del archivo...",
            font=FUENTE_PEQUEÑA, fg=GRIS_TEXTO, bg=BLANCO, pady=20
        ).pack()

        # Botones de selección masiva
        btns = tk.Frame(col, bg=BLANCO, pady=PAD_S)
        btns.pack(fill='x')

        ttk.Button(
            btns, text="Seleccionar todo",
            style='Secundario.TButton', command=self._seleccionar_todo,
        ).pack(side='left', padx=(0, PAD_S))

        ttk.Button(
            btns, text="Deseleccionar todo",
            style='Secundario.TButton', command=self._deseleccionar_todo,
        ).pack(side='left')

    def _cargar_pestanas_async(self):
        """
        Inicia la lectura de pestañas en un hilo secundario para no bloquear
        la interfaz. Muestra un indicador de carga animado mientras trabaja.
        """
        self._iniciar_animacion_carga()
        threading.Thread(target=self._leer_pestanas_hilo, daemon=True).start()

    def _iniciar_animacion_carga(self):
        """Muestra un texto animado con puntos mientras se leen las pestañas."""
        self._puntos = 0

        def animar():
            if not self.winfo_exists():
                return
            # Solo animar si todavía estamos en estado de carga
            hijos = self.frame_checks.winfo_children()
            if hijos and isinstance(hijos[0], tk.Label):
                self._puntos = (self._puntos + 1) % 4
                hijos[0].configure(
                    text="Leyendo pestañas del archivo" + "." * self._puntos
                )
                self._anim_id = self.after(400, animar)

        self._anim_id = self.after(400, animar)

    def _leer_pestanas_hilo(self):
        """Función que corre en el hilo secundario: lee el archivo y notifica a la UI."""
        primer_archivo = self.app.estado['archivos'][0]
        try:
            opcionales = obtener_pestanas_opcionales(primer_archivo)
            # Actualizar la UI siempre desde el hilo principal
            self.after(0, lambda: self._poblar_checkboxes(opcionales))
        except Exception as e:
            self.after(0, lambda: self._mostrar_error_carga(str(e)))

    def _mostrar_error_carga(self, mensaje):
        """Muestra el error de lectura de pestañas en la UI."""
        if self._anim_id:
            self.after_cancel(self._anim_id)
        messagebox.showerror("Error al leer pestañas",
                             f"No se pudieron leer las pestañas:\n{mensaje}")

    def _poblar_checkboxes(self, opcionales):
        """Crea los checkboxes para las pestañas adicionales detectadas."""
        # Detener la animación de carga antes de modificar los widgets
        if self._anim_id:
            self.after_cancel(self._anim_id)
            self._anim_id = None

        for w in self.frame_checks.winfo_children():
            w.destroy()
        self._vars_opcionales.clear()

        if not opcionales:
            tk.Label(
                self.frame_checks,
                text="No se encontraron pestañas adicionales.",
                font=FUENTE_PEQUEÑA, fg=GRIS_TEXTO, bg=BLANCO, pady=20
            ).pack()
            return

        seleccion_previa = set(self.app.estado.get('pestanas_extra', []))

        for nombre in opcionales:
            var = tk.BooleanVar(value=(nombre in seleccion_previa))
            self._vars_opcionales[nombre] = var

            fila = tk.Frame(self.frame_checks, bg=BLANCO, pady=3)
            fila.pack(fill='x', padx=PAD_S)

            tk.Checkbutton(
                fila, text=nombre, variable=var,
                font=FUENTE_CUERPO, fg=NEGRO_SUAVE, bg=BLANCO,
                activebackground=BLANCO, selectcolor=BLANCO,
                command=self._actualizar_conteo,
            ).pack(side='left')

            tk.Frame(self.frame_checks, bg=GRIS_CLARO, height=1).pack(
                fill='x', padx=PAD_S
            )

        self.lbl_conteo_opc.configure(text=str(len(opcionales)), bg=GRIS_MEDIO)
        self._actualizar_conteo()

    def _seleccionar_todo(self):
        for var in self._vars_opcionales.values():
            var.set(True)
        self._actualizar_conteo()

    def _deseleccionar_todo(self):
        for var in self._vars_opcionales.values():
            var.set(False)
        self._actualizar_conteo()

    def _actualizar_conteo(self):
        """Actualiza el badge con la cantidad de pestañas extra seleccionadas."""
        n = sum(1 for v in self._vars_opcionales.values() if v.get())
        self.lbl_conteo_opc.configure(
            text=str(n), bg=AZUL_PRIMARIO if n > 0 else GRIS_MEDIO
        )

    def _actualizar_scroll(self, event=None):
        self.canvas_opc.configure(scrollregion=self.canvas_opc.bbox('all'))

    def _ajustar_ancho(self, event):
        self.canvas_opc.itemconfig(self.canvas_window, width=event.width)

    def _ir_siguiente(self):
        """Guarda la selección y avanza a la pantalla de destino."""
        self.app.estado['pestanas_extra'] = [
            nombre for nombre, var in self._vars_opcionales.items() if var.get()
        ]
        self.app.ir_a_pantalla(3)


# ──────────────────────────────────────────────────────────────────────────────
# SECCIÓN 5 — PANTALLA 3: DESTINO Y NOMBRE DEL ARCHIVO
# ──────────────────────────────────────────────────────────────────────────────

class PantallaDestino(tk.Frame):
    """
    Tercera pantalla del flujo.
    El usuario elige la carpeta de destino y el nombre del archivo de salida.
    Muestra una vista previa de la ruta completa antes de ejecutar.
    """

    def __init__(self, padre, app):
        super().__init__(padre, bg=BLANCO)
        self.app = app
        self._construir_ui()
        self._cargar_estado()

    def _construir_ui(self):
        """Construye todos los elementos visuales de la pantalla."""

        tk.Label(
            self, text="Destino y nombre del archivo",
            font=FUENTE_TITULO, fg=NEGRO_SUAVE, bg=BLANCO
        ).pack(anchor='w', pady=(0, 4))

        tk.Label(
            self,
            text="Define dónde guardar el archivo consolidado y cómo nombrarlo.",
            font=FUENTE_CUERPO, fg=GRIS_TEXTO, bg=BLANCO,
        ).pack(anchor='w', pady=(0, PAD_L))

        # ── Navegación (primero para garantizar espacio) ──────────────────────
        tk.Frame(self, bg=GRIS_CLARO, height=1).pack(
            side='bottom', fill='x', pady=(PAD_L, 0)
        )
        nav = tk.Frame(self, bg=BLANCO)
        nav.pack(side='bottom', fill='x', pady=(PAD_S, 0))

        ttk.Button(
            nav, text="←  Atrás",
            style='Secundario.TButton',
            command=lambda: self.app.ir_a_pantalla(2)
        ).pack(side='left')

        ttk.Button(
            nav, text="▶  Ejecutar consolidación",
            style='Primario.TButton', command=self._ejecutar,
        ).pack(side='right')

        # ── Resumen de configuración ──────────────────────────────────────────
        panel_resumen = tk.Frame(self, bg=AZUL_CLARO, padx=PAD_M, pady=PAD_S)
        panel_resumen.pack(fill='x', pady=(0, PAD_L))

        n_archivos = len(self.app.estado['archivos'])
        n_fijas    = len(PESTANAS_FIJAS)
        n_extra    = len(self.app.estado.get('pestanas_extra', []))

        tk.Label(
            panel_resumen,
            text=f"📁 {n_archivos} archivo(s)  •  📋 {n_fijas + n_extra} pestaña(s) "
                 f"({n_fijas} obligatorias + {n_extra} adicionales)",
            font=FUENTE_CUERPO, fg=AZUL_OSCURO, bg=AZUL_CLARO
        ).pack(anchor='w')

        # ── Carpeta de destino ────────────────────────────────────────────────
        tk.Label(
            self, text="Carpeta de destino",
            font=FUENTE_CUERPO_BOLD, fg=NEGRO_SUAVE, bg=BLANCO
        ).pack(anchor='w', pady=(0, PAD_S))

        fila_carpeta = tk.Frame(self, bg=BLANCO)
        fila_carpeta.pack(fill='x', pady=(0, PAD_L))

        self.var_carpeta = tk.StringVar()
        tk.Entry(
            fila_carpeta, textvariable=self.var_carpeta,
            font=FUENTE_CUERPO, bg=GRIS_FONDO, fg=NEGRO_SUAVE,
            relief='flat', bd=0, highlightthickness=1,
            highlightbackground=GRIS_CLARO, highlightcolor=AZUL_PRIMARIO,
            state='readonly', readonlybackground=GRIS_FONDO,
        ).pack(side='left', fill='x', expand=True, ipady=8, padx=(0, PAD_S))

        ttk.Button(
            fila_carpeta, text="📂  Explorar",
            style='Secundario.TButton',
            command=self._seleccionar_carpeta,
        ).pack(side='right')

        # ── Nombre del archivo ────────────────────────────────────────────────
        tk.Label(
            self, text="Nombre del archivo  (.xlsx se agrega automáticamente)",
            font=FUENTE_CUERPO_BOLD, fg=NEGRO_SUAVE, bg=BLANCO
        ).pack(anchor='w', pady=(0, PAD_S))

        self.var_nombre = tk.StringVar()
        self.var_nombre.trace_add('write', self._actualizar_vista_previa)

        tk.Entry(
            self, textvariable=self.var_nombre,
            font=FUENTE_CUERPO, bg=BLANCO, fg=NEGRO_SUAVE,
            relief='flat', bd=0, highlightthickness=1,
            highlightbackground=GRIS_CLARO, highlightcolor=AZUL_PRIMARIO,
        ).pack(fill='x', ipady=8, pady=(0, PAD_S))

        # ── Vista previa de la ruta ───────────────────────────────────────────
        tk.Label(
            self, text="Archivo de salida:",
            font=FUENTE_PEQUEÑA_BOLD, fg=GRIS_TEXTO, bg=BLANCO
        ).pack(anchor='w', pady=(PAD_S, 2))

        self.lbl_preview = tk.Label(
            self, text="—",
            font=FUENTE_MONO, fg=NEGRO_SUAVE, bg=GRIS_FONDO,
            anchor='w', padx=PAD_S, pady=6,
            wraplength=660, justify='left',
        )
        self.lbl_preview.pack(fill='x')

    def _seleccionar_carpeta(self):
        """Abre el explorador de carpetas y guarda la ruta seleccionada."""
        carpeta = filedialog.askdirectory(title="Seleccionar carpeta de destino")
        if carpeta:
            self.var_carpeta.set(carpeta)
            self.app.estado['carpeta_destino'] = carpeta
            self._actualizar_vista_previa()

    def _actualizar_vista_previa(self, *_):
        """Actualiza la etiqueta de vista previa de la ruta completa."""
        carpeta = self.var_carpeta.get()
        nombre  = self.var_nombre.get().strip()

        if carpeta and nombre:
            self.lbl_preview.configure(
                text=os.path.join(carpeta, nombre + '.xlsx'), fg=NEGRO_SUAVE
            )
        elif carpeta:
            self.lbl_preview.configure(
                text="Ingresa el nombre del archivo.", fg=GRIS_TEXTO
            )
        else:
            self.lbl_preview.configure(
                text="Selecciona una carpeta de destino.", fg=GRIS_TEXTO
            )

    def _cargar_estado(self):
        """Pre-rellena los campos si ya hay valores guardados en el estado."""
        carpeta = self.app.estado.get('carpeta_destino', '')
        nombre  = self.app.estado.get('nombre_archivo', '')

        if carpeta:
            self.var_carpeta.set(carpeta)
        if nombre:
            self.var_nombre.set(nombre)
        else:
            fecha = datetime.datetime.now().strftime('%Y%m%d')
            self.var_nombre.set(f"consolidado_bases_datos_{fecha}")

        self._actualizar_vista_previa()

    def _ejecutar(self):
        """Valida los campos y lanza el proceso de consolidación."""
        carpeta = self.var_carpeta.get().strip()
        nombre  = self.var_nombre.get().strip()

        if not carpeta:
            messagebox.showwarning("Falta carpeta",
                                   "Debes seleccionar una carpeta de destino.")
            return
        if not nombre:
            messagebox.showwarning("Falta nombre",
                                   "Debes ingresar un nombre para el archivo.")
            return

        caracteres_invalidos = r'\/:*?"<>|'
        if any(c in nombre for c in caracteres_invalidos):
            messagebox.showwarning(
                "Nombre inválido",
                f"El nombre no puede contener los caracteres: {caracteres_invalidos}"
            )
            return

        ruta_final = os.path.join(carpeta, nombre + '.xlsx')

        if os.path.exists(ruta_final):
            if not messagebox.askyesno(
                "Archivo existente",
                f"Ya existe un archivo con ese nombre:\n{ruta_final}\n\n¿Deseas reemplazarlo?"
            ):
                return

        self.app.estado['carpeta_destino'] = carpeta
        self.app.estado['nombre_archivo']  = nombre
        self.app.estado['ruta_salida']     = ruta_final
        self.app.ir_a_pantalla(4)


# ──────────────────────────────────────────────────────────────────────────────
# SECCIÓN 6 — PANTALLA 4: PROGRESO Y RESULTADOS
# ──────────────────────────────────────────────────────────────────────────────

class PantallaResumen(tk.Frame):
    """
    Cuarta pantalla del flujo.
    Ejecuta el proceso en un hilo secundario para no bloquear la UI.
    Muestra barra de progreso y log en tiempo real.
    """

    def __init__(self, padre, app):
        super().__init__(padre, bg=BLANCO)
        self.app = app
        self._proceso_terminado = False
        self._construir_ui()
        self.after(150, self._lanzar_proceso)

    def _construir_ui(self):
        """Construye todos los elementos visuales de la pantalla."""

        self.lbl_titulo = tk.Label(
            self, text="Consolidando datos...",
            font=FUENTE_TITULO, fg=NEGRO_SUAVE, bg=BLANCO
        )
        self.lbl_titulo.pack(anchor='w', pady=(0, 4))

        self.lbl_subtitulo = tk.Label(
            self, text="El proceso está en curso. Por favor espera.",
            font=FUENTE_CUERPO, fg=GRIS_TEXTO, bg=BLANCO
        )
        self.lbl_subtitulo.pack(anchor='w', pady=(0, PAD_M))

        # ── Barra de progreso ─────────────────────────────────────────────────
        frame_prog = tk.Frame(self, bg=BLANCO)
        frame_prog.pack(fill='x', pady=(0, PAD_S))

        self.barra = ttk.Progressbar(
            frame_prog, style='SOS.Horizontal.TProgressbar',
            mode='determinate', maximum=100,
        )
        self.barra.pack(fill='x', pady=(0, 4))

        self.lbl_porcentaje = tk.Label(
            frame_prog, text="0%",
            font=FUENTE_PEQUEÑA_BOLD, fg=AZUL_PRIMARIO, bg=BLANCO
        )
        self.lbl_porcentaje.pack(anchor='e')

        # ── Log de eventos ────────────────────────────────────────────────────
        tk.Label(
            self, text="Registro del proceso:",
            font=FUENTE_CUERPO_BOLD, fg=NEGRO_SUAVE, bg=BLANCO
        ).pack(anchor='w', pady=(PAD_S, 4))

        marco_log = tk.Frame(self, bg=GRIS_CLARO, padx=1, pady=1)
        marco_log.pack(fill='both', expand=True)

        frame_log = tk.Frame(marco_log, bg=BLANCO)
        frame_log.pack(fill='both', expand=True)

        scroll_log = ttk.Scrollbar(frame_log)
        scroll_log.pack(side='right', fill='y')

        self.txt_log = tk.Text(
            frame_log, font=FUENTE_MONO, bg=BLANCO, fg=NEGRO_SUAVE,
            state='disabled', borderwidth=0, highlightthickness=0,
            relief='flat', yscrollcommand=scroll_log.set, wrap='word',
        )
        self.txt_log.pack(fill='both', expand=True, padx=8, pady=8)
        scroll_log.configure(command=self.txt_log.yview)

        self.txt_log.tag_configure('info',   foreground=NEGRO_SUAVE)
        self.txt_log.tag_configure('exito',  foreground=VERDE_EXITO)
        self.txt_log.tag_configure('error',  foreground=ROJO_ERROR)
        self.txt_log.tag_configure('aviso',  foreground=NARANJA_AVISO)

        # ── Navegación ────────────────────────────────────────────────────────
        tk.Frame(self, bg=GRIS_CLARO, height=1).pack(fill='x', pady=(PAD_M, 0))

        nav = tk.Frame(self, bg=BLANCO)
        nav.pack(fill='x', pady=(PAD_S, 0))

        self.btn_abrir = ttk.Button(
            nav, text="📂  Abrir carpeta de destino",
            style='Secundario.TButton',
            command=self._abrir_carpeta, state='disabled',
        )
        self.btn_abrir.pack(side='left', padx=(0, PAD_S))

        self.btn_nuevo = ttk.Button(
            nav, text="↺  Nuevo consolidado",
            style='Secundario.TButton',
            command=self._nuevo_proceso, state='disabled',
        )
        self.btn_nuevo.pack(side='left')

        self.btn_cerrar = ttk.Button(
            nav, text="✕  Cerrar",
            style='Primario.TButton',
            command=self.app.destroy, state='disabled',
        )
        self.btn_cerrar.pack(side='right')

    # ── Proceso en hilo secundario ────────────────────────────────────────────

    def _lanzar_proceso(self):
        """Inicia el proceso de consolidación en un hilo secundario."""
        threading.Thread(target=self._ejecutar_proceso, daemon=True).start()

    def _ejecutar_proceso(self):
        """Llama al procesador y actualiza la UI de forma segura con .after()."""
        estado = self.app.estado
        try:
            procesar(
                archivos           = estado['archivos'],
                pestanas_extra_sel = estado.get('pestanas_extra', []),
                ruta_salida        = estado['ruta_salida'],
                callback_log       = self._cb_log,
                callback_progreso  = self._cb_progreso,
            )
            self.after(0, self._proceso_exitoso)
        except Exception as e:
            self.after(0, lambda: self._proceso_fallido(str(e)))

    def _cb_log(self, mensaje, tipo='info'):
        self.after(0, lambda m=mensaje, t=tipo: self._escribir_log(m, t))

    def _cb_progreso(self, valor):
        self.after(0, lambda v=valor: self._actualizar_barra(v))

    def _escribir_log(self, mensaje, tipo='info'):
        self.txt_log.configure(state='normal')
        self.txt_log.insert('end', mensaje + '\n', tipo)
        self.txt_log.see('end')
        self.txt_log.configure(state='disabled')

    def _actualizar_barra(self, valor):
        self.barra['value'] = valor
        self.lbl_porcentaje.configure(text=f"{valor}%")

    def _proceso_exitoso(self):
        self.lbl_titulo.configure(text="✅ Consolidación completada", fg=VERDE_EXITO)
        self.lbl_subtitulo.configure(
            text="El archivo fue generado exitosamente.", fg=VERDE_EXITO
        )
        self._actualizar_barra(100)
        self.btn_abrir.configure(state='normal')
        self.btn_nuevo.configure(state='normal')
        self.btn_cerrar.configure(state='normal')

    def _proceso_fallido(self, error):
        self.lbl_titulo.configure(text="❌ Error en el proceso", fg=ROJO_ERROR)
        self.lbl_subtitulo.configure(
            text="Ocurrió un error durante la consolidación. Revisa el log.",
            fg=ROJO_ERROR
        )
        self._escribir_log(f"\n❌ Error: {error}", 'error')
        self.btn_nuevo.configure(state='normal')
        self.btn_cerrar.configure(state='normal')

    def _abrir_carpeta(self):
        """Abre la carpeta de destino en el explorador del sistema operativo."""
        carpeta = self.app.estado.get('carpeta_destino', '')
        if not carpeta or not os.path.exists(carpeta):
            return
        sistema = platform.system()
        if sistema == 'Windows':
            os.startfile(carpeta)
        elif sistema == 'Darwin':
            subprocess.Popen(['open', carpeta])
        else:
            subprocess.Popen(['xdg-open', carpeta])

    def _nuevo_proceso(self):
        """Reinicia la aplicación desde la pantalla 1 con el estado limpio."""
        self.app.estado = {
            'archivos': [], 'pestanas_extra': [],
            'carpeta_fuente': '', 'carpeta_destino': '',
            'nombre_archivo': '', 'ruta_salida': '',
        }
        self.app.ir_a_pantalla(1)


# ──────────────────────────────────────────────────────────────────────────────
# SECCIÓN 7 — VENTANA PRINCIPAL Y NAVEGACIÓN
# ──────────────────────────────────────────────────────────────────────────────

# Definición de los pasos del flujo
PASOS = [
    (1, "Archivos",  PantallaArchivos),
    (2, "Pestañas",  PantallaPestanas),
    (3, "Destino",   PantallaDestino),
    (4, "Resultado", PantallaResumen),
]


class AplicacionConcatenador(tk.Tk):
    """
    Ventana principal de la aplicación.
    Gestiona la navegación entre pantallas, el estado compartido
    y los elementos comunes: encabezado e indicador de pasos.
    """

    def __init__(self):
        super().__init__()

        self.title("Consolidador de Bases de Datos — Aldeas Infantiles SOS")
        self.geometry("820x640")
        self.minsize(720, 560)
        self.configure(bg=BLANCO)
        self.resizable(True, True)
        self._centrar_ventana(820, 640)

        self._estilo = ttk.Style(self)
        configurar_estilos(self._estilo)

        # ── Estado compartido entre todas las pantallas ───────────────────────
        self.estado = {
            'archivos':        [],
            'pestanas_extra':  [],
            'carpeta_fuente':  '',    # Carpeta escaneada en la pantalla 1
            'carpeta_destino': '',
            'nombre_archivo':  '',
            'ruta_salida':     '',
        }

        self._construir_encabezado()
        self._construir_indicador_pasos()
        self._construir_contenido()

        self._pantalla_actual = None
        self.ir_a_pantalla(1)

    def _centrar_ventana(self, ancho, alto):
        """Posiciona la ventana en el centro del monitor principal."""
        self.update_idletasks()
        x = (self.winfo_screenwidth()  // 2) - (ancho // 2)
        y = (self.winfo_screenheight() // 2) - (alto  // 2)
        self.geometry(f"{ancho}x{alto}+{x}+{y}")

    def _construir_encabezado(self):
        """Construye el encabezado superior con logo y título."""
        encabezado = tk.Frame(self, bg=AZUL_PRIMARIO, height=68)
        encabezado.pack(fill='x')
        encabezado.pack_propagate(False)

        logo_img = self._cargar_logo()
        if logo_img:
            self._logo_ref = logo_img
            tk.Label(
                encabezado, image=logo_img,
                bg=AZUL_PRIMARIO, padx=16
            ).pack(side='left', pady=8)
        else:
            tk.Label(
                encabezado, text="ALDEAS INFANTILES SOS",
                font=("Segoe UI", 11, "bold"),
                fg=BLANCO, bg=AZUL_PRIMARIO, padx=16
            ).pack(side='left', pady=8)

        tk.Frame(encabezado, bg='#4DC3EF', width=1).pack(
            side='left', fill='y', pady=14
        )
        tk.Label(
            encabezado, text="  Consolidador de Bases de Datos",
            font=("Segoe UI", 14, "bold"),
            fg=BLANCO, bg=AZUL_PRIMARIO
        ).pack(side='left', padx=PAD_M)

    def _cargar_logo(self):
        """
        Intenta cargar el logo desde la carpeta 'assets' junto al script.
        Retorna PhotoImage o None si no se encuentra o Pillow no está instalado.
        """
        try:
            from PIL import Image, ImageTk
            ruta = self._ruta_recurso(os.path.join("assets", "logo.jpg"))
            img = Image.open(ruta)
            img.thumbnail((210, 50))
            return ImageTk.PhotoImage(img)
        except Exception:
            return None

    def _ruta_recurso(self, ruta_relativa):
        """
        Resuelve rutas de recursos compatibles con PyInstaller.
        En modo ejecutable usa el directorio temporal _MEIPASS.
        """
        if hasattr(sys, '_MEIPASS'):
            return os.path.join(sys._MEIPASS, ruta_relativa)
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), ruta_relativa)

    def _construir_indicador_pasos(self):
        """Construye el indicador visual de progreso con círculos y conectores."""
        self._frame_indicador = tk.Frame(self, bg=GRIS_FONDO)
        self._frame_indicador.pack(fill='x')

        self._canvas_pasos = tk.Canvas(
            self._frame_indicador,
            height=56, bg=GRIS_FONDO, highlightthickness=0
        )
        self._canvas_pasos.pack(fill='x')
        self._canvas_pasos.bind('<Configure>', lambda e: self._dibujar_pasos())
        self._paso_activo = 1

    def _dibujar_pasos(self):
        """Dibuja los círculos, líneas y etiquetas del indicador de pasos."""
        c = self._canvas_pasos
        c.delete('all')

        ancho = c.winfo_width()
        if ancho < 10:
            return

        cy     = 28
        radio  = 14
        n      = len(PASOS)
        margen = 80
        paso_x = (ancho - 2 * margen) / (n - 1)
        posiciones = [margen + i * paso_x for i in range(n)]

        for i, (num, nombre, _) in enumerate(PASOS):
            x = posiciones[i]

            # Línea conectora
            if i < n - 1:
                color_linea = VERDE_EXITO if num < self._paso_activo else GRIS_CLARO
                c.create_line(
                    x + radio, cy, posiciones[i + 1] - radio, cy,
                    fill=color_linea, width=2
                )

            # Estado del paso
            if num < self._paso_activo:
                estado = 'completado'
            elif num == self._paso_activo:
                estado = 'activo'
            else:
                estado = 'pendiente'

            # Círculo del paso
            if estado == 'activo':
                c.create_oval(
                    x - radio, cy - radio, x + radio, cy + radio,
                    fill=AZUL_PRIMARIO, outline=AZUL_PRIMARIO
                )
                c.create_text(x, cy, text=str(num), fill=BLANCO,
                              font=("Segoe UI", 9, "bold"))
            elif estado == 'completado':
                c.create_oval(
                    x - radio, cy - radio, x + radio, cy + radio,
                    fill=VERDE_EXITO, outline=VERDE_EXITO
                )
                c.create_text(x, cy, text="✓", fill=BLANCO,
                              font=("Segoe UI", 9, "bold"))
            else:
                c.create_oval(
                    x - radio, cy - radio, x + radio, cy + radio,
                    fill=GRIS_FONDO, outline=GRIS_CLARO, width=2
                )
                c.create_text(x, cy, text=str(num), fill=GRIS_MEDIO,
                              font=("Segoe UI", 9))

            # Etiqueta del paso
            color_texto = (
                AZUL_PRIMARIO if estado == 'activo'
                else VERDE_EXITO if estado == 'completado'
                else GRIS_TEXTO
            )
            fuente_texto = (
                ("Segoe UI", 8, "bold") if estado == 'activo'
                else ("Segoe UI", 8)
            )
            c.create_text(x, cy + radio + 10,
                          text=nombre, fill=color_texto, font=fuente_texto)

    def _construir_contenido(self):
        """Crea el frame contenedor de las pantallas del flujo."""
        self._frame_contenido = tk.Frame(self, bg=BLANCO)
        self._frame_contenido.pack(fill='both', expand=True,
                                   padx=PAD_M * 2, pady=PAD_M)

    def ir_a_pantalla(self, numero):
        """
        Destruye la pantalla actual, crea la nueva e
        actualiza el indicador de pasos.
        """
        if self._pantalla_actual:
            self._pantalla_actual.destroy()

        self._paso_activo = numero
        self.after(10, self._dibujar_pasos)

        mapa = {num: clase for num, _, clase in PASOS}
        clase = mapa.get(numero)
        if clase:
            self._pantalla_actual = clase(self._frame_contenido, self)
            self._pantalla_actual.pack(fill='both', expand=True)


# ──────────────────────────────────────────────────────────────────────────────
# PUNTO DE ENTRADA
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = AplicacionConcatenador()
    app.mainloop()
