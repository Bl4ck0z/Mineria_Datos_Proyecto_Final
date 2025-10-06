"""
Pipeline ETL - Arquitectura Medallion: Bronze → Silver → Gold → Gold_ML
Procesa datos de ventas aplicando limpieza, transformación y ML
"""

import pandas as pd
import numpy as np
import pymssql
import warnings
import os
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import silhouette_score, accuracy_score

warnings.filterwarnings('ignore')

CONFIG = {
    'server': os.getenv('MSSQL_HOST', '10.0.0.13'),
    'port': int(os.getenv('MSSQL_PORT', '1433')),
    'database': os.getenv('MSSQL_DATABASE', 'ProyectoFinalMineraDatos'),
    'user': os.getenv('MSSQL_USER', 'sa'),
    'password': os.getenv('MSSQL_PASSWORD', 'M@st3rk3y'),
    'archivo_csv': '../data/1.BASE DATOS.csv',
    'archivo_log': '../data/proyecto_mineria_log.txt'
}

def log_mensaje(mensaje, archivo=CONFIG['archivo_log']):
    """Registra mensaje en consola y archivo log"""
    print(mensaje)
    with open(archivo, 'a', encoding='utf-8') as f:
        f.write(f"{mensaje}\n")

def limpiar_numerico(valor):
    """Convierte a numérico o retorna None"""
    if pd.isna(valor) or valor == '' or valor is None:
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    try:
        valor_str = str(valor).strip().replace(',', '').replace(' ', '')
        return None if valor_str == '-' else float(valor_str)
    except:
        return None

def parse_fecha_robusta(fecha_str):
    """Parsea fechas con múltiples formatos"""
    if pd.isna(fecha_str) or fecha_str == '':
        return None

    for fmt in ['%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d', '%m-%d-%Y']:
        try:
            return pd.to_datetime(fecha_str, format=fmt)
        except:
            continue
    return None

def tabla_existe_con_datos(cursor, nombre_tabla):
    """Verifica existencia y contenido de tabla"""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {nombre_tabla}")
        return cursor.fetchone()[0] > 0
    except:
        return False

def crear_bronze_layer(conn, df_original):
    """Carga datos raw sin transformar"""
    cursor = conn.cursor()

    log_mensaje("\n" + "="*80)
    log_mensaje("CAPA BRONZE - DATOS CRUDOS")
    log_mensaje("="*80)

    if tabla_existe_con_datos(cursor, 'Bronze'):
        count = cursor.execute("SELECT COUNT(*) FROM Bronze").fetchone()[0]
        log_mensaje(f"✓ Bronze existe ({count:,} registros). Saltando...")
        return count

    cursor.execute("DROP TABLE IF EXISTS Bronze")
    cursor.execute("""
        CREATE TABLE Bronze (
            id INT IDENTITY(1,1) PRIMARY KEY,
            Fecha_Venta VARCHAR(50), Nit VARCHAR(50), Nombre_Cliente VARCHAR(255),
            Tipo_Cliente VARCHAR(100), Sucursal VARCHAR(50), Cod_Depto VARCHAR(50),
            Cod_Municipio VARCHAR(50), Cod_Vendedor VARCHAR(30), Cod_Categoria VARCHAR(30),
            Nombre_Categoria VARCHAR(100), Cod_Linea VARCHAR(30), Nombre_Linea VARCHAR(100),
            Cod_Producto VARCHAR(30), Cantidad VARCHAR(50), Precio_Unit VARCHAR(50),
            Costo_Unit VARCHAR(50), Venta VARCHAR(50), Costos VARCHAR(50),
            Fecha_Carga DATETIME DEFAULT GETDATE()
        );
    """)
    conn.commit()

    columnas = ['Fecha_Venta', 'Nit', 'Nombre_Cliente', 'Tipo_Cliente', 'Sucursal',
                'Cod_Depto', 'Cod_Municipio', 'Cod_Vendedor', 'Cod_Categoria',
                'Nombre_Categoria', 'Cod_Linea', 'Nombre_Linea', 'Cod_Producto',
                'Cantidad', 'Precio_Unit', 'Costo_Unit', 'Venta', 'Costos']

    sql_insert = f"INSERT INTO Bronze ({', '.join(columnas)}) VALUES ({', '.join(['%s'] * len(columnas))})"

    for _, fila in df_original.iterrows():
        valores = [None if pd.isna(fila[col]) else str(fila[col]).strip() for col in columnas]
        cursor.execute(sql_insert, valores)

    conn.commit()
    log_mensaje(f"✓ Bronze creado: {len(df_original):,} registros")
    return len(df_original)

def crear_silver_layer(conn):
    """Limpia datos, elimina duplicados y calcula márgenes"""
    cursor = conn.cursor()

    log_mensaje("\n" + "="*80)
    log_mensaje("CAPA SILVER - LIMPIEZA Y TRANSFORMACIÓN")
    log_mensaje("="*80)

    if tabla_existe_con_datos(cursor, 'Silver'):
        log_mensaje(f"✓ Silver existe. Saltando...")
        return pd.read_sql("SELECT * FROM Silver", conn)

    df = pd.read_sql("SELECT * FROM Bronze", conn)
    log_mensaje(f"\n1. Extracción: {len(df):,} registros")

    # Limpieza fechas
    log_mensaje("\n2. Limpieza fechas:")
    df['Fecha_Venta'] = df['Fecha_Venta'].apply(parse_fecha_robusta)
    log_mensaje(f"   ✓ Válidas: {df['Fecha_Venta'].notna().sum():,}")
    log_mensaje(f"   ✗ Inválidas: {df['Fecha_Venta'].isna().sum()}")

    # Limpieza numéricos
    log_mensaje("\n3. Limpieza numéricos:")
    cols_num = ['Cantidad', 'Precio_Unit', 'Costo_Unit', 'Venta', 'Costos']
    for col in cols_num:
        df[col] = df[col].apply(limpiar_numerico)
    log_mensaje(f"   ✓ Procesadas: {', '.join(cols_num)}")

    # Márgenes
    log_mensaje("\n4. Cálculo márgenes:")
    df['margen_unit'] = df['Precio_Unit'] - df['Costo_Unit']
    df['margen_unit_pct'] = np.where(df['Precio_Unit'] > 0,
                                      (df['margen_unit'] / df['Precio_Unit'] * 100).round(2), None)
    df['margen_venta'] = df['Venta'] - df['Costos']
    df['margen_venta_pct'] = np.where(df['Venta'] > 0,
                                       (df['margen_venta'] / df['Venta'] * 100).round(2), None)

    log_mensaje(f"   ✓ Margen unitario promedio: {df['margen_unit'].mean():.2f}")
    log_mensaje(f"   ✓ Margen venta promedio: {df['margen_venta_pct'].mean():.2f}%")

    # Duplicados
    log_mensaje("\n5. Manejo duplicados:")
    clave = ['Nit', 'Sucursal', 'Cod_Vendedor', 'Cod_Producto', 'Cantidad', 'Fecha_Venta']
    grupos = df.groupby(clave)

    registros_orig = len(df)
    exactos = consolidados = contradictorios = 0
    filas_procesadas = []

    for _, grupo in grupos:
        if len(grupo) == 1:
            filas_procesadas.append(grupo.iloc[0])
        elif grupo.drop_duplicates().shape[0] == 1:
            filas_procesadas.append(grupo.iloc[0])
            exactos += len(grupo) - 1
        else:
            # Intentar consolidar
            fila_cons = {}
            contradiccion = False

            for col in df.columns:
                if col in clave or col == 'id':
                    fila_cons[col] = grupo.iloc[0][col]
                else:
                    unicos = grupo[col].dropna().unique()
                    if len(unicos) == 0:
                        fila_cons[col] = None
                    elif len(unicos) == 1:
                        fila_cons[col] = unicos[0]
                    else:
                        contradiccion = True
                        break

            if contradiccion:
                filas_procesadas.extend([fila for _, fila in grupo.iterrows()])
                contradictorios += len(grupo)
            else:
                filas_procesadas.append(pd.Series(fila_cons))
                consolidados += 1

    df = pd.DataFrame(filas_procesadas).reset_index(drop=True)

    # Marcar contradicciones
    df['tiene_contradiccion'] = False
    for _, grupo in df.groupby(clave):
        if len(grupo) > 1:
            df.loc[grupo.index, 'tiene_contradiccion'] = True

    log_mensaje(f"   ✓ Duplicados exactos: {exactos:,}")
    log_mensaje(f"   ✓ Consolidados: {consolidados:,}")
    log_mensaje(f"   ✗ Contradicciones: {contradictorios:,}")
    log_mensaje(f"   → Total: {registros_orig:,} → {len(df):,}")

    # Guardar
    log_mensaje("\n6. Guardando Silver:")
    cursor.execute("DROP TABLE IF EXISTS Silver")
    cursor.execute("""
        CREATE TABLE Silver (
            id INT IDENTITY(1,1) PRIMARY KEY,
            Fecha_Venta DATE, Nit BIGINT, Nombre_Cliente VARCHAR(255),
            Tipo_Cliente VARCHAR(100), Sucursal VARCHAR(50), Cod_Depto INT,
            Cod_Municipio INT, Cod_Vendedor VARCHAR(30), Cod_Categoria VARCHAR(30),
            Nombre_Categoria VARCHAR(100), Cod_Linea VARCHAR(30), Nombre_Linea VARCHAR(100),
            Cod_Producto VARCHAR(30), Cantidad INT, Precio_Unit DECIMAL(12,2),
            Costo_Unit DECIMAL(12,2), Venta DECIMAL(15,2), Costos DECIMAL(15,2),
            margen_unit DECIMAL(15,2), margen_unit_pct DECIMAL(5,2),
            margen_venta DECIMAL(15,2), margen_venta_pct DECIMAL(5,2),
            tiene_contradiccion BIT DEFAULT 0, Fecha_Procesamiento DATETIME DEFAULT GETDATE()
        );
    """)
    conn.commit()

    cols_insert = ['Fecha_Venta', 'Nit', 'Nombre_Cliente', 'Tipo_Cliente', 'Sucursal',
                   'Cod_Depto', 'Cod_Municipio', 'Cod_Vendedor', 'Cod_Categoria',
                   'Nombre_Categoria', 'Cod_Linea', 'Nombre_Linea', 'Cod_Producto',
                   'Cantidad', 'Precio_Unit', 'Costo_Unit', 'Venta', 'Costos',
                   'margen_unit', 'margen_unit_pct', 'margen_venta', 'margen_venta_pct',
                   'tiene_contradiccion']

    sql = f"INSERT INTO Silver ({', '.join(cols_insert)}) VALUES ({', '.join(['%s'] * len(cols_insert))})"
    for _, fila in df.iterrows():
        cursor.execute(sql, [None if pd.isna(fila[col]) else fila[col] for col in cols_insert])

    conn.commit()
    log_mensaje(f"   ✓ {len(df):,} registros guardados")
    return df

def crear_gold_layer(conn, df_silver):
    """Filtra datos limpios y agrega dimensiones temporales"""
    cursor = conn.cursor()

    log_mensaje("\n" + "="*80)
    log_mensaje("CAPA GOLD - DATOS ANALÍTICOS")
    log_mensaje("="*80)

    if tabla_existe_con_datos(cursor, 'Gold'):
        log_mensaje(f"✓ Gold existe. Saltando...")
        return pd.read_sql("SELECT * FROM Gold", conn)

    # Filtrar datos limpios
    df = df_silver[(df_silver['tiene_contradiccion'] == False) &
                   (df_silver['Fecha_Venta'].notna())].copy()

    log_mensaje(f"\n1. Filtrado:")
    log_mensaje(f"   → Silver: {len(df_silver):,}")
    log_mensaje(f"   ✗ Excluidos: {len(df_silver) - len(df):,}")
    log_mensaje(f"   ✓ Gold: {len(df):,}")

    # Dimensiones temporales
    log_mensaje("\n2. Dimensiones temporales:")
    df['Año'] = df['Fecha_Venta'].dt.year
    df['Mes'] = df['Fecha_Venta'].dt.month
    df['Dia'] = df['Fecha_Venta'].dt.day
    df['Trimestre'] = df['Fecha_Venta'].dt.quarter
    df['Dia_Semana'] = df['Fecha_Venta'].dt.dayofweek
    df['Nombre_Mes'] = df['Fecha_Venta'].dt.strftime('%B')
    df['Año_Mes'] = df['Fecha_Venta'].dt.to_period('M').astype(str)
    df['PeriodoKey'] = df['Año'] * 100 + df['Mes']
    log_mensaje(f"   ✓ 8 campos agregados")

    # Guardar
    log_mensaje("\n3. Guardando Gold:")
    cursor.execute("DROP TABLE IF EXISTS Gold")
    cursor.execute("""
        CREATE TABLE Gold (
            id INT IDENTITY(1,1) PRIMARY KEY,
            Fecha_Venta DATE, Año INT, Mes INT, Dia INT, Trimestre INT,
            Dia_Semana INT, Nombre_Mes VARCHAR(20), Año_Mes VARCHAR(10), PeriodoKey INT,
            Nit BIGINT, Nombre_Cliente VARCHAR(255), Tipo_Cliente VARCHAR(100),
            Sucursal VARCHAR(50), Cod_Depto INT, Cod_Municipio INT, Cod_Vendedor VARCHAR(30),
            Cod_Categoria VARCHAR(30), Nombre_Categoria VARCHAR(100), Cod_Linea VARCHAR(30),
            Nombre_Linea VARCHAR(100), Cod_Producto VARCHAR(30), Cantidad INT,
            Precio_Unit DECIMAL(12,2), Costo_Unit DECIMAL(12,2), Venta DECIMAL(15,2),
            Costos DECIMAL(15,2), margen_unit DECIMAL(15,2), margen_unit_pct DECIMAL(5,2),
            margen_venta DECIMAL(15,2), margen_venta_pct DECIMAL(5,2),
            Fecha_Procesamiento DATETIME DEFAULT GETDATE()
        );
    """)
    conn.commit()

    cols = ['Fecha_Venta', 'Año', 'Mes', 'Dia', 'Trimestre', 'Dia_Semana', 'Nombre_Mes',
            'Año_Mes', 'PeriodoKey', 'Nit', 'Nombre_Cliente', 'Tipo_Cliente', 'Sucursal',
            'Cod_Depto', 'Cod_Municipio', 'Cod_Vendedor', 'Cod_Categoria', 'Nombre_Categoria',
            'Cod_Linea', 'Nombre_Linea', 'Cod_Producto', 'Cantidad', 'Precio_Unit',
            'Costo_Unit', 'Venta', 'Costos', 'margen_unit', 'margen_unit_pct',
            'margen_venta', 'margen_venta_pct']

    sql = f"INSERT INTO Gold ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"
    for _, fila in df.iterrows():
        cursor.execute(sql, [None if pd.isna(fila[col]) else fila[col] for col in cols])

    conn.commit()
    log_mensaje(f"   ✓ {len(df):,} registros guardados")
    return df

def crear_gold_ml_layer(conn, df_gold):
    """Aplica normalización, encoding y modelos ML"""
    cursor = conn.cursor()

    log_mensaje("\n" + "="*80)
    log_mensaje("CAPA GOLD_ML - MACHINE LEARNING")
    log_mensaje("="*80)

    if tabla_existe_con_datos(cursor, 'Gold_ML_Metricas'):
        log_mensaje(f"✓ Gold_ML existe. Saltando...")
        cursor.execute("SELECT Valor FROM Gold_ML_Metricas WHERE Metrica = 'Silhouette_Score'")
        silhouette = float(cursor.fetchone()[0])
        cursor.execute("SELECT Valor FROM Gold_ML_Metricas WHERE Metrica = 'Accuracy'")
        accuracy = float(cursor.fetchone()[0])
        return None, silhouette, accuracy

    df = df_gold.copy()

    # Normalización
    log_mensaje("\n1. Normalización (StandardScaler):")
    cols_norm = ['Cantidad', 'Precio_Unit', 'Costo_Unit', 'Venta', 'Costos', 'margen_venta']
    scaler = StandardScaler()
    df[cols_norm] = scaler.fit_transform(df[cols_norm])
    log_mensaje(f"   ✓ {len(cols_norm)} columnas normalizadas")

    # One-Hot Encoding
    log_mensaje("\n2. One-Hot Encoding:")
    for col in ['Tipo_Cliente', 'Sucursal', 'Nombre_Categoria']:
        dummies = pd.get_dummies(df[col], prefix=col, drop_first=True)
        df = pd.concat([df, dummies], axis=1)
        log_mensaje(f"   ✓ {col}: {len(dummies.columns)} columnas")

    # K-Means Clustering
    log_mensaje("\n3. K-Means Clustering:")
    X_cluster = df[['Venta', 'margen_venta', 'Cantidad']].dropna()
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_cluster)

    df['Cluster'] = -1
    df.loc[X_cluster.index, 'Cluster'] = clusters

    silhouette = silhouette_score(X_cluster, clusters)
    log_mensaje(f"   ✓ Silhouette Score: {silhouette:.4f}")

    for i in range(3):
        cluster_data = df_gold[df['Cluster'] == i]
        log_mensaje(f"   → Cluster {i}: {len(cluster_data):,} registros, "
                   f"Venta avg: ${cluster_data['Venta'].mean():,.0f}")

    # Random Forest
    log_mensaje("\n4. Random Forest:")
    df_model = df[df['Tipo_Cliente'].notna()].copy()
    features = ['Venta', 'margen_venta', 'Cantidad', 'Mes', 'Dia_Semana']
    X = df_model[features]
    y = df_model['Tipo_Cliente']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2,
                                                          random_state=42, stratify=y)

    rf = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
    rf.fit(X_train, y_train)
    accuracy = accuracy_score(y_test, rf.predict(X_test))

    log_mensaje(f"   ✓ Accuracy: {accuracy:.2%}")
    df['Tipo_Cliente_Predicho'] = rf.predict(df[features])

    # Guardar métricas
    log_mensaje("\n5. Guardando métricas ML:")
    cursor.execute("DROP TABLE IF EXISTS Gold_ML_Metricas")
    cursor.execute("""
        CREATE TABLE Gold_ML_Metricas (
            id INT IDENTITY(1,1) PRIMARY KEY,
            Modelo VARCHAR(50), Metrica VARCHAR(50), Valor DECIMAL(18,2),
            Descripcion VARCHAR(255), Fecha_Ejecucion DATETIME DEFAULT GETDATE()
        );
    """)
    conn.commit()

    cursor.execute("INSERT INTO Gold_ML_Metricas (Modelo, Metrica, Valor, Descripcion) VALUES (%s,%s,%s,%s)",
                   ('KMeans', 'Silhouette_Score', float(silhouette), 'Calidad segmentación (k=3)'))
    cursor.execute("INSERT INTO Gold_ML_Metricas (Modelo, Metrica, Valor, Descripcion) VALUES (%s,%s,%s,%s)",
                   ('RandomForest', 'Accuracy', float(accuracy), 'Precisión clasificación'))

    # Métricas por cluster
    for i in range(3):
        cluster_data = df_gold.loc[df[df['Cluster'] == i].index]
        cursor.execute("INSERT INTO Gold_ML_Metricas (Modelo, Metrica, Valor, Descripcion) VALUES (%s,%s,%s,%s)",
                      ('KMeans', f'Cluster_{i}_Count', float(len(cluster_data)), f'Registros cluster {i}'))
        cursor.execute("INSERT INTO Gold_ML_Metricas (Modelo, Metrica, Valor, Descripcion) VALUES (%s,%s,%s,%s)",
                      ('KMeans', f'Cluster_{i}_AvgVenta', float(cluster_data['Venta'].mean()), f'Venta avg cluster {i}'))

    conn.commit()
    log_mensaje(f"   ✓ 11 métricas guardadas")
    return df, silhouette, accuracy

def crear_vistas(conn):
    """Crea vistas analíticas de negocio"""
    cursor = conn.cursor()

    log_mensaje("\n" + "="*80)
    log_mensaje("VISTAS ANALÍTICAS")
    log_mensaje("="*80)

    try:
        cursor.execute("SELECT COUNT(*) FROM Vista_Ventas_Mes")
        log_mensaje("✓ Vistas existen. Saltando...")
        return
    except:
        pass

    vistas = {
        "Vista_Ventas_Mes": """
            SELECT Año, Mes, SUM(Venta) AS Total_Ventas, SUM(Costos) AS Total_Costos,
                   SUM(margen_venta) AS Total_Margen, COUNT(*) AS Num_Transacciones,
                   AVG(Venta) AS Ticket_Promedio, AVG(margen_venta_pct) AS Margen_Promedio_Pct
            FROM Gold GROUP BY Año, Mes
        """,
        "Vista_Ventas_Categoria": """
            SELECT Nombre_Categoria, COUNT(*) AS Num_Transacciones, SUM(Cantidad) AS Total_Cantidad,
                   SUM(Venta) AS Total_Ventas, SUM(Costos) AS Total_Costos, SUM(margen_venta) AS Total_Margen,
                   AVG(margen_venta_pct) AS Margen_Promedio_Pct, AVG(Venta) AS Venta_Promedio
            FROM Gold GROUP BY Nombre_Categoria
        """,
        "Vista_Ventas_Sucursal": """
            SELECT Sucursal, COUNT(*) AS Num_Transacciones, SUM(Cantidad) AS Total_Cantidad,
                   SUM(Venta) AS Total_Ventas, SUM(Costos) AS Total_Costos, SUM(margen_venta) AS Total_Margen,
                   AVG(margen_venta_pct) AS Margen_Promedio_Pct, AVG(Venta) AS Venta_Promedio
            FROM Gold GROUP BY Sucursal
        """,
        "Vista_Top_Clientes": """
            SELECT Nit, Nombre_Cliente, Tipo_Cliente, COUNT(*) AS Num_Transacciones,
                   SUM(Venta) AS Total_Ventas, SUM(Costos) AS Total_Costos, SUM(margen_venta) AS Total_Margen,
                   AVG(margen_venta_pct) AS Margen_Promedio_Pct, AVG(Venta) AS Ticket_Promedio,
                   MAX(Fecha_Venta) AS Ultima_Compra
            FROM Gold GROUP BY Nit, Nombre_Cliente, Tipo_Cliente
        """,
        "Vista_Ventas_Linea": """
            SELECT Nombre_Linea, Nombre_Categoria, COUNT(*) AS Num_Transacciones,
                   SUM(Cantidad) AS Total_Cantidad, SUM(Venta) AS Total_Ventas,
                   SUM(margen_venta) AS Total_Margen, AVG(margen_venta_pct) AS Margen_Promedio_Pct
            FROM Gold GROUP BY Nombre_Linea, Nombre_Categoria
        """,
        "Vista_Ventas_Vendedor": """
            SELECT Cod_Vendedor, COUNT(*) AS Num_Transacciones, COUNT(DISTINCT Nit) AS Num_Clientes_Unicos,
                   SUM(Venta) AS Total_Ventas, SUM(margen_venta) AS Total_Margen,
                   AVG(margen_venta_pct) AS Margen_Promedio_Pct, AVG(Venta) AS Venta_Promedio
            FROM Gold GROUP BY Cod_Vendedor
        """,
        "Vista_Ventas_Trimestre": """
            SELECT Año, Trimestre, COUNT(*) AS Num_Transacciones, SUM(Venta) AS Total_Ventas,
                   SUM(margen_venta) AS Total_Margen, AVG(margen_venta_pct) AS Margen_Promedio_Pct,
                   AVG(Venta) AS Ticket_Promedio
            FROM Gold GROUP BY Año, Trimestre
        """,
        "Vista_Ventas_DiaSemana": """
            SELECT Dia_Semana,
                   CASE Dia_Semana
                       WHEN 0 THEN 'Lunes' WHEN 1 THEN 'Martes' WHEN 2 THEN 'Miércoles'
                       WHEN 3 THEN 'Jueves' WHEN 4 THEN 'Viernes' WHEN 5 THEN 'Sábado'
                       WHEN 6 THEN 'Domingo'
                   END AS Nombre_Dia,
                   COUNT(*) AS Num_Transacciones, SUM(Venta) AS Total_Ventas,
                   AVG(Venta) AS Venta_Promedio, SUM(margen_venta) AS Total_Margen
            FROM Gold GROUP BY Dia_Semana
        """
    }

    for nombre, query in vistas.items():
        cursor.execute(f"DROP VIEW IF EXISTS {nombre}")
        cursor.execute(f"CREATE VIEW {nombre} AS {query}")
        conn.commit()
        log_mensaje(f"   ✓ {nombre}")

    log_mensaje(f"\n✓ {len(vistas)} vistas creadas")

def main():
    """Pipeline completo ETL + ML"""

    with open(CONFIG['archivo_log'], 'w', encoding='utf-8') as f:
        f.write(f"PIPELINE MINERÍA DE DATOS - LOG\n")
        f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*80 + "\n\n")

    try:
        log_mensaje("Conectando a SQL Server...")
        conn = pymssql.connect(
            server=CONFIG['server'], port=CONFIG['port'],
            database=CONFIG['database'], user=CONFIG['user'],
            password=CONFIG['password']
        )
        log_mensaje("✓ Conexión exitosa\n")

        log_mensaje(f"Cargando: {CONFIG['archivo_csv']}")
        df = pd.read_csv(CONFIG['archivo_csv'], encoding='utf-8')

        # Normalizar columnas
        df = df.rename(columns={
            'Fecha Venta': 'Fecha_Venta', 'Nombre Cliente': 'Nombre_Cliente',
            'Tipo Cliente': 'Tipo_Cliente', 'Nombre Categoría': 'Nombre_Categoria',
            'Nombre Linea': 'Nombre_Linea', 'Precio Unit': 'Precio_Unit',
            'Costo Unit': 'Costo_Unit'
        })

        log_mensaje(f"✓ {len(df):,} registros, {len(df.columns)} columnas\n")

        crear_bronze_layer(conn, df)
        df_silver = crear_silver_layer(conn)
        df_gold = crear_gold_layer(conn, df_silver)
        df_ml, silhouette, accuracy = crear_gold_ml_layer(conn, df_gold)
        crear_vistas(conn)

        log_mensaje("\n" + "="*80)
        log_mensaje("RESUMEN FINAL")
        log_mensaje("="*80)
        log_mensaje(f"\nTablas:")
        log_mensaje(f"  • Bronze: {len(df):,} (raw)")
        log_mensaje(f"  • Silver: {len(df_silver):,} (clean)")
        log_mensaje(f"  • Gold: {len(df_gold):,} (analytics)")
        log_mensaje(f"  • Gold_ML_Metricas: 11 (ML)")
        log_mensaje(f"\nVistas: 8 vistas analíticas")
        log_mensaje(f"\nML Metrics:")
        log_mensaje(f"  • K-Means: {silhouette:.4f}")
        log_mensaje(f"  • Random Forest: {accuracy:.2%}")
        log_mensaje(f"\n✓ Pipeline completado")
        log_mensaje(f"✓ Log: {CONFIG['archivo_log']}")

        conn.close()

    except Exception as e:
        log_mensaje(f"\n❌ ERROR: {str(e)}")
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise

if __name__ == "__main__":
    main()
