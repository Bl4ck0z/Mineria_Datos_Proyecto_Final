-- ============================================================================
-- VISTAS ANALÍTICAS - PROYECTO MINERÍA DE DATOS
-- Base de datos: ProyectoFinalMineraDatos
-- Todas las vistas se basan en la tabla Gold
-- ============================================================================

-- ============================================================================
-- 1. Vista_Ventas_Mes
-- Agregación mensual de ventas
-- ============================================================================
DROP VIEW IF EXISTS Vista_Ventas_Mes;
GO

CREATE VIEW Vista_Ventas_Mes AS
SELECT
    Año,
    Mes,
    SUM(Venta) AS Total_Ventas,
    SUM(Costos) AS Total_Costos,
    SUM(margen_venta) AS Total_Margen,
    COUNT(*) AS Num_Transacciones,
    AVG(Venta) AS Ticket_Promedio,
    AVG(margen_venta_pct) AS Margen_Promedio_Pct
FROM Gold
GROUP BY Año, Mes;
GO

-- ============================================================================
-- 2. Vista_Ventas_Categoria
-- Análisis por categoría de productos
-- ============================================================================
DROP VIEW IF EXISTS Vista_Ventas_Categoria;
GO

CREATE VIEW Vista_Ventas_Categoria AS
SELECT
    Nombre_Categoria,
    COUNT(*) AS Num_Transacciones,
    SUM(Cantidad) AS Total_Cantidad,
    SUM(Venta) AS Total_Ventas,
    SUM(Costos) AS Total_Costos,
    SUM(margen_venta) AS Total_Margen,
    AVG(margen_venta_pct) AS Margen_Promedio_Pct,
    AVG(Venta) AS Venta_Promedio
FROM Gold
GROUP BY Nombre_Categoria;
GO

-- ============================================================================
-- 3. Vista_Ventas_Sucursal
-- Análisis por sucursal
-- ============================================================================
DROP VIEW IF EXISTS Vista_Ventas_Sucursal;
GO

CREATE VIEW Vista_Ventas_Sucursal AS
SELECT
    Sucursal,
    COUNT(*) AS Num_Transacciones,
    SUM(Cantidad) AS Total_Cantidad,
    SUM(Venta) AS Total_Ventas,
    SUM(Costos) AS Total_Costos,
    SUM(margen_venta) AS Total_Margen,
    AVG(margen_venta_pct) AS Margen_Promedio_Pct,
    AVG(Venta) AS Venta_Promedio
FROM Gold
GROUP BY Sucursal;
GO

-- ============================================================================
-- 4. Vista_Top_Clientes
-- Ranking y análisis de clientes
-- ============================================================================
DROP VIEW IF EXISTS Vista_Top_Clientes;
GO

CREATE VIEW Vista_Top_Clientes AS
SELECT
    Nit,
    Nombre_Cliente,
    Tipo_Cliente,
    COUNT(*) AS Num_Transacciones,
    SUM(Venta) AS Total_Ventas,
    SUM(Costos) AS Total_Costos,
    SUM(margen_venta) AS Total_Margen,
    AVG(margen_venta_pct) AS Margen_Promedio_Pct,
    AVG(Venta) AS Ticket_Promedio,
    MAX(Fecha_Venta) AS Ultima_Compra
FROM Gold
GROUP BY Nit, Nombre_Cliente, Tipo_Cliente;
GO

-- ============================================================================
-- 5. Vista_Ventas_Linea
-- Análisis por línea de producto
-- ============================================================================
DROP VIEW IF EXISTS Vista_Ventas_Linea;
GO

CREATE VIEW Vista_Ventas_Linea AS
SELECT
    Nombre_Linea,
    Nombre_Categoria,
    COUNT(*) AS Num_Transacciones,
    SUM(Cantidad) AS Total_Cantidad,
    SUM(Venta) AS Total_Ventas,
    SUM(margen_venta) AS Total_Margen,
    AVG(margen_venta_pct) AS Margen_Promedio_Pct
FROM Gold
GROUP BY Nombre_Linea, Nombre_Categoria;
GO

-- ============================================================================
-- 6. Vista_Ventas_Vendedor
-- Desempeño de vendedores
-- ============================================================================
DROP VIEW IF EXISTS Vista_Ventas_Vendedor;
GO

CREATE VIEW Vista_Ventas_Vendedor AS
SELECT
    Cod_Vendedor,
    COUNT(*) AS Num_Transacciones,
    COUNT(DISTINCT Nit) AS Num_Clientes_Unicos,
    SUM(Venta) AS Total_Ventas,
    SUM(margen_venta) AS Total_Margen,
    AVG(margen_venta_pct) AS Margen_Promedio_Pct,
    AVG(Venta) AS Venta_Promedio
FROM Gold
GROUP BY Cod_Vendedor;
GO

-- ============================================================================
-- 7. Vista_Ventas_Trimestre
-- Agregación trimestral
-- ============================================================================
DROP VIEW IF EXISTS Vista_Ventas_Trimestre;
GO

CREATE VIEW Vista_Ventas_Trimestre AS
SELECT
    Año,
    Trimestre,
    COUNT(*) AS Num_Transacciones,
    SUM(Venta) AS Total_Ventas,
    SUM(margen_venta) AS Total_Margen,
    AVG(margen_venta_pct) AS Margen_Promedio_Pct,
    AVG(Venta) AS Ticket_Promedio
FROM Gold
GROUP BY Año, Trimestre;
GO

-- ============================================================================
-- 8. Vista_Ventas_DiaSemana
-- Patrón de ventas por día de la semana
-- ============================================================================
DROP VIEW IF EXISTS Vista_Ventas_DiaSemana;
GO

CREATE VIEW Vista_Ventas_DiaSemana AS
SELECT
    Dia_Semana,
    CASE Dia_Semana
        WHEN 0 THEN 'Lunes'
        WHEN 1 THEN 'Martes'
        WHEN 2 THEN 'Miércoles'
        WHEN 3 THEN 'Jueves'
        WHEN 4 THEN 'Viernes'
        WHEN 5 THEN 'Sábado'
        WHEN 6 THEN 'Domingo'
    END AS Nombre_Dia,
    COUNT(*) AS Num_Transacciones,
    SUM(Venta) AS Total_Ventas,
    AVG(Venta) AS Venta_Promedio,
    SUM(margen_venta) AS Total_Margen
FROM Gold
GROUP BY Dia_Semana;
GO

-- ============================================================================
-- QUERIES DE EJEMPLO PARA USAR LAS VISTAS
-- ============================================================================

-- Ejemplo 1: Top 10 meses con mayores ventas
-- SELECT TOP 10 * FROM Vista_Ventas_Mes ORDER BY Total_Ventas DESC;

-- Ejemplo 2: Top 5 categorías más rentables
-- SELECT TOP 5 * FROM Vista_Ventas_Categoria ORDER BY Total_Margen DESC;

-- Ejemplo 3: Top 20 clientes por volumen de compra
-- SELECT TOP 20 * FROM Vista_Top_Clientes ORDER BY Total_Ventas DESC;

-- Ejemplo 4: Comparación de sucursales
-- SELECT * FROM Vista_Ventas_Sucursal ORDER BY Total_Ventas DESC;

-- Ejemplo 5: Mejor día de la semana para ventas
-- SELECT * FROM Vista_Ventas_DiaSemana ORDER BY Total_Ventas DESC;

-- Ejemplo 6: Tendencia trimestral
-- SELECT * FROM Vista_Ventas_Trimestre ORDER BY Año, Trimestre;

-- Ejemplo 7: Top 10 vendedores
-- SELECT TOP 10 * FROM Vista_Ventas_Vendedor ORDER BY Total_Ventas DESC;

-- Ejemplo 8: Análisis de líneas de producto
-- SELECT * FROM Vista_Ventas_Linea ORDER BY Total_Ventas DESC;
