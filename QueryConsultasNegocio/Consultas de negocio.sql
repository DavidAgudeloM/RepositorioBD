
-- ===================================================================
-- CONSULTAS DE NEGOCIO (STAGING) CON RANGO AUTOMÁTICO
-- Schema: jardineria_stg
-- Este script detecta automáticamente @ini y @fin a partir de los datos
-- para evitar resultados vacíos cuando no se ejecutan los SET manuales.
-- MySQL 8.0+
-- ===================================================================

USE jardineria_stg;

-- Derivar fechas automáticamente desde los pedidos
SET @ini = COALESCE(@ini, (SELECT MIN(fecha_pedido) FROM vw_pedido_clean));
SET @fin = COALESCE(@fin, (SELECT MAX(fecha_pedido) FROM vw_pedido_clean));

-- (Para pagos puede que no haya datos; si hay, derivamos rango aparte)
SET @ini_pg = (SELECT MIN(fecha_pago) FROM vw_pago_clean);
SET @fin_pg = (SELECT MAX(fecha_pago) FROM vw_pago_clean);

-- 1) KPI principales
SELECT
  SUM(d.importe_linea) AS ventas_totales,
  SUM(d.cantidad) AS unidades,
  COUNT(DISTINCT d.ID_pedido) AS pedidos,
  ROUND(SUM(d.importe_linea) / NULLIF(COUNT(DISTINCT d.ID_pedido),0), 2) AS ticket_promedio,
  ROUND((SUM(d.importe_linea) - SUM(d.cantidad * p.precio_proveedor)) / NULLIF(SUM(d.importe_linea),0) * 100, 2) AS margen_pct
FROM vw_detalle_clean d
JOIN vw_pedido_clean  pe ON pe.ID_pedido = d.ID_pedido
JOIN vw_producto_clean p ON p.ID_producto = d.ID_producto
WHERE (@ini IS NULL OR pe.fecha_pedido >= @ini)
  AND (@fin IS NULL OR pe.fecha_pedido <= @fin);

-- 2) Serie temporal: ventas mensuales
SELECT DATE_FORMAT(pe.fecha_pedido, '%Y-%m') AS periodo,
       SUM(d.importe_linea) AS ventas
FROM vw_detalle_clean d
JOIN vw_pedido_clean pe ON pe.ID_pedido = d.ID_pedido
WHERE (@ini IS NULL OR pe.fecha_pedido >= @ini)
  AND (@fin IS NULL OR pe.fecha_pedido <= @fin)
GROUP BY periodo
ORDER BY periodo;

-- 3) Ventas por país y ciudad
SELECT c.pais, c.ciudad,
       SUM(d.importe_linea) AS ventas,
       COUNT(DISTINCT d.ID_pedido) AS pedidos
FROM vw_detalle_clean d
JOIN vw_pedido_clean  pe ON pe.ID_pedido = d.ID_pedido
JOIN vw_cliente_clean c  ON c.ID_cliente = pe.ID_cliente
WHERE (@ini IS NULL OR pe.fecha_pedido >= @ini)
  AND (@fin IS NULL OR pe.fecha_pedido <= @fin)
GROUP BY c.pais, c.ciudad
ORDER BY ventas DESC, pedidos DESC;

-- 4) Top 10 productos por ventas
SELECT p.CodigoProducto, p.nombre,
       SUM(d.importe_linea) AS ventas,
       SUM(d.cantidad) AS unidades
FROM vw_detalle_clean d
JOIN vw_producto_clean p ON p.ID_producto = d.ID_producto
JOIN vw_pedido_clean  pe ON pe.ID_pedido = d.ID_pedido
WHERE (@ini IS NULL OR pe.fecha_pedido >= @ini)
  AND (@fin IS NULL OR pe.fecha_pedido <= @fin)
GROUP BY p.ID_producto, p.CodigoProducto, p.nombre
ORDER BY ventas DESC
LIMIT 10;

-- 5) Top 10 productos por margen
SELECT p.CodigoProducto, p.nombre,
       SUM(d.importe_linea - d.cantidad * p.precio_proveedor) AS margen,
       SUM(d.importe_linea) AS ventas,
       SUM(d.cantidad) AS unidades
FROM vw_detalle_clean d
JOIN vw_producto_clean p ON p.ID_producto = d.ID_producto
JOIN vw_pedido_clean  pe ON pe.ID_pedido = d.ID_pedido
WHERE (@ini IS NULL OR pe.fecha_pedido >= @ini)
  AND (@fin IS NULL OR pe.fecha_pedido <= @fin)
GROUP BY p.ID_producto, p.CodigoProducto, p.nombre
ORDER BY margen DESC
LIMIT 10;

-- 6) Ventas por categoría
SELECT p.Categoria AS categoria_id,
       SUM(d.importe_linea) AS ventas,
       SUM(d.cantidad) AS unidades
FROM vw_detalle_clean d
JOIN vw_producto_clean p ON p.ID_producto = d.ID_producto
JOIN vw_pedido_clean  pe ON pe.ID_pedido = d.ID_pedido
WHERE (@ini IS NULL OR pe.fecha_pedido >= @ini)
  AND (@fin IS NULL OR pe.fecha_pedido <= @fin)
GROUP BY p.Categoria
ORDER BY ventas DESC;

-- 7) Filtrar por estado
SELECT pe.estado_norm,
       COUNT(DISTINCT pe.ID_pedido) AS pedidos,
       SUM(d.importe_linea) AS ventas
FROM vw_pedido_clean pe
LEFT JOIN vw_detalle_clean d ON d.ID_pedido = pe.ID_pedido
WHERE (@ini IS NULL OR pe.fecha_pedido >= @ini)
  AND (@fin IS NULL OR pe.fecha_pedido <= @fin)
GROUP BY pe.estado_norm WITH ROLLUP;

-- 8) Tasa de entrega a tiempo por mes
SELECT DATE_FORMAT(pe.fecha_esperada, '%Y-%m') AS periodo,
       ROUND(AVG(CASE
           WHEN pe.dias_diferencia_entrega IS NULL THEN NULL
           WHEN pe.dias_diferencia_entrega <= 0 THEN 1 ELSE 0 END) * 100, 2) AS tasa_entrega_a_tiempo_pct
FROM vw_pedido_clean pe
WHERE (@ini IS NULL OR pe.fecha_esperada >= @ini)
  AND (@fin IS NULL OR pe.fecha_esperada <= @fin)
GROUP BY periodo
ORDER BY periodo;

-- 9) Retraso promedio por país
SELECT c.pais,
       ROUND(AVG(pe.dias_diferencia_entrega), 2) AS demora_promedio_dias,
       SUM(CASE WHEN pe.dias_diferencia_entrega > 0 THEN 1 ELSE 0 END) AS entregas_tarde,
       SUM(CASE WHEN pe.dias_diferencia_entrega <= 0 THEN 1 ELSE 0 END) AS entregas_a_tiempo
FROM vw_pedido_clean pe
JOIN vw_cliente_clean c ON c.ID_cliente = pe.ID_cliente
WHERE pe.fecha_entrega IS NOT NULL
  AND (@ini IS NULL OR pe.fecha_entrega >= @ini)
  AND (@fin IS NULL OR pe.fecha_entrega <= @fin)
GROUP BY c.pais
ORDER BY demora_promedio_dias DESC;

-- 10) Ticket promedio por país/mes
SELECT c.pais, DATE_FORMAT(pe.fecha_pedido, '%Y-%m') AS periodo,
       ROUND(SUM(d.importe_linea) / NULLIF(COUNT(DISTINCT d.ID_pedido),0), 2) AS ticket_promedio
FROM vw_detalle_clean d
JOIN vw_pedido_clean  pe ON pe.ID_pedido = d.ID_pedido
JOIN vw_cliente_clean c  ON c.ID_cliente = pe.ID_cliente
WHERE (@ini IS NULL OR pe.fecha_pedido >= @ini)
  AND (@fin IS NULL OR pe.fecha_pedido <= @fin)
GROUP BY c.pais, periodo
ORDER BY c.pais, periodo;

-- 11) Pagos por forma y mes (solo si hay pagos)
SELECT DATE_FORMAT(pg.fecha_pago, '%Y-%m') AS periodo,
       pg.forma_pago,
       SUM(pg.total) AS total_pagado,
       COUNT(*) AS n_pagos
FROM vw_pago_clean pg
WHERE (@ini_pg IS NULL OR pg.fecha_pago >= @ini_pg)
  AND (@fin_pg IS NULL OR pg.fecha_pago <= @fin_pg)
GROUP BY periodo, pg.forma_pago
ORDER BY periodo, total_pagado DESC;
