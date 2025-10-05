-- Cargar clientes
INSERT INTO jardineria_dw.dim_cliente (id_cliente_nat, nombre_cliente, nombre_contacto,
    apellido_contacto, telefono, ciudad, region, pais, codigo_postal, limite_credito)
SELECT ID_cliente, nombre_cliente, nombre_contacto, apellido_contacto,
       telefono, ciudad, region, pais, codigo_postal, limite_credito
FROM jardineria_stg.vw_cliente_clean;

-- Cargar productos
INSERT INTO jardineria_dw.dim_producto (id_producto_nat, codigo_producto, nombre, categoria_id, proveedor, precio_venta, precio_proveedor)
SELECT ID_producto, CodigoProducto, nombre, Categoria, proveedor, precio_venta, precio_proveedor
FROM jardineria_stg.vw_producto_clean;

-- Cargar estados
INSERT INTO jardineria_dw.dim_estado_pedido (estado_norm)
SELECT DISTINCT estado_norm
FROM jardineria_stg.vw_pedido_clean;

INSERT INTO jardineria_dw.fact_ventas
(sk_cliente, sk_producto, sk_estado, sk_fecha_pedido, sk_fecha_esperada, sk_fecha_entrega,
 id_pedido_nat, id_detalle_nat, numero_linea, cantidad, precio_unidad, importe_linea, dias_dif_entrega)
SELECT 
  c.sk_cliente,
  p.sk_producto,
  e.sk_estado,
  f1.sk_fecha, f2.sk_fecha, f3.sk_fecha,
  dp.ID_pedido, dp.ID_detalle_pedido, dp.numero_linea,
  dp.cantidad, dp.precio_unidad, dp.importe_linea,
  pe.dias_diferencia_entrega
FROM jardineria_stg.vw_detalle_clean dp
JOIN jardineria_stg.vw_pedido_clean pe ON dp.ID_pedido = pe.ID_pedido
JOIN jardineria_dw.dim_cliente c ON pe.ID_cliente = c.id_cliente_nat
JOIN jardineria_dw.dim_producto p ON dp.ID_producto = p.id_producto_nat
JOIN jardineria_dw.dim_estado_pedido e ON pe.estado_norm = e.estado_norm
JOIN jardineria_dw.dim_fecha f1 ON f1.fecha = pe.fecha_pedido
LEFT JOIN jardineria_dw.dim_fecha f2 ON f2.fecha = pe.fecha_esperada
LEFT JOIN jardineria_dw.dim_fecha f3 ON f3.fecha = pe.fecha_entrega;