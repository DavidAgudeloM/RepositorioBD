-- ======================================================
-- SCRIPT ETL COMPLETO: Jardinería → Staging → DW (Modelo Estrella)
-- Incluye creación de staging, vistas clean, dimensiones y hechos.
-- ======================================================

-- 1. Preparación de esquemas
DROP DATABASE IF EXISTS jardineria_stg;
DROP DATABASE IF EXISTS jardineria_dw;

CREATE DATABASE jardineria_stg CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE jardineria_dw  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 2. Tablas Staging
USE jardineria_stg;

CREATE TABLE stg_cliente (
  ID_cliente INT, nombre_cliente VARCHAR(50),
  nombre_contacto VARCHAR(30), apellido_contacto VARCHAR(30),
  telefono VARCHAR(15), fax VARCHAR(15),
  linea_direccion1 VARCHAR(50), linea_direccion2 VARCHAR(50),
  ciudad VARCHAR(50), region VARCHAR(50),
  pais VARCHAR(50), codigo_postal VARCHAR(10),
  ID_empleado_rep_ventas INT, limite_credito DECIMAL(15,2),
  src_system VARCHAR(30) DEFAULT 'OLTP_Jardineria',
  extract_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_producto (
  ID_producto INT, CodigoProducto VARCHAR(15), nombre VARCHAR(70),
  Categoria INT, dimensiones VARCHAR(25), proveedor VARCHAR(50),
  descripcion TEXT, cantidad_en_stock SMALLINT,
  precio_venta DECIMAL(15,2), precio_proveedor DECIMAL(15,2),
  src_system VARCHAR(30) DEFAULT 'OLTP_Jardineria',
  extract_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_pedido (
  ID_pedido INT, fecha_pedido DATE, fecha_esperada DATE,
  fecha_entrega DATE, estado VARCHAR(15), comentarios TEXT,
  ID_cliente INT,
  src_system VARCHAR(30) DEFAULT 'OLTP_Jardineria',
  extract_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_detalle_pedido (
  ID_detalle_pedido INT, ID_pedido INT, ID_producto INT,
  cantidad INT, precio_unidad DECIMAL(15,2), numero_linea SMALLINT,
  src_system VARCHAR(30) DEFAULT 'OLTP_Jardineria',
  extract_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_pago (
  ID_pago INT, ID_cliente INT, forma_pago VARCHAR(40),
  id_transaccion VARCHAR(50), fecha_pago DATE, total DECIMAL(15,2),
  src_system VARCHAR(30) DEFAULT 'OLTP_Jardineria',
  extract_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Vistas de limpieza
CREATE OR REPLACE VIEW vw_cliente_clean AS
SELECT
  ID_cliente,
  TRIM(REPLACE(nombre_cliente, CHAR(13), '')) AS nombre_cliente,
  NULLIF(TRIM(nombre_contacto),'') AS nombre_contacto,
  NULLIF(TRIM(apellido_contacto),'') AS apellido_contacto,
  TRIM(telefono) AS telefono,
  NULLIF(TRIM(fax),'') AS fax,
  TRIM(linea_direccion1) AS linea_direccion1,
  NULLIF(TRIM(linea_direccion2),'') AS linea_direccion2,
  UPPER(TRIM(ciudad))  AS ciudad,
  UPPER(TRIM(COALESCE(region,''))) AS region,
  UPPER(TRIM(COALESCE(pais,'')))   AS pais,
  NULLIF(TRIM(codigo_postal),'') AS codigo_postal,
  ID_empleado_rep_ventas,
  limite_credito
FROM stg_cliente;

CREATE OR REPLACE VIEW vw_producto_clean AS
SELECT
  ID_producto, CodigoProducto, TRIM(nombre) AS nombre,
  Categoria, NULLIF(TRIM(dimensiones),'') AS dimensiones,
  TRIM(COALESCE(proveedor,'')) AS proveedor,
  NULLIF(TRIM(descripcion),'') AS descripcion,
  GREATEST(cantidad_en_stock,0) AS cantidad_en_stock,
  precio_venta, precio_proveedor
FROM stg_producto;

CREATE OR REPLACE VIEW vw_pedido_clean AS
SELECT
  p.*,
  CASE UPPER(p.estado)
    WHEN 'ENTREGADO'  THEN 'ENTREGADO'
    WHEN 'PENDIENTE'  THEN 'PENDIENTE'
    WHEN 'RECHAZADO'  THEN 'RECHAZADO'
    ELSE 'OTRO'
  END AS estado_norm,
  DATEDIFF(p.fecha_entrega, p.fecha_esperada) AS dias_diferencia_entrega
FROM stg_pedido p;

CREATE OR REPLACE VIEW vw_detalle_clean AS
SELECT
  ID_detalle_pedido, ID_pedido, ID_producto,
  GREATEST(cantidad,0) AS cantidad,
  GREATEST(precio_unidad,0) AS precio_unidad,
  numero_linea,
  (GREATEST(cantidad,0) * GREATEST(precio_unidad,0)) AS importe_linea
FROM stg_detalle_pedido;

CREATE OR REPLACE VIEW vw_pago_clean AS
SELECT * FROM stg_pago;

-- 4. Data Mart: Dimensiones y Hechos
USE jardineria_dw;

CREATE TABLE dim_fecha (
  sk_fecha INT PRIMARY KEY,
  fecha DATE NOT NULL,
  anio SMALLINT, trimestre TINYINT, mes TINYINT, dia TINYINT,
  nombre_mes VARCHAR(10), nombre_dia VARCHAR(10),
  es_fin_de_semana TINYINT
);

CREATE TABLE dim_cliente (
  sk_cliente INT AUTO_INCREMENT PRIMARY KEY,
  id_cliente_nat INT UNIQUE,
  nombre_cliente VARCHAR(50),
  nombre_contacto VARCHAR(30),
  apellido_contacto VARCHAR(30),
  telefono VARCHAR(15), ciudad VARCHAR(50),
  region VARCHAR(50), pais VARCHAR(50),
  codigo_postal VARCHAR(10),
  limite_credito DECIMAL(15,2)
);

CREATE TABLE dim_producto (
  sk_producto INT AUTO_INCREMENT PRIMARY KEY,
  id_producto_nat INT UNIQUE,
  codigo_producto VARCHAR(15),
  nombre VARCHAR(70),
  categoria_id INT,
  categoria_desc VARCHAR(50),
  proveedor VARCHAR(50),
  precio_venta DECIMAL(15,2),
  precio_proveedor DECIMAL(15,2)
);

CREATE TABLE dim_estado_pedido (
  sk_estado INT AUTO_INCREMENT PRIMARY KEY,
  estado_norm VARCHAR(15) UNIQUE
);

CREATE TABLE fact_ventas (
  sk_venta BIGINT AUTO_INCREMENT PRIMARY KEY,
  sk_cliente INT NOT NULL,
  sk_producto INT NOT NULL,
  sk_estado INT NOT NULL,
  sk_fecha_pedido INT NOT NULL,
  sk_fecha_esperada INT,
  sk_fecha_entrega INT,
  id_pedido_nat INT NOT NULL,
  id_detalle_nat INT,
  numero_linea SMALLINT,
  cantidad INT NOT NULL,
  precio_unidad DECIMAL(15,2) NOT NULL,
  importe_linea DECIMAL(15,2) NOT NULL,
  dias_dif_entrega INT NULL,
  FOREIGN KEY (sk_cliente) REFERENCES dim_cliente(sk_cliente),
  FOREIGN KEY (sk_producto) REFERENCES dim_producto(sk_producto),
  FOREIGN KEY (sk_estado) REFERENCES dim_estado_pedido(sk_estado),
  FOREIGN KEY (sk_fecha_pedido) REFERENCES dim_fecha(sk_fecha),
  FOREIGN KEY (sk_fecha_esperada) REFERENCES dim_fecha(sk_fecha),
  FOREIGN KEY (sk_fecha_entrega) REFERENCES dim_fecha(sk_fecha)
);
