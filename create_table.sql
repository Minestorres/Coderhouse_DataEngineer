#Creación de tabla para Redshift

CREATE TABLE IF NOT EXISTS cotizaciones (
    id_cotizacion INTEGER PRIMARY KEY,
    moneda VARCHAR(255),
    compra FLOAT,
    venta FLOAT,
    fecha DATE,
    casa VARCHAR(255)
);