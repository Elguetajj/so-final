CREATE DATABASE finaldb;
USE finaldb;

CREATE TABLE IF NOT EXISTS leads(
    id INT,
    nombre VARCHAR(255),
    telefono VARCHAR(255),
    fecha DATE,
    ciudad VARCHAR(255),
    productor_id INT,
    fechahora_ingesta TIMESTAMP,
    PRIMARY KEY (lead_id)
);

CREATE TABLE IF NOT EXISTS buyers(
    lead_id INT,
    comprador INT NOT NULL,
    monto INT,
    fechahora_ingesta TIMESTAMP,
    FOREIGN KEY (lead_id) 
        REFERENCES leads(id)
);
