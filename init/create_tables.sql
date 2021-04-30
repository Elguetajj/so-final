CREATE DATABASE finaldb;
USE finaldb;

CREATE TABLE IF NOT EXISTS leads(
    lead_id INT AUTO_INCREMENT,
    id_file INT NOT NULL,
    nombre VARCHAR(255),
    telefono VARCHAR(255),
    fecha DATE,
    ciudad VARCHAR(255),
    productor_id INT NOT NULL,
    fechahora_ingesta TIMESTAMP,
    PRIMARY KEY (lead_id)
);

CREATE TABLE IF NOT EXISTS buyers(
    lead_id INT,
    comprador INT NOT NULL,
    monto INT,
    fechahora_ingesta TIMESTAMP,
    FOREIGN KEY (lead_id) 
        REFERENCES leads(lead_id)
);
