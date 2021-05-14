CREATE DATABASE finaldb;
USE finaldb;

CREATE TABLE leads (
	id INTEGER NOT NULL AUTO_INCREMENT, 
	nombre VARCHAR(255), 
	telefono VARCHAR(255), 
	fecha DATE, 
	ciudad VARCHAR(255), 
	productor_id INTEGER, 
    fechahora_ingesta TIMESTAMP,
	PRIMARY KEY (id)
);

CREATE TABLE buyers (
	id INTEGER NOT NULL AUTO_INCREMENT, 
	lead_id INTEGER, 
	comprador VARCHAR(255), 
	monto INTEGER, 
    fechahora_ingesta TIMESTAMP,
	PRIMARY KEY (id), 
	FOREIGN KEY(lead_id) REFERENCES leads (id)
);


