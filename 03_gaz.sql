CREATE TABLE clients (
    id_client INT PRIMARY KEY,
    nom VARCHAR(100),
    type_client VARCHAR(50),
    ville VARCHAR(100)
);

CREATE TABLE compteurs (
    id_compteur INT PRIMARY KEY,
    type_compteur VARCHAR(50),
    date_installation DATE
);

CREATE TABLE contrats (
    id_contrat INT PRIMARY KEY,
    id_client INT,
    id_compteur INT,
    date_debut DATE,
    date_fin DATE,
    FOREIGN KEY (id_client) REFERENCES clients(id_client),
    FOREIGN KEY (id_compteur) REFERENCES compteurs(id_compteur)
);

CREATE TABLE consommations (
    id_consommation INT PRIMARY KEY,
    id_contrat INT,
    date_consommation DATE,
    volume DECIMAL(10,2),
    FOREIGN KEY (id_contrat) REFERENCES contrats(id_contrat)
);

CREATE TABLE factures (
    id_facture INT PRIMARY KEY,
    id_contrat INT,
    date_facture DATE,
    montant DECIMAL(10,2),
    statut VARCHAR(50),
    FOREIGN KEY (id_contrat) REFERENCES contrats(id_contrat)
);
