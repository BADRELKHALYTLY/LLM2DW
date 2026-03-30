# DW Architect - Data Warehouse Star Schema Generator

Outil algorithmique qui transforme un schema OLTP (SQL) + un contexte metier en un **schema en etoile (star schema)** pour Data Warehouse.

## Fonctionnalites

- **100% algorithmique** — zero hardcoding, pas de LLM requis
- **Supporte francais et anglais** (axes: `par X`, `by X`, `selon X`, `according to X`)
- **Detection automatique** du fait, des dimensions, des mesures
- **Validation contexte/schema** — detecte les contextes incoherents
- **7 etapes** avec sortie JSON a chaque etape
- **SQL complet** genere (CREATE TABLE avec FK + `out_timestamp`)
- **Valide sur 1,000,000 de tests** avec 100% de reussite

## Utilisation

```bash
python dw_architect.py <schema.sql> <context.txt>
```

### Exemple

```bash
python dw_architect.py examples/01_hopital.sql examples/01_hopital.txt
```

### Mode Ollama (optionnel)

```bash
python dw_architect.py schema.sql context.txt --ollama llama3
```

## Algorithme en 7 etapes

| Etape | Description |
|-------|-------------|
| 0 | Validation schema + contexte |
| 1 | Identification du FAIT (table centrale) |
| 2 | Extraction des DIMENSIONS depuis le contexte |
| 3 | Mapping dimensions → tables sources |
| 4 | Classification des attributs (quantitatif/qualitatif/temporel) |
| 5 | Generation des tables DIMENSION |
| 6 | Generation de la table FAIT (FK + mesures + out_timestamp) |
| 7 | Ajout Dim_Temps + generation SQL final |

## Exemples inclus

| # | Domaine | Tables | Dimensions |
|---|---------|--------|------------|
| 01 | Hopital - Consultations | 4 | Temps, Patient, Diagnostic, Medecin |
| 02 | E-commerce - Commandes | 4 | Temps, Produit, Client, Commande |
| 03 | Gaz - Consommation | 5 | Temps, Client, Compteur |
| 04 | Restaurant - Commandes | 5 | Temps, Plat, Client, Serveur |
| 05 | Banque - Transactions | 3 | Temps, Client, Agence |
| 06 | Transport - Livraisons | 3 | Temps, Chauffeur, Vehicule |
| 07 | Universite - Notes | 3 | Temps, Etudiant, Matiere |
| 08 | Hotel - Reservations | 3 | Temps, Client, Chambre |
| 09 | Cinema - Seances | 3 | Temps, Film, Salle |
| 10 | Agriculture - Recoltes | 3 | Temps, Parcelle, Culture |

## Fichiers de sortie

- `dw_result.json` — resultat complet (7 etapes en JSON)
- `dw_star_schema.sql` — schema SQL du Data Warehouse

## Prerequis

- Python 3.10+
- Aucune dependance externe (mode standalone)
- `requests` uniquement pour le mode Ollama (`pip install requests`)
