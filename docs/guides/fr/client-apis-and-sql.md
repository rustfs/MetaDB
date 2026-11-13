# APIs Clientes, Services gRPC & 20 Modèles SQL (Français)

* **Retour à l'index**: [Présentation Français](./README.md) | [Portail](../../README.md)

---

## 1. Surfaces d'Accès Client

1. **SDK Rust embarqué (`metadb-server`)** : Appels directs en mémoire sans sérialisation (API synchrone disponible dans `v0.1.0-prototype`, API asynchrone en **Phase 6**).
2. **Services gRPC `tonic` (`metadb.v1` — Contrats Phase 1 / Serveur Phase 6)** :
   * `MetadataService` : `CreateFile`, `Mkdir`, `Unlink`, `Rmdir`, `SetAttr`, `Lookup`, `DirStat`, `ListDir`, `ExecuteSql`, `GetQuota`, `SetQuotaLimit`, `ListQuotas`, `GetSecondaryIndex`, `ScanSecondaryIndex`.
   * `TransactionService` : `Rename` (`1-RTT` local ou `2PC` inter-shards automatique) ainsi que `Prepare2pc`, `CommitPrimary2pc`, `CommitSecondary2pc`, `Abort2pc`, `CheckTxnStatus`, `ApplyAuxBatch`.
   * `ControlPlaneService` : `GetRoutes`, `AllocateIdBlock`, `StoreHeartbeat`, `TriggerShardSplit`, `EnableDualModeSplit`, `ScaleShardReplicas`, `RegisterCell`.
3. **20 Modèles SQL Légers (`metadb-proxy`)** : Compilation directe en `<5 µs` sans optimiseur SQL généraliste (8 modèles littéraux actifs aujourd'hui ; 20 modèles paramétrés `?` en **Phase 5**).

> **Nom réservé** : Le nom `'_attr_'` (ainsi que tout nom contenant `'\0'`) est interdit pour les fichiers/répertoires utilisateurs (`MetaDbError::ReservedEntryName`) et réservé au modèle #7 (`DirStat`).

---

## 2. Catalogue Complet des 20 Modèles SQL (`metadb-proxy`)

| # | Modèle SQL | Sémantique & Chemin d'Exécution | État |
| :--- | :--- | :--- | :--- |
| **1** | `INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (?, ?, ?, 'FILE', ?, ?, ?, ?)` | Création de fichier (`1-RTT` Pushdown + files async) | Prototype / Phase 5 (`?`) |
| **2** | `INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (?, ?, ?, 'DIR', ?, ?)` | Création de répertoire (`1-RTT` colocalisé ou `2PC`) | Prototype / Phase 5 (`?`) |
| **3** | `DELETE FROM MetaTable WHERE pid = ? AND name = ?` | Suppression de fichier `Unlink` (`1-RTT`) | Prototype / Phase 5 (`?`) |
| **4** | `RMDIR FROM MetaTable WHERE pid = ? AND name = ?` | Suppression de répertoire vide (scan faisant foi) | Prototype / Phase 5 (`?`) |
| **5** | `UPDATE MetaTable SET pid = ?, name = ? WHERE pid = ? AND name = ?` | Renommage exclusif (`AlreadyExists` si la cible existe) | Prototype / Phase 5 (`?`) |
| **6** | `REPLACE INTO MetaTable (pid, name) SELECT ?, ? FROM MetaTable WHERE pid = ? AND name = ?` | Renommage POSIX avec écrasement atomique | Phase 5 |
| **7** | `SELECT * FROM MetaTable WHERE pid = ? AND name = '_attr_'` | `DirStat` linéarisable (`ReadIndex` + réconciliation) | Prototype / Phase 5 (`?`) |
| **8** | `SELECT * FROM MetaTable WHERE pid = ? AND name = ?` | Recherche ponctuelle `Lookup` (`ReadIndex`) | Prototype / Phase 5 (`?`) |
| **9** | `SELECT * FROM MetaTable WHERE pid = ? ORDER BY name ASC` | Parcours de répertoire (borné par limite par défaut) | Prototype / Phase 5 (`?`) |
| **10** | `SELECT * FROM MetaTable WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT ?` | `ListDir` paginé par curseur (recommandé) | Phase 5 |
| **11** | `SELECT * FROM MetaTable WHERE pid = ? AND name LIKE ? ORDER BY name ASC LIMIT ?` | `ListDir` filtré par préfixe (`'prefix%'`) | Phase 5 |
| **12** | `UPDATE MetaTable SET mode = ? WHERE pid = ? AND name = ?` | Modification des permissions `chmod` (`1-RTT`) | Phase 5 |
| **13** | `UPDATE MetaTable SET uid = ? WHERE pid = ? AND name = ?` | Changement de propriétaire `chown` + transfert quota | Phase 5 |
| **14** | `UPDATE MetaTable SET size = ?, fid = ? WHERE pid = ? AND name = ?` | Validation taille / `FID` objet + deltas | Phase 5 |
| **15** | `UPDATE MetaTable SET modify_time = ? WHERE pid = ? AND name = ?` | Mise à jour `modify_time` (`utimes`, `1-RTT`) | Phase 5 |
| **16** | `SELECT * FROM QuotaTable WHERE uid = ?` | Consultation du quota utilisateur (`used_bytes`, `used_inodes`) | Phase 5 |
| **17** | `INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (?, ?) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = ?` | Définition / mise à jour du plafond de quota | Phase 5 |
| **18** | `SELECT * FROM QuotaTable WHERE uid >= ? ORDER BY uid ASC LIMIT ?` | Audit paginé de `QuotaTable` | Phase 5 |
| **19** | `SELECT * FROM SecondaryIndexTable WHERE fid = ?` | Index inverse `FID -> (PID, name, create_time)` | Phase 5 |
| **20** | `SELECT * FROM SecondaryIndexTable WHERE fid > ? ORDER BY fid ASC LIMIT ?` | Scan paginé par curseur pour GC / vérification | Phase 5 |
