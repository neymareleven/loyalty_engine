# Brief 11 Partie B — Anonymisation / suppression client (frontend admin)

Document autonome pour le développeur frontend. Le backend (Partie A) et la route boutique (Partie C) sont déjà livrés. **Ne pas coder de logique métier côté front** : seulement UI, appels API et messages.

Toutes les routes admin exigent :

- Header **`X-Brand`** : marque courante
- **`Authorization`** : Basic auth API

Prérequis : migrations à jour (`alembic upgrade head`), puis :

```http
GET /admin/health/schema
X-Brand: <brand>
```

Doit répondre `{"ok": true, ...}` et inclure notamment :

- `customers.anonymized_at`
- `erased_identities`
- `unomi_delete_queue`

---

## Objectif produit

Sur la **fiche client**, remplacer le bouton « Supprimer » par une action guidée qui propose :

1. **Exporter** les données (recommandé avant toute suppression)
2. **Anonymiser** (recommandé) — retire l’identité, garde l’historique et les chiffres
3. **Supprimer définitivement** — effacement complet (demande légale), irréversible

Après succès : toast / message clair, puis **retour à la liste clients**.

---

## Concepts (à respecter côté UI)

| Concept | Comportement |
|--------|----------------|
| **Anonymiser** | Mode par défaut. Plus d’email / sexe / naissance ; `profile_id` devient `anon_…` ; coupons non utilisés annulés ; garanties révoquées ; montants / mouvements / transactions conservés. |
| **Effacer** | Supprime toutes les lignes liées. Exige de retaper le `profile_id` dans l’UI **et** de l’envoyer en query `confirm`. |
| **Déjà anonymisé** | Relancer anonymize renvoie `200` + `alreadyAnonymized: true` (résumé vide). Afficher un message dédié, pas une erreur. |
| **Liste** | Le serveur **exclut** les clients `ANONYMIZED` par défaut. Option UI : filtre lecture seule pour les afficher. |
| **Bypass / empreinte** | Géré côté serveur. Le front n’a rien à calculer. Champ règle optionnel : `customer.previously_erased`. |

### Ce que le front ne doit PAS faire

- Ne pas appeler l’ancien DELETE « hard delete » sans `mode` en pensant effacer : **sans query, le défaut est `anonymize`**.
- Ne pas inventer un second appel Unomi / plugin WP depuis l’admin.
- Ne pas afficher de PII dans les toasts de succès (le résumé serveur n’en contient pas).
- Ne pas exposer par défaut `purge_fingerprint` / `keep_sales` (options avancées / rare ; hors MVP).

---

## 1. Fiche client — dialogue « Supprimer ce client »

### Entrée

Remplacer le bouton actuel « Supprimer » par :

**Supprimer ce client** → ouvre une modale / drawer.

### Contenu de la modale

1. **Titre** : Supprimer ce client  
2. **Lien / bouton secondaire** : **Exporter les données**  
   - Appelle l’export (section 2)  
   - Télécharge un fichier JSON (`customer-{profileId}-export.json`)  
   - Peut rester ouvert après téléchargement  
3. **Deux choix exclusifs** (radio ou cartes) :

| Choix | Libellé | Texte d’aide |
|-------|---------|--------------|
| A (défaut) | **Anonymiser (recommandé)** | Retire le nom, l’email et les données personnelles. L’historique et les chiffres sont conservés. Les coupons non utilisés sont annulés. |
| B | **Supprimer définitivement** | Efface toutes les données du client (demande d’effacement). Action irréversible. |

4. Si choix B : champ obligatoire  
   - Label : « Pour confirmer, retapez l’identifiant du client »  
   - Placeholder / valeur attendue = `profile_id` affiché sur la fiche  
   - Bouton de confirmation **désactivé** tant que la saisie ≠ `profile_id` (comparaison exacte, trim)

5. Boutons pied de modale :
   - Annuler  
   - Confirmer (libellé dynamique : « Anonymiser » / « Supprimer définitivement »)

---

## 2. Export

```http
GET /customers/{brand}/{profile_id}/export
X-Brand: {brand}
Authorization: Basic …
```

### Réponse 200 (forme)

```json
{
  "brand": "batira",
  "profileId": "wp_123",
  "customer": { "...": "fiche sérialisée" },
  "transactions": [],
  "pointMovements": [],
  "purchaseMovements": [],
  "cashMovements": [],
  "coupons": [],
  "rewards": [],
  "redemptions": [],
  "guarantees": [],
  "loyaltyHistory": []
}
```

### UX

- Bouton « Exporter les données » dans la modale **et** (optionnel) dans la barre d’actions de la fiche.
- Spinner pendant l’appel ; en erreur 404 : « Client introuvable ».
- Téléchargement : `JSON.stringify(data, null, 2)` → fichier.

---

## 3. Anonymiser

```http
DELETE /customers/{brand}/{profile_id}?mode=anonymize
X-Brand: {brand}
Authorization: Basic …
```

### Réponse succès (client actif)

```json
{
  "deleted": true,
  "mode": "anonymize",
  "alreadyAnonymized": false,
  "brand": "batira",
  "profileId": "anon_…",
  "counts": {
    "transactions": 12,
    "redemptions": 1,
    "customer_coupons": 2,
    "customer_rewards": 1,
    "guarantees": 1,
    "aliases": 0,
    "segment_members": 0,
    "storefront_audit_events": 0,
    "webhook_outbox": 0,
    "point_movements": 0,
    "purchase_movements": 0,
    "cash_movements": 0
  },
  "unomi": { "ok": true }
}
```

`unomi` est optionnel (présent si sync Unomi tentée après commit).

### Réponse déjà anonymisé (200)

```json
{
  "deleted": false,
  "alreadyAnonymized": true,
  "mode": "anonymize",
  "brand": "batira",
  "profileId": "anon_…",
  "counts": {}
}
```

**Message UI** : « Ce client est déjà anonymisé. » puis retour liste (ou fermer la modale).

### Message succès (nouveau)

« Client anonymisé. Les données personnelles ont été retirées ; l’historique est conservé. »  
Puis `router.push` / navigation vers la **liste clients**.

> Après anonymisation, l’ancien `profile_id` ne résout plus la fiche. Ne pas rester sur l’URL détail avec l’ancien id.

---

## 4. Supprimer définitivement (erase)

```http
DELETE /customers/{brand}/{profile_id}?mode=erase&confirm={profile_id}
X-Brand: {brand}
Authorization: Basic …
```

`confirm` **doit** être égal au `profile_id` du path (même valeur que le champ de confirmation UI).

### Réponse succès

```json
{
  "deleted": true,
  "mode": "erase",
  "brand": "batira",
  "profileId": "wp_123",
  "counts": { "...": "tables touchées" },
  "fingerprintPurged": false,
  "keepSales": false
}
```

### Erreurs à gérer

| HTTP | Cause typique | Message UI suggéré |
|------|---------------|-------------------|
| 400 | `confirm` manquant / ≠ `profile_id` | Afficher le `detail` serveur tel quel |
| 404 | Client introuvable | « Client introuvable » |
| 502 | Échec métier | Afficher `detail` (ex. `Customer delete failed: …`), **pas** seulement « 502 » |

### Message succès

« Client définitivement supprimé. » → retour liste.

---

## 5. Liste clients

```http
GET /customers?limit=100&offset=0
```

Les clients `status=ANONYMIZED` **n’apparaissent plus** (filtre serveur).

### Filtre optionnel (lecture seule)

Toggle / filtre : **Afficher les clients anonymisés**

```http
GET /customers?status=ANONYMIZED&limit=100&offset=0
```

UX :

- Liste en lecture seule (pas d’édition d’email, pas d’actions wallet).
- Badge `Anonymisé` sur la ligne.
- Action possible : aucune suppression utile (déjà anonymisé) ; export probablement 404 — ne pas proposer export sur ces lignes sauf si le backend évolue.

---

## 6. Règles (catalogue conditions) — optionnel mais utile

Nouveau champ booléen dans les conditions de règles :

- Champ : `customer.previously_erased`
- Label catalogue : « Email déjà anonymisé/effacé (empreinte) »
- Usage : exclure des avantages de bienvenue **autres** que le bypass (le bypass premier achat est déjà bloqué côté serveur si l’empreinte a `bypass_used=true`).

Si le sélecteur de champs est alimenté par `GET /admin/...` (catalogues), le champ apparaît déjà côté API — brancher le widget booléen comme les autres.

---

## 7. Gestion d’erreurs (règle générale)

Toujours lire le corps JSON d’erreur FastAPI :

```json
{ "detail": "…" }
```

ou

```json
{ "detail": { "message": "…", "...": "…" } }
```

Afficher une string lisible à l’utilisateur. Ne jamais se contenter du status code seul (surtout 502).

Pendant l’appel DELETE : désactiver les boutons + spinner pour éviter le double-clic. L’anonymize est idempotent ; l’erase sur un client déjà effacé → 404.

---

## 8. Hors scope frontend admin

- Plugin WooCommerce / hooks WordPress (Partie C plugin)
- Route boutique `POST /v1/storefront/customers/{profileId}/anonymize` (déjà backend ; scope `storefront:anonymize`)
- Afficher ou éditer `erased_identities` / hash email
- Options `purge_fingerprint` / `keep_sales` (sauf demande produit explicite)

---

## Checklist QA frontend

- [ ] Bouton « Supprimer » remplacé par « Supprimer ce client » + modale
- [ ] Export JSON téléchargeable avant confirmation
- [ ] Anonymiser (défaut) → succès → message → retour liste ; fiche ancienne URL = introuvable
- [ ] Effacer : bouton inactif tant que `profile_id` non retapé ; `confirm` envoyé
- [ ] Effacer → succès → retour liste ; client absent de la liste
- [ ] `alreadyAnonymized` → message dédié (pas d’erreur rouge)
- [ ] Erreur 400 / 502 : texte `detail` visible
- [ ] Liste par défaut sans anonymisés
- [ ] (Option) filtre `status=ANONYMIZED` lecture seule + badge
- [ ] (Option) champ règle `customer.previously_erased` sélectionnable

---

## Récap des endpoints

| Action | Méthode | Path |
|--------|---------|------|
| Export | `GET` | `/customers/{brand}/{profile_id}/export` |
| Anonymiser | `DELETE` | `/customers/{brand}/{profile_id}?mode=anonymize` |
| Effacer | `DELETE` | `/customers/{brand}/{profile_id}?mode=erase&confirm={profile_id}` |
| Liste | `GET` | `/customers` |
| Liste anonymisés | `GET` | `/customers?status=ANONYMIZED` |
