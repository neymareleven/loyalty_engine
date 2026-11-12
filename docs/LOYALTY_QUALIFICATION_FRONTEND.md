# Qualification paliers + bypass premier achat — guide frontend

Ce document décrit **ce que le frontend admin doit afficher et brancher** pour :

1. le **mode de qualification** `POINTS` | `PURCHASES` ;
2. les **seuils duals** sur les paliers ;
3. le **bypass premier achat** (activable / configurable) ;
4. la **fiche client** (compteurs, progression, `lastChange`) ;
5. le comportement produit (pas une règle métier à coder côté front).

Toutes les routes admin exigent :

- Header **`X-Brand`** : marque courante.
- **`Authorization`** : Basic auth API.

Prérequis backend : migrations à jour (`alembic upgrade head`), puis :

```http
GET /admin/health/schema
X-Brand: <brand>
```

Doit répondre `{"ok": true, ...}` et inclure notamment :

- `brand_loyalty_settings.qualification_mode`
- `brand_loyalty_settings.first_purchase_bypass_enabled`
- `loyalty_tiers.min_purchase_count`
- `customers.purchase_count`
- `purchase_movements`

---

## Concepts produit (à respecter côté UI)

| Concept | Comportement |
|--------|----------------|
| **Mode POINTS** | Le palier dépend de `status_points` vs `min_status_points` des paliers. Il faut des **règles** `earn_points` sur `sale` pour accumuler. |
| **Mode PURCHASES** | Le palier dépend de **deux** compteurs : `purchaseCount` **ET** `purchaseAmountTotal` vs `min_purchase_count` **ET** `min_purchase_amount`. Chaque `sale` PROCESSED est comptée **automatiquement** (+1 achat, +montant). **Pas de règle** de qualification à créer pour ça. |
| **Les deux modes sont exclusifs** | Une marque n’utilise qu’un mode à la fois (`qualification_mode`). Les compteurs points **et** achats continuent d’être enregistrés en parallèle, mais **seul le mode actif** décide du palier affiché. |
| **Bypass premier achat** | Option marque : si activé, le **tout premier** `sale` d’un montant ≥ seuil accorde immédiatement un **palier existant** choisi. Ce n’est **pas** une règle. |
| **Changement de mode** | Recalcule les paliers pour le **nouveau** mode (ce n’est **ni** upgrade, **ni** downgrade, **ni** renew). Les `sale` historiques sont d’abord converties en compteurs d’achats. |
| **Upgrade / downgrade / renew** | Inchangés pour la vie normale d’un palier (upgrade immédiat ; downgrade seulement après expiration de fenêtre ; renew = même palier + fenêtre prolongée). |

### Ce que le front ne doit PAS faire

- Ne pas inventer un écran « créer une règle de bypass » — le bypass vit dans **loyalty-settings**.
- Ne pas afficher un premier achat comme `upgrade` / `downgrade` / `renew` si `lastChange === "first_purchase_bypass"` ou `"mode_changed"`.
- En mode PURCHASES, ne pas masquer / désactiver les règles `earn_points` sans prévenir : elles restent utiles pour le wallet points / coupons, mais **ne pilotent plus le palier**.

---

## 1. Paramètres fidélité (écran Settings)

### Lire

```http
GET /admin/loyalty-settings
X-Brand: batira
```

Réponse (champs snake_case) :

```json
{
  "brand": "batira",
  "qualification_mode": "POINTS",
  "points_validity_days": 365,
  "loyalty_status_validity_days": 365,
  "purchase_validity_days": 365,
  "first_purchase_bypass_enabled": false,
  "first_purchase_bypass_min_amount": null,
  "first_purchase_bypass_tier_key": null
}
```

### Mettre à jour

```http
PATCH /admin/loyalty-settings
X-Brand: batira
Content-Type: application/json

{
  "qualification_mode": "PURCHASES",
  "purchase_validity_days": 365,
  "first_purchase_bypass_enabled": true,
  "first_purchase_bypass_min_amount": 80000,
  "first_purchase_bypass_tier_key": "argent"
}
```

Tous les champs sont optionnels (PATCH partiel).

### UX recommandée — mode

- Contrôle **radio / segmented control** : `POINTS` | `PURCHASES`.
- Texte d’aide :
  - **POINTS** : « Les paliers suivent les points de statut. Configurez des règles `earn_points` sur les ventes. »
  - **PURCHASES** : « Les paliers suivent le nombre d’achats **et** le montant total. Chaque vente est comptée automatiquement. »
- **Avant de changer de mode** : l’admin doit saisir les **vrais** seuils du mode cible sur chaque palier (pas de 1, 2, 3 auto).
- Preview : `GET /admin/loyalty-settings/preview-qualification-mode?target=PURCHASES` → `up` / `down` / `unchanged` / `errors`.
- Appliquer : `PATCH` avec `qualification_mode` **et** `confirm_mode_switch: true` (sinon **409** + preview). Un save bypass / validité **n’est pas** une bascule.
- `mode_switch_policy` : `IMMEDIATE` (palier mérité maintenant, les **garanties** bypass/admin restent) ou `HOLD_UNTIL_WINDOW` (garde le palier actuel jusqu’à expiration).
- Événement `QUALIFICATION_MODE_CHANGED` / `TIER_CHANGED` **seulement** si le palier effectif change.

### UX recommandée — bypass premier achat

Section dédiée (switch + champs) :

| Champ API | Widget | Règles UI |
|-----------|--------|-----------|
| `first_purchase_bypass_enabled` | switch | Off par défaut |
| `first_purchase_bypass_min_amount` | number ≥ 1 | Visible / requis si enabled |
| `first_purchase_bypass_tier_key` | select des paliers **actifs** | Visible / requis si enabled ; options = `GET /admin/loyalty-tiers` (`key` + `name`) |

Validation côté UI (le backend renvoie aussi 400) :

- Si enabled → montant ≥ 1 **et** tier_key non vide.
- Le tier doit exister et être **actif**.

Erreurs backend typiques (`detail` string) :

- `first_purchase_bypass_min_amount must be >= 1 when bypass is enabled`
- `first_purchase_bypass_tier_key is required when enabled`
- `first_purchase_bypass_tier_key must match an existing active loyalty tier`

Aide utilisateur :

> Si le premier achat d’un client atteint au moins ce montant, il reçoit immédiatement le palier choisi, sans attendre les seuils du mode. Uniquement sur la toute première vente.

### Immersion mode — settings (obligatoire)

L’API renvoie **tous** les champs (l’autre mode tourne en arrière-plan). L’UI n’affiche **que** le mode actif. Source machine-readable :

```http
GET /admin/loyalty-settings/ui-catalog
```

| Champ | POINTS | PURCHASES |
|-------|--------|-----------|
| `qualification_mode` | visible | visible |
| `loyalty_status_validity_days` (durée du palier) | visible | visible |
| `points_validity_days` | **visible** | **masquer** |
| `purchase_validity_days` | **masquer** | **visible** |
| Bypass premier achat (`enabled` + montant + palier) | **visible** | **visible** |

Le bypass **n’est pas** un réglage POINTS. Même formulaire dans les deux modes : switch, montant minimum du **premier achat**, palier cible. Jamais de champ « points » dans cette section. Libellé : « Bypass premier achat ».

Au switch de mode : masquer / afficher immédiatement, sans laisser les champs de l’autre mode en lecture seule.

---

## 2. Paliers (Loyalty tiers)

Catalogue machine-readable :

```http
GET /admin/loyalty-tiers/ui-catalog
```

### Créer / éditer

Les payloads utilisent **snake_case** :

```json
{
  "name": "Argent",
  "min_status_points": 100,
  "min_purchase_count": 3,
  "min_purchase_amount": 50000,
  "active": true
}
```

| Champ | Mode POINTS | Mode PURCHASES |
|-------|-------------|----------------|
| `min_status_points` | **Décide** le palier — champ visible | **Masquer** (le serveur aligne automatiquement pour l’unicité / le rank) |
| `min_purchase_count` | **Masquer** (auto-aligné) | **Requis avec** le montant |
| `min_purchase_amount` | **Masquer** (auto-aligné) | **Requis avec** le compteur |

**Règle produit** en mode PURCHASES : le client doit atteindre **les deux** seuils pour un palier.

### UX formulaires paliers

- **Masquer** les champs du mode inactif. Ne pas les laisser en lecture seule / secondaire.
  - POINTS → uniquement `name`, `min_status_points`, `active`
  - PURCHASES → uniquement `name`, `min_purchase_count`, `min_purchase_amount`, `active`
- Omettre les champs cachés (ou les envoyer à `0`) : le backend les réécrit pour qu’ils restent **strictement croissants** dans l’ordre du mode actif, afin qu’un switch de mode ne permute pas les ranks.
- Palier de **base** du mode POINTS : `min_status_points = 0`. Palier de **base** du mode PURCHASES : `min_purchase_count = 0` **et** `min_purchase_amount = 0`. `POST /admin/loyalty-tiers/ensure-base-tier` crée / réactive le palier 0/0/0.
- Empêcher de sauvegarder un palier non-base avec count **et** amount à 0 si le mode actif est PURCHASES.
- Ne pas proposer le palier de base comme cible de bypass sauf intention explicite.

Liste :

```http
GET /admin/loyalty-tiers
```

Options légères (selects) :

```http
GET /admin/ui-options/loyalty-tiers
```

---

## 3. Fiche client — statut fidélité

```http
GET /customers/{brand}/{profileId}/loyalty
X-Brand: {brand}
```

### Payload (camelCase)

```json
{
  "brand": "batira",
  "profileId": "cust-1",
  "qualificationMode": "PURCHASES",
  "loyaltyStatus": "argent",
  "statusPoints": 40,
  "pointsBalance": 40,
  "pointsExpiresAt": null,
  "purchaseCount": 1,
  "purchaseAmountTotal": 90000,
  "purchasesExpiresAt": "2027-10-30T23:59:59",
  "lastChange": "first_purchase_bypass",
  "currentTier": {
    "key": "argent",
    "name": "Argent",
    "rank": 1,
    "minStatusPoints": 100,
    "minPurchaseCount": 3,
    "minPurchaseAmount": 50000
  },
  "nextTier": {
    "key": "or",
    "name": "Or",
    "rank": 2,
    "minStatusPoints": 500,
    "minPurchaseCount": 8,
    "minPurchaseAmount": 150000
  },
  "pointsToNextTier": null,
  "purchasesToNextTier": 7,
  "amountToNextTier": 60000,
  "tiers": [ /* … */ ]
}
```

### Affichage selon `qualificationMode` — masquer le reste

Ne **pas** afficher les champs de l’autre mode (pas en secondaire non plus).

| Mode | **Afficher** | **Masquer** |
|------|----------------|---------------|
| `POINTS` | palier, `statusPoints` / `pointsBalance`, `pointsExpiresAt`, `pointsToNextTier`, ledger points | `purchaseCount`, `purchaseAmountTotal`, `purchasesExpiresAt`, `purchasesToNextTier`, `amountToNextTier`, onglet / ledger `purchase-movements`, seuils achats du palier suivant |
| `PURCHASES` | palier, `purchaseCount`, `purchaseAmountTotal`, `purchasesExpiresAt`, `purchasesToNextTier` **et** `amountToNextTier`, ledger `purchase-movements` | `statusPoints` / `pointsBalance`, `pointsExpiresAt`, `pointsToNextTier`, onglet / ledger points (sauf si l’écran wallet points est un module séparé hors « qualification palier ») |

Toujours afficher : `qualificationMode`, `loyaltyStatus` / palier courant, `lastChange`, `nextTier` **filtré** (libellés de seuils du mode actif seulement).

Dans `currentTier` / `nextTier` / liste `tiers` :

- POINTS : montrer `minStatusPoints` ; cacher `minPurchaseCount` / `minPurchaseAmount`
- PURCHASES : montrer `minPurchaseCount` + `minPurchaseAmount` ; cacher `minStatusPoints`

`lastChange` : afficher le label du mode (y compris `mode_changed` / `first_purchase_bypass`). Ne pas inventer un badge upgrade pour le bypass.

### `lastChange` — labels UI obligatoires

| Valeur API | Label FR suggéré | Ne pas confondre avec |
|------------|------------------|------------------------|
| `upgrade` | Montée de palier | — |
| `downgrade` | Descente de palier | — |
| `no_change` | Renouvellement | — |
| `admin_override` | Modification admin | — |
| `mode_changed` | Changement de mode de qualification | upgrade / downgrade |
| `first_purchase_bypass` | Bypass premier achat | upgrade |
| `null` | — / inconnu | — |

### Historique paliers

```http
GET /customers/{brand}/{profileId}/loyalty/history
```

Inclut désormais aussi les types :

- `QUALIFICATION_MODE_CHANGED`
- `FIRST_PURCHASE_BYPASS`

Mapper ces types aux mêmes labels que `lastChange` (`mode_changed`, `first_purchase_bypass`).

### Mouvements d’achats (ledger)

```http
GET /customers/{brand}/{profileId}/purchase-movements?limit=100&offset=0
```

Champs utiles : `quantity`, `amount`, `type` (`EARN` | `DEDUCT` | `EXPIRE`), `expires_at`, `status`, `isExpired`.

**Masquer** ce ledger en mode POINTS. **Masquer** le ledger points dans le bloc qualification palier en mode PURCHASES.

---

## 4. Règles : ce qui change (et ce qui ne change pas)

| Action | Toujours utile ? | Impact sur le palier |
|--------|------------------|----------------------|
| `earn_points` | Oui (wallet / marketing) | **Oui seulement** en mode `POINTS` |
| `issue_coupon` | Oui | Non |
| `reset_status_points` | Oui | Impact POINTS |

**Aucune nouvelle action de règle** n’est requise pour PURCHASES ni pour le bypass.

UX rules (recommandation) :

- Si `qualification_mode === "PURCHASES"`, afficher un bandeau info sur l’éditeur de règles :
  > Le palier suit le nombre d’achats et le montant. Les règles `earn_points` n’influencent plus le palier tant que ce mode est actif.

---

## 5. Quand le recalcul a lieu (important)

Au **PATCH `/admin/loyalty-settings`** (changement de mode, ou sauvegarde alors que le mode est déjà `PURCHASES`) :

1. Le backend rejoue les transactions **`sale` PROCESSED** manquantes → lots d’achats (nombre + montant).
2. La **date de la vente** (`processed_at` sinon `created_at`) sert de base à la validité : `expires_at = date_vente + purchase_validity_days` (si configuré). Une vente trop ancienne n’est plus comptée.
3. Un job `MAINT_RECOMPUTE_CUSTOMERS_LOYALTY_STATUS` applique ensuite le palier du mode choisi **tout de suite** (pas d’anti-downgrade « on garde Or parce que la fenêtre points n’est pas finie »).
4. L’historique reçoit `QUALIFICATION_MODE_CHANGED` (`lastChange: "mode_changed"`), pas upgrade/downgrade/renew.

Les ventes **futures** sont comptées à l’ingestion (plus besoin de règle).

À l’ouverture d’une **fiche client** en mode `PURCHASES`, le backend rejoue aussi les `sale` de **ce** client puis aligne le palier, pour que les compteurs ne restent pas à 0.

Le bypass premier achat **n’est pas** appliqué rétroactivement. Il ne se déclenche que si **cette** `sale` est la **toute première** `sale` PROCESSED du client (historique inclus : un client qui a déjà acheté n’est plus éligible, même si le bypass vient d’être activé).

---

## 6. Séquence d’un achat (pour tooltips / docs UI)

1. Transaction `sale` ingérée → règles exécutées.
2. Backend enregistre +1 purchase / +montant (automatique).
3. Recalcul du palier selon le mode.
4. Si bypass **on** + **premier** achat + montant ≥ seuil → palier cible (floor) ; un palier déjà plus haut n’est pas abaissé.
5. Achats suivants : plus de bypass ; seuils du mode uniquement.

---

## 7. Checklist d’implémentation frontend

### Settings

- [ ] Lire / PATCH `qualification_mode`
- [ ] Confirmation avant switch de mode
- [ ] Section bypass (enabled + amount + tier select)
- [ ] Validation enabled ⇒ amount + tier
- [ ] POINTS : uniquement `points_validity_days` ; PURCHASES : uniquement `purchase_validity_days`
- [ ] Bypass visible **dans les deux modes** (libellé premier achat, pas points)

### Paliers

- [ ] Champs visibles **selon le mode** uniquement (masquer l’autre mode, voir §2)
- [ ] Ne pas envoyer les seuils cachés (le serveur auto-aligne)
- [ ] Select bypass alimenté par les paliers actifs

### Fiche client

- [ ] Lire `qualificationMode` et **masquer** tout champ / onglet de l’autre mode (voir §3)
- [ ] POINTS : points + `pointsToNextTier` uniquement
- [ ] PURCHASES : achats + montant + `purchasesToNextTier` et `amountToNextTier` uniquement
- [ ] Labels `lastChange` pour `mode_changed` et `first_purchase_bypass`
- [ ] Historique : nouveaux types d’événements
- [ ] Onglet / section `purchase-movements`

### Règles

- [ ] Bandeau informatif si mode PURCHASES (pas de nouvelle action)

### QA manuelle suggérée

1. Mode POINTS + règles earn_points → upgrade classique.
2. Switch vers PURCHASES → compteurs alimentés depuis l’historique `sale` ; palier du mode achats ; `lastChange` / historique = mode_changed (pas downgrade). Les ventes expirées (date + validité) ne comptent plus.
3. Bypass on, premier sale ≥ seuil → palier cible + `first_purchase_bypass`.
4. Deuxième sale → pas de re-bypass.
5. Bypass off → premier gros achat ne force plus le palier.

---

## 8. Référence API rapide

| Besoin | Endpoint |
|--------|----------|
| Mode + validités + bypass | `GET/PATCH /admin/loyalty-settings` |
| Preview bascule de mode | `GET /admin/loyalty-settings/preview-qualification-mode?target=PURCHASES` |
| Champs visibles par mode | `GET /admin/loyalty-settings/ui-catalog` |
| Santé schéma | `GET /admin/health/schema` |
| CRUD paliers | `/admin/loyalty-tiers` |
| Hints formulaires paliers | `GET /admin/loyalty-tiers/ui-catalog` |
| Options select paliers | `GET /admin/ui-options/loyalty-tiers` |
| Fiche loyalty client | `GET /customers/{brand}/{profileId}/loyalty` |
| Historique paliers | `GET /customers/{brand}/{profileId}/loyalty/history` |
| Ledger achats | `GET /customers/{brand}/{profileId}/purchase-movements` |

Convention naming :

- Admin settings / tiers payloads : **snake_case**
- Customer loyalty payload : **camelCase**

---

## Prompt frontend (à coller tel quel)

Immersion totale dans `qualification_mode` (POINTS | PURCHASES). L’autre mode continue en backend : **ne jamais l’afficher**. Lire `GET /admin/loyalty-settings` + `GET /admin/loyalty-settings/ui-catalog` et n’afficher que `visibleByMode[mode]`.

**Settings**
- Radio POINTS / PURCHASES. Avant switch : formulaire des **vrais** seuils du mode cible, puis `GET .../preview-qualification-mode?target=...`, puis PATCH `confirm_mode_switch: true`. Afficher up/down/unchanged. 409 si confirm manquant. 400 si seuils incomplets.
- `mode_switch_policy` IMMEDIATE | HOLD_UNTIL_WINDOW. Un save bypass/validité n’est pas une bascule.
- POINTS seulement : `points_validity_days`. PURCHASES seulement : `purchase_validity_days`.
- Les deux modes : `loyalty_status_validity_days`, bypass premier achat (montant + palier, jamais « points »).

**Paliers**
- POINTS : `name`, `min_status_points`, `active`. PURCHASES : `name`, `min_purchase_count` **et** `min_purchase_amount` (>=1 hors base). Ne pas inventer 1,2,3.
- Avant une bascule, permettre de saisir les seuils de l’autre mode (écran de préparation).
- Base POINTS = 0 points. Base PURCHASES = 0 achats ET 0 montant.

**Règles**
- Tout changement de palier émet `TIER_CHANGED` (`payload.cause` = thresholds | first_purchase_bypass | mode_switch | admin | expiration) **en plus** du type précis. Une règle « tout changement » écoute `TIER_CHANGED`. Une règle bypass écoute `FIRST_PURCHASE_BYPASS`.

**Fiche client / ledgers / nextTier**
- POINTS : palier, points, expiration points, pointsToNext, ledger points. Masquer tout achat.
- PURCHASES : palier, purchaseCount, purchaseAmountTotal, expiration achats, purchasesToNext + amountToNext, ledger purchase-movements. Masquer tout point de qualification.
- `lastChange` `first_purchase_bypass` et `mode_changed` ≠ upgrade / downgrade / renew.

**Règles (suite)**
- En PURCHASES : bandeau « les règles earn_points n’attribuent plus le palier ».
