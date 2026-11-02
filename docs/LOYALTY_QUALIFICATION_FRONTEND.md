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
| **Changement de mode** | Recalcule immédiatement tous les clients pour le nouveau mode. Ce n’est **ni** upgrade, **ni** downgrade, **ni** renew. |
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
- **Confirmation** avant changement de mode : « Tous les clients seront recalculés immédiatement avec le nouveau mode. Ce n’est pas un upgrade / downgrade. »

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

### Validités

| Champ | Sens |
|-------|------|
| `points_validity_days` | Expiration des lots de points (`null` = pas d’expiration) |
| `purchase_validity_days` | Expiration des lots d’achats/montants |
| `loyalty_status_validity_days` | Fenêtre du palier (anti-downgrade / renew) |

En mode PURCHASES, afficher clairement `purchase_validity_days`. En mode POINTS, `points_validity_days` reste prioritaire dans l’UI, mais les deux peuvent coexister.

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
| `min_status_points` | **Décide** le palier | Conservé / éditable (utilisé si on repasse en POINTS) |
| `min_purchase_count` | Conservé | **Requis avec** le montant |
| `min_purchase_amount` | Conservé | **Requis avec** le compteur |

**Règle produit** en mode PURCHASES : le client doit atteindre **les deux** seuils pour un palier.

### UX formulaires paliers

- Afficher **toujours** les 3 seuils (recommandé), avec un badge / hint selon le mode actif :
  - POINTS → highlight `min_status_points`
  - PURCHASES → highlight `min_purchase_count` + `min_purchase_amount`
- Empêcher de sauvegarder un palier « Or » avec count/amount à 0 si le mode actif est PURCHASES (sauf palier de base).
- Le palier de **base** (seuils 0) est créé automatiquement — ne pas le proposer comme cible de bypass sauf intention explicite.

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

### Affichage selon `qualificationMode`

| Mode | Afficher en premier | Progression vers le palier suivant |
|------|---------------------|------------------------------------|
| `POINTS` | `statusPoints` / `pointsBalance`, `pointsExpiresAt` | `pointsToNextTier` |
| `PURCHASES` | `purchaseCount`, `purchaseAmountTotal`, `purchasesExpiresAt` | `purchasesToNextTier` **et** `amountToNextTier` (les deux) |

Les compteurs « non actifs » peuvent rester visibles en secondaire (ex. points en mode PURCHASES), mais le **palier** et la **barre de progression** doivent suivre le mode.

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

Afficher ce ledger surtout en mode PURCHASES (miroir des point-movements).

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

## 5. Séquence d’un achat (pour tooltips / docs UI)

1. Transaction `sale` ingérée → règles exécutées.
2. Backend enregistre +1 purchase / +montant (automatique).
3. Recalcul du palier selon le mode.
4. Si bypass **on** + **premier** achat + montant ≥ seuil → palier cible (floor) ; un palier déjà plus haut n’est pas abaissé.
5. Achats suivants : plus de bypass ; seuils du mode uniquement.

---

## 6. Checklist d’implémentation frontend

### Settings

- [ ] Lire / PATCH `qualification_mode`
- [ ] Confirmation avant switch de mode
- [ ] Section bypass (enabled + amount + tier select)
- [ ] Validation enabled ⇒ amount + tier
- [ ] Afficher / éditer `purchase_validity_days`

### Paliers

- [ ] Champs `min_purchase_count` + `min_purchase_amount` dans create/edit
- [ ] Hints / highlight selon mode actif
- [ ] Select bypass alimenté par les paliers actifs

### Fiche client

- [ ] Lire `qualificationMode` et brancher l’UI principale dessus
- [ ] Afficher compteurs achats + progression duale en PURCHASES
- [ ] Labels `lastChange` pour `mode_changed` et `first_purchase_bypass`
- [ ] Historique : nouveaux types d’événements
- [ ] Onglet / section `purchase-movements`

### Règles

- [ ] Bandeau informatif si mode PURCHASES (pas de nouvelle action)

### QA manuelle suggérée

1. Mode POINTS + règles earn_points → upgrade classique.
2. Switch vers PURCHASES → clients recalculés ; `lastChange` / historique = mode_changed (pas downgrade).
3. Bypass on, premier sale ≥ seuil → palier cible + `first_purchase_bypass`.
4. Deuxième sale → pas de re-bypass.
5. Bypass off → premier gros achat ne force plus le palier.

---

## 7. Référence API rapide

| Besoin | Endpoint |
|--------|----------|
| Mode + validités + bypass | `GET/PATCH /admin/loyalty-settings` |
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
