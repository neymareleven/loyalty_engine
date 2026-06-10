# Synchronisation profils Loyalty ↔ Unomi

**Guide de déploiement pas à pas (CDP + loyalty)** : [UNOMI_PROFILE_SYNC_DEPLOY.md](./UNOMI_PROFILE_SYNC_DEPLOY.md)

Indépendant du mode de segmentation (`INTERNAL` / `UNOMI`). Dès que `UNOMI_BASE_URL` + `UNOMI_PASSWORD` sont configurés pour une marque, le moteur pousse l’état fidélité vers Unomi et accepte les suppressions entrantes.

## Activation

| Variable | Défaut | Rôle |
|----------|--------|------|
| `UNOMI_BASE_URL` / `UNOMI_PASSWORD` | — | Requis |
| `UNOMI_PROFILE_SYNC` | `true` | Désactiver globalement : `false` |
| `UNOMI_PROFILE_SYNC_DISABLED` | — | Liste de marques exclues (`batira,qilinsa`) |
| `UNOMI_PROFILE_SYNC_BRANDS` | — | Opt-in (sinon toutes les marques avec credentials) |
| `UNOMI_PROFILE_SYNC_STRICT` | `false` | Si `true`, une erreur Unomi fait échouer l’opération loyalty |
| `UNOMI_WEBHOOK_SECRET` | — | Secret optionnel pour `POST /integrations/unomi/profile-events` |

Vérification : `GET /admin/segments/segmentation-mode` → `profileSyncEnabled`.

## Loyalty → Unomi (push automatique)

Déclenché après :

- `POST /customers/upsert` (création / mise à jour)
- Changement de `loyalty_status` / points (`update_customer_status`, transactions, admin tier)
- Recalcul métriques client (`MAINT_RECOMPUTE_CUSTOMER_METRICS`)

API Unomi : `POST /cxs/profiles` avec `itemId` = `profile_id`, `systemProperties.scope` = scope marque.

Propriétés poussées (extrait) :

| Loyalty | Unomi `properties` |
|---------|-------------------|
| `loyalty_status` | `loyaltyStatus` |
| `status_points` | `statusPoints` |
| Solde wallet | `loyaltyPointsBalance` |
| Métriques | `metrics.transactions_count_30d`, … |
| Genre / naissance | `gender`, `birthDate` |

## Unomi → Loyalty (déjà en place)

`loyalty_profile.groovy` appelle `POST /customers/upsert`.

**Important** : ajouter l’en-tête pour éviter une boucle push → Unomi :

```http
X-Profile-Sync-Source: unomi
```

## Suppression

### Loyalty → Unomi

```http
DELETE /customers/{brand}/{profile_id}
X-Brand: {brand}
```

Supprime le client loyalty (mouvements, coupons, rewards, métriques, segment_members…) puis le profil Unomi (`DELETE /cxs/privacy/profiles/{id}?withData=true`).

Les transactions historiques (`transactions` par `profile_id`) sont conservées.

### Unomi → Loyalty

Configurer une action Unomi (groovy / règle) sur suppression profil :

```http
POST /integrations/unomi/profile-events
Authorization: Basic …
X-Unomi-Webhook-Secret: {UNOMI_WEBHOOK_SECRET}   # si défini
Content-Type: application/json

{
  "event": "profile_deleted",
  "brand": "batira",
  "profileId": "abc-123"
}
```

Le moteur supprime le client loyalty **sans** rappeler Unomi (`skip_unomi`).

## Prochaine étape (segmentation native Unomi)

Une fois les profils alignés, les segments dynamiques pourront s’appuyer sur le recalcul Unomi (`/match`, `/impacted`) au lieu de `segment_members` — hors scope de ce livrable.
