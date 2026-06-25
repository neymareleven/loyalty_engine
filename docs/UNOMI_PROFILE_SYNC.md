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
| `UNOMI_PROFILE_SYNC_MODE` | `minimal` | `minimal` = push **uniquement** champs fidélité (ne réécrit pas `scopeEmail` / `mergeIdentifier`). `full` = comportement legacy |
| `UNOMI_PROFILE_SYNC_SET_MERGE_IDENTIFIER` | `false` | Ne pas alimenter `systemProperties.mergeIdentifier` (la règle Unomi `mergeProfilesOnEmail` le gère déjà) |
| `UNOMI_PROFILE_SYNC_SKIP_UNCHANGED` | `true` | Skip push si `statusPoints` / `loyaltyStatus` inchangés sur Unomi |
| `UNOMI_WEBHOOK_SECRET` | — | Secret optionnel pour `POST /integrations/unomi/profile-events` |

Vérification : `GET /admin/segments/segmentation-mode` → `profileSyncEnabled`.

## Upsert loyalty (champs de base)

`POST /customers/upsert` — identité fidélité persistée en base **et** poussée vers Unomi :

| Champ | Obligatoire | Stockage loyalty | Unomi `properties` |
|-------|-------------|----------------|-------------------|
| `profileId` | oui | `customers.profile_id` | `itemId` |
| `brand` | oui | `customers.brand` | `brand` + `systemProperties.scope` |
| `email` | souvent | `customers.email` | `email` + `scopeEmail` = `{brand}-{email}` |
| `gender` | souvent | `customers.gender` | `gender` |
| `birthdate` | souvent | `customers.birthdate` (+ month/day/year) | `birthDate` (epoch ms si date complète) |

`properties.*` reste optionnel pour des champs CDP additionnels (`firstName`, `phone`, …) sans remplacer l'identité loyalty.

Réponse upsert : `unomi_sync.synced` indique si le push Unomi a réussi.

## Loyalty → Unomi (push automatique)

Déclenché après :

- `POST /customers/upsert` (création / mise à jour)
- Changement de `loyalty_status` / points (`update_customer_status`, transactions, admin tier)
- Recalcul métriques client (`MAINT_RECOMPUTE_CUSTOMER_METRICS`)

API Unomi (mode `eventcollector`, défaut) :

1. **`POST /cxs/profiles`** — garantit le profil avec `itemId` = `profile_id` loyalty (sinon Unomi assigne un UUID aléatoire)
2. **`POST /cxs/eventcollector`** — événement **`contactInfoSubmitted`** pour déclencher vos règles CDP

Variables :
|----------|--------|------|
| `UNOMI_PROFILE_SYNC_TRANSPORT` | `eventcollector` | `eventcollector` ou `profiles` |
| `UNOMI_PROFILE_SYNC_EVENT_TYPE` | `contactInfoSubmitted` | Type d'événement custom (si pas peer key) |
| `UNOMI_PROFILE_SYNC_PEER_KEY` | — | Clé tierce (`X-Unomi-Peer`) pour `updateProperties` — IP loyalty doit être whitelistée côté Unomi |

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

**Important** : ajouter l’en-tête pour éviter une boucle push pendant le traitement :

```http
X-Profile-Sync-Source: unomi
```

Pendant cet upsert, le push Loyalty → Unomi est **suspendu** (évite le ping-pong sur les champs CDP).

**Après** l’upsert (token libéré), le moteur pousse **automatiquement** les champs fidélité vers Unomi via `POST /cxs/profiles` uniquement (`transport_override=profiles`, sans `contactInfoSubmitted`) — pour que le profil CDP ait `loyaltyStatus`, `statusPoints`, etc. dès l’inscription.

Redéployer `loyalty_profile.groovy` (garde anti-écho sur `profileUpdated` sans delta contact) pour éviter une boucle upsert infinie.

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
