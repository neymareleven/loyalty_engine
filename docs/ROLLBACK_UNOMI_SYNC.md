# Rollback synchronisation Loyalty → Unomi (juin 2026)

La push **Loyalty → Unomi** (`POST /cxs/profiles`, champs fidélité) a été **désactivée par défaut** car elle déclenchait des tempêtes `profileUpdated` dans Unomi (fusion profils, lenteurs, impossibilité de créer/supprimer des contacts).

## Ce qui reste actif

| Flux | Statut |
|------|--------|
| **Unomi → Loyalty** (`loyalty_profile.groovy` → `POST /customers/upsert`) | Actif — inscription, ventes |
| **Segments Unomi** (`unomi_segment_service`) | Inchangé |
| **Ventes** (`sale.groovy` → `POST /transactions`) | Inchangé |

## Ce qui est désactivé (défaut)

| Flux | Statut |
|------|--------|
| Push fidélité vers Unomi après upsert / vente / changement statut | **OFF** (`UNOMI_PROFILE_SYNC=false`) |
| Suppression profil Unomi depuis `DELETE /customers/...` | **OFF** par défaut |

## Déploiement production

### 1. Loyalty Engine

```bash
cd /var/www/html/loyalty_engine
git pull
```

Dans `.env` :

```env
UNOMI_PROFILE_SYNC=false
```

Supprimer ou commenter si présents :

```env
# UNOMI_PROFILE_SYNC_TRANSPORT=profiles
# UNOMI_PROFILE_SYNC_MODE=minimal
```

```bash
sudo systemctl restart loyalty_engine
```

### 2. Unomi — restaurer le groovy simple

```bash
cp /var/www/html/loyalty_engine/loyalty_profile.groovy /opt/unomi/groovy-scripts/
# reload groovy / restart Unomi
```

Version restaurée : **sans** `X-Profile-Sync-Source`, **sans** garde `profileUpdated`, **sans** push retour vers Unomi.

### 3. Vérification

- Inscription → log `[loyalty_profile] Profile upserted ... code=200`
- **Aucun** log Loyalty `unomi profile sync ok` après vente
- Création / suppression contact Unomi redevient fluide

## Réactiver plus tard (refonte)

Quand la synchro sera refaite proprement (lecture seule côté frontend via API Loyalty, ou push minimal sans `profileUpdated` storm) :

```env
UNOMI_PROFILE_SYNC=true
UNOMI_PROFILE_SYNC_TRANSPORT=profiles
UNOMI_PROFILE_SYNC_MODE=minimal
UNOMI_PROFILE_SYNC_SET_MERGE_IDENTIFIER=false
```

Voir [UNOMI_PROFILE_SYNC.md](./UNOMI_PROFILE_SYNC.md) pour le design cible.
