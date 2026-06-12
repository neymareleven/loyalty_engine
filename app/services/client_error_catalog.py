"""Known API error `detail` strings → French UI labels (segments, internal jobs)."""

SEGMENT_CLIENT_ERRORS = [
    {
        "apiDetail": "A segment with this name already exists for this brand",
        "fr": "Un segment avec ce nom existe déjà pour cette marque. Choisissez un autre nom.",
        "field": "name",
    },
    {
        "apiDetail": "Static segments cannot have conditions",
        "fr": "Un segment statique ne peut pas avoir de conditions.",
        "field": "conditions",
    },
    {
        "apiDetail": "Dynamic segments require conditions",
        "fr": "Un segment dynamique doit avoir au moins une condition.",
        "field": "conditions",
    },
    {
        "apiDetail": "payload.brand does not match active brand context",
        "fr": "La marque envoyée ne correspond pas à la marque active (X-Brand).",
        "field": "brand",
    },
    {
        "apiDetail": "Segment could not be saved",
        "fr": "Impossible d'enregistrer le segment. Vérifiez les données et réessayez.",
    },
    {
        "apiDetail": "Cannot manually edit members of a dynamic segment",
        "fr": "Les membres d'un segment dynamique ne se gèrent pas manuellement.",
    },
]

INTERNAL_JOB_CLIENT_ERRORS = [
    {
        "apiDetail": "Internal job name already exists",
        "fr": "Un job interne avec ce nom existe déjà. Choisissez un autre nom.",
        "field": "name",
    },
    {
        "apiDetail": "name is required",
        "fr": "Le nom du job est obligatoire.",
        "field": "name",
    },
    {
        "apiDetail": "Unknown segment_id for this brand",
        "fr": "Segment introuvable pour cette marque.",
        "field": "segment_id",
    },
    {
        "apiDetail": "Unknown/inactive transaction_type or not INTERNAL. Create it in /admin/transaction-types first.",
        "fr": "Type de transaction INTERNAL introuvable ou inactif. Créez-le d'abord dans Types de transaction.",
        "field": "transaction_type",
    },
    {
        "apiDetail": "This internal job is system-managed",
        "fr": "Ce job est géré par le système et ne peut pas être modifié ici.",
    },
    {
        "apiDetail": "payload.brand does not match active brand context",
        "fr": "La marque envoyée ne correspond pas à la marque active (X-Brand).",
        "field": "brand",
    },
]

CLIENT_ERROR_EXTRACTION = {
    "pattern": "FastAPI HTTPException → { detail: string }. Validation 422 → { detail: [{ msg, loc }] }",
    "rule": "Always display response.detail when present; never replace with generic HTTP status text only.",
    "genericFallbackFr": {
        "400": "Requête invalide. Vérifiez les champs du formulaire.",
        "404": "Ressource introuvable.",
        "409": "Conflit — l'opération n'a pas pu aboutir.",
        "502": "Erreur de synchronisation CDP Unomi. Réessayez ou contactez l'administrateur.",
        "500": "Erreur serveur. Réessayez plus tard.",
    },
}
