from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer import Customer
from app.models.segment import Segment
from app.models.segment_member import SegmentMember
from app.schemas.segment import (
    SegmentCreate,
    SegmentMembersBulkAdd,
    SegmentMembersBulkRemove,
    SegmentMembersBulkResult,
    SegmentMemberCreate,
    SegmentMemberOut,
    SegmentMembersListResponse,
    SegmentOut,
    SegmentRecomputeResult,
    SegmentUpdate,
)
from app.services.segment_members_list_service import list_segment_members as list_segment_members_payload
from app.services.segment_condition_unomi import loyalty_ast_to_unomi_condition
from app.services.segment_admin_service import (
    apply_segment_type_transition,
    assert_segment_deletable,
    recompute_brand_dynamic_segments,
    serialize_segment_out,
    trigger_recompute_if_needed,
)
from app.services.unomi_segment_service import (
    add_customers_to_unomi_manual_segment,
    create_unomi_segment_mirror,
    delete_unomi_segment,
    remove_customers_from_unomi_manual_segment,
    sync_unomi_scope_segments_to_registry,
    sync_manual_list_segment_to_unomi,
)
from app.services.unomi_client import UnomiClientError
from app.services.unomi_settings_service import (
    unomi_enabled_for_brand,
    unomi_env_status,
)


router = APIRouter(prefix="/admin/segments", tags=["admin-segments"])


def _pgcode(err: IntegrityError) -> str | None:
    orig = getattr(err, "orig", None)
    code = getattr(orig, "pgcode", None)
    if code:
        return str(code)
    return None


def _model_json_schema(model_cls):
    fn = getattr(model_cls, "model_json_schema", None)
    if callable(fn):
        return fn()
    return model_cls.schema()


@router.get("/ui-catalog")
def get_segments_ui_catalog():
    return {
        "headers": {"brand": "X-Brand (required on every call)", "auth": "Authorization Basic"},
        "discoverMode": {"endpoint": "GET /admin/segments/segmentation-mode", "brandVia": "X-Brand"},
        "providerMatrix": {
            "INTERNAL": {
                "when": "currentBrandUsesUnomi=false",
                "dynamicCreate": "POST /admin/segments { is_dynamic:true, conditions: loyaltyAST } + ?recompute=true",
                "staticCreate": "POST /admin/segments { is_dynamic:false }",
                "staticMembers": "segment_members table; GET/POST/DELETE …/members",
                "dynamicMembers": "segment_members source=DYNAMIC after recompute",
                "recompute": "POST …/recompute or MAINT_RECOMPUTE_SEGMENTS",
            },
            "UNOMI": {
                "when": "currentBrandUsesUnomi=true",
                "registry": "segments.id (UUID) stored in engine; unomi_segment_id in Unomi CDP",
                "dynamicCreate": "POST /admin/segments { is_dynamic:true, conditions: loyaltyAST } → translated to Unomi",
                "staticCreate": "POST /admin/segments { is_dynamic:false } → empty OR(itemId) list in Unomi",
                "staticMembers": "POST …/members adds profileId to manual_profile_ids + sync Unomi",
                "dynamicMembers": "segment_members source=DYNAMIC (engine AST on customers, same as INTERNAL)",
                "recompute": "POST …/recompute — membership in engine; condition still synced to CDP",
                "rulesJobsRef": "Always engine UUID (segment.id), never unomi_segment_id",
            },
        },
        "workflow": {
            "order": [
                "GET /admin/segments/segmentation-mode",
                "choose_static_or_dynamic",
                "create_segment",
                "if_static_add_members",
                "link_rules_segment_ids_or_jobs_segment_id",
            ],
            "summary": (
                "Marque courante via X-Brand. Unomi si .env configuré (voir segmentation-mode). "
                "Même API pour INTERNAL et UNOMI ; champs provider / unomiSegmentId sur SegmentOut."
            ),
        },
        "create": {
            "fieldHelp": {
                "is_dynamic": "true = membership calculée par conditions ; false = liste manuelle.",
                "conditions": "AST identique aux règles, champs customer.* / customer.metrics.* / system.* uniquement.",
            },
            "jsonSchema": _model_json_schema(SegmentCreate),
            "uiHints": {
                "brand": {"widget": "hidden"},
                "conditions": {
                    "widget": "rule_condition_builder",
                    "catalog": {"endpoint": "/admin/segments/ui-options/condition-fields", "method": "GET"},
                    "visibleWhen": {"is_dynamic": True},
                },
            },
        },
        "update": {
            "jsonSchema": _model_json_schema(SegmentUpdate),
            "uiHints": {
                "conditions": {
                    "widget": "rule_condition_builder",
                    "catalog": {"endpoint": "/admin/segments/ui-options/condition-fields", "method": "GET"},
                },
            },
            "queryParams": {
                "clear_static_on_dynamic": (
                    "Si passage static→dynamic : supprime les membres STATIC existants au lieu de renvoyer 409."
                ),
            },
        },
        "delete": {
            "policy": (
                "DELETE autorisé seulement si can_delete=true (aucune règle segment_ids ni job segment_id). "
                "Sinon recommended_action=detach_references."
            ),
            "fieldsOnList": [
                "canDelete",
                "referencingRulesCount",
                "referencingInternalJobsCount",
                "recommendedAction",
            ],
        },
        "recompute": {
            "brandEndpoint": "POST /admin/segments/recompute",
            "segmentEndpoint": "POST /admin/segments/{segment_id}/recompute",
            "needsRecomputeField": "needsRecompute",
            "maintenanceJobKey": "MAINT_RECOMPUTE_SEGMENTS",
            "note": (
                "Dynamic membership is always recomputed in the engine (segment_members). "
                "UNOMI mode still pushes segment definition to the CDP."
            ),
        },
        "unomi": {
            "segmentationModeEndpoint": "GET /admin/segments/segmentation-mode",
            "manualMemberSync": "POST /admin/segments/{segment_id}/sync-unomi",
            "manualAddSemantics": "Adds customer.profileId to manual_profile_ids and rebuilds Unomi OR(itemId) condition.",
            "membersList": (
                "GET /admin/segments/{segment_id}/members?limit=&offset= "
                "(all modes; dynamic: ?refresh=true recompute, ?verify=true audit page)"
            ),
        },
        "rules": {
            "field": "Rule.segment_ids",
            "type": "UUID[]",
            "semantics": "Customer must be in at least one segment (OR). Uses segment_members or Unomi match per provider.",
            "validateOnSave": "POST/PATCH /admin/rules checks segment exists for brand",
        },
        "internalJobs": {
            "field": "InternalJob.segment_id",
            "type": "UUID | null",
            "semantics": "Restrict job customer set to segment members, then apply selector AST.",
            "preview": "Use job preview/run endpoints with segment_id set",
        },
        "frontendDoc": "docs/SEGMENTS_FRONTEND.md",
        "frontendDocAnchors": {
            "unomiStaticMembers": "unomi--segment-statique--membres-et-suppression",
            "deleteSegment": "unomi--segment-statique--membres-et-suppression",
        },
        "dependencies": {
            "conditionFields": {"endpoint": "/admin/segments/ui-options/condition-fields", "method": "GET"},
            "segmentsOptions": {
                "endpoint": "/admin/ui-options/segments",
                "method": "GET",
                "brandVia": "X-Brand",
            },
        },
    }


@router.get("/ui-options/condition-fields")
def list_segment_condition_fields(brand: str = Depends(get_active_brand)):
    customer_fields = [
        "customer.gender",
        "customer.status",
        "customer.loyalty_status",
        "customer.status_points",
        "customer.birthdate",
        "customer.created_at",
        "customer.last_activity_at",
        "customer.rewards",
        "customer.metrics.last_transaction_at",
        "customer.metrics.transactions_count_30d",
        "customer.metrics.transactions_count_90d",
    ]
    system_fields: list[str] = []
    items = sorted({*customer_fields, *system_fields})
    return {
        "brand": brand,
        "sources": {
            "customer": customer_fields,
            "system": system_fields,
        },
        "note": "Segment conditions are evaluated without a transaction payload; payload.* fields are not supported.",
        "valuePresets": {
            "datetime": [
                {"id": "system.now", "label": "Now", "value": {"$system": "now"}},
                {"id": "system.today", "label": "Today", "value": {"$system": "today"}},
            ],
            "number": [
                {"id": "system.weekday", "label": "Weekday (0=Mon .. 6=Sun)", "value": {"$system": "weekday"}},
                {
                    "id": "system.customer_created_days",
                    "label": "Days since customer created",
                    "value": {"$system": "customer_created_days"},
                },
                {
                    "id": "system.customer_last_activity_days",
                    "label": "Days since last activity",
                    "value": {"$system": "customer_last_activity_days"},
                },
            ],
        },
        "fieldMeta": {
            "customer.loyalty_status": {
                "valueKind": "enum",
                "ui": {
                    "widget": "remote_select",
                    "datasource": {
                        "endpoint": "/admin/ui-options/loyalty-tiers",
                        "method": "GET",
                        "brandVia": "X-Brand",
                        "valueField": "key",
                        "labelField": "name",
                    },
                },
            },
            "customer.rewards": {
                "valueKind": "set",
                "ui": {
                    "widget": "remote_multi_select",
                    "datasource": {
                        "endpoint": "/admin/ui-options/rewards",
                        "method": "GET",
                        "brandVia": "X-Brand",
                        "valueField": "id",
                        "labelField": "name",
                    },
                },
            },
            "customer.gender": {
                "valueKind": "enum",
                "ui": {"widget": "select", "options": ["M", "F", "OTHER", "UNKNOWN"]},
            },
            "customer.birthdate": {"valueKind": "date", "ui": {"widget": "date"}},
            "customer.created_at": {"valueKind": "datetime", "ui": {"widget": "datetime"}},
            "customer.last_activity_at": {"valueKind": "datetime", "ui": {"widget": "datetime"}},
            "customer.metrics.last_transaction_at": {"valueKind": "datetime", "ui": {"widget": "datetime"}},
            "customer.metrics.transactions_count_30d": {"valueKind": "number", "ui": {"widget": "number"}},
            "customer.metrics.transactions_count_90d": {"valueKind": "number", "ui": {"widget": "number"}},
        },
        "items": items,
    }


@router.get("/segmentation-mode")
def read_segmentation_mode(active_brand: str = Depends(get_active_brand)):
    """Unomi vs INTERNAL for the current request brand (X-Brand / ?brand=)."""
    return unomi_env_status(brand=active_brand)


@router.get("", response_model=list[SegmentOut])
def list_segments(
    response: Response,
    active_brand: str = Depends(get_active_brand),
    active: bool | None = None,
    sync_unomi: bool = Query(
        False,
        description=(
            "UNOMI mode only: pull segment definitions from the CDP before listing. "
            "Can take tens of seconds for many segments; default is local registry only."
        ),
    ),
    db: Session = Depends(get_db),
):
    if unomi_enabled_for_brand(brand=active_brand):
        q = db.query(Segment).filter(Segment.brand == active_brand).filter(Segment.provider == "UNOMI")
        if sync_unomi:
            try:
                sync_unomi_scope_segments_to_registry(db, brand=active_brand, keep_orphans=True)
                db.commit()
                response.headers["X-Unomi-Sync"] = "ok"
            except ValueError as e:
                db.rollback()
                raise HTTPException(status_code=400, detail=str(e))
            except UnomiClientError as e:
                db.rollback()
                response.headers["X-Unomi-Sync"] = "failed"
                response.headers["X-Unomi-Sync-Detail"] = str(e)[:500]
            except Exception as e:
                db.rollback()
                response.headers["X-Unomi-Sync"] = "failed"
                response.headers["X-Unomi-Sync-Detail"] = str(e)[:500]
        else:
            response.headers["X-Unomi-Sync"] = "skipped"

        if active is not None:
            q = q.filter(Segment.active.is_(active))
        items = q.order_by(Segment.created_at.desc()).all()
    else:
        q = db.query(Segment).filter(Segment.brand == active_brand)
        if active is not None:
            q = q.filter(Segment.active.is_(active))
        items = q.order_by(Segment.created_at.desc()).all()

    if active is not None:
        items = [seg for seg in items if bool(seg.active) is bool(active)]
    return [serialize_segment_out(db, seg=obj) for obj in items]


@router.post("/recompute", response_model=SegmentRecomputeResult)
def recompute_brand_segments(
    active_brand: str = Depends(get_active_brand),
    batch_size: int = Query(500, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    stats = recompute_brand_dynamic_segments(db, brand=active_brand, batch_size=batch_size)
    db.commit()
    return stats


@router.post("", response_model=SegmentOut)
def create_segment(
    payload: SegmentCreate,
    active_brand: str = Depends(get_active_brand),
    recompute: bool = Query(True, description="Recompute membership after creating a dynamic segment"),
    batch_size: int = Query(500, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    if payload.is_dynamic:
        if payload.conditions is None:
            raise HTTPException(status_code=400, detail="Dynamic segments require conditions")
    else:
        if payload.conditions is not None:
            raise HTTPException(status_code=400, detail="Static segments cannot have conditions")

    try:
        if unomi_enabled_for_brand(brand=active_brand):
            obj = create_unomi_segment_mirror(
                db,
                brand=active_brand,
                name=payload.name,
                description=payload.description,
                is_dynamic=payload.is_dynamic,
                conditions=payload.conditions,
                manual_profile_ids=[],
                active=payload.active,
                unomi_condition_override=payload.unomi_condition,
            )
            trigger_recompute_if_needed(db, seg=obj, recompute=recompute, batch_size=batch_size)
        else:
            obj = Segment(
                brand=active_brand,
                name=payload.name,
                description=payload.description,
                is_dynamic=payload.is_dynamic,
                conditions=payload.conditions,
                active=payload.active,
                provider="INTERNAL",
            )
            db.add(obj)
            db.flush()
            trigger_recompute_if_needed(db, seg=obj, recompute=recompute, batch_size=batch_size)
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Segment could not be saved")
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except UnomiClientError as e:
        db.rollback()
        raise HTTPException(status_code=502, detail=f"Unomi segment create failed: {e}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=502, detail=f"Unomi segment create failed: {e}")
    db.refresh(obj)
    return serialize_segment_out(db, seg=obj)


@router.get("/{segment_id}", response_model=SegmentOut)
def get_segment(
    segment_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(Segment).filter(Segment.id == segment_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")
    return serialize_segment_out(db, seg=obj)


@router.patch("/{segment_id}", response_model=SegmentOut)
def update_segment(
    segment_id: UUID,
    payload: SegmentUpdate,
    active_brand: str = Depends(get_active_brand),
    clear_static_on_dynamic: bool = Query(
        False,
        description="When switching to dynamic, remove existing STATIC members instead of 409",
    ),
    recompute: bool = Query(True, description="Recompute after updating a dynamic segment"),
    batch_size: int = Query(500, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    obj = db.query(Segment).filter(Segment.id == segment_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")

    if obj.provider == "UNOMI" and payload.is_dynamic is False and obj.is_dynamic:
        raise HTTPException(
            status_code=400,
            detail="Cannot switch a Unomi-backed segment to static via engine; manage in Unomi or recreate segment",
        )

    data = payload.model_dump(exclude_unset=True)
    conditions_changed = "conditions" in data
    was_dynamic = obj.is_dynamic

    if obj.provider == "UNOMI":
        data.pop("is_dynamic", None)

    if "is_dynamic" in data:
        apply_segment_type_transition(
            db,
            seg=obj,
            next_is_dynamic=bool(data["is_dynamic"]),
            clear_static_on_dynamic=clear_static_on_dynamic,
        )
        data.pop("is_dynamic", None)

    next_is_dynamic = obj.is_dynamic
    next_conditions = data.get("conditions", obj.conditions)

    if next_is_dynamic:
        if next_conditions is None:
            raise HTTPException(status_code=400, detail="Dynamic segments require conditions")
    else:
        if "conditions" in data and data["conditions"] is not None:
            raise HTTPException(status_code=400, detail="Static segments cannot have conditions")

    unomi_condition_in_payload = data.pop("unomi_condition", None)
    for k, v in data.items():
        setattr(obj, k, v)
    if obj.provider == "UNOMI" and (conditions_changed or unomi_condition_in_payload is not None) and (
        obj.conditions or unomi_condition_in_payload
    ):
        try:
            if obj.is_dynamic:
                from app.services.unomi_segment_service import (
                    build_unomi_segment_definition,
                    get_unomi_client,
                    resolve_segment_scope,
                )
                from app.services.unomi_settings_service import resolve_unomi_connection

                if unomi_condition_in_payload is not None:
                    obj.unomi_condition = unomi_condition_in_payload
                elif obj.conditions:
                    obj.unomi_condition = loyalty_ast_to_unomi_condition(obj.conditions)

                cfg = resolve_unomi_connection(brand=obj.brand)
                client = get_unomi_client(db, brand=obj.brand)
                if cfg and client and obj.unomi_segment_id and obj.unomi_condition:
                    definition = build_unomi_segment_definition(
                        segment_id=obj.unomi_segment_id,
                        name=obj.name,
                        scope=resolve_segment_scope(obj, cfg),
                        description=obj.description,
                        condition=obj.unomi_condition,
                    )
                    client.save_segment(definition)
            else:
                sync_manual_list_segment_to_unomi(db, seg=obj)
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=502, detail=f"Unomi segment update failed: {e}")

    should_recompute = (
        recompute
        and obj.is_dynamic
        and obj.active
        and obj.conditions is not None
        and (conditions_changed or not was_dynamic or ("active" in data and bool(data["active"])))
    )

    try:
        db.flush()
        if should_recompute:
            trigger_recompute_if_needed(db, seg=obj, recompute=True, batch_size=batch_size)
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Segment could not be saved")
    except HTTPException:
        db.rollback()
        raise
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

    db.refresh(obj)
    return serialize_segment_out(db, seg=obj)


@router.post("/{segment_id}/recompute", response_model=SegmentRecomputeResult)
def recompute_segment(
    segment_id: UUID,
    active_brand: str = Depends(get_active_brand),
    batch_size: int = Query(500, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    obj = db.query(Segment).filter(Segment.id == segment_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")
    if not obj.is_dynamic:
        raise HTTPException(status_code=400, detail="Only dynamic segments can be recomputed")
    if not obj.active:
        raise HTTPException(status_code=400, detail="Segment is inactive")
    if obj.conditions is None:
        raise HTTPException(status_code=400, detail="Dynamic segment has no conditions")

    try:
        stats = trigger_recompute_if_needed(db, seg=obj, recompute=True, batch_size=batch_size)
        if stats is None:
            raise HTTPException(status_code=400, detail="Segment could not be recomputed")
        db.commit()
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

    return stats


@router.delete("/{segment_id}")
def delete_segment(
    segment_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(Segment).filter(Segment.id == segment_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")

    assert_segment_deletable(db, seg=obj)
    try:
        delete_unomi_segment(db, seg=obj)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Unomi segment deletion failed: {e}")
    db.delete(obj)
    db.commit()
    return {"deleted": True}


@router.post("/{segment_id}/sync-unomi")
def sync_segment_to_unomi(
    segment_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(Segment).filter(Segment.id == segment_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")
    if obj.provider != "UNOMI":
        raise HTTPException(status_code=400, detail="Segment is not backed by Unomi")
    try:
        result = sync_manual_list_segment_to_unomi(db, seg=obj)
        db.commit()
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=502, detail=f"Unomi sync failed: {e}")
    return result


@router.get("/{segment_id}/members", response_model=SegmentMembersListResponse)
def list_segment_members(
    segment_id: UUID,
    active_brand: str = Depends(get_active_brand),
    source: str | None = None,
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    refresh: bool = Query(
        False,
        description=(
            "Dynamic segments only: recompute segment_members from conditions before listing "
            "(same as POST …/recompute for this segment)."
        ),
    ),
    verify: bool = Query(
        False,
        description=(
            "Dynamic segments only: for each member on this page, re-evaluate conditions live "
            "and set matches_conditions (audit drift since last_computed_at)."
        ),
    ),
    db: Session = Depends(get_db),
):
    seg = db.query(Segment).filter(Segment.id == segment_id).first()
    if not seg or seg.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")

    if refresh and not seg.is_dynamic:
        raise HTTPException(status_code=400, detail="refresh is only for dynamic segments")
    if verify and not seg.is_dynamic:
        raise HTTPException(status_code=400, detail="verify is only for dynamic segments")
    if refresh and seg.is_dynamic and seg.conditions is None:
        raise HTTPException(
            status_code=400,
            detail="Dynamic segment has no loyalty conditions; cannot refresh membership",
        )

    try:
        payload = list_segment_members_payload(
            db,
            seg=seg,
            limit=limit,
            offset=offset,
            source=source,
            refresh=refresh,
            verify=verify,
        )
        if refresh or verify:
            db.commit()
        return payload
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{segment_id}/members", response_model=SegmentMemberOut)
def add_segment_member(
    segment_id: UUID,
    payload: SegmentMemberCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    seg = db.query(Segment).filter(Segment.id == segment_id).first()
    if not seg or seg.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")
    if seg.is_dynamic:
        raise HTTPException(status_code=400, detail="Cannot manually edit members of a dynamic segment")

    cust = db.query(Customer).filter(Customer.id == payload.customer_id).first()
    if not cust or cust.brand != active_brand:
        raise HTTPException(status_code=400, detail="Customer not found for this brand")

    if seg.provider == "UNOMI":
        try:
            stats = add_customers_to_unomi_manual_segment(db, seg=seg, customer_ids=[payload.customer_id])
            db.commit()
        except ValueError as e:
            db.rollback()
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=502, detail=f"Unomi member add failed: {e}")
        if stats.get("created", 0) == 0:
            raise HTTPException(status_code=409, detail="Customer already in segment or not found")
        return SegmentMemberOut(
            segment_id=segment_id,
            customer_id=payload.customer_id,
            source="UNOMI",
            computed_at=None,
            created_at=None,
        )

    m = SegmentMember(segment_id=segment_id, customer_id=payload.customer_id, source="STATIC")
    db.add(m)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Customer already in segment")
    db.refresh(m)
    return m


@router.post("/{segment_id}/members/bulk", response_model=SegmentMembersBulkResult)
def bulk_add_segment_members(
    segment_id: UUID,
    payload: SegmentMembersBulkAdd,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    seg = db.query(Segment).filter(Segment.id == segment_id).first()
    if not seg or seg.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")
    if seg.is_dynamic:
        raise HTTPException(status_code=400, detail="Cannot manually edit members of a dynamic segment")

    ids = payload.customer_ids or []
    seen = set()
    unique_ids: list[UUID] = []
    for cid in ids:
        if cid in seen:
            continue
        seen.add(cid)
        unique_ids.append(cid)

    if seg.provider == "UNOMI":
        try:
            stats = add_customers_to_unomi_manual_segment(db, seg=seg, customer_ids=unique_ids)
            db.commit()
        except ValueError as e:
            db.rollback()
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=502, detail=f"Unomi bulk add failed: {e}")
        return {
            "created": stats.get("created", 0),
            "skipped_existing": stats.get("skipped_existing", 0),
            "deleted": 0,
            "missing": stats.get("missing", 0),
            "invalid": 0,
            "errors": [],
        }

    created = 0
    skipped_existing = 0
    deleted = 0
    missing = 0
    invalid = 0
    errors: list[dict] = []

    for customer_id in unique_ids:
        try:
            cust = db.query(Customer).filter(Customer.id == customer_id).first()
            if not cust or cust.brand != active_brand:
                missing += 1
                continue

            exists = (
                db.query(SegmentMember)
                .filter(SegmentMember.segment_id == segment_id)
                .filter(SegmentMember.customer_id == customer_id)
                .first()
            )
            if exists:
                skipped_existing += 1
                continue

            m = SegmentMember(segment_id=segment_id, customer_id=customer_id, source="STATIC")
            db.add(m)
            db.flush()
            created += 1
        except Exception as e:
            db.rollback()
            errors.append({"customer_id": str(customer_id), "error": str(e)})

    db.commit()
    return {
        "created": created,
        "skipped_existing": skipped_existing,
        "deleted": deleted,
        "missing": missing,
        "invalid": invalid,
        "errors": errors,
    }


@router.delete("/{segment_id}/members/{customer_id}")
def remove_segment_member(
    segment_id: UUID,
    customer_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    seg = db.query(Segment).filter(Segment.id == segment_id).first()
    if not seg or seg.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")
    if seg.is_dynamic:
        raise HTTPException(status_code=400, detail="Cannot manually edit members of a dynamic segment")

    m = (
        db.query(SegmentMember)
        .filter(SegmentMember.segment_id == segment_id)
        .filter(SegmentMember.customer_id == customer_id)
        .first()
    )
    if seg.provider == "UNOMI":
        try:
            stats = remove_customers_from_unomi_manual_segment(db, seg=seg, customer_ids=[customer_id])
            db.commit()
        except ValueError as e:
            db.rollback()
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=502, detail=f"Unomi member remove failed: {e}")
        if stats.get("deleted", 0) == 0:
            raise HTTPException(status_code=404, detail="Segment member not found")
        return {"deleted": True}

    if not m:
        raise HTTPException(status_code=404, detail="Segment member not found")

    db.delete(m)
    db.commit()
    return {"deleted": True}


@router.post("/{segment_id}/members/bulk-delete", response_model=SegmentMembersBulkResult)
def bulk_remove_segment_members(
    segment_id: UUID,
    payload: SegmentMembersBulkRemove,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    seg = db.query(Segment).filter(Segment.id == segment_id).first()
    if not seg or seg.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")
    if seg.is_dynamic:
        raise HTTPException(status_code=400, detail="Cannot manually edit members of a dynamic segment")

    created = 0
    skipped_existing = 0
    deleted = 0
    missing = 0
    invalid = 0
    errors: list[dict] = []

    ids = payload.customer_ids or []
    seen = set()
    unique_ids: list[UUID] = []
    for cid in ids:
        if cid in seen:
            continue
        seen.add(cid)
        unique_ids.append(cid)

    if seg.provider == "UNOMI":
        try:
            stats = remove_customers_from_unomi_manual_segment(db, seg=seg, customer_ids=unique_ids)
            db.commit()
        except ValueError as e:
            db.rollback()
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=502, detail=f"Unomi bulk remove failed: {e}")
        return {
            "created": 0,
            "skipped_existing": 0,
            "deleted": stats.get("deleted", 0),
            "missing": stats.get("missing", 0),
            "invalid": 0,
            "errors": [],
        }

    for customer_id in unique_ids:
        try:
            m = (
                db.query(SegmentMember)
                .filter(SegmentMember.segment_id == segment_id)
                .filter(SegmentMember.customer_id == customer_id)
                .first()
            )
            if not m:
                missing += 1
                continue
            db.delete(m)
            db.flush()
            deleted += 1
        except Exception as e:
            db.rollback()
            errors.append({"customer_id": str(customer_id), "error": str(e)})

    db.commit()
    return {
        "created": created,
        "skipped_existing": skipped_existing,
        "deleted": deleted,
        "missing": missing,
        "invalid": invalid,
        "errors": errors,
    }
