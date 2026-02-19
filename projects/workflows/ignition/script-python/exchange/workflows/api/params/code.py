# exchange/workflows/api/params.py
"""
Workflow parameter templates + enums.

This module stores and retrieves:
- Parameter templates (versioned, append-only) backed by workflows.param_templates
- Enum registries (versioned, append-only) backed by workflows.param_enums

Template strategy:
- Engineers design Perspective Form props in Designer (to take advantange of the form builder).
- We store the Form props "almost as-is" (minus runtime-only root validation state).
- We also store extracted meta (paramKeys, widgetTypes, enumRefs, defaultData) so
  the UI doesn't need to parse arbitrary layouts later.

Enum strategy:
- Enums are stored in the DB as canonical option objects:
  {"label": "...", "value": "...", "isDisabled": false}
- Templates reference enums via a placeholder convention (in widget options):
  label/text: "@enum:EnumName" or "@enum:EnumName:3"
  value: "__enum__" (recommended)
- When loading a template for UI rendering, enum options are expanded into the
  widget's options list.
- TODO add default value so we can store that this is an enum but with a default value

Status values:
- DB schema uses: "active" | "obsolete"
- Public helpers accept: "active" | "obsolete" | "inactive" (inactive maps to obsolete)

Public API (stable, used by Perspective + programmatic callers):
Templates:
- listTemplates
- getTemplate
- createTemplateVersion
- setTemplateStatus
- setTemplateActive
- deleteTemplateVersion
- deleteTemplate
- expandTemplateForUi
- getTemplateDropdownOptions
- copyTemplateToWorkflow

Enums:
- listEnums
- getEnum
- createEnumVersion
- setEnumStatus

Run helpers (optional convenience):
- computeOverridesData
- resolveInputs
- startWithTemplate

"""

from exchange.workflows.engine.db import DB, nowMs
from exchange.workflows.api import service as workflowsService


log = system.util.getLogger(
    "exchange." + system.util.getProjectName().lower() + ".api.params"
)

DEFAULT_DB_NAME = "WorkflowsDB"

STATUS_ACTIVE = "active"
STATUS_OBSOLETE = "obsolete"


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------

try:
    _STRING_TYPES = (basestring,)  # noqa: F821 (Jython 2.7)
except:
    _STRING_TYPES = (str,)


def _isString(value):
    """
    Return True if value is a string type in this runtime.

    Args:
        value (object): Value payload to persist or publish.

    Returns:
        object: Result object returned by this call.
    """
    return isinstance(value, _STRING_TYPES)


def _deepCopy(obj):
    """
    Deep-copy dict/list data safely (Jython-safe).

                    Uses Ignition JSON encode/decode so Java/PyDictionary objects round-trip cleanly.

    Args:
        obj (object): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    try:
        return system.util.jsonDecode(system.util.jsonEncode(obj))
    except:
        return obj


def _asDict(obj):
    """
    Normalize input into a Python dict.

    Args:
        obj (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return _deepCopy(obj) or {}
    if _isString(obj):
        try:
            decoded = system.util.jsonDecode(obj)
            return decoded if isinstance(decoded, dict) else {}
        except:
            return {}
    # Attempt to serialize unknown mapping-like objects
    try:
        return _deepCopy(obj) or {}
    except:
        return {}


def _formatEpochMs(epochMs, fmt="dd-MMM-yy HH:mm:ss (z)"):
    """
    Format epoch ms into a readable timestamp string (Gateway locale/timezone).

    Args:
        epochMs (object): Input value for this call.
        fmt (object): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    try:
        return system.date.format(system.date.fromMillis(epochMs), fmt)
    except:
        return None


def _normalizeStatus(statusValue):
    """
    Normalize external status strings to DB status values.

                    Accepts:
                    - active
                    - obsolete
                    - inactive (alias -> obsolete)

    Args:
        statusValue (object): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    s = str(statusValue or "").strip().lower()
    if s == "inactive":
        return STATUS_OBSOLETE
    if s in (STATUS_ACTIVE, STATUS_OBSOLETE):
        return s
    # Default: treat unknown as obsolete-safe
    return STATUS_OBSOLETE


def _requireNonEmpty(value, fieldName):
    """
    Raise ValueError if value is falsy/empty.

    Args:
        value (object): Value payload to persist or publish.
        fieldName (object): Input value for this call.

    Returns:
        None: No explicit value is returned.
    """
    if value is None:
        raise ValueError("%s_required" % fieldName)
    if _isString(value) and not str(value).strip():
        raise ValueError("%s_required" % fieldName)


def _nextVersionTx(db, tx, sqlLatest, args):
    """
    Compute next version by locking the latest row (if exists), else return 1.

                    Best-effort concurrency safety for "append-only versions" patterns.

    Args:
        db (object): Input value for this call.
        tx (object): Input value for this call.
        sqlLatest (object): Input value for this call.
        args (str): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    ds = db.query(sqlLatest, args, tx=tx)
    if ds is None or ds.getRowCount() == 0:
        return 1
    try:
        return int(ds.getValueAt(0, 0)) + 1
    except:
        return 1


# -----------------------------------------------------------------------------
# Form props parsing + template meta extraction
# -----------------------------------------------------------------------------

_VALUE_WIDGET_TYPES = set(
    [
        "checkbox",
        "date-picker",
        "date-time-picker",
        "dropdown",
        "email",
        "number",
        "password",
        "radio",
        "slider",
        "tel",
        "text",
        "text-area",
        "time-picker",
        "toggle",
        "url",
    ]
)

_ENUM_CAPABLE_TYPES = set(["dropdown", "radio", "checkbox", "toggle"])


def _iterWidgets(formProps):
    """
    Yield widget dicts found under columns.items[].rows.items[].widgets[].

                    Expected Perspective Form layout:
                    columns.items[*].rows.items[*].widgets[*] = widgetObject

                    Yields:
                            dict: widget object

    Args:
        formProps (object): Input value for this call.

    Returns:
        None: No explicit value is returned.
    """
    try:
        columns = (formProps or {}).get("columns", {}) or {}
        colItems = columns.get("items", []) or []
        for col in colItems:
            rows = (col or {}).get("rows", {}) or {}
            rowItems = rows.get("items", []) or []
            for row in rowItems:
                widgets = (row or {}).get("widgets", []) or []
                for w in widgets:
                    if isinstance(w, dict):
                        yield w
    except:
        return


def _ensureDataKeysForAllWidgets(formProps):
    """
    Ensure Form.props.data has a key for every widget id.

                    This makes template defaults deterministic and makes downstream merging simple.
                    Any missing key is added with value None.

    Args:
        formProps (object): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    props = formProps or {}
    data = props.get("data", None)
    if not isinstance(data, dict):
        data = {}
    for w in _iterWidgets(props):
        wId = str((w.get("id", "") or "")).strip()
        if not wId:
            continue
        if wId not in data:
            data[wId] = None
    props["data"] = data
    return props


def _sanitizeFormProps(formProps, forcedName=None):
    """
    Sanitize Perspective Form props before storing as a template.

                    We intentionally keep the props "almost as-is" so Designer-authored configuration
                    is preserved. We remove ONLY runtime-only root validation state.

    Args:
        formProps (object): Input value for this call.
        forcedName (object): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    props = _asDict(formProps) or {}

    # Remove runtime-only root validation state (not design-time config).
    try:
        props.pop("validation", None)
    except:
        pass

    # Stabilize name (helps avoid accidental cross-template name coupling).
    name = props.get("name", "")
    if forcedName:
        name = forcedName
    if not name:
        name = "workflows.form"
    props["name"] = name

    # Ensure core structures exist
    if "columns" not in props or props["columns"] is None:
        props["columns"] = {"items": [], "style": {"classes": ""}}
    if "data" not in props or props["data"] is None:
        props["data"] = {}

    # Guarantee data keys exist for every widget id
    _ensureDataKeysForAllWidgets(props)

    return props


def _extractEnumRefFromWidget(widget):
    """
    Detect @enum placeholders in widget options.

                    Convention:
                    - widget.<type>.options includes a placeholder option:
                      label/text: "@enum:EnumName" or "@enum:EnumName:3"
                      value: "__enum__" (recommended)

    Args:
        widget (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    try:
        wType = str(widget.get("type", "") or "").strip()
        if wType not in _ENUM_CAPABLE_TYPES:
            return None

        optList = None
        if wType == "dropdown":
            optList = (widget.get("dropdown", {}) or {}).get("options", None)
        elif wType == "radio":
            optList = (widget.get("radio", {}) or {}).get("options", None)
        elif wType == "checkbox":
            optList = (widget.get("checkbox", {}) or {}).get("options", None)
        elif wType == "toggle":
            optList = (widget.get("toggle", {}) or {}).get("options", None)

        if not optList or not isinstance(optList, list):
            return None

        first = optList[0] if len(optList) > 0 else None
        if not isinstance(first, dict):
            return None

        # dropdown uses label; others often use text
        label = first.get("label", None)
        if label is None:
            label = first.get("text", "")

        label = str(label or "").strip()
        if not label.startswith("@enum:"):
            return None

        # Parse "@enum:Name" or "@enum:Name:3"
        parts = label.split(":")
        if len(parts) < 2:
            return None

        enumName = parts[1].strip()
        enumVersion = None
        if len(parts) >= 3:
            try:
                enumVersion = int(parts[2])
            except:
                enumVersion = None

        if not enumName:
            return None

        return {"enumName": enumName, "enumVersion": enumVersion}
    except:
        return None


def _extractMetaFromFormProps(formProps):
    """
    Extract meta from sanitized Form props.

                    Meta fields:
                    - paramKeys: ordered widget ids
                    - widgetTypes: widget type by id
                    - defaultData: Form.props.data (deep-copied)
                    - enumRefs: widgetId -> {"enumName","enumVersion"}

    Args:
        formProps (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    paramKeys = []
    widgetTypes = {}
    enumRefs = {}

    propsData = (formProps or {}).get("data", {}) or {}
    defaultData = _deepCopy(propsData) or {}

    for w in _iterWidgets(formProps):
        wId = str((w.get("id", "") or "")).strip()
        wType = str((w.get("type", "") or "")).strip()
        if not wId:
            continue

        if wId not in widgetTypes:
            paramKeys.append(wId)
            widgetTypes[wId] = wType

        enumRef = _extractEnumRefFromWidget(w)
        if enumRef:
            enumRefs[wId] = enumRef

    return {
        "paramKeys": paramKeys,
        "widgetTypes": widgetTypes,
        "defaultData": defaultData,
        "enumRefs": enumRefs,
    }


# -----------------------------------------------------------------------------
# Enum expansion into templates for UI rendering
# -----------------------------------------------------------------------------


def _enumOptionsToWidgetOptions(enumOptions, widgetType):
    """
    Convert canonical enum options to widget-specific option objects.

                    Canonical enum option:
                    {"label": "...", "value": "...", "isDisabled": false}

                    Widget mappings:
                    - dropdown: {"label","value","isDisabled"}
                    - radio: {"text","value"}
                    - toggle: {"text","value"}
                    - checkbox: {"text","value","triState": false}

    Args:
        enumOptions (object): Input value for this call.
        widgetType (object): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    out = []
    enumOptions = enumOptions or []
    for o in enumOptions:
        if not isinstance(o, dict):
            continue
        label = o.get("label", "")
        val = o.get("value", "")
        isDisabled = bool(o.get("isDisabled", False))

        if widgetType == "dropdown":
            out.append({"label": label, "value": val, "isDisabled": isDisabled})
        elif widgetType == "radio":
            out.append({"text": label, "value": val})
        elif widgetType == "toggle":
            out.append({"text": label, "value": val})
        elif widgetType == "checkbox":
            out.append({"text": label, "value": val, "triState": False})
        else:
            out.append({"label": label, "value": val, "isDisabled": isDisabled})
    return out


def expandTemplateForUi(templateObj, dbName=DEFAULT_DB_NAME):
    """
    Expand enum refs into widget options for UI rendering.

    Args:
        templateObj (object): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    if not templateObj:
        return None

    out = _deepCopy(templateObj) or {}
    schema = out.get("schema", {}) or {}
    formProps = schema.get("formProps", {}) or {}
    meta = schema.get("meta", {}) or {}

    enumRefs = meta.get("enumRefs", {}) or {}
    if not enumRefs:
        return out

    enumCache = {}
    enumVersions = {}

    for w in _iterWidgets(formProps):
        wId = str((w.get("id", "") or "")).strip()
        wType = str((w.get("type", "") or "")).strip()
        if not wId or wId not in enumRefs:
            continue

        ref = enumRefs.get(wId, {}) or {}
        enumName = ref.get("enumName", None)
        enumVersion = ref.get("enumVersion", None)
        if not enumName:
            continue

        cacheKey = "%s:%s" % (
            enumName,
            str(enumVersion) if enumVersion is not None else "latest",
        )
        if cacheKey not in enumCache:
            enumObj = getEnum(enumName, enumVersion=enumVersion, dbName=dbName)
            if not enumObj:
                enumCache[cacheKey] = []
                enumVersions[enumName] = None
            else:
                enumCache[cacheKey] = enumObj.get("values", []) or []
                enumVersions[enumName] = enumObj.get("enumVersion", None)

        enumOptions = enumCache[cacheKey]
        widgetOptions = _enumOptionsToWidgetOptions(enumOptions, wType)

        # Write options into the correct sub-dict for this widget type
        try:
            if wType == "dropdown":
                w["dropdown"]["options"] = widgetOptions
            elif wType == "radio":
                w["radio"]["options"] = widgetOptions
            elif wType == "checkbox":
                w["checkbox"]["options"] = widgetOptions
            elif wType == "toggle":
                w["toggle"]["options"] = widgetOptions
        except Exception as e:
            log.warn(
                "evt=params.expandTemplateForUi widget=%s type=%s err=%s"
                % (wId, wType, e)
            )

    # Optional: stamp which enum versions were used
    meta["enumVersions"] = enumVersions
    schema["meta"] = meta
    schema["formProps"] = formProps
    out["schema"] = schema
    return out


# -----------------------------------------------------------------------------
# Templates (DB-backed)
# -----------------------------------------------------------------------------


def listTemplates(
    workflowName, status=STATUS_ACTIVE, latestOnly=True, dbName=DEFAULT_DB_NAME
):
    """
    List templates for a workflow.

    Args:
        workflowName (object): Input value for this call.
        status (object): Input value for this call.
        latestOnly (object): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    _requireNonEmpty(workflowName, "workflowName")
    db = DB(dbName)

    args = [workflowName]
    statusSql = ""
    if status is not None:
        statusSql = " AND status = ? "
        args.append(_normalizeStatus(status))

    if latestOnly:
        sql = (
            """
		SELECT DISTINCT ON (template_name)
			template_name, template_version, status, description, created_by, created_at_epoch_ms
		FROM workflows.param_templates
		WHERE workflow_name = ?
		%s
		ORDER BY template_name, template_version DESC
		"""
            % statusSql
        )
    else:
        sql = (
            """
		SELECT
			template_name, template_version, status, description, created_by, created_at_epoch_ms
		FROM workflows.param_templates
		WHERE workflow_name = ?
		%s
		ORDER BY template_name, template_version DESC
		"""
            % statusSql
        )

    ds = db.query(sql, args)
    out = []
    for r in range(ds.getRowCount()):
        epochMs = long(ds.getValueAt(r, 5))
        out.append(
            {
                "workflowName": workflowName,
                "templateName": str(ds.getValueAt(r, 0)),
                "templateVersion": int(ds.getValueAt(r, 1)),
                "status": str(ds.getValueAt(r, 2)),
                "description": ds.getValueAt(r, 3),
                "createdBy": ds.getValueAt(r, 4),
                "createdAt": _formatEpochMs(epochMs),
            }
        )
    return out


def getTemplate(
    workflowName,
    templateName,
    templateVersion=None,
    dbName=DEFAULT_DB_NAME,
    activeOnly=True,
):
    """
    Get a single template version.

    Args:
        workflowName (object): Input value for this call.
        templateName (object): Input value for this call.
        templateVersion (object): Input value for this call.
        dbName (object): Input value for this call.
        activeOnly (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    _requireNonEmpty(workflowName, "workflowName")
    _requireNonEmpty(templateName, "templateName")

    db = DB(dbName)

    if templateVersion is None:
        sql = """
		SELECT template_version, schema_json, status, description, created_by, created_at_epoch_ms
		FROM workflows.param_templates
		WHERE workflow_name = ? AND template_name = ?
		%s
		ORDER BY template_version DESC
		LIMIT 1
		""" % (
            "AND status = 'active'" if activeOnly else ""
        )
        args = [workflowName, templateName]
    else:
        sql = """
		SELECT template_version, schema_json, status, description, created_by, created_at_epoch_ms
		FROM workflows.param_templates
		WHERE workflow_name = ? AND template_name = ? AND template_version = ?
		LIMIT 1
		"""
        args = [workflowName, templateName, int(templateVersion)]

    ds = db.query(sql, args)
    if ds is None or ds.getRowCount() == 0:
        return None

    schemaRaw = ds.getValueAt(0, 1)
    schema = system.util.jsonDecode(schemaRaw) if schemaRaw else {}
    epochMs = long(ds.getValueAt(0, 5))

    return {
        "workflowName": workflowName,
        "templateName": templateName,
        "templateVersion": int(ds.getValueAt(0, 0)),
        "schema": schema,
        "status": str(ds.getValueAt(0, 2)),
        "description": ds.getValueAt(0, 3),
        "createdBy": ds.getValueAt(0, 4),
        "createdAtEpochMs": epochMs,
        "createdAt": _formatEpochMs(epochMs),
    }


def createTemplateVersion(
    workflowName,
    templateName,
    formProps,
    description=None,
    createdBy=None,
    activate=True,
    dbName=DEFAULT_DB_NAME,
):
    """
    Create a new template version from Perspective Form props.

                    Stored schema_json format:
                    {
                            "formProps": <sanitized form props>,
                            "meta": {
                                    "paramKeys": [...],
                                    "widgetTypes": {...},
                                    "defaultData": {...},
                                    "enumRefs": {...}
                            }
                    }

    Args:
        workflowName (object): Input value for this call.
        templateName (object): Input value for this call.
        formProps (object): Input value for this call.
        description (object): Input value for this call.
        createdBy (object): Input value for this call.
        activate (bool): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    createdBy = createdBy or "admin"
    _requireNonEmpty(workflowName, "workflowName")
    _requireNonEmpty(templateName, "templateName")

    db = DB(dbName)
    tx = db.begin()
    try:
        sqlLatest = """
		SELECT template_version
		FROM workflows.param_templates
		WHERE workflow_name = ? AND template_name = ?
		ORDER BY template_version DESC
		LIMIT 1
		FOR UPDATE
		"""
        nextVersion = _nextVersionTx(db, tx, sqlLatest, [workflowName, templateName])

        forcedName = "%s.%s" % (workflowName, templateName)
        sanitized = _sanitizeFormProps(formProps, forcedName=forcedName)
        meta = _extractMetaFromFormProps(sanitized)

        schemaObj = {"formProps": sanitized, "meta": meta}

        sqlIns = """
		INSERT INTO workflows.param_templates(
			workflow_name, template_name, template_version,
			schema_json, status, description, created_by, created_at_epoch_ms
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		"""
        status = STATUS_ACTIVE if activate else STATUS_OBSOLETE
        db.update(
            sqlIns,
            [
                workflowName,
                templateName,
                int(nextVersion),
                system.util.jsonEncode(schemaObj),
                status,
                description,
                createdBy,
                long(nowMs()),
            ],
            tx=tx,
        )

        db.commit(tx)
        return {"ok": True, "templateVersion": int(nextVersion)}
    except Exception as e:
        db.rollback(tx)
        log.error(
            "evt=params.createTemplateVersion workflow=%s template=%s err=%s"
            % (workflowName, templateName, e)
        )
        return {"ok": False, "error": str(e)}
    finally:
        db.close(tx)


def setTemplateStatus(
    workflowName,
    templateName,
    templateVersion,
    status,
    actor=None,
    dbName=DEFAULT_DB_NAME,
):
    """
    Set template status for a specific version.

    Args:
        workflowName (object): Input value for this call.
        templateName (object): Input value for this call.
        templateVersion (object): Input value for this call.
        status (object): Input value for this call.
        actor (object): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    actor = actor or "admin"
    _requireNonEmpty(workflowName, "workflowName")
    _requireNonEmpty(templateName, "templateName")

    statusDb = _normalizeStatus(status)

    db = DB(dbName)
    sql = """
	UPDATE workflows.param_templates
	SET status = ?
	WHERE workflow_name = ? AND template_name = ? AND template_version = ?
	"""
    n = db.update(sql, [statusDb, workflowName, templateName, int(templateVersion)])
    return {"ok": True, "rows": int(n), "actor": actor, "status": statusDb}


def setTemplateActive(
    workflowName,
    templateName,
    templateVersion,
    isActive,
    actor=None,
    dbName=DEFAULT_DB_NAME,
):
    """
    Convenience helper to flip a template version active/obsolete.

    Args:
        workflowName (object): Input value for this call.
        templateName (object): Input value for this call.
        templateVersion (object): Input value for this call.
        isActive (object): Input value for this call.
        actor (object): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    status = STATUS_ACTIVE if bool(isActive) else STATUS_OBSOLETE
    return setTemplateStatus(
        workflowName, templateName, templateVersion, status, actor=actor, dbName=dbName
    )


def deleteTemplateVersion(
    workflowName, templateName, templateVersion, actor=None, dbName=DEFAULT_DB_NAME
):
    """
    Hard-delete a specific template version.

                    Use with care: this permanently removes the version row from the DB.

    Args:
        workflowName (object): Input value for this call.
        templateName (object): Input value for this call.
        templateVersion (object): Input value for this call.
        actor (object): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    actor = actor or "admin"
    _requireNonEmpty(workflowName, "workflowName")
    _requireNonEmpty(templateName, "templateName")

    db = DB(dbName)
    sql = """
	DELETE FROM workflows.param_templates
	WHERE workflow_name = ? AND template_name = ? AND template_version = ?
	"""
    n = db.update(sql, [workflowName, templateName, int(templateVersion)])
    return {"ok": True, "rows": int(n), "actor": actor}


def deleteTemplate(workflowName, templateName, actor=None, dbName=DEFAULT_DB_NAME):
    """
    Hard-delete ALL versions of a template for a workflow.

                    Use with care: this permanently removes all versions.

    Args:
        workflowName (object): Input value for this call.
        templateName (object): Input value for this call.
        actor (object): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    actor = actor or "admin"
    _requireNonEmpty(workflowName, "workflowName")
    _requireNonEmpty(templateName, "templateName")

    db = DB(dbName)
    sql = """
	DELETE FROM workflows.param_templates
	WHERE workflow_name = ? AND template_name = ?
	"""
    n = db.update(sql, [workflowName, templateName])
    return {"ok": True, "rows": int(n), "actor": actor}


def getTemplateDropdownOptions(
    workflowName, status=STATUS_ACTIVE, latestOnly=True, dbName=DEFAULT_DB_NAME
):
    """
    Return a list suitable for Perspective dropdown options.

                    Value is a JSON string containing templateName + templateVersion, so UI can
                    round-trip without needing multiple bound props.

    Args:
        workflowName (object): Input value for this call.
        status (object): Input value for this call.
        latestOnly (object): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    items = listTemplates(
        workflowName, status=status, latestOnly=latestOnly, dbName=dbName
    )
    out = []
    for t in items:
        label = "%s v%s" % (t.get("templateName"), t.get("templateVersion"))
        val = system.util.jsonEncode(
            t
        )  # {"templateName": t.get("templateName"), "templateVersion": t.get("templateVersion")})
        out.append({"label": label, "value": val})
    return out


def copyTemplateToWorkflow(
    sourceWorkflowName,
    templateName,
    targetWorkflowName,
    newTemplateName=None,
    templateVersion=None,
    createdBy=None,
    dbName=DEFAULT_DB_NAME,
):
    """
    Copy a template (latest active or specific version) to another workflow by creating a new version there.

                    This re-saves the Form props via createTemplateVersion(), ensuring:
                    - sanitization rules apply
                    - meta is re-extracted cleanly
                    - forced name is updated to target workflow/template

    Args:
        sourceWorkflowName (object): Input value for this call.
        templateName (object): Input value for this call.
        targetWorkflowName (object): Input value for this call.
        newTemplateName (object): Input value for this call.
        templateVersion (object): Input value for this call.
        createdBy (object): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    createdBy = createdBy or "admin"
    newTemplateName = newTemplateName or templateName

    tpl = getTemplate(
        sourceWorkflowName,
        templateName,
        templateVersion=templateVersion,
        dbName=dbName,
        activeOnly=False,
    )
    if not tpl:
        return {"ok": False, "error": "template_not_found"}

    schema = tpl.get("schema", {}) or {}
    formProps = schema.get("formProps", {}) or {}

    desc = "Copied from %s.%s v%s" % (
        sourceWorkflowName,
        templateName,
        tpl.get("templateVersion"),
    )
    return createTemplateVersion(
        workflowName=targetWorkflowName,
        templateName=newTemplateName,
        formProps=formProps,
        description=desc,
        createdBy=createdBy,
        activate=True,
        dbName=dbName,
    )


# -----------------------------------------------------------------------------
# Enums (DB-backed)
# -----------------------------------------------------------------------------


def listEnums(status=STATUS_ACTIVE, latestOnly=True, dbName=DEFAULT_DB_NAME):
    """
    List enums.

    Args:
        status (object): Input value for this call.
        latestOnly (object): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    db = DB(dbName)

    args = []
    statusSql = ""
    if status is not None:
        statusSql = " WHERE status = ? "
        args = [_normalizeStatus(status)]

    if latestOnly:
        sql = (
            """
		SELECT DISTINCT ON (enum_name)
			enum_name, enum_version, status, description, created_by, created_at_epoch_ms
		FROM workflows.param_enums
		%s
		ORDER BY enum_name, enum_version DESC
		"""
            % statusSql
        )
    else:
        sql = (
            """
		SELECT
			enum_name, enum_version, status, description, created_by, created_at_epoch_ms
		FROM workflows.param_enums
		%s
		ORDER BY enum_name, enum_version DESC
		"""
            % statusSql
        )

    ds = db.query(sql, args)
    out = []
    for r in range(ds.getRowCount()):
        epochMs = long(ds.getValueAt(r, 5))
        out.append(
            {
                "enumName": str(ds.getValueAt(r, 0)),
                "enumVersion": int(ds.getValueAt(r, 1)),
                "status": str(ds.getValueAt(r, 2)),
                "description": ds.getValueAt(r, 3),
                "createdBy": ds.getValueAt(r, 4),
                "createdAtEpochMs": epochMs,
                "createdAt": _formatEpochMs(epochMs),
            }
        )
    return out


def getEnum(enumName, enumVersion=None, dbName=DEFAULT_DB_NAME, activeOnly=True):
    """
    Get an enum (latest active by default).

    Args:
        enumName (object): Input value for this call.
        enumVersion (object): Input value for this call.
        dbName (object): Input value for this call.
        activeOnly (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    _requireNonEmpty(enumName, "enumName")
    db = DB(dbName)

    if enumVersion is None:
        sql = """
		SELECT enum_version, values_json, status, description, created_by, created_at_epoch_ms
		FROM workflows.param_enums
		WHERE enum_name = ?
		%s
		ORDER BY enum_version DESC
		LIMIT 1
		""" % (
            "AND status = 'active'" if activeOnly else ""
        )
        args = [enumName]
    else:
        sql = """
		SELECT enum_version, values_json, status, description, created_by, created_at_epoch_ms
		FROM workflows.param_enums
		WHERE enum_name = ? AND enum_version = ?
		LIMIT 1
		"""
        args = [enumName, int(enumVersion)]

    ds = db.query(sql, args)
    if ds is None or ds.getRowCount() == 0:
        return None

    valuesRaw = ds.getValueAt(0, 1)
    values = system.util.jsonDecode(valuesRaw) if valuesRaw else []
    epochMs = long(ds.getValueAt(0, 5))

    return {
        "enumName": enumName,
        "enumVersion": int(ds.getValueAt(0, 0)),
        "values": values or [],
        "status": str(ds.getValueAt(0, 2)),
        "description": ds.getValueAt(0, 3),
        "createdBy": ds.getValueAt(0, 4),
        "createdAtEpochMs": epochMs,
        "createdAt": _formatEpochMs(epochMs),
    }


def createEnumVersion(
    enumName,
    enumValues,
    description=None,
    createdBy=None,
    activate=True,
    dbName=DEFAULT_DB_NAME,
):
    """
    Create a new enum version (append-only).

    Args:
        enumName (object): Input value for this call.
        enumValues (object): Input value for this call.
        description (object): Input value for this call.
        createdBy (object): Input value for this call.
        activate (bool): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    createdBy = createdBy or "admin"
    _requireNonEmpty(enumName, "enumName")

    db = DB(dbName)
    tx = db.begin()
    try:
        sqlLatest = """
		SELECT enum_version
		FROM workflows.param_enums
		WHERE enum_name = ?
		ORDER BY enum_version DESC
		LIMIT 1
		FOR UPDATE
		"""
        nextVersion = _nextVersionTx(db, tx, sqlLatest, [enumName])

        sqlIns = """
		INSERT INTO workflows.param_enums(
			enum_name, enum_version, values_json, status, description, created_by, created_at_epoch_ms
		) VALUES (?, ?, ?, ?, ?, ?, ?)
		"""
        status = STATUS_ACTIVE if activate else STATUS_OBSOLETE
        db.update(
            sqlIns,
            [
                enumName,
                int(nextVersion),
                system.util.jsonEncode(enumValues),
                status,
                description,
                createdBy,
                long(nowMs()),
            ],
            tx=tx,
        )

        db.commit(tx)
        return {"ok": True, "enumVersion": int(nextVersion)}
    except Exception as e:
        db.rollback(tx)
        log.error("evt=params.createEnumVersion enum=%s err=%s" % (enumName, e))
        return {"ok": False, "error": str(e)}
    finally:
        db.close(tx)


def setEnumStatus(enumName, enumVersion, status, actor=None, dbName=DEFAULT_DB_NAME):
    """
    Set enum status (active/obsolete).

    Args:
        enumName (object): Input value for this call.
        enumVersion (object): Input value for this call.
        status (object): Input value for this call.
        actor (object): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    actor = actor or "admin"
    _requireNonEmpty(enumName, "enumName")

    statusDb = _normalizeStatus(status)

    db = DB(dbName)
    sql = """
	UPDATE workflows.param_enums
	SET status = ?
	WHERE enum_name = ? AND enum_version = ?
	"""
    n = db.update(sql, [statusDb, enumName, int(enumVersion)])
    return {"ok": True, "rows": int(n), "actor": actor, "status": statusDb}


def deleteEnumVersion(enumName, enumVersion, actor=None, dbName=DEFAULT_DB_NAME):
    """
    Hard-delete a specific enum version.

                    Use with care: this permanently removes the version row from the DB.

    Args:
        enumName (object): Input value for this call.
        enumVersion (object): Input value for this call.
        actor (object): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    actor = actor or "admin"
    _requireNonEmpty(enumName, "enumName")

    db = DB(dbName)
    sql = """
	DELETE FROM workflows.param_enums
	WHERE enum_name = ? AND enum_version = ?
	"""
    n = db.update(sql, [enumName, int(enumVersion)])
    return {"ok": True, "rows": int(n), "actor": actor}


def deleteEnum(enumName, actor=None, dbName=DEFAULT_DB_NAME):
    """
    Hard-delete ALL versions of an enum.

                    Use with care: this permanently removes all versions for enum_name.

    Args:
        enumName (object): Input value for this call.
        actor (object): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    actor = actor or "admin"
    _requireNonEmpty(enumName, "enumName")

    db = DB(dbName)
    sql = """
	DELETE FROM workflows.param_enums
	WHERE enum_name = ?
	"""
    n = db.update(sql, [enumName])
    return {"ok": True, "rows": int(n), "actor": actor}


# -----------------------------------------------------------------------------
# Run helpers (template-only)
# -----------------------------------------------------------------------------


def computeOverridesData(baseData, currentData):
    """
    Compute overrides relative to a base dict.

    Args:
        baseData (object): Input value for this call.
        currentData (object): Input value for this call.

    Returns:
        object: Result object returned by this call.
    """
    baseData = baseData or {}
    currentData = currentData or {}

    out = {}
    for k in currentData.keys():
        if currentData.get(k) != baseData.get(k):
            out[k] = currentData.get(k)
    return out


def resolveInputs(
    workflowName,
    templateName=None,
    overridesData=None,
    templateVersion=None,
    actor=None,
    dbName=DEFAULT_DB_NAME,
):
    """
    Resolve final workflow inputs (resolved + paramSource), template-only.

                    Behavior:
                    - If templateName is None: resolved = overridesData or {}
                    - If templateName provided:
                            - base = template meta.defaultData
                            - resolved = base merged with overridesData
                            - extra override keys not in template are rejected

    Args:
        workflowName (object): Input value for this call.
        templateName (object): Input value for this call.
        overridesData (object): Input value for this call.
        templateVersion (object): Input value for this call.
        actor (object): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    actor = actor or "admin"
    overridesData = overridesData or {}

    if not templateName:
        return {
            "ok": True,
            "inputs": {"resolved": overridesData, "paramSource": None},
            "errors": [],
            "warnings": [],
        }

    tpl = getTemplate(
        workflowName,
        templateName,
        templateVersion=templateVersion,
        dbName=dbName,
        activeOnly=False,
    )
    if not tpl:
        return {
            "ok": False,
            "inputs": None,
            "errors": ["template_not_found"],
            "warnings": [],
        }

    schema = tpl.get("schema", {}) or {}
    meta = schema.get("meta", {}) or {}
    paramKeys = meta.get("paramKeys", []) or []
    defaultData = meta.get("defaultData", {}) or {}

    # Reject unknown keys
    for k in overridesData.keys():
        if k not in paramKeys:
            return {
                "ok": False,
                "inputs": None,
                "errors": ["override_key_not_in_template:%s" % k],
                "warnings": [],
            }

    # Merge (ensure all keys exist)
    resolved = _deepCopy(defaultData) or {}
    for k in overridesData.keys():
        resolved[k] = overridesData.get(k)

    # Ensure resolved has all template keys (fill missing from defaults)
    for k in paramKeys:
        if k not in resolved:
            resolved[k] = None

    paramSource = {
        "template": {"name": templateName, "version": int(tpl.get("templateVersion"))},
        "overrides": sorted(list(overridesData.keys())),
        "actor": actor,
    }

    return {
        "ok": True,
        "inputs": {"resolved": resolved, "paramSource": paramSource},
        "errors": [],
        "warnings": [],
    }


def startWithTemplate(
    workflowName,
    templateName,
    overridesData=None,
    templateVersion=None,
    actor="admin",
    queueName="default",
    partitionKey=None,
    priority=0,
    deduplicationId=None,
    applicationVersion=None,
    timeoutSeconds=None,
    dbName=DEFAULT_DB_NAME,
):
    """
    Convenience: resolveInputs() then workflowsService.start(...).

    Args:
        workflowName (object): Input value for this call.
        templateName (object): Input value for this call.
        overridesData (object): Input value for this call.
        templateVersion (object): Input value for this call.
        actor (object): Input value for this call.
        queueName (object): Input value for this call.
        partitionKey (object): Input value for this call.
        priority (int): Priority value used when ordering queued workflows.
        deduplicationId (object): Input value for this call.
        applicationVersion (object): Input value for this call.
        timeoutSeconds (object): Input value for this call.
        dbName (object): Input value for this call.

    Returns:
        dict: Structured payload with results or status details.
    """
    r = resolveInputs(
        workflowName=workflowName,
        templateName=templateName,
        overridesData=overridesData,
        templateVersion=templateVersion,
        actor=actor,
        dbName=dbName,
    )
    if not r.get("ok"):
        return r

    wid = workflowsService.start(
        workflowName=workflowName,
        inputs=r.get("inputs"),
        queueName=queueName,
        partitionKey=partitionKey,
        priority=priority,
        deduplicationId=deduplicationId,
        applicationVersion=applicationVersion,
        timeoutSeconds=timeoutSeconds,
        dbName=dbName,
    )
    return {"ok": True, "workflowId": wid, "inputs": r.get("inputs")}
