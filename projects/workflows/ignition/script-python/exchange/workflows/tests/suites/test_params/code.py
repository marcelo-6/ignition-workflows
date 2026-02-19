"""Params API suite focused on template listing and retrieval behavior."""

from java.util import UUID

from exchange.workflows import settings
from exchange.workflows.api import params as params_api
from exchange.workflows.tests.support.base_case import WorkflowTestCase
from exchange.workflows.tests.support.helpers import profiles
from exchange.workflows.tests.support import helpers


class TestParamsSuite(WorkflowTestCase):
    """Template/listing contract checks for exchange.workflows.api.params."""

    SUITE_NAME = "params"

    def setUp(self):
        super(TestParamsSuite, self).setUp()
        self._nameSeq = 0
        self._trackedTemplates = []
        self._trackedTemplateSet = set()
        self._trackedEnums = []
        self._trackedEnumSet = set()

    def tearDown(self):
        cleanupErrors = []
        for workflowName, templateName in reversed(self._trackedTemplates):
            try:
                params_api.deleteTemplate(
                    workflowName=workflowName,
                    templateName=templateName,
                    actor="tests.params.cleanup",
                    dbName=self.dbName,
                )
            except Exception as e:
                cleanupErrors.append(
                    "template %s.%s err=%s" % (workflowName, templateName, e)
                )

        for enumName in reversed(self._trackedEnums):
            try:
                params_api.deleteEnum(
                    enumName=enumName,
                    actor="tests.params.cleanup",
                    dbName=self.dbName,
                )
            except Exception as e:
                cleanupErrors.append("enum %s err=%s" % (enumName, e))

        super(TestParamsSuite, self).tearDown()
        if cleanupErrors:
            self.fail("params cleanup failed: %s" % "; ".join(cleanupErrors))

    def _nextToken(self, label):
        self._nameSeq += 1
        methodName = str(getattr(self, "_testMethodName", "case") or "case")
        return "%s_%s_%s_%s_%s" % (
            self.SUITE_NAME,
            str(label or "x"),
            methodName,
            long(helpers.nowMs()),
            int(self._nameSeq),
        )

    def _newWorkflowName(self, label="workflow"):
        return "tests.params.%s" % self._nextToken(label)

    def _newTemplateName(self, label="template"):
        return "template_%s" % self._nextToken(label)

    def _newTemplateIdentity(self, label="template"):
        token = self._nextToken(label)
        return ("tests.params.%s" % token, "template_%s" % token)

    def _trackTemplate(self, workflowName, templateName):
        key = (str(workflowName), str(templateName))
        if key not in self._trackedTemplateSet:
            self._trackedTemplateSet.add(key)
            self._trackedTemplates.append(key)

    def _trackEnum(self, enumName):
        name = str(enumName)
        if name not in self._trackedEnumSet:
            self._trackedEnumSet.add(name)
            self._trackedEnums.append(name)

    def _buildFormProps(self, value, key="line"):
        key = str(key or "line")
        return {
            "name": "tests.params.form",
            "columns": {
                "items": [
                    {
                        "rows": {
                            "items": [
                                {
                                    "widgets": [
                                        {
                                            "id": key,
                                            "type": "text",
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            },
            "data": {key: value},
            "validation": {"isValid": True},
        }

    def _createTemplateVersion(
        self,
        workflowName,
        templateName,
        value,
        activate=True,
        description=None,
        key="line",
    ):
        self._trackTemplate(workflowName, templateName)
        resp = params_api.createTemplateVersion(
            workflowName=workflowName,
            templateName=templateName,
            formProps=self._buildFormProps(value, key=key),
            description=description,
            createdBy="tests.params",
            activate=bool(activate),
            dbName=self.dbName,
        )
        self.assertTrue(resp.get("ok"), "createTemplateVersion failed: %r" % resp)
        self.assertTrue(resp.get("templateVersion"), "templateVersion missing: %r" % resp)
        return int(resp.get("templateVersion"))

    def _createEnumVersion(self, enumName, options, activate=True, description=None):
        self._trackEnum(enumName)
        resp = params_api.createEnumVersion(
            enumName=enumName,
            enumValues=options,
            description=description,
            createdBy="tests.params",
            activate=bool(activate),
            dbName=self.dbName,
        )
        self.assertTrue(resp.get("ok"), "createEnumVersion failed: %r" % resp)
        self.assertTrue(resp.get("enumVersion"), "enumVersion missing: %r" % resp)
        return int(resp.get("enumVersion"))

    @profiles("smoke", "full")
    def test_list_templates_latest_only_returns_latest_version(self):
        """latestOnly should return the highest template version."""
        workflowName, templateName = self._newTemplateIdentity("latest")
        self._createTemplateVersion(
            workflowName=workflowName,
            templateName=templateName,
            value="v1",
            activate=True,
            description="v1",
        )
        self._createTemplateVersion(
            workflowName=workflowName,
            templateName=templateName,
            value="v2",
            activate=True,
            description="v2",
        )

        rows = params_api.listTemplates(
            workflowName=workflowName,
            status="active",
            latestOnly=True,
            dbName=self.dbName,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("templateName"), templateName)
        self.assertEqual(int(rows[0].get("templateVersion")), 2)

    @profiles("smoke", "full")
    def test_list_templates_latest_only_returns_one_row_per_template_name(self):
        """latestOnly should dedupe by template name for one workflow."""
        workflowName = self._newWorkflowName("list_one_row")
        templateA = self._newTemplateName("alpha")
        templateB = self._newTemplateName("beta")

        self._createTemplateVersion(workflowName, templateA, value="a1", activate=True)
        self._createTemplateVersion(workflowName, templateA, value="a2", activate=True)
        self._createTemplateVersion(workflowName, templateB, value="b1", activate=True)

        rows = params_api.listTemplates(
            workflowName=workflowName,
            status="active",
            latestOnly=True,
            dbName=self.dbName,
        )
        self.assertEqual(len(rows), 2)

        byName = {}
        for row in rows:
            byName[str(row.get("templateName"))] = int(row.get("templateVersion"))

        self.assertEqual(set(byName.keys()), set([templateA, templateB]))
        self.assertEqual(byName.get(templateA), 2)
        self.assertEqual(byName.get(templateB), 1)

    @profiles("smoke", "full")
    def test_list_templates_status_filter_active_obsolete(self):
        """Status filters should continue to work with latestOnly."""
        workflowName, templateName = self._newTemplateIdentity("status_filters")
        v1 = self._createTemplateVersion(
            workflowName=workflowName,
            templateName=templateName,
            value="active-v1",
            activate=True,
        )
        v2 = self._createTemplateVersion(
            workflowName=workflowName,
            templateName=templateName,
            value="active-v2",
            activate=True,
        )
        upd = params_api.setTemplateStatus(
            workflowName=workflowName,
            templateName=templateName,
            templateVersion=v2,
            status="obsolete",
            actor="tests.params",
            dbName=self.dbName,
        )
        self.assertTrue(upd.get("ok"), "setTemplateStatus failed: %r" % upd)
        self.assertEqual(int(upd.get("rows", 0)), 1)

        activeRows = params_api.listTemplates(
            workflowName=workflowName,
            status="active",
            latestOnly=True,
            dbName=self.dbName,
        )
        obsoleteRows = params_api.listTemplates(
            workflowName=workflowName,
            status="obsolete",
            latestOnly=True,
            dbName=self.dbName,
        )

        self.assertEqual(len(activeRows), 1)
        self.assertEqual(len(obsoleteRows), 1)
        self.assertEqual(int(activeRows[0].get("templateVersion")), v1)
        self.assertEqual(int(obsoleteRows[0].get("templateVersion")), v2)

    @profiles("full")
    def test_list_templates_full_history_order_desc(self):
        """Non-latest listing should return all versions in descending order."""
        workflowName, templateName = self._newTemplateIdentity("history_desc")
        self._createTemplateVersion(workflowName, templateName, value="v1", activate=True)
        self._createTemplateVersion(workflowName, templateName, value="v2", activate=True)
        self._createTemplateVersion(workflowName, templateName, value="v3", activate=True)

        rows = params_api.listTemplates(
            workflowName=workflowName,
            status="active",
            latestOnly=False,
            dbName=self.dbName,
        )
        versions = [int(row.get("templateVersion")) for row in rows]
        self.assertEqual(versions, [3, 2, 1])

    @profiles("full")
    def test_get_template_default_returns_latest_active(self):
        """getTemplate default path should return latest active version only."""
        workflowName, templateName = self._newTemplateIdentity("default_latest_active")
        v1 = self._createTemplateVersion(
            workflowName=workflowName,
            templateName=templateName,
            value="active-v1",
            activate=True,
        )
        v2 = self._createTemplateVersion(
            workflowName=workflowName,
            templateName=templateName,
            value="obsolete-v2",
            activate=False,
        )

        latestActive = params_api.getTemplate(
            workflowName=workflowName,
            templateName=templateName,
            dbName=self.dbName,
        )
        self.assertIsNotNone(latestActive)
        self.assertEqual(int(latestActive.get("templateVersion")), v1)
        self.assertEqual(str(latestActive.get("status")), "active")

        latestAny = params_api.getTemplate(
            workflowName=workflowName,
            templateName=templateName,
            dbName=self.dbName,
            activeOnly=False,
        )
        self.assertIsNotNone(latestAny)
        self.assertEqual(int(latestAny.get("templateVersion")), v2)
        self.assertEqual(str(latestAny.get("status")), "obsolete")

    @profiles("full")
    def test_get_template_specific_version_roundtrip(self):
        """Explicit version fetch should return the matching schema payload."""
        workflowName, templateName = self._newTemplateIdentity("version_roundtrip")
        v1 = self._createTemplateVersion(
            workflowName=workflowName,
            templateName=templateName,
            value="roundtrip-v1",
            activate=True,
        )
        v2 = self._createTemplateVersion(
            workflowName=workflowName,
            templateName=templateName,
            value="roundtrip-v2",
            activate=False,
        )

        tplV1 = params_api.getTemplate(
            workflowName=workflowName,
            templateName=templateName,
            templateVersion=v1,
            dbName=self.dbName,
        )
        tplV2 = params_api.getTemplate(
            workflowName=workflowName,
            templateName=templateName,
            templateVersion=v2,
            dbName=self.dbName,
        )
        self.assertIsNotNone(tplV1)
        self.assertIsNotNone(tplV2)
        self.assertEqual(int(tplV1.get("templateVersion")), v1)
        self.assertEqual(int(tplV2.get("templateVersion")), v2)
        self.assertEqual(
            (((tplV1.get("schema") or {}).get("formProps") or {}).get("data") or {}).get(
                "line"
            ),
            "roundtrip-v1",
        )
        self.assertEqual(
            (((tplV2.get("schema") or {}).get("formProps") or {}).get("data") or {}).get(
                "line"
            ),
            "roundtrip-v2",
        )

    @profiles("smoke", "full")
    def test_params_start_with_template_returns_uuid(self):
        """startWithTemplate should return a workflowId UUID string on success."""
        workflowName = "tests.fast_enqueue_target"
        templateName = self._newTemplateName("start_with_template_uuid")
        self._createTemplateVersion(
            workflowName=workflowName,
            templateName=templateName,
            value="from-template",
            activate=True,
            key="value",
        )

        ret = params_api.startWithTemplate(
            workflowName=workflowName,
            templateName=templateName,
            overridesData={"value": "from-override"},
            actor="tests.params",
            queueName=settings.QUEUE_DEFAULT,
            priority=3,
            dbName=self.dbName,
        )
        self.assertTrue(ret.get("ok"), "startWithTemplate failed: %r" % ret)

        workflowId = ret.get("workflowId")
        self.assertTrue(isinstance(workflowId, basestring), "workflowId must be a string")
        self.assertTrue(str(workflowId).strip(), "workflowId must be non-empty")
        UUID.fromString(str(workflowId))

        self.trackWorkflow(str(workflowId))
        st = self.tickUntilTerminal(str(workflowId), timeoutS=10.0)
        self.assertTrue(st is not None and st.get("status") == settings.STATUS_SUCCESS)
