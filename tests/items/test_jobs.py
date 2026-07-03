"""Tests for the Jobs API helpers (run_on_demand / run_notebook / schedules)."""

from __future__ import annotations

from typing import ClassVar
from unittest.mock import MagicMock

import pytest

from pyfabric.items.jobs import (
    _param_type,
    _schedule_job_type,
    create_schedule,
    delete_schedule,
    get_schedule,
    list_job_instances,
    list_schedules,
    run_notebook,
    run_on_demand,
    update_schedule,
)


def _client(responses: list) -> MagicMock:
    """A mock FabricClient whose ``raw_request`` yields ``responses`` in order."""
    c = MagicMock()
    c._build_url = lambda path, params=None: f"https://api/{path}?{params}"
    c.raw_request.side_effect = responses
    return c


class TestRunOnDemand:
    def test_waits_for_completion_and_reports_progress(self, mock_http_response):
        mk = mock_http_response
        client = _client(
            [
                mk(
                    202,
                    headers={"Location": "https://api/inst/JID", "Retry-After": "0"},
                ),
                mk(200, {"status": "InProgress"}),
                mk(200, {"status": "Completed", "id": "JID"}),
            ]
        )
        seen: list[str] = []
        result = run_on_demand(
            client,
            "WS",
            "ITEM",
            "RunNotebook",
            poll_interval=0,
            on_progress=seen.append,
        )
        assert result["status"] == "Completed"
        assert seen == ["InProgress", "Completed"]

    def test_sync_200_returns_immediately(self, mock_http_response):
        client = _client([mock_http_response(200, {"status": "Completed"})])
        assert (
            run_on_demand(client, "WS", "ITEM", "RunNotebook")["status"] == "Completed"
        )

    def test_failed_raises_with_failure_reason(self, mock_http_response):
        mk = mock_http_response
        client = _client(
            [
                mk(202, headers={"Location": "L", "Retry-After": "0"}),
                mk(200, {"status": "Failed", "failureReason": {"message": "boom"}}),
            ]
        )
        with pytest.raises(RuntimeError, match="boom"):
            run_on_demand(client, "WS", "ITEM", "RunNotebook", poll_interval=0)

    def test_no_wait_returns_location(self, mock_http_response):
        client = _client([mock_http_response(202, headers={"Location": "LOC"})])
        out = run_on_demand(client, "WS", "ITEM", "RunNotebook", wait=False)
        assert out == {"status": "Accepted", "location": "LOC"}

    def test_202_without_location_raises(self, mock_http_response):
        client = _client([mock_http_response(202, headers={})])
        with pytest.raises(RuntimeError, match="no Location"):
            run_on_demand(client, "WS", "ITEM", "RunNotebook")


class TestRunNotebook:
    def test_injects_typed_parameters(self, mock_http_response):
        client = _client([mock_http_response(200, {"status": "Completed"})])
        run_notebook(
            client, "WS", "NB", parameters={"folder": "x", "n": 3, "flag": True}
        )
        method, _url, body = client.raw_request.call_args[0]
        assert method == "POST"
        params = body["executionData"]["parameters"]
        assert params["folder"] == {"value": "x", "type": "string"}
        assert params["n"] == {"value": 3, "type": "int"}
        assert params["flag"] == {"value": True, "type": "bool"}

    def test_no_parameters_sends_no_body(self, mock_http_response):
        client = _client([mock_http_response(200, {"status": "Completed"})])
        run_notebook(client, "WS", "NB")
        assert client.raw_request.call_args[0][2] is None

    def test_uses_run_notebook_job_type(self, mock_http_response):
        client = _client([mock_http_response(200, {"status": "Completed"})])
        run_notebook(client, "WS", "NB")
        url = client.raw_request.call_args[0][1]
        assert "RunNotebook" in url and "items/NB/jobs/instances" in url


def test_param_type():
    assert _param_type(True) == "bool"
    assert _param_type(3) == "int"
    assert _param_type(3.5) == "float"
    assert _param_type("x") == "string"


# ── Schedules ────────────────────────────────────────────────────────────────


_DAILY_CONFIG = {
    "type": "Daily",
    "times": ["07:00"],
    "localTimeZoneId": "Central Standard Time",
    "startDateTime": "2026-07-01T00:00:00",
    "endDateTime": "2036-07-01T00:00:00",
}


class TestScheduleJobTypeAliases:
    def test_mlv_instances_spelling_maps_to_schedules_spelling(self):
        # Job instances report "MaterializedLakeViews", but the schedules
        # endpoint 400s (InvalidJobType) on it — only the "Refresh"-prefixed
        # form works. The alias map absorbs the asymmetry.
        assert (
            _schedule_job_type("MaterializedLakeViews")
            == "RefreshMaterializedLakeViews"
        )

    def test_schedules_spellings_pass_through(self):
        assert (
            _schedule_job_type("RefreshMaterializedLakeViews")
            == "RefreshMaterializedLakeViews"
        )
        # Verified against the live API 2026-07-03: schedules accept
        # RunNotebook for notebooks (bare "Notebook" 400s) and Execute
        # for pipelines.
        assert _schedule_job_type("RunNotebook") == "RunNotebook"
        assert _schedule_job_type("Execute") == "Execute"


class TestListSchedules:
    def test_normalizes_job_type_in_path(self):
        c = MagicMock()
        c.get_paged.return_value = []
        list_schedules(c, "WS", "ITEM", "MaterializedLakeViews")
        c.get_paged.assert_called_once_with(
            "workspaces/WS/items/ITEM/jobs/RefreshMaterializedLakeViews/schedules"
        )

    def test_returns_schedules(self):
        c = MagicMock()
        c.get_paged.return_value = [{"id": "s1", "enabled": True}]
        out = list_schedules(c, "WS", "ITEM", "RunNotebook")
        assert out == [{"id": "s1", "enabled": True}]


class TestGetSchedule:
    def test_path_includes_schedule_id(self):
        c = MagicMock()
        get_schedule(c, "WS", "ITEM", "MaterializedLakeViews", "SCHED")
        c.get.assert_called_once_with(
            "workspaces/WS/items/ITEM/jobs/RefreshMaterializedLakeViews/schedules/SCHED"
        )


class TestCreateSchedule:
    def test_body_shape(self):
        c = MagicMock()
        create_schedule(
            c,
            "WS",
            "ITEM",
            "RefreshMaterializedLakeViews",
            configuration=_DAILY_CONFIG,
        )
        path, body = c.post.call_args[0]
        assert path == (
            "workspaces/WS/items/ITEM/jobs/RefreshMaterializedLakeViews/schedules"
        )
        assert body == {"enabled": True, "configuration": _DAILY_CONFIG}

    def test_enabled_false(self):
        c = MagicMock()
        create_schedule(
            c, "WS", "ITEM", "Execute", configuration=_DAILY_CONFIG, enabled=False
        )
        assert c.post.call_args[0][1]["enabled"] is False


class TestUpdateSchedule:
    def test_patches_full_replacement(self):
        c = MagicMock()
        update_schedule(
            c,
            "WS",
            "ITEM",
            "MaterializedLakeViews",
            "SCHED",
            configuration=_DAILY_CONFIG,
            enabled=False,
        )
        path, body = c.patch.call_args[0]
        assert path == (
            "workspaces/WS/items/ITEM/jobs/RefreshMaterializedLakeViews/schedules/SCHED"
        )
        assert body == {"enabled": False, "configuration": _DAILY_CONFIG}


class TestDeleteSchedule:
    def test_deletes_by_id(self):
        c = MagicMock()
        delete_schedule(c, "WS", "ITEM", "RunNotebook", "SCHED")
        c.delete.assert_called_once_with(
            "workspaces/WS/items/ITEM/jobs/RunNotebook/schedules/SCHED"
        )


class TestListJobInstances:
    _INSTANCES: ClassVar[list[dict]] = [
        {"id": "1", "jobType": "MaterializedLakeViews", "status": "Failed"},
        {"id": "2", "jobType": "RunNotebook", "status": "Completed"},
    ]

    def _client(self):
        c = MagicMock()
        c.get_paged.return_value = list(self._INSTANCES)
        return c

    def test_unfiltered_returns_all(self):
        c = self._client()
        out = list_job_instances(c, "WS", "ITEM")
        c.get_paged.assert_called_once_with("workspaces/WS/items/ITEM/jobs/instances")
        assert len(out) == 2

    def test_filter_matches_instances_spelling(self):
        c = self._client()
        out = list_job_instances(c, "WS", "ITEM", job_type="MaterializedLakeViews")
        assert [i["id"] for i in out] == ["1"]

    def test_filter_matches_schedules_spelling(self):
        # The same runs must be found with EITHER spelling — this is the
        # asymmetry that hid a failing schedule.
        c = self._client()
        out = list_job_instances(
            c, "WS", "ITEM", job_type="RefreshMaterializedLakeViews"
        )
        assert [i["id"] for i in out] == ["1"]
