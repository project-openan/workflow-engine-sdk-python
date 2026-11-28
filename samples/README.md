# Scenario samples: the SPN cross-city complaint

The Python counterpart of the Java `samples` module's `SpringSpnDemo` and `SpringSpnDemoE2ETest`:
the same business scenario driven through this SDK, plus the tests that pin its behaviour down.

| File | What it is |
|---|---|
| `test_psop_reporting_scenario.py` | Retrieval → dispatch → reporting, as tests |
| `stub_center.py` | A standard-library stub center: login, search, PSOP load, event collector |
| `conftest.py` | Fixtures: the catalog, the stub center, an account session, isolated ports |
| `data/*.json` | The two SPN PSOPs the center serves in the scenario |
| `../examples/psop_reporting_demo.py` | The runnable demo these tests exercise |

## Run the scenario tests

```powershell
cd E:\OpenAN\workflow-engine-sdk-python
pip install -e ".[samples]"     # fastapi + uvicorn, for the tests that start agents
python -m pytest samples -v
```

No deployed service is needed: the stub center answers the four endpoints the SDK uses, and the
tests start two stub agents on loopback ports. The event collector acknowledges batches the way the
real center does, so a test can assert exactly what a run reported.

The suite is grouped by capability:

| Test | Capability |
|---|---|
| `test_search_ranks_candidates_and_names_their_declared_hosts` | Retrieval keeps the center's ranking and every candidate's declared host |
| `test_loaded_workflow_carries_what_reporting_needs` | A PSOP only reports when it has a published revision and a reporting identity |
| `test_selection_skips_other_hosts_and_prefers_a_declaration` | A host declaration beats a workflow that declares nothing |
| `test_scenario_run_dispatches_in_parallel_and_reports` | Both diagnostics run in parallel, the merge waits, every event reaches the center |
| `test_live_center_serves_the_scenario_workflow` | The same retrieval against a real deployment (skipped by default) |

## Run the live variant

The last test runs only when the environment names a center and an account that can read PSOPs:

```powershell
$env:OPENAN_TEST_DEMO_CENTER_URL = "https://127.0.0.1:5001"
$env:OPENAN_TEST_DEMO_USERNAME   = "<account>"
$env:OPENAN_TEST_DEMO_PASSWORD   = "<password>"
$env:OPENAN_TEST_DEMO_VERIFY_SERVER = "false"   # self-signed certificate
python -m pytest samples -v -k live
```

## Run the demo itself

`examples/psop_reporting_demo.py` starts one host agent and two mock diagnostic workers on
loopback HTTP. The caller submits a Task-T complaint to the host. The host uses the workflow
engine SDK to search and load a PSOP, dispatch Task-T requests to the workers, merge their
outputs, and report the execution to the center by REST.

```powershell
cd E:\OpenAN\workflow-engine-sdk-python
python -m examples.psop_reporting_demo
```

It reads `ORCH_URL`, `ORCH_USERNAME`, `ORCH_PASSWORD` and `ORCH_VERIFY_SERVER` from the environment
or from `./.env`, and exits 0 only when the center acknowledged the whole run.

The demo uses the A2A-T SDK's bundled private-line complaint template and renders it from structured
demo slots. Each receiver checks the Task-T header, message extension, template URI, and prompt
against the structured part. Worker diagnosis values are simulated. The demo's content generation
and validation are deterministic and run locally. The host returns the SDK's execution ID in its
Task-T result, and the printed ID opens the corresponding record in the center.
