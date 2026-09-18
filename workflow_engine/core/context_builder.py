# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Select structured upstream results according to workflow dependencies."""

from collections import deque
from typing import Dict, List, Optional
from loguru import logger

from workflow_engine.core.models import (
    TaskExecutionResult, UpstreamStepResult, Workflow, WorkflowInput, WorkflowStep,
)


class ContextBuilder:
    def __init__(self, workflow: Workflow, runtime_intent: str = ""):
        self.workflow = workflow
        self.runtime_intent = runtime_intent
        self._step_index = {s.name: i for i, s in enumerate(workflow.steps)}

    def get_step_predecessors(self, step_name: str) -> List[str]:
        predecessors = []
        for s in self.workflow.steps:
            if s.next:
                for jc in s.next:
                    if jc.step == step_name and s.name != step_name:
                        predecessors.append(s.name)
                        break
        return predecessors

    def get_all_predecessors(self, step_name: str) -> List[str]:
        ancestors = []
        seen = set()
        queue = deque([step_name])
        while queue:
            current = queue.popleft()
            for s in self.workflow.steps:
                if s.next:
                    for jc in s.next:
                        if jc.step == current and s.name != current and s.name not in seen:
                            seen.add(s.name)
                            ancestors.append(s.name)
                            queue.append(s.name)
                            break
        return ancestors

    def build_workflow_input(
        self,
        step: WorkflowStep,
        step_results: Dict[str, list[TaskExecutionResult]],
    ) -> WorkflowInput:
        if step.context_from and "*" in step.context_from:
            selected = [name for name in self.get_all_predecessors(step.name) if name in step_results]
        elif step.context_from:
            selected = [name for name in step.context_from if name in step_results]
        elif step.context_from is None:
            selected = [name for name in self.get_step_predecessors(step.name) if name in step_results]
        else:
            selected = []
        upstream = tuple(
            UpstreamStepResult(name, tuple(step_results[name])) for name in selected
        )
        result_count = sum(len(item.task_results) for item in upstream)
        logger.info(
            f"[Context] Step {step.name}: selected {len(upstream)} upstream step(s), "
            f"{result_count} task result(s)"
        )
        return WorkflowInput(self.runtime_intent, upstream)

    def find_step_index(self, step_name: str) -> Optional[int]:
        return self._step_index.get(step_name)
