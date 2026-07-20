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

"""Data models for the Workflow Execution SDK."""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Dict, Any


class StepType(Enum):
    ALL_SUCCESS = "AllSuccess"
    ANY_SUCCESS = "AnySuccess"


class TaskStatus(Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class JumpCondition:
    step: str
    condition: str = ""


@dataclass
class Task:
    agent: str
    skill: str = ""
    description: str = ""
    status: TaskStatus = TaskStatus.PENDING


@dataclass
class WorkflowStep:
    name: str
    subtasks: List[Task] = field(default_factory=list)
    next: List[JumpCondition] = field(default_factory=list)
    layer: int = 0
    context_from: Optional[List[str]] = None
    step_type: StepType = StepType.ALL_SUCCESS


@dataclass
class Workflow:
    id: str = ""
    name: str = ""
    description: str = ""
    steps: List[WorkflowStep] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Workflow":
        steps = []
        for s in data.get("steps", []):
            subtasks = [Task(agent=t.get("agent",""), skill=t.get("skill",""), description=t.get("description","")) for t in (s.get("subtasks") or [])]
            next_list = [JumpCondition(step=jc.get("step",""), condition=jc.get("condition","")) for jc in (s.get("next") or [])]
            st = s.get("step_type", s.get("type", "AllSuccess"))
            try:
                step_type = StepType(st)
            except ValueError:
                try:
                    step_type = StepType(st.lower())
                except ValueError:
                    step_type = StepType.ALL_SUCCESS
            cf = s.get("context_from")
            if cf and not isinstance(cf, list): cf = [cf]
            steps.append(WorkflowStep(name=s.get("name",""), subtasks=subtasks, next=next_list, layer=s.get("layer",0), context_from=cf, step_type=step_type))
        return cls(id=data.get("id",""), name=data.get("name",""), description=data.get("description",""), steps=steps)

    @classmethod
    def from_json(cls, json_str: str) -> "Workflow":
        import json
        return cls.from_dict(json.loads(json_str))


@dataclass
class SendMessageResult:
    text: str = ""
    task: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    task_state: str = ""


@dataclass
class TaskRequest:
    agent_name: str
    skill: str
    message: str
    context: str
    step_name: str
    subtask_index: int = 0
    description: str = ""


@dataclass
class TaskResponse:
    success: bool
    output: str = ""
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class RouteDecision:
    next_step: str
    reason: str = ""


@dataclass
class ExecutionResult:
    success: bool
    history: List[Dict[str, Any]] = field(default_factory=list)
    step_outputs: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    error: Optional[str] = None
