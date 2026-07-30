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

"""Context assembly for the Workflow Execution SDK."""

from collections import deque
from typing import Dict, Any, List, Optional
from loguru import logger

from a2at_engine.core.models import Workflow, WorkflowStep


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
        ancestors = set()
        queue = deque([step_name])
        while queue:
            current = queue.popleft()
            for s in self.workflow.steps:
                if s.next:
                    for jc in s.next:
                        if jc.step == current and s.name != current and s.name not in ancestors:
                            ancestors.add(s.name)
                            queue.append(s.name)
                            break
        return list(ancestors)

    def build_context(self, step: WorkflowStep, step_outputs: Dict[str, Dict[str, Any]]) -> str:
        if step.layer <= 0:
            if self.runtime_intent:
                logger.info(f"[Context] Step {step.name}: layer 0, using runtime intent only")
                return f"## Runtime Context\n\n{self.runtime_intent}"
            logger.info(f"[Context] Step {step.name}: layer 0, no context")
            return ""
        parts = []
        if self.runtime_intent:
            parts.append(f"## Runtime Context\n\n{self.runtime_intent}")
        parts.append("## Previous Step Execution Results\n")
        if step.context_from and "*" in step.context_from:
            all_pred = self.get_all_predecessors(step.name)
            ref_pairs = [(n, step_outputs[n]) for n in all_pred if n in step_outputs]
            logger.info(f"[Context] Step {step.name}: using ALL predecessors ({len(ref_pairs)} available)")
        elif step.context_from:
            ref_pairs = [(n, step_outputs[n]) for n in step.context_from if n in step_outputs]
            logger.info(f"[Context] Step {step.name}: using context_from={step.context_from} ({len(ref_pairs)} available)")
        else:
            pred_names = self.get_step_predecessors(step.name)
            ref_pairs = [(n, step_outputs[n]) for n in pred_names if n in step_outputs]
            logger.info(f"[Context] Step {step.name}: using direct predecessors={pred_names} ({len(ref_pairs)} available)")
        for ref_step_name, ref_results in ref_pairs:
            parts.append(f"### {ref_step_name} Results\n")
            for task_desc, output in ref_results.items():
                text = output if isinstance(output, str) else str(output)
                parts.append(f"**Task**: {task_desc}\n**Output**: {text}\n\n")
        result = "\n".join(parts).strip()
        logger.info(f"[Context] Step {step.name}: built context ({len(result)} chars)")
        if result:
            logger.info(f"[Context] Content:\n{result[:2000]}")
        return result

    def build_task_message(self, task_description: str, context_message: str, lang: str = "zh") -> str:
        lang_hint = ""
        if lang == "en":
            lang_hint = "\n\nPlease respond in English."
        elif lang == "zh":
            lang_hint = "\n\n请用中文回复。"
        if context_message:
            return f"{context_message}\n\n## Current Task\n{task_description}{lang_hint}"
        return f"{task_description}{lang_hint}"

    def find_step_index(self, step_name: str) -> Optional[int]:
        return self._step_index.get(step_name)
