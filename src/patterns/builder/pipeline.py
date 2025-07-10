import logging

import pandas as pd

from src.patterns.builder.stage import PipelineStage


class Pipeline:
    def __init__(self,
                 name: str,
                 stages: list[PipelineStage] | None = None,
                 data: pd.DataFrame | None = None,
                 ):
        self.name: str = name
        self.stages: list[PipelineStage] = stages if stages is not None else []
        self.data: pd.DataFrame | list[pd.DataFrame] | None = data.copy() if data is not None else None

    async def run(self) -> pd.DataFrame | list[pd.DataFrame] | None:
        logging.info(f"Pipeline: {repr(self)} started...")
        for stage in self.stages:
            self.data: pd.DataFrame | list[pd.DataFrame] = await stage.run(self.data)
        logging.info(f"Pipeline: {repr(self)} finished.")
        return self.data

    def add_stage(self, stage: PipelineStage) -> 'Pipeline':
        self.stages.append(stage)
        return self

    def build(self) -> 'Pipeline':
        self.stages = [stage.build() for stage in self.stages]
        return self

    def __repr__(self):
        return f"Pipeline(name={self.name}, stages={self.stages})"

    def __str__(self):
        return f"{self.name}"
