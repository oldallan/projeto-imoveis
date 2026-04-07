__all__ = ["PipelineRunner"]


def __getattr__(name: str):
    if name == "PipelineRunner":
        from workflow.runner import PipelineRunner

        return PipelineRunner
    raise AttributeError(name)
