# Arc Batch Inference Pipeline

Standalone Dagster pipeline for batch inference and knowledge graph dataset maintenance.
It ingests legislative data from LegiScan, could eventually run Arc's custom classification models, and update the dataset used by our knowledge graph.

## Local setup

```bash
cd inference_pipeline
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

