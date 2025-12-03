import os
import sys
import types
from typing import List, Dict, Optional, Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd

# Feast imports
from feast import (
    FeatureStore,
    OnDemandFeatureView,
    FeatureView,
    Field,
    RequestSource,
)
from feast.types import Float64, String, Int64

# Initialize FastAPI
app = FastAPI(title="Feast Config-Driven Registrar")

# --- Configuration ---
REPO_PATH = "./" 

# Initialize the Feature Store
try:
    store = FeatureStore(repo_path=REPO_PATH)
except Exception as e:
    print(f"Warning: Could not connect to feature store at {REPO_PATH}. Error: {e}")
    store = None

# --- New Config Models ---

class TransformOperation(BaseModel):
    """
    Defines a single operation to generate code for.
    Example: output_col = input_col + value
    """
    type: str  # e.g., "add", "multiply", "concat"
    input_col: str
    value: Union[float, int, str]
    output_col: str

class FeatureDefinition(BaseModel):
    name: str
    dtype: str

class RequestSourceDef(BaseModel):
    name: str
    schema_fields: Dict[str, str]

class OnDemandViewRegistration(BaseModel):
    view_name: str
    input_request_sources: Optional[List[RequestSourceDef]] = []
    input_feature_views: Optional[List[str]] = []
    features: List[FeatureDefinition]
    
    # REPLACED: transformation_code -> transformation_config
    transformation_config: List[TransformOperation]

# --- Helper Functions ---

def get_dtype(dtype_str: str):
    mapping = {"Float64": Float64, "Int64": Int64, "String": String}
    return mapping.get(dtype_str, String)

def generate_transformation_code(operations: List[TransformOperation]) -> str:
    """
    Dynamically builds the Python source code string based on the config.
    """
    code_lines = [
        "import pandas as pd",
        "",
        "def transformation_fn(inputs_df: pd.DataFrame) -> pd.DataFrame:",
        "    df = pd.DataFrame()",
        "    # Auto-generated logic based on config"
    ]
    
    for op in operations:
        # Sanitize inputs in a real app to prevent injection!
        if op.type == "add":
            line = f"    df['{op.output_col}'] = inputs_df['{op.input_col}'] + {op.value}"
            code_lines.append(line)
        elif op.type == "multiply":
            line = f"    df['{op.output_col}'] = inputs_df['{op.input_col}'] * {op.value}"
            code_lines.append(line)
        elif op.type == "copy":
             line = f"    df['{op.output_col}'] = inputs_df['{op.input_col}']"
             code_lines.append(line)
        else:
            # You can expand this to support complex logic or lookups
            pass

    code_lines.append("    return df")
    
    full_code = "\n".join(code_lines)
    
    # Debug print to see what was generated
    print("--- Generated Code ---")
    print(full_code)
    print("----------------------")
    
    return full_code

def compile_function(code_str: str):
    local_scope = {}
    try:
        exec(code_str, globals(), local_scope)
        if "transformation_fn" not in local_scope:
            raise ValueError("Generated code missing 'transformation_fn'")
        return local_scope["transformation_fn"]
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Code generation failed: {str(e)}")

# --- Endpoints ---

@app.get("/")
def health_check():
    return {"status": "active"}

@app.post("/register/on-demand")
def register_on_demand_view(payload: OnDemandViewRegistration):
    if not store:
        raise HTTPException(status_code=500, detail="Feature Store not initialized")

    # 1. Prepare Input Sources
    sources = {}
    for req_src in payload.input_request_sources:
        schema = [Field(name=k, dtype=get_dtype(v)) for k, v in req_src.schema_fields.items()]
        sources[req_src.name] = RequestSource(name=req_src.name, schema=schema)

    for fv_name in payload.input_feature_views:
        try:
            sources[fv_name] = store.get_feature_view(fv_name)
        except Exception:
            raise HTTPException(status_code=404, detail=f"Feature View '{fv_name}' not found")

    # 2. Generate and Compile Code from Config
    code_str = generate_transformation_code(payload.transformation_config)
    transform_fn = compile_function(code_str)

    # 3. Define Output Schema
    schema = [Field(name=f.name, dtype=get_dtype(f.dtype)) for f in payload.features]

    # 4. Create OnDemandFeatureView
    odfv = OnDemandFeatureView(
        name=payload.view_name,
        sources=list(sources.values()),
        schema=schema,
        udf=transform_fn,
    )

    # 5. Apply
    store.apply([odfv])

    return {
        "message": f"Successfully registered {payload.view_name}",
        "generated_logic": code_str
    }

@app.get("/views/{name}")
def get_view_details(name: str):
    if not store:
        raise HTTPException(status_code=500, detail="Feature Store not initialized")
    try:
        view = store.get_on_demand_feature_view(name)
        return {"name": view.name, "features": [f.name for f in view.schema]}
    except Exception:
        raise HTTPException(status_code=404, detail="View not found")

if __name__ == "__main__":
    import uvicorn
    # Mock feature_store.yaml creation for demo
    if not os.path.exists("feature_store.yaml"):
        with open("feature_store.yaml", "w") as f:
            f.write("project: my_feature_repo\nregistry: data/registry.db\nprovider: local_online_store\nonline_store:\n    path: data/online_store.db")
        os.makedirs("data", exist_ok=True)
        
    uvicorn.run(app, host="0.0.0.0", port=8000)
