from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, ValidationError
from typing import List, Dict, Any

class IConfigTable(BaseModel, ABC):
    """Abstract Base Model for all DB-backed configurations."""
    
    # We use ConfigDict to allow arbitrary types if needed
    model_config = {"extra": "forbid"} 

    @property
    @abstractmethod
    def table_name(self) -> str:
        pass

    @classmethod
    def from_db(cls, raw_rows: List[Dict[str, Any]]):
        """
        Factory method to validate raw DB rows against the Pydantic schema.
        """
        try:
            # Most ML configs are either a single row of params 
            # or a list of objects (like layers).
            return cls.parse_rows(raw_rows)
        except ValidationError as e:
            print(f"DQ Failure for table {cls.table_name}")
            raise e

    @classmethod
    @abstractmethod
    def parse_rows(cls, raw_rows: List[Dict[str, Any]]):
        """Each table defines how to map its specific SQL shape to the model."""
        pass



class HyperparamsConfig(IConfigTable):
    table_name: str = "model_hyperparams"
    
    # Define your expected schema here
    learning_rate: float = Field(gt=0, le=1.0)
    batch_size: int = Field(default=32, ge=1)
    optimizer: str = "adam"

    @classmethod
    def parse_rows(cls, raw_rows: List[Dict[str, Any]]):
        # Flattening a Key-Value table into a single dict for Pydantic
        data_map = {row['param_key']: row['param_value'] for row in raw_rows}
        return cls(**data_map)

class LayerSchema(BaseModel):
    """Sub-model for individual layers."""
    layer_name: str
    units: int = Field(gt=0)
    activation: str = "relu"

class TrainingLayersConfig(IConfigTable):
    table_name: str = "training_layers"
    
    # This model expects a list of layers
    layers: List[LayerSchema]

    @classmethod
    def parse_rows(cls, raw_rows: List[Dict[str, Any]]):
        return cls(layers=raw_rows)

class ConfLoader:
    def __init__(self, db_engine, execution_profile_id: str):
        self.db = db_engine
        self.profile_id = execution_profile_id

    def _read_raw(self, table_name: str) -> List[Dict[str, Any]]:
        query = f"SELECT * FROM {table_name} WHERE execution_profile_id = :pid"
        return self.db.execute(query, {"pid": self.profile_id}).mappings().all()

    def load(self, config_cls: type[IConfigTable]):
        """
        Takes a CLASS (not an instance), reads the DB, 
        and returns a validated instance.
        """
        raw_data = self._read_raw(config_cls.table_name)
        return config_cls.from_db(raw_data)


##Implemantation
loader = ConfLoader(db_engine, "exec_42")

# Returns a fully typed HyperparamsConfig object
try:
    m_params = loader.load(HyperparamsConfig)
    print(f"LR: {m_params.learning_rate}") 
except ValidationError as e:
    # Stop the pipeline before expensive GPU allocation!
    log.error("Pipeline aborted due to invalid config")

