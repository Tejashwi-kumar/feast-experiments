import re
import json
from typing import Any, Dict, List, Optional, Tuple

class NormalizerValidator:
    def __init__(self, config: Dict):
        self.config = config.get("normalizer_config", {})
        self.transformations = self.config.get("transformations", [])
        self.errors = []

    def _apply_function(self, value: Any, func_cfg: Dict) -> Any:
        """Handles user-defined small transformations."""
        func_name = func_cfg.get("function")
        params = func_cfg.get("params", {})
        
        try:
            if func_name == "round":
                return round(float(value), params.get("decimals", 0))
            elif func_name == "to_lowercase":
                return str(value).lower()
            elif func_name == "trim_whitespace":
                return str(value).strip()
            elif func_name == "abs":
                return abs(float(value))
            return value
        except Exception as e:
            self.errors.append(f"Function {func_name} failed on value {value}: {str(e)}")
            return value

    def _validate_type(self, value: Any, expected_type: str, field_name: str):
        """Checks if the value matches the expected ML feature type."""
        types = {"float": float, "int": int, "string": str, "bool": bool}
        if expected_type in types:
            if not isinstance(value, types[expected_type]):
                self.errors.append(f"TYPE_MISMATCH: Field '{field_name}' expected {expected_type}, got {type(value).__name__}")

    def process(self, payload: Dict) -> Tuple[Dict, List[str]]:
        self.errors = []
        normalized_output = {}
        mapped_keys = set()

        for trans in self.transformations:
            trans_type = trans.get("type")

            # 1. Handle Field Mapping (Exact & Regex)
            if trans_type == "field_mapping":
                for mapping in trans.get("mappings", []):
                    m_type = mapping.get("match_type", "exact")
                    pattern = mapping.get("source_pattern")
                    target = mapping.get("target_pattern")

                    if m_type == "exact":
                        if pattern in payload:
                            val = payload[pattern]
                            self._validate_type(val, mapping.get("data_type"), pattern)
                            for f in mapping.get("apply_functions", []):
                                val = self._apply_function(val, f)
                            normalized_output[target] = val
                            mapped_keys.add(pattern)
                        elif mapping.get("required"):
                            self.errors.append(f"MISSING_REQUIRED_FIELD: {pattern}")

                    elif m_type == "regex":
                        regex = re.compile(pattern)
                        matched_any = False
                        for k, v in payload.items():
                            match = regex.match(k)
                            if match:
                                matched_any = True
                                # Handle group substitution ($1 -> group 1)
                                target_name = target.replace("$1", match.group(1)) if match.groups() else target
                                val = v
                                self._validate_type(val, mapping.get("data_type"), k)
                                for f in mapping.get("apply_functions", []):
                                    val = self._apply_function(val, f)
                                normalized_output[target_name] = val
                                mapped_keys.add(k)
                        if not matched_any and mapping.get("required"):
                            self.errors.append(f"MISSING_REGEX_MATCH: No keys matched {pattern}")

            # 2. Handle Coalesce
            elif trans_type == "coalesce":
                for mapping in trans.get("mappings", []):
                    target = mapping.get("target_feature_name")
                    found_val = None
                    for source in mapping.get("source_fields", []):
                        if source in payload: # Simplified for flat payload
                            found_val = payload[source]
                            mapped_keys.add(source)
                            break
                    
                    final_val = found_val if found_val is not None else mapping.get("default_value")
                    for f in mapping.get("apply_functions", []):
                        final_val = self._apply_function(final_val, f)
                    normalized_output[target] = final_val

            # 3. Handle Passthrough
            elif trans_type == "default_passthrough":
                if trans.get("passthrough_strategy") == "include_all_unmapped":
                    prefix = trans.get("prefix_unmapped_features_with", "")
                    exclude = trans.get("exclude_unmapped_fields", [])
                    for k, v in payload.items():
                        if k not in mapped_keys and k not in exclude:
                            normalized_output[f"{prefix}{k}"] = v

        return normalized_output, self.errors

# --- Example Usage ---
config_json = {
    "normalizer_config": {
        "transformations": [
            {
                "type": "field_mapping",
                "mappings": [
                    {
                        "match_type": "exact",
                        "source_pattern": "transaction_amount",
                        "target_pattern": "f_amount",
                        "required": True,
                        "data_type": "float",
                        "apply_functions": [{"function": "round", "params": {"decimals": 2}}]
                    },
                    {
                        "match_type": "regex",
                        "source_pattern": "^sensor_([a-z]+)_value$",
                        "target_pattern": "f_sensor_$1",
                        "data_type": "float"
                    }
                ]
            },
            {
                "type": "default_passthrough",
                "passthrough_strategy": "include_all_unmapped",
                "prefix_unmapped_features_with": "raw_",
                "exclude_unmapped_fields": ["internal_id"]
            }
        ]
    }
}

sample_payload = {
    "transaction_amount": 99.9876,
    "sensor_temp_value": 24.5,
    "sensor_vibration_value": 0.002,
    "internal_id": "secret_123",
    "user_tag": "premium"
}

nv = NormalizerValidator(config_json)
features, validation_errors = nv.process(sample_payload)

print("--- Normalized Features ---")
print(json.dumps(features, indent=2))
print("\n--- Validation Errors ---")
print(validation_errors if validation_errors else "No errors found. System green.")
