import json
from functools import reduce

class RequestNormalizer:
    """
    A class to normalize ML inference payloads based on a JSON configuration.

    This normalizer processes an input dictionary (payload) and transforms it
    into a feature dictionary suitable for ML models. It supports:
    - Field mapping: Direct mapping from source to target fields.
    - Coalesce: Selecting the first non-null value from a list of fields.
    - Nested Extraction: Retrieving values from nested objects using dot notation.
    - Passthrough: Handling fields not explicitly defined in the config.
    - Custom Functions: Applying simple transformations like rounding, trimming, etc.
    """

    def __init__(self, config: dict):
        """
        Initializes the normalizer with a configuration dictionary.

        Args:
            config (dict): The configuration dictionary, typically loaded from a JSON file.
        """
        self.config = config.get("normalizer_config", {})
        self.transformations = self.config.get("transformations", [])
        
        # A dispatch table for custom functions makes it easy to extend.
        self.available_functions = {
            "round": lambda val, p: round(val, p.get("decimals", 0)),
            "trim_whitespace": lambda val, p: str(val).strip(),
            "to_lowercase": lambda val, p: str(val).lower()
        }

    def _get_nested_value(self, data: dict, path: str, default=None):
        """
        Retrieves a value from a nested dictionary using dot notation.
        
        Example: _get_nested_value(payload, "customer.address.zipcode")
        """
        try:
            return reduce(lambda d, key: d.get(key) if isinstance(d, dict) else None, path.split('.'), data)
        except (TypeError, AttributeError):
            return default

    def _apply_functions(self, value, functions_config: list):
        """Applies a list of functions sequentially to a value."""
        if value is None or not functions_config:
            return value
        
        for func_info in functions_config:
            func_name = func_info["function"]
            if func_name in self.available_functions:
                params = func_info.get("params", {})
                value = self.available_functions[func_name](value, params)
        return value

    def _handle_field_mapping(self, payload: dict, mappings: list, processed_fields: set):
        """Processes the 'field_mapping' transformations."""
        features = {}
        for mapping in mappings:
            source_field = mapping["source_field"]
            target_name = mapping["target_feature_name"]
            
            value = payload.get(source_field)
            processed_fields.add(source_field)
            
            if value is None:
                if mapping.get("required", False):
                    raise ValueError(f"Required field '{source_field}' is missing from payload.")
                value = mapping.get("default_value")
            
            value = self._apply_functions(value, mapping.get("apply_functions"))

            # Optional: Enforce data type
            if "data_type" in mapping and value is not None:
                try:
                    dtype = __builtins__.get(mapping["data_type"])
                    if dtype:
                        value = dtype(value)
                except (ValueError, TypeError):
                    raise TypeError(f"Could not cast value for '{target_name}' to {mapping['data_type']}.")

            features[target_name] = value
        return features

    def _handle_coalesce(self, payload: dict, mappings: list, processed_fields: set):
        """Processes the 'coalesce' transformations."""
        features = {}
        for mapping in mappings:
            target_name = mapping["target_feature_name"]
            value = None
            
            for source_field in mapping["source_fields"]:
                # Use _get_nested_value to support both flat and nested fields
                found_value = self._get_nested_value(payload, source_field)
                processed_fields.add(source_field.split('.')[0]) # Add top-level key to processed
                if found_value is not None:
                    value = found_value
                    break
            
            if value is None:
                value = mapping.get("default_value")
                
            value = self._apply_functions(value, mapping.get("apply_functions"))
            features[target_name] = value
        return features

    def _handle_nested_extraction(self, payload: dict, extractions: list, processed_fields: set):
        """Processes 'nested_field_extraction' transformations."""
        features = {}
        for extraction in extractions:
            source_path = extraction["source_path"]
            target_name = extraction["target_feature_name"]
            
            value = self._get_nested_value(payload, source_path)
            processed_fields.add(source_path.split('.')[0])
            
            if value is None:
                if extraction.get("required", False):
                    raise ValueError(f"Required nested field '{source_path}' is missing.")
                value = extraction.get("default_value")
            
            features[target_name] = value # Custom functions can be added here if needed
        return features
        
    def _handle_passthrough(self, payload: dict, processed_fields: set, config: dict):
        """Handles fields not explicitly mapped."""
        features = {}
        if config.get("passthrough_strategy") != "include_all_unmapped":
            return features
            
        prefix = config.get("prefix_unmapped_features_with", "")
        exclude_fields = set(config.get("exclude_unmapped_fields", []))
        
        for key, value in payload.items():
            if key not in processed_fields and key not in exclude_fields:
                features[f"{prefix}{key}"] = value
        return features

    def transform(self, payload: dict) -> dict:
        """
        Transforms an input payload into a feature dictionary.

        Args:
            payload (dict): The raw input dictionary (inference payload).

        Returns:
            dict: The normalized feature dictionary.
