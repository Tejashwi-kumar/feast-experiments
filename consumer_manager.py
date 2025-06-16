# app/services.py
import multiprocessing
from typing import List, Dict, Any
from feast import FeatureStore
from app.consumer import KafkaConsumerProcess

class ConsumerServiceManager:
    """
    Manages the lifecycle of multiple KafkaConsumerProcess instances.
    This class is designed to be a singleton within the FastAPI app state.
    """
    def __init__(self, feature_repo_path: str):
        self.store = FeatureStore(repo_path=feature_repo_path)
        self.processes: List[KafkaConsumerProcess] = []
        self._initialize_processes()

    def _initialize_processes(self):
        """
        Initializes a consumer process for each StreamFeatureView found in the repo.
        """
        print("--- Discovering Stream Feature Views ---")
        stream_fvs = self.store.list_stream_feature_views()
        if not stream_fvs:
            print("WARNING: No StreamFeatureViews found in the repository.")
            return

        for fv in stream_fvs:
            feature_id = fv.tags.get("feature_id")
            if not feature_id:
                print(f"WARNING: Skipping FeatureView '{fv.name}' because it lacks a 'feature_id' tag.")
                continue

            process = KafkaConsumerProcess(
                feature_store_path=self.store.repo_path,
                feature_view_name=fv.name,
                feature_id_to_consume=feature_id
            )
            self.processes.append(process)
        print(f"--- Initialized {len(self.processes)} consumer processes. ---")


    def start_all_consumers(self):
        """
        Starts all initialized consumer processes if they are not already alive.
        """
        if not self.processes:
            raise RuntimeError("No consumer processes initialized. Cannot start ingestion.")

        print("--- Starting all consumer processes ---")
        for p in self.processes:
            if not p.is_alive():
                p.start()
                print(f"Started process for Feature ID: {p.feature_id_to_consume}")

    def stop_all_consumers(self):
        """
        Stops all running consumer processes.
        """
        print("--- Stopping all consumer processes ---")
        for p in self.processes:
            if p.is_alive():
                p.terminate() # Sends SIGTERM
                p.join(timeout=5) # Waits for the process to exit
                if p.is_alive():
                    p.kill() # Force kill if it doesn't terminate gracefully
                print(f"Stopped process for Feature ID: {p.feature_id_to_consume}")
        # Clear the list after stopping
        self.processes = []


    def get_all_statuses(self) -> List[Dict[str, Any]]:
        """
        Gets the status of each managed consumer process.
        """
        if not self.processes:
            return [{"message": "No consumers are configured or running."}]

        statuses = []
        for p in self.processes:
            statuses.append({
                "feature_view_name": p.feature_view_name,
                "feature_id": p.feature_id_to_consume,
                "process_id": p.pid,
                "is_alive": p.is_alive()
            })
        return statuses
