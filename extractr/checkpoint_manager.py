# checkpoint_manager.py

import json
import os

class CheckpointManager:
    def __init__(self, checkpoint_file: str, logger):
        self.checkpoint_file = checkpoint_file
        self.logger = logger

    def save_checkpoint(self, data: dict):
        temp_checkpoint_file = self.checkpoint_file + '.tmp'
        with open(temp_checkpoint_file, 'w') as f:
            json.dump(data, f)
        os.replace(temp_checkpoint_file, self.checkpoint_file)
        self.logger.debug("Checkpoint saved.")

    def load_checkpoint(self) -> dict:
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)
            self.logger.debug("Checkpoint loaded.")
            return data
        else:
            return {}
    
    def remove_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)
            self.logger.debug("Checkpoint removed.")
