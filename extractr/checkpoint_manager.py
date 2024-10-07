import json
import os

class CheckpointManager:
    def __init__(self, checkpoint_file: str, logger):
        """
        Initialize the CheckpointManager.

        Args:
            checkpoint_file (str): Path to the checkpoint file.
            logger: Logger instance to log messages.
        """
        self.checkpoint_file = checkpoint_file
        self.logger = logger

    def save_checkpoint(self, data: dict):
        """
        Save checkpoint data to a file.

        Args:
            data (dict): The checkpoint data to be saved.
        """
        try:
            temp_checkpoint_file = self.checkpoint_file + '.tmp'
            with open(temp_checkpoint_file, 'w') as f:
                json.dump(data, f)
            os.replace(temp_checkpoint_file, self.checkpoint_file)
            self.logger.debug("Checkpoint saved successfully.")
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")

    def load_checkpoint(self) -> dict:
        """
        Load checkpoint data from a file.

        Returns:
            dict: The loaded checkpoint data, or an empty dictionary if no checkpoint exists.
        """
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                self.logger.debug("Checkpoint loaded successfully.")
                return data
            except Exception as e:
                self.logger.error(f"Failed to load checkpoint: {e}")
                return {}
        else:
            self.logger.debug("No checkpoint file found.")
            return {}

    def remove_checkpoint(self):
        """
        Remove the checkpoint file if it exists.
        """
        if os.path.exists(self.checkpoint_file):
            try:
                os.remove(self.checkpoint_file)
                self.logger.debug("Checkpoint removed successfully.")
            except Exception as e:
                self.logger.error(f"Failed to remove checkpoint: {e}")
