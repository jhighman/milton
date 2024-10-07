import signal
import sys

def register_signal_handlers(save_checkpoint_callback):
    """
    Register signal handlers to save checkpoint on interruption.

    Args:
        save_checkpoint_callback (callable): Function to call to save checkpoint.
    """
    def handler(sig, frame):
        print(f"Signal {sig} received, saving checkpoint...")
        save_checkpoint_callback()
        sys.exit(0)
    
    # Register handler for SIGINT (Ctrl+C)
    signal.signal(signal.SIGINT, handler)
    
    # Register handler for SIGTERM (termination signal)
    signal.signal(signal.SIGTERM, handler)
