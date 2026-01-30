"""Log capture utilities for GMX Historical Data CLI.

This module provides context managers for capturing all CLI output to log files,
including Rich console output, standard print() statements, and stderr messages.
"""

import re
import sys
from pathlib import Path
from typing import Optional, TextIO


class TeeWriter:
    """Write to multiple output streams simultaneously.

    Writes to a primary stream (console) and a secondary stream (file).
    Automatically strips ANSI escape codes from file output for clean text logs.

    :param primary: Primary output stream (typically console with colors)
    :param secondary: Secondary output stream (typically log file)
    """

    # Regex pattern for matching ANSI escape codes
    _ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

    def __init__(self, primary: TextIO, secondary: TextIO):
        self.primary = primary
        self.secondary = secondary

    def write(self, text: str) -> None:
        """Write text to both streams.

        :param text: Text to write
        """
        # Write original text (with colors) to primary stream
        self.primary.write(text)

        # Strip ANSI codes and write clean text to secondary stream
        clean_text = self._strip_ansi(text)
        self.secondary.write(clean_text)

    def flush(self) -> None:
        """Flush both output streams."""
        self.primary.flush()
        self.secondary.flush()

    @classmethod
    def _strip_ansi(cls, text: str) -> str:
        """Remove ANSI escape codes from text.

        :param text: Text containing ANSI codes
        :return: Clean text without ANSI codes
        """
        return cls._ANSI_ESCAPE.sub('', text)


class LogCapture:
    """Context manager for capturing all CLI output to a log file.

    Captures three types of output:
    1. Rich console.print() output (strips ANSI codes for file)
    2. Standard print() statements
    3. stderr error messages

    Supports two modes:
    - Dual output (default): Output to both console and file
    - Quiet mode: Output to file only, suppress console

    :param log_path: Path to log file
    :param quiet: If True, suppress console output (file-only mode)

    Example usage::

        with LogCapture(Path("./logs/output.log"), quiet=False):
            print("This goes to both console and file")
            console.print("[green]Colorful console output[/green]")

    Example quiet mode::

        with LogCapture(Path("./logs/output.log"), quiet=True):
            print("This only goes to the log file")
    """

    def __init__(self, log_path: Path, quiet: bool = False):
        self.log_path = log_path
        self.quiet = quiet
        self.log_file: Optional[TextIO] = None
        self.original_stdout: Optional[TextIO] = None
        self.original_stderr: Optional[TextIO] = None
        self.original_console_file: Optional[TextIO] = None

    def __enter__(self) -> "LogCapture":
        """Enter the log capture context.

        Sets up output redirection to log file.

        :return: Self
        :raises OSError: If log directory or file cannot be created
        """
        # Create logs directory if it doesn't exist
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            print(f"Error: Cannot create log directory: {e}", file=sys.stderr)
            raise

        # Open log file for writing
        try:
            self.log_file = open(self.log_path, 'w', encoding='utf-8')
        except OSError as e:
            print(f"Error: Cannot create log file: {e}", file=sys.stderr)
            raise

        # Save original streams
        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr

        # Configure output redirection based on mode
        if self.quiet:
            # Quiet mode: file-only output
            sys.stdout = self.log_file
            sys.stderr = self.log_file
        else:
            # Dual output mode: both console and file
            sys.stdout = TeeWriter(self.original_stdout, self.log_file)
            sys.stderr = TeeWriter(self.original_stderr, self.log_file)

        # Configure Rich console for file output
        # Import here to avoid circular dependencies
        from gmx_historical_data.cli import console

        self.original_console_file = console.file

        if self.quiet:
            # Quiet mode: console writes to file only
            console.file = self.log_file
        else:
            # Dual output mode: console writes to both
            console.file = TeeWriter(self.original_console_file, self.log_file)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Exit the log capture context.

        Restores original output streams and closes the log file.

        :param exc_type: Exception type (if any)
        :param exc_val: Exception value (if any)
        :param exc_tb: Exception traceback (if any)
        :return: False (don't suppress exceptions)
        """
        # Restore original streams
        sys.stdout = self.original_stdout
        sys.stderr = self.original_stderr

        # Restore Rich console
        from gmx_historical_data.cli import console
        console.file = self.original_console_file

        # Close log file
        if self.log_file:
            self.log_file.flush()
            self.log_file.close()

        # Print log location to original console (not to file)
        if self.original_stdout and not self.quiet:
            self.original_stdout.write(f"\n✓ Log saved to: {self.log_path}\n")
            self.original_stdout.flush()

        # Don't suppress exceptions - let them propagate normally
        return False
