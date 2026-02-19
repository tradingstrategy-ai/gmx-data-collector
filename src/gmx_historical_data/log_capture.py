"""Log capture utilities for GMX Historical Data CLI.

This module provides context managers for capturing all CLI output to log files
while optionally maintaining console output.
"""

import re
import sys
from pathlib import Path
from typing import TextIO

import typer


class LogCapture:
    """Context manager for capturing all output to log file.

    Captures:
    - Rich console.print() output (strips ANSI codes)
    - Standard print() statements
    - stderr error messages

    :param log_path: Path to log file
    :param quiet: If True, suppress console output (file-only mode)
    """

    def __init__(self, log_path: Path, quiet: bool = False):
        self.log_path = log_path
        self.quiet = quiet
        self.log_file = None
        self.original_stdout = None
        self.original_stderr = None
        self.original_console_file = None

    def __enter__(self):
        # Import console here to avoid circular imports
        from gmx_historical_data.cli import console

        # Create logs directory
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            print(f"Error: Cannot create log directory: {e}", file=sys.stderr)
            raise typer.Exit(1)

        # Open log file
        try:
            self.log_file = open(self.log_path, "w", encoding="utf-8")
        except OSError as e:
            print(f"Error: Cannot create log file: {e}", file=sys.stderr)
            raise typer.Exit(1)

        # Save original streams
        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr

        # Create tee writer if dual output
        if self.quiet:
            # File-only mode
            sys.stdout = self.log_file
            sys.stderr = self.log_file
        else:
            # Dual output mode
            sys.stdout = TeeWriter(self.original_stdout, self.log_file)
            sys.stderr = TeeWriter(self.original_stderr, self.log_file)

        # Configure Rich console for file output
        self.original_console_file = console.file

        if self.quiet:
            console.file = self.log_file
        else:
            console.file = TeeWriter(self.original_console_file, self.log_file)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Import console here to avoid circular imports
        from gmx_historical_data.cli import console

        # Restore original streams
        sys.stdout = self.original_stdout
        sys.stderr = self.original_stderr

        # Restore console
        console.file = self.original_console_file

        # Close log file
        if self.log_file:
            self.log_file.flush()
            self.log_file.close()

        # Print log location (to original console, not file)
        if not self.quiet:
            self.original_stdout.write(f"\n✓ Log saved to: {self.log_path}\n")

        return False  # Don't suppress exceptions


class TeeWriter:
    """Write to multiple output streams simultaneously.

    :param primary: Primary output stream (console)
    :param secondary: Secondary output stream (file)
    """

    def __init__(self, primary: TextIO, secondary: TextIO):
        self.primary = primary
        self.secondary = secondary

    def write(self, text: str) -> int:
        """Write text to both streams, stripping ANSI codes for file output.

        :param text: Text to write
        :return: Number of characters written
        """
        # Strip ANSI codes for file output
        clean_text = self._strip_ansi(text)

        # Write to both streams
        self.primary.write(text)  # Console gets colors
        self.secondary.write(clean_text)  # File gets clean text

        return len(text)

    def flush(self) -> None:
        """Flush both output streams."""
        self.primary.flush()
        self.secondary.flush()

    def isatty(self) -> bool:
        """Check if primary stream is a TTY.

        :return: True if primary stream is a TTY
        """
        return self.primary.isatty() if hasattr(self.primary, "isatty") else False

    def writable(self) -> bool:
        """Check if stream is writable.

        :return: Always True for TeeWriter
        """
        return True

    @property
    def encoding(self) -> str:
        """Get encoding from primary stream.

        :return: Encoding name
        """
        enc = getattr(self.primary, "encoding", None)
        return enc if enc is not None else "utf-8"

    def __getattr__(self, name: str):
        """Delegate unknown attributes to primary stream.

        :param name: Attribute name
        :return: Attribute value from primary stream
        """
        return getattr(self.primary, name)

    @staticmethod
    def _strip_ansi(text: str) -> str:
        """Remove ANSI escape codes from text.

        :param text: Text potentially containing ANSI codes
        :return: Clean text without ANSI codes
        """
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        return ansi_escape.sub("", text)
